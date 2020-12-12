// Package bqstorageio provides an Apacbe Beam I/O connector which uses the BigQuery Storage API.
// It supports reading from BigQuery in parallel using a fixed number of tasks.
package bqstorageio

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"strconv"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/grpc"

	bqStorage "cloud.google.com/go/bigquery/storage/apiv1"
	bqStoragepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1"
)

// rpcOpts is used to configure the underlying gRPC client to accept large
// messages. The BigQuery Storage API may send message blocks up to 10MB
// in size.
var rpcOpts = gax.WithGRPCOptions(
	grpc.MaxCallRecvMsgSize(1024 * 1024 * 11),
)

func init() {
	beam.RegisterType(reflect.TypeOf((*splitStreamFn)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*readFn)(nil)).Elem())
	// beam.RegisterType(reflect.TypeOf((*decodeFn)(nil)).Elem())
}

// ReadStorage reads from BigQuery using the Storage API and returns Arvo rows to be parsed.
// It accepts information about the table, fields to return, restrictions like a where clause, and a fixed number
// of streams to try and open.
func ReadStorage(s beam.Scope, project, dataset, table string, fields []string, restriction string, shards int) (beam.PCollection, beam.PCollection) {
	s = s.Scope("bqstorageio.ReadStorage")
	return query(s, project, dataset, table, fields, restriction, shards)
}

func query(s beam.Scope, project, dataset, table string, fields []string, restriction string, shards int) (beam.PCollection, beam.PCollection) {
	imp := beam.Impulse(s)
	streams, schema := beam.ParDo2(s, &splitStreamFn{Project: project, Dataset: dataset, Table: table, Fields: fields, Restriction: restriction, Shards: shards}, imp)
	g := beam.GroupByKey(s, streams)
	return beam.ParDo(s, &readFn{}, g), schema
}

type splitStreamFn struct {
	Project     string   `json:"project"`
	Dataset     string   `json:"dataset"`
	Table       string   `json:"table"`
	Fields      []string `json:"fields"`
	Restriction string   `json:"restriction"`
	Shards      int      `json:"shards"`
}

// ProcessElement takes a splitStream configuration and emits streams to read results from and the schema.
func (s *splitStreamFn) ProcessElement(ctx context.Context, _ []byte, emitStream func(k string, val string), emitSchema func(streamName string)) error {
	client, err := bqStorage.NewBigQueryReadClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	req := &bqStoragepb.CreateReadSessionRequest{
		Parent: fmt.Sprintf("projects/%s", s.Project),
		ReadSession: &bqStoragepb.ReadSession{
			Table:      fmt.Sprintf("projects/%s/datasets/%s/tables/%s", s.Project, s.Dataset, s.Table),
			DataFormat: bqStoragepb.DataFormat_AVRO,
			ReadOptions: &bqStoragepb.ReadSession_TableReadOptions{
				SelectedFields: s.Fields,
				RowRestriction: s.Restriction,
			},
		},
		MaxStreamCount: int32(s.Shards),
	}

	session, err := client.CreateReadSession(ctx, req, rpcOpts)
	if err != nil {
		log.Errorf(ctx, "CreateReadSession: %v", err)
		return err
	}

	if len(session.GetStreams()) == 0 {
		log.Fatalf(ctx, "No streams found, quitting.")
	}

	schema := session.GetAvroSchema().GetSchema()
	emitSchema(schema)
	for i, stream := range session.GetStreams() {
		emitStream(strconv.Itoa(i), stream.Name)
	}
	return nil
}

type readFn struct{}

// ProcessElement takes a stream and starts emitting rows.
func (f *readFn) ProcessElement(ctx context.Context, _ string, v func(*string) bool, emit func([]byte)) error {
	client, err := bqStorage.NewBigQueryReadClient(ctx)
	if err != nil {
		return err
	}

	var streamName string
	v(&streamName)

	var offset int64
	retryLimit := 3

	log.Debugf(ctx, "Reading from stream: %s.", streamName)

	for {
		retries := 0
		rowStream, err := client.ReadRows(ctx, &bqStoragepb.ReadRowsRequest{
			ReadStream: streamName, Offset: offset,
		})
		if err != nil {
			return fmt.Errorf("Couldn't invoke ReadRows: %v", err)
		}

		for {
			r, err := rowStream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				retries++
				if retries >= retryLimit {
					return fmt.Errorf("processStream retries exhausted: %v", err)
				}
				break
			}

			rc := r.GetRowCount()
			if rc > 0 {
				offset = offset + rc
				retries = 0

				undecoded := r.GetAvroRows().GetSerializedBinaryRows()
				emit(undecoded)
			}
		}
	}
}
