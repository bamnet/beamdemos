package bqstorageio_test

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/linkedin/goavro"

	"github.com/bamnet/beamdemos/bqstorageio"
)

type Names struct {
	State  string
	Name   string
	Number int
}

func Example() {
	beam.Init()

	ctx := context.Background()
	p := beam.NewPipeline()
	s := p.Root()

	arvoRows, schema := bqstorageio.ReadStorage(s,
		"bigquery-public-data", "usa_names", "usa_1910_2013",
		[]string{"state", "name", "number"}, // Read 3 fields from the table.
		`year > 1950`,                       // Only read rows after 1950.
		5,                                   // Read up to 5 streams.
	)

	rows := beam.ParDo(s, func(undecoded []byte, schema string, emit func(Names)) error {
		// Load the schema.
		codec, err := goavro.NewCodec(schema)
		if err != nil {
			return err
		}

		// Loop and decode rows.
		for len(undecoded) > 0 {
			datum, remainingBytes, err := codec.NativeFromBinary(undecoded)
			if err != nil {
				if err == io.EOF {
					break
				}
				return fmt.Errorf("decoding error with %d bytes remaining", remainingBytes)
			}
			undecoded = remainingBytes

			decoded, err := decodeArvo(ctx, datum)
			if err != nil {
				log.Errorf(ctx, "error decoding %v", err)
				continue
			}

			emit(*decoded)
			undecoded = remainingBytes
		}
		return nil
	}, arvoRows, beam.SideInput{Input: schema})

	stats.CountElms(s, rows)

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}

// decodeArvo converts the arvo interface into a proper Go struct.
func decodeArvo(ctx context.Context, datum interface{}) (*Names, error) {
	m, ok := datum.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("type casting error")
	}

	state, ok := m["state"].(string)
	if !ok {
		log.Info(ctx, "null state")
		return nil, nil
	}

	name, ok := m["name"].(string)
	if !ok {
		log.Info(ctx, "null name")
		return nil, nil
	}

	number, ok := m["number"].(int)
	if !ok {
		log.Info(ctx, "null number")
		return nil, nil
	}

	return &Names{
		State:  state,
		Name:   name,
		Number: number,
	}, nil
}
