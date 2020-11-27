// Package main is a copy of bigquery_read/main.go.
// It will demonstrate differential privacy when I figure it out.
package main

import (
	"context"
	"flag"
	"fmt"
	"reflect"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/bigqueryio"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
)

// CommentRow models 1 row of HackerNews comments.
type CommentRow struct {
	Text string `bigquery:"text"`
}

const query = `SELECT text
FROM ` + "`bigquery-public-data.hacker_news.comments`" + `
WHERE time_ts BETWEEN '2013-01-01' AND '2014-01-01'
LIMIT 1000
`

// ngram extracts variable sizes of ngrams from a string.
func ngram(str string, ns ...int) []string {
	split := strings.Split(str, " ")
	results := []string{}

	for _, n := range ns {
		i := 0
		for i < len(split)-n+1 {
			results = append(results, strings.Join(split[i:i+n], " "))
			i++
		}
	}

	return results
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()
	p := beam.NewPipeline()
	s := p.Root()
	project := gcpopts.GetProject(ctx)

	// Build a PCollection<CommentRow> by querying BigQuery.
	rows := bigqueryio.Query(s, project, query,
		reflect.TypeOf(CommentRow{}), bigqueryio.UseStandardSQL())

	// Extract unigrams, bigrams, and trigrams from each comment.
	// Builds a PCollection<string> where each string is a single
	// occurrence of an ngram.
	ngrams := beam.ParDo(s, func(row CommentRow, emit func(string)) {
		for _, gram := range ngram(row.Text, 1, 2, 3) {
			emit(gram)
		}
	}, rows)

	// Count the occurrence of each ngram.
	// Returns a PCollection<{string, count: int}>.
	counts := stats.Count(s, ngrams)

	// Convert each count row into a string suitable for printing.
	// Returns a PCollection<string>.
	formatted := beam.ParDo(s, func(gram string, c int) string {
		return fmt.Sprintf("%s,%v", gram, c)
	}, counts)

	// Write each string to a line in the output file.
	textio.Write(s, "/tmp/output.txt", formatted)

	// Now that the pipeline is fully constructed, we execute it.
	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
