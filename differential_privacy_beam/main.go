// Package main is a copy of bigquery_read/main.go.
// It will demonstrate differential privacy when I figure it out.
package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/bigqueryio"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/google/differential-privacy/privacy-on-beam/pbeam"
)

// CommentRow models 1 row of HackerNews comments.
type CommentRow struct {
	Author string `bigquery:"author"`
	Text   string `bigquery:"text"`
}

// AuthorNgram represents an ngram and it's author.
type AuthorNgram struct {
	Author string
	Ngram  string
}

const query = `SELECT author, text
FROM ` + "`bigquery-public-data.hacker_news.comments`" + `
WHERE time_ts BETWEEN '2013-01-01' AND '2014-01-01'
AND author IS NOT NULL AND text IS NOT NULL
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
	// Builds a PCollection<AuthorNgram> to keep track of the author
	// of each Ngram. We need an identifier for a user to use
	// differential privacy.
	authorNgrams := beam.ParDo(s, func(row CommentRow, emit func(AuthorNgram)) {
		for _, gram := range ngram(row.Text, 1, 2, 3) {
			emit(AuthorNgram{Author: row.Author, Ngram: gram})
		}
	}, rows)

	// Configure differential privacy parameters.
	epsilon := float64(4)   // ε = 4
	delta := math.Pow10(-4) // Δ = 1e-4.
	spec := pbeam.NewPrivacySpec(epsilon, delta)

	// Convert PCollection<AuthorNgram> to PrivatePcollection<AuthorNgram>.
	// This PrivatePCollection tracks the "Author" field in our AuthorNgram struct
	// as the user id when applying differential privacy.
	pgrams := pbeam.MakePrivateFromStruct(s, authorNgrams, spec, "Author")

	// Convert a PrivatePCollection<AuthorNgram> to a PrivatePCollection<string>,
	// where the string represents each ngram. This method discards the
	// Author item from our struct so we can count it like normal.
	// Internally, the PrivatePCollection keeps track of this identifier.
	ngrams := pbeam.ParDo(s, func(row AuthorNgram, emit func(string)) {
		emit(row.Ngram)
	}, pgrams)

	// Count the occurrence of each ngram.
	// Returns a PCollection<{string, count: int}>.
	counts := pbeam.Count(s, ngrams, pbeam.CountParams{
		// MaxPartitionsContributed is the max number of distinct ngrams a user
		// can contribute to. If a user contributes more than this number of
		// unique ngrams some will be randomly dropped.
		MaxPartitionsContributed: 700,
		// MaxValue is the number of times a user may contribute
		// to the same ngram. If a user contributes more than this
		// amount to the same ngram, additional contributions will
		// be dropped.
		MaxValue: 2,
	})

	// Convert each count row into a string suitable for printing.
	// Returns a PCollection<string>.
	formatted := beam.ParDo(s, func(gram string, c int64) string {
		return fmt.Sprintf("%s,%v", gram, c)
	}, counts)

	// Write each string to a line in the output file.
	textio.Write(s, "/tmp/output.txt", formatted)

	// Now that the pipeline is fully constructed, we execute it.
	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
