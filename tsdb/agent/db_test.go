// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package agent

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/promslog"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/tsdb/wlog"
	"github.com/prometheus/prometheus/util/compression"
)

const testSegmentSize = 4 * walPageSize

// segmentContents is what one WAL segment holds, with sample refs resolved
// against that segment's own series records only.
type segmentContents struct {
	series      map[chunks.HeadSeriesRef]string
	sampleNames []string // Resolved series name of every sample, "" if the ref was undefined.
	firstRef    chunks.HeadSeriesRef
}

func readSegments(t *testing.T, walDir string) map[int]*segmentContents {
	t.Helper()
	first, last, err := wlog.Segments(walDir)
	require.NoError(t, err)

	out := map[int]*segmentContents{}
	for i := first; i <= last; i++ {
		seg, err := wlog.OpenReadSegment(wlog.SegmentName(walDir, i))
		require.NoError(t, err)
		sr := wlog.NewSegmentBufReader(seg)
		r := wlog.NewReader(sr)
		dec := record.NewDecoder(labels.NewSymbolTable(), promslog.NewNopLogger())
		c := &segmentContents{series: map[chunks.HeadSeriesRef]string{}}
		var samples []record.RefSample
		for r.Next() {
			rec := r.Record()
			switch dec.Type(rec) {
			case record.Series:
				series, err := dec.Series(rec, nil)
				require.NoError(t, err)
				for _, s := range series {
					if c.firstRef == 0 {
						c.firstRef = s.Ref
					}
					c.series[s.Ref] = s.Labels.Get("__name__")
				}
			case record.Samples, record.SamplesV2:
				var err error
				samples, err = dec.Samples(rec, samples[:0])
				require.NoError(t, err)
				for _, s := range samples {
					c.sampleNames = append(c.sampleNames, c.series[s.Ref])
				}
			default:
			}
		}
		require.NoError(t, r.Err())
		require.NoError(t, sr.Close())
		out[i] = c
	}
	return out
}

func openTestDB(t *testing.T) (*DB, string) {
	t.Helper()
	return openTestDBWithSegmentSize(t, testSegmentSize)
}

func openTestDBWithSegmentSize(t *testing.T, size int) (*DB, string) {
	t.Helper()
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.WALSegmentSize = size
	opts.WALCompression = compression.None
	db, err := Open(promslog.NewNopLogger(), nil, nil, dir, opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db, filepath.Join(dir, "wal")
}

// A series that keeps being scraped is re-emitted in every segment it appears
// in, with a ref that only means something inside that segment.
func TestAppendAcrossRotationReemitsSeriesWithSegmentLocalRefs(t *testing.T) {
	db, walDir := openTestDB(t)

	const (
		numSeries  = 50
		numCommits = 300
	)
	// Long values so a few commits fill a segment.
	pad := strings.Repeat("x", 300)
	lsets := make([]labels.Labels, numSeries)
	for i := range lsets {
		lsets[i] = labels.FromStrings("__name__", fmt.Sprintf("m%d", i), "pad", pad)
	}

	for commit := range numCommits {
		app := db.Appender(t.Context())
		for _, ls := range lsets {
			ref, err := app.Append(0, ls, int64(commit), float64(commit))
			require.NoError(t, err)
			require.Equal(t, storage.SeriesRef(0), ref, "the agent appender is series-less")
		}
		require.NoError(t, app.Commit())
	}

	segs := readSegments(t, walDir)
	require.Greater(t, len(segs), 2, "commits should have rotated the WAL")

	totalSamples := 0
	for i, c := range segs {
		if len(c.sampleNames) == 0 {
			continue
		}
		// Refs restart in each segment.
		require.Equal(t, chunks.HeadSeriesRef(1), c.firstRef, "segment %d", i)
		for _, name := range c.sampleNames {
			require.NotEmpty(t, name, "segment %d has a sample whose ref is not defined in that segment", i)
		}
		// Every series appended in a segment is defined in that segment.
		require.Len(t, c.series, numSeries, "segment %d", i)
		totalSamples += len(c.sampleNames)
	}
	require.Equal(t, numCommits*numSeries, totalSamples)
	require.Zero(t, testCounterValue(t, db.metrics.rotationMisestimates))
}

// The same series can get different refs in different segments.
func TestRefsAreNotStableAcrossSegments(t *testing.T) {
	db, walDir := openTestDB(t)
	pad := strings.Repeat("x", 300)

	commit := func(names ...string) {
		app := db.Appender(t.Context())
		for _, n := range names {
			_, err := app.Append(0, labels.FromStrings("__name__", n, "pad", pad), 1, 1)
			require.NoError(t, err)
		}
		require.NoError(t, app.Commit())
	}

	commit("a", "b")
	db.segMu.Lock()
	require.NoError(t, db.rotateLocked())
	db.segMu.Unlock()
	// b now comes first, so it gets ref 1 in the new segment.
	commit("b", "a")

	segs := readSegments(t, walDir)
	require.Len(t, segs, 2)
	require.Equal(t, map[chunks.HeadSeriesRef]string{1: "a", 2: "b"}, segs[0].series)
	require.Equal(t, map[chunks.HeadSeriesRef]string{1: "b", 2: "a"}, segs[1].series)
	require.Equal(t, []string{"b", "a"}, segs[1].sampleNames)
}

// Truncation deletes only segments up to the given fully-sent segment and never
// the one being written.
func TestTruncateKeepsUnsentAndActiveSegments(t *testing.T) {
	db, walDir := openTestDB(t)

	for range 4 {
		app := db.Appender(t.Context())
		_, err := app.Append(0, labels.FromStrings("__name__", "m"), 1, 1)
		require.NoError(t, err)
		require.NoError(t, app.Commit())
		db.segMu.Lock()
		require.NoError(t, db.rotateLocked())
		db.segMu.Unlock()
	}
	first, last, err := wlog.Segments(walDir)
	require.NoError(t, err)
	require.Equal(t, 0, first)
	require.Equal(t, 4, last)

	for _, tc := range []struct {
		sent      int
		wantFirst int
	}{
		{sent: -1, wantFirst: 0}, // A destination that has sent nothing blocks deletion.
		{sent: 1, wantFirst: 2},
		{sent: 100, wantFirst: 4}, // Never past the active segment.
	} {
		require.NoError(t, db.truncateSegments(tc.sent))
		first, last, err = wlog.Segments(walDir)
		require.NoError(t, err)
		require.Equal(t, tc.wantFirst, first, "sent=%d", tc.sent)
		require.Equal(t, 4, last)
	}
}

func testCounterValue(t *testing.T, c interface{ Write(*dto.Metric) error }) float64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, c.Write(&m))
	return m.GetCounter().GetValue()
}

// One big commit is split so that series records and samples of a chunk
// always end up in the same segment.
func TestLargeCommitKeepsSeriesAndSamplesInSameSegment(t *testing.T) {
	db, walDir := openTestDBWithSegmentSize(t, 64*walPageSize)

	const numSeries = 30000
	for round := range 3 {
		app := db.Appender(t.Context())
		for i := range numSeries {
			_, err := app.Append(0, labels.FromStrings("__name__", fmt.Sprintf("m%d", i), "job", "big"), int64(round), 1)
			require.NoError(t, err)
		}
		require.NoError(t, app.Commit())
	}

	segs := readSegments(t, walDir)
	require.Greater(t, len(segs), 1)
	total := 0
	for i, c := range segs {
		for _, name := range c.sampleNames {
			require.NotEmpty(t, name, "segment %d has a sample with an undefined ref", i)
		}
		total += len(c.sampleNames)
	}
	require.Equal(t, 3*numSeries, total)
	require.Zero(t, testCounterValue(t, db.metrics.rotationMisestimates))
}
