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

package wlog

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/util/compression"
)

// segmentedMock keeps one series table like a queue manager: entries are
// stamped with their segment and SeriesReset drops older segments.
type segmentedMock struct {
	events   []string
	table    map[chunks.HeadSeriesRef]labels.Labels
	stamps   map[chunks.HeadSeriesRef]int
	lastSent int
	// resolved records the labels each sample ref resolved to when it was appended.
	resolved []string
}

func newSegmentedMock(lastSent int) *segmentedMock {
	return &segmentedMock{
		table:    map[chunks.HeadSeriesRef]labels.Labels{},
		stamps:   map[chunks.HeadSeriesRef]int{},
		lastSent: lastSent,
	}
}

func (m *segmentedMock) StoreSeries(s []record.RefSeries, idx int) {
	for _, e := range s {
		m.table[e.Ref] = e.Labels
		m.stamps[e.Ref] = idx
	}
}

func (m *segmentedMock) SeriesReset(idx int) {
	m.events = append(m.events, fmt.Sprintf("reset:%d", idx))
	for ref, s := range m.stamps {
		if s < idx {
			delete(m.stamps, ref)
			delete(m.table, ref)
		}
	}
}

func (m *segmentedMock) Append(s []record.RefSample) bool {
	for _, e := range s {
		m.resolved = append(m.resolved, m.table[e.Ref].String())
	}
	return true
}

func (m *segmentedMock) SegmentDone(n int)                       { m.events = append(m.events, fmt.Sprintf("done:%d", n)) }
func (m *segmentedMock) LastSentSegment() int                    { return m.lastSent }
func (*segmentedMock) AppendExemplars([]record.RefExemplar) bool { return true }
func (*segmentedMock) AppendHistograms([]record.RefHistogramSample) bool {
	return true
}

func (*segmentedMock) AppendFloatHistograms([]record.RefFloatHistogramSample) bool {
	return true
}
func (*segmentedMock) StoreMetadata([]record.RefMetadata)          {}
func (*segmentedMock) UpdateSeriesSegment([]record.RefSeries, int) {}

// Each segment reuses ref 1 for a different series. The watcher must drop the
// previous segment's table on entering the next one, or samples would resolve
// to the wrong labels.
func TestWatcherSegmentedDropsSeriesTableOnLeavingSegment(t *testing.T) {
	for _, tc := range []struct {
		name     string
		lastSent int
		expected []string
		resolved []string
	}{
		{
			name:     "from start",
			lastSent: -1,
			expected: []string{"reset:0", "done:0", "reset:1", "done:1", "reset:2", "done:2"},
			resolved: []string{`{__name__="seg0"}`, `{__name__="seg1"}`, `{__name__="seg2"}`},
		},
		{
			name:     "resume after sent segment",
			lastSent: 0,
			expected: []string{"reset:1", "done:1", "reset:2", "done:2"},
			resolved: []string{`{__name__="seg1"}`, `{__name__="seg2"}`},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			wdir := filepath.Join(dir, "wal")
			require.NoError(t, os.Mkdir(wdir, 0o777))
			w, err := NewSize(nil, nil, wdir, 8*pageSize, compression.None)
			require.NoError(t, err)
			defer func() { require.NoError(t, w.Close()) }()

			enc := record.Encoder{}
			// Segments 0-2 hold data. Segment 3 is active and only makes 0-2 "finished".
			for i := range 3 {
				lbls := labels.FromStrings("__name__", fmt.Sprintf("seg%d", i))
				require.NoError(t, w.Log(
					enc.Series([]record.RefSeries{{Ref: 1, Labels: lbls}}, nil),
					enc.Samples([]record.RefSample{{Ref: 1, T: 1, V: 1}}, nil),
				))
				_, err := w.NextSegmentSync()
				require.NoError(t, err)
			}

			mock := newSegmentedMock(tc.lastSent)
			watcher := NewWatcher(wMetrics, nil, nil, "segmented", mock, dir, false, false, false, nil)
			watcher.SetMetrics()
			watcher.MaxSegment = 2
			require.NoError(t, watcher.Run())

			require.Equal(t, tc.expected, mock.events)
			require.Equal(t, tc.resolved, mock.resolved)
			// Only the last read segment's table is left.
			require.Len(t, mock.table, 1)
			require.Equal(t, `{__name__="seg2"}`, mock.table[1].String())
		})
	}
}
