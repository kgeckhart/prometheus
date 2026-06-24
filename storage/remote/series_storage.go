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

package remote

import (
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/record"
)

// seriesEntry holds the labels and metadata for a single active (non-dropped) series.
type seriesEntry struct {
	labels labels.Labels
	meta   *metadata.Metadata
}

// SeriesStorage holds per-series labels, metadata, and originating WAL-segment
// indexes for a QueueManager. All exported methods are safe to call from any
// goroutine.
//
// The internal label/metadata layout is selected at construction by withMeta:
//
//   - withMeta=false (RWv1, or RWv2 without metadata-wal-records): each Append
//     lookup requires a single map access.
//   - withMeta=true (RWv2 with metadata-wal-records enabled): each Append lookup
//     retrieves both labels and metadata together in a single map access.
type SeriesStorage struct {
	mu       sync.Mutex
	withMeta bool

	labels  map[chunks.HeadSeriesRef]labels.Labels // populated when withMeta=false
	entries map[chunks.HeadSeriesRef]seriesEntry   // populated when withMeta=true
	dropped map[chunks.HeadSeriesRef]struct{}

	segmentMu      sync.Mutex
	segmentIndexes map[chunks.HeadSeriesRef]int
}

// NewSeriesStorage constructs a SeriesStorage with its maps allocated and its
// internal layout selected based on withMeta.
func NewSeriesStorage(withMeta bool) *SeriesStorage {
	s := &SeriesStorage{
		withMeta:       withMeta,
		dropped:        make(map[chunks.HeadSeriesRef]struct{}),
		segmentIndexes: make(map[chunks.HeadSeriesRef]int),
	}
	if withMeta {
		s.entries = make(map[chunks.HeadSeriesRef]seriesEntry)
	} else {
		s.labels = make(map[chunks.HeadSeriesRef]labels.Labels)
	}
	return s
}

// Lookup returns the labels and metadata for ref. active is true when the series
// is known and not dropped. dropped is only meaningful when active=false: true
// means the series was explicitly filtered by relabelling, false means it was
// never seen.
func (s *SeriesStorage) Lookup(ref chunks.HeadSeriesRef) (lbls labels.Labels, meta *metadata.Metadata, active, dropped bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.withMeta {
		e, ok := s.entries[ref]
		if !ok {
			_, d := s.dropped[ref]
			return labels.EmptyLabels(), nil, false, d
		}
		return e.labels, e.meta, true, false
	}
	l, ok := s.labels[ref]
	if !ok {
		_, d := s.dropped[ref]
		return labels.EmptyLabels(), nil, false, d
	}
	return l, nil, true, false
}

// Update records a batch of series, tagging each with index as its originating
// WAL segment. For each entry, relabel transforms the raw labels and decides
// whether to keep the series; refs with keep=false are marked dropped.
//
// relabel must not call back into s.
func (s *SeriesStorage) Update(series []record.RefSeries, index int, relabel func(labels.Labels) (lbls labels.Labels, keep bool)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.segmentMu.Lock()
	defer s.segmentMu.Unlock()
	for _, sr := range series {
		s.segmentIndexes[sr.Ref] = index
		lbls, keep := relabel(sr.Labels)
		if !keep {
			s.dropLocked(sr.Ref)
			continue
		}
		s.storeLocked(sr.Ref, lbls)
	}
}

// UpdateSegment records index as the originating WAL segment for each series,
// without touching the label or metadata maps.
func (s *SeriesStorage) UpdateSegment(series []record.RefSeries, index int) {
	s.segmentMu.Lock()
	defer s.segmentMu.Unlock()
	for _, sr := range series {
		s.segmentIndexes[sr.Ref] = index
	}
}

// Reset forgets every series whose last-known WAL segment is older than
// beforeIndex.
func (s *SeriesStorage) Reset(beforeIndex int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.segmentMu.Lock()
	defer s.segmentMu.Unlock()
	for k, v := range s.segmentIndexes {
		if v < beforeIndex {
			delete(s.segmentIndexes, k)
			s.deleteLocked(k)
		}
	}
}

// UpdateMetadata applies a batch of metadata records to active series. Records
// for unknown refs are ignored. A no-op when withMeta is false.
func (s *SeriesStorage) UpdateMetadata(meta []record.RefMetadata) {
	if !s.withMeta {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range meta {
		if e, ok := s.entries[m.Ref]; ok {
			e.meta = &metadata.Metadata{
				Type: record.ToMetricType(m.Type),
				Unit: m.Unit,
				Help: m.Help,
			}
			s.entries[m.Ref] = e
		}
	}
}

// ActiveLen returns the number of active (non-dropped) series.
func (s *SeriesStorage) ActiveLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.withMeta {
		return len(s.entries)
	}
	return len(s.labels)
}

// storeLocked records ref as an active series with the given labels, preserving
// any existing metadata. Must be called with mu held.
func (s *SeriesStorage) storeLocked(ref chunks.HeadSeriesRef, lbls labels.Labels) {
	delete(s.dropped, ref)
	if s.withMeta {
		existing := s.entries[ref]
		s.entries[ref] = seriesEntry{labels: lbls, meta: existing.meta}
	} else {
		s.labels[ref] = lbls
	}
}

// dropLocked marks ref as an explicitly-dropped series and removes it from the
// active map. Must be called with mu held.
func (s *SeriesStorage) dropLocked(ref chunks.HeadSeriesRef) {
	if s.withMeta {
		delete(s.entries, ref)
	} else {
		delete(s.labels, ref)
	}
	s.dropped[ref] = struct{}{}
}

// deleteLocked removes ref from all label/metadata maps. Must be called with mu held.
func (s *SeriesStorage) deleteLocked(ref chunks.HeadSeriesRef) {
	if s.withMeta {
		delete(s.entries, ref)
	} else {
		delete(s.labels, ref)
	}
	delete(s.dropped, ref)
}
