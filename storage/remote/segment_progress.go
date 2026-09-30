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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"go.uber.org/atomic"
)

const progressDirName = "remote-progress"

type segmentMark struct {
	segment int
	// target is the QueueManager's enqueuedTotal when the watcher finished the segment.
	target uint64
}

// segmentProgress tracks the last WAL segment a destination has fully sent and
// persists it, so the WAL can be truncated and the watcher can resume.
type segmentProgress struct {
	path     string // Empty disables persistence.
	lastSent atomic.Int64

	mu      sync.Mutex
	pending []segmentMark
}

func (p *segmentProgress) init(dir, name string) {
	p.lastSent.Store(-1)
	if dir == "" {
		return
	}
	p.path = filepath.Join(dir, progressDirName, strings.NewReplacer("/", "_", string(os.PathSeparator), "_").Replace(name))
	b, err := os.ReadFile(p.path)
	if err != nil {
		return
	}
	if n, err := strconv.Atoi(strings.TrimSpace(string(b))); err == nil {
		p.lastSent.Store(int64(n))
	}
}

// persist writes n atomically (tmp file plus rename).
func (p *segmentProgress) persist(n int) error {
	if p.path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(p.path), 0o777); err != nil {
		return err
	}
	tmp := p.path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.Itoa(n)), 0o666); err != nil {
		return err
	}
	return os.Rename(tmp, p.path)
}

// LastSentSegment returns the last WAL segment fully sent by this queue, or -1.
func (t *QueueManager) LastSentSegment() int {
	return int(t.segProg.lastSent.Load())
}

// SegmentDone is called by the WAL watcher once it has enqueued all of segment n.
// The segment counts as sent when everything enqueued so far has been sent or
// dropped. The count is cumulative across shards, so it can mark a segment early
// when a later item overtakes an earlier one.
func (t *QueueManager) SegmentDone(n int) {
	t.segProg.mu.Lock()
	t.segProg.pending = append(t.segProg.pending, segmentMark{segment: n, target: t.enqueuedTotal.Load()})
	t.segProg.mu.Unlock()
	t.advanceSentSegments()
}

func (t *QueueManager) advanceSentSegments() {
	p := &t.segProg
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.pending) == 0 {
		return
	}
	processed := t.processedTotal.Load()
	done := -1
	i := 0
	for ; i < len(p.pending) && processed >= p.pending[i].target; i++ {
		done = p.pending[i].segment
	}
	if done < 0 {
		return
	}
	p.pending = p.pending[i:]
	if err := p.persist(done); err != nil {
		t.logger.Error("Failed to persist sent segment", "segment", done, "err", err)
	}
	p.lastSent.Store(int64(done))
}

// RemoveProgress deletes the persisted progress. Call it when the destination is
// removed, otherwise it would block WAL truncation.
func (t *QueueManager) RemoveProgress() {
	if t.segProg.path == "" {
		return
	}
	if err := os.Remove(t.segProg.path); err != nil && !os.IsNotExist(err) {
		t.logger.Error("Failed to remove sent segment progress", "err", err)
	}
}
