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
	"errors"
	"path/filepath"
	"testing"
	"time"

	remoteapi "github.com/prometheus/client_golang/exp/api/remote"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/util/testwal"
)

// A segment counts as sent only after everything enqueued before the watcher
// finished it has been sent, and the progress survives a restart.
func TestQueueManagerSegmentDoneWaitsForSend(t *testing.T) {
	cfg := testDefaultQueueConfig()
	cfg.MaxShards = 1
	recs := testwal.GenerateRecords(recCase{NoST: true, Series: 10, SamplesPerSeries: 10})

	c := NewTestWriteClient(remoteapi.WriteV1MessageType)
	c.SetReturnError(RecoverableError{error: errors.New("backend down")})
	m := newTestQueueManager(t, cfg, config.DefaultMetadataConfig, defaultFlushDeadline, c, remoteapi.WriteV1MessageType, false)
	m.StoreSeries(recs.Series, 0)
	m.Start()
	defer m.Stop()

	require.Equal(t, -1, m.LastSentSegment())
	m.Append(recs.Samples)
	m.SegmentDone(0)
	require.Never(t, func() bool { return m.LastSentSegment() == 0 }, 500*time.Millisecond, 50*time.Millisecond, "segment marked sent while the backend is failing")

	c.SetReturnError(nil)
	require.Eventually(t, func() bool { return m.LastSentSegment() == 0 }, 10*time.Second, 20*time.Millisecond)

	// A new queue with the same name and dir resumes from the persisted progress.
	var p segmentProgress
	p.init(filepath.Dir(filepath.Dir(m.segProg.path)), c.Name())
	require.Equal(t, int64(0), p.lastSent.Load())
}

// A segment with nothing enqueued is sent immediately.
func TestQueueManagerEmptySegmentDone(t *testing.T) {
	c := NewTestWriteClient(remoteapi.WriteV1MessageType)
	m := newTestQueueManager(t, testDefaultQueueConfig(), config.DefaultMetadataConfig, defaultFlushDeadline, c, remoteapi.WriteV1MessageType, false)
	m.SegmentDone(3)
	require.Equal(t, 3, m.LastSentSegment())
}

// Refs are segment-local: after the reset that comes with entering a segment,
// the same ref resolves to the new segment's labels only.
func TestQueueManagerSeriesTableDroppedOnLeavingSegment(t *testing.T) {
	cfg := testDefaultQueueConfig()
	cfg.MaxShards = 1
	c := NewTestWriteClient(remoteapi.WriteV1MessageType)
	m := newTestQueueManager(t, cfg, config.DefaultMetadataConfig, defaultFlushDeadline, c, remoteapi.WriteV1MessageType, false)
	m.Start()
	defer m.Stop()

	seg0 := []record.RefSeries{{Ref: 0, Labels: labels.FromStrings("__name__", "old")}}
	seg1 := []record.RefSeries{{Ref: 0, Labels: labels.FromStrings("__name__", "new")}}
	sample := []record.RefSample{{Ref: 0, T: time.Now().UnixMilli(), V: 1}}

	m.SeriesReset(0)
	m.StoreSeries(seg0, 0)
	require.Equal(t, 1, m.series.ActiveLen())

	m.SeriesReset(1)
	require.Zero(t, m.series.ActiveLen(), "series table of segment 0 must be dropped")
	m.StoreSeries(seg1, 1)
	require.Equal(t, 1, m.series.ActiveLen())

	c.expectSamples(sample, seg1)
	m.Append(sample)
	c.waitForExpectedData(t, 10*time.Second)
}

func TestLowestSentSegmentIsMinAcrossDestinations(t *testing.T) {
	newQM := func(sent int) *QueueManager {
		qm := &QueueManager{}
		qm.segProg.lastSent.Store(int64(sent))
		return qm
	}

	rws := &WriteStorage{queues: map[string]*QueueManager{}}
	require.Equal(t, -1, rws.LowestSentSegment(), "no destinations means nothing may be deleted")

	rws.queues["a"] = newQM(7)
	rws.queues["b"] = newQM(4)
	require.Equal(t, 4, rws.LowestSentSegment())

	rws.queues["c"] = newQM(-1)
	require.Equal(t, -1, rws.LowestSentSegment(), "a destination that sent nothing blocks deletion")
}
