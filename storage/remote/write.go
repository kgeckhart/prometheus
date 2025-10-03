// Copyright 2017 The Prometheus Authors
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
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/promslog"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/wlog"
)

var (
	samplesIn = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "samples_in_total",
		Help:      "Samples in to remote storage, compare to samples out for queue managers.",
	})
	exemplarsIn = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "exemplars_in_total",
		Help:      "Exemplars in to remote storage, compare to exemplars out for queue managers.",
	})
	histogramsIn = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "histograms_in_total",
		Help:      "HistogramSamples in to remote storage, compare to histograms out for queue managers.",
	})
	currentSegmentMetric prometheus.Gauge
)

// WriteStorage represents all the remote write storage.
type WriteStorage struct {
	logger *slog.Logger
	reg    prometheus.Registerer
	mtx    sync.Mutex

	watcherMetrics    *wlog.WatcherMetrics
	liveReaderMetrics *wlog.LiveReaderMetrics
	externalLabels    labels.Labels
	dir               string
	queues            map[string]*QueueManager
	samplesIn         *ewmaRate
	flushDeadline     time.Duration
	interner          *pool
	scraper           ReadyScrapeManager
	quit              chan struct{}

	// For timestampTracker.
	highestTimestamp           *maxTimestamp
	segmentTracker             *segmentTracker
	previouslyAppliedAConfig   bool
	queuesForTimestampTracking []string
}

// NewWriteStorage creates and runs a WriteStorage.
func NewWriteStorage(logger *slog.Logger, reg prometheus.Registerer, dir string, flushDeadline time.Duration, sm ReadyScrapeManager) *WriteStorage {
	if logger == nil {
		logger = promslog.NewNopLogger()
	}
	rws := &WriteStorage{
		queues:            make(map[string]*QueueManager),
		watcherMetrics:    wlog.NewWatcherMetrics(reg),
		liveReaderMetrics: wlog.NewLiveReaderMetrics(reg),
		logger:            logger,
		reg:               reg,
		flushDeadline:     flushDeadline,
		samplesIn:         newEWMARate(ewmaWeight, shardUpdateDuration),
		dir:               dir,
		interner:          newPool(),
		scraper:           sm,
		quit:              make(chan struct{}),
		highestTimestamp: &maxTimestamp{
			Gauge: prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "highest_timestamp_in_seconds",
				Help:      "Highest timestamp that has come into the remote storage via the Appender interface, in seconds since epoch. Initialized to 0 when no data has been received yet.",
			}),
		},
	}
	if reg != nil {
		reg.MustRegister(rws.highestTimestamp)
	}
	go rws.run()
	return rws
}

func (rws *WriteStorage) run() {
	shardTicker := time.NewTicker(shardUpdateDuration)
	defer shardTicker.Stop()

	// TODO make this configurable and only track timestamps if at least one queue has ReplayUnsentData enabled.
	queueTimestampTrackingTicker := time.NewTicker(time.Second)
	defer queueTimestampTrackingTicker.Stop()
	for {
		select {
		case <-shardTicker.C:
			rws.samplesIn.tick()
		case <-queueTimestampTrackingTicker.C:
			rws.trackCurrentQueueTimestamps()
		case <-rws.quit:
			return
		}
	}
}

func (rws *WriteStorage) Notify() {
	rws.mtx.Lock()
	defer rws.mtx.Unlock()

	for _, q := range rws.queues {
		// These should all be non blocking
		q.watcher.Notify()
	}
}

// ApplyConfig updates the state as the new config requires.
// Only stop & create queues which have changes.
func (rws *WriteStorage) ApplyConfig(conf *config.Config) error {
	rws.mtx.Lock()
	defer rws.mtx.Unlock()

	// Remote write queues only need to change if the remote write config or
	// external labels change.
	externalLabelUnchanged := labels.Equal(conf.GlobalConfig.ExternalLabels, rws.externalLabels)
	rws.externalLabels = conf.GlobalConfig.ExternalLabels

	newQueues := make(map[string]*QueueManager)
	var newHashes []string
	var queuesForTimestampTracking []string
	var currentQueueTimestamps map[string]int64
	if !rws.previouslyAppliedAConfig {
		currentQueueTimestamps = rws.readCurrentQueueTimestamps()
	}

	for _, rwConf := range conf.RemoteWriteConfigs {
		hash, err := toHash(rwConf)
		if err != nil {
			return err
		}

		// Don't allow duplicate remote write configs.
		if _, ok := newQueues[hash]; ok {
			return fmt.Errorf("duplicate remote write configs are not allowed, found duplicate for URL: %s", rwConf.URL)
		}

		// Set the queue name to the config hash if the user has not set
		// a name in their remote write config so we can still differentiate
		// between queues that have the same remote write endpoint.
		name := hash[:6]
		if rwConf.Name != "" {
			name = rwConf.Name
		}

		c, err := NewWriteClient(name, &ClientConfig{
			URL:              rwConf.URL,
			WriteProtoMsg:    rwConf.ProtobufMessage,
			Timeout:          rwConf.RemoteTimeout,
			HTTPClientConfig: rwConf.HTTPClientConfig,
			SigV4Config:      rwConf.SigV4Config,
			AzureADConfig:    rwConf.AzureADConfig,
			GoogleIAMConfig:  rwConf.GoogleIAMConfig,
			Headers:          rwConf.Headers,
			RetryOnRateLimit: rwConf.QueueConfig.RetryOnRateLimit,
			RoundRobinDNS:    rwConf.RoundRobinDNS,
		})
		if err != nil {
			return err
		}

		queue, ok := rws.queues[hash]
		if externalLabelUnchanged && ok {
			// Update the client in case any secret configuration has changed.
			queue.SetClient(c)
			newQueues[hash] = queue
			delete(rws.queues, hash)
			continue
		}

		// Redacted to remove any passwords in the URL (that are
		// technically accepted but not recommended) since this is
		// only used for metric labels.
		endpoint := rwConf.URL.Redacted()
		var replayFrom int64 = math.MaxInt64
		if rwConf.ReplayUnsentData && !rws.previouslyAppliedAConfig && currentQueueTimestamps != nil {
			if ts, ok := currentQueueTimestamps[hash]; ok {
				replayFrom = ts
			}
		}

		newQueues[hash] = NewQueueManager(
			newQueueManagerMetrics(rws.reg, name, endpoint),
			rws.watcherMetrics,
			rws.liveReaderMetrics,
			rws.logger,
			rws.dir,
			rws.samplesIn,
			rwConf.QueueConfig,
			rwConf.MetadataConfig,
			conf.GlobalConfig.ExternalLabels,
			rwConf.WriteRelabelConfigs,
			c,
			rws.flushDeadline,
			rws.interner,
			rws.highestTimestamp,
			rws.scraper,
			rwConf.SendExemplars,
			rwConf.SendNativeHistograms,
			rwConf.ProtobufMessage,
			replayFrom,
		)
		// Keep track of which queues are new so we know which to start.
		newHashes = append(newHashes, hash)
		if rwConf.ReplayUnsentData {
			queuesForTimestampTracking = append(queuesForTimestampTracking, hash)
		}
	}

	// Anything remaining in rws.queues is a queue who's config has
	// changed or was removed from the overall remote write config.
	for _, q := range rws.queues {
		q.Stop()
	}

	for _, hash := range newHashes {
		newQueues[hash].Start()
	}

	rws.queues = newQueues
	if rws.segmentTracker != nil {
		rws.segmentTracker.updateQueues(rws.queues)
	}
	rws.queuesForTimestampTracking = queuesForTimestampTracking
	if !rws.previouslyAppliedAConfig {
		rws.previouslyAppliedAConfig = true
	}
	return nil
}

// Appender implements storage.Storage.
func (rws *WriteStorage) Appender(_ context.Context) storage.Appender {
	return &timestampTracker{
		writeStorage:         rws,
		highestRecvTimestamp: rws.highestTimestamp,
	}
}

// LowestSentTimestamp returns the lowest sent timestamp across all queues.
func (rws *WriteStorage) LowestSentTimestamp() int64 {
	rws.mtx.Lock()
	defer rws.mtx.Unlock()

	var lowestTs int64 = math.MaxInt64

	for _, q := range rws.queues {
		ts := int64(q.metrics.highestSentTimestamp.Get() * 1000)
		if ts < lowestTs {
			lowestTs = ts
		}
	}
	if len(rws.queues) == 0 {
		lowestTs = 0
	}

	return lowestTs
}

// Close closes the WriteStorage.
func (rws *WriteStorage) Close() error {
	rws.mtx.Lock()
	defer rws.mtx.Unlock()
	for _, q := range rws.queues {
		q.Stop()
	}
	close(rws.quit)
	return nil
}

func (rws *WriteStorage) OnSegmentChange(f SegmentChangeFunc) {
	rws.mtx.Lock()
	defer rws.mtx.Unlock()

	if rws.segmentTracker == nil {
		rws.segmentTracker = newSegmentTracker(f, rws.queues)
	} else {
		rws.segmentTracker.updateChangeFunc(f)
	}
}

type queueTimestamp struct {
	hash string
	ts   int64
}

const timestampFileName = "queue_timestamps"

func (rws *WriteStorage) trackCurrentQueueTimestamps() {
	rws.mtx.Lock()
	defer rws.mtx.Unlock()

	if len(rws.queuesForTimestampTracking) == 0 {
		return
	}

	queueTimestamps := make([]queueTimestamp, 0, len(rws.queuesForTimestampTracking))
	for _, q := range rws.queuesForTimestampTracking {
		if qm, ok := rws.queues[q]; ok {
			queueTimestamps = append(queueTimestamps, queueTimestamp{
				hash: q,
				ts:   int64(qm.metrics.highestSentTimestamp.Get() * 1000),
			})
		}
	}

	// Serialize queueTimestamps to a delimited string and write to a file at rws.dir.
	var sb strings.Builder
	for i, qt := range queueTimestamps {
		if i > 0 {
			sb.WriteString("\n")
		}
		sb.WriteString(fmt.Sprintf("%s:%d", qt.hash, qt.ts))
	}
	filePath := filepath.Join(rws.dir, timestampFileName)
	if err := os.WriteFile(filePath, []byte(sb.String()), 0644); err != nil {
		rws.logger.Warn("failed to write queue timestamps to file", "file", filePath, "err", err)
	}

	rws.logger.Info("tracked current queue timestamps", "file", filePath, "queue_count", len(queueTimestamps))
}

func (rws *WriteStorage) readCurrentQueueTimestamps() map[string]int64 {
	filePath := filepath.Join(rws.dir, timestampFileName)
	data, err := os.ReadFile(filePath)
	if err != nil {
		rws.logger.Warn("failed to read queue timestamps file", "file", filePath, "err", err)
		return nil
	}

	result := make(map[string]int64)
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		ts, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			continue
		}
		result[parts[0]] = ts
	}
	return result
}

// TODO test me.
type segmentTracker struct {
	mux                       sync.Mutex
	currentSegment            int
	queueHashToCurrentSegment map[string]int
	onChange                  SegmentChangeFunc
}

func newSegmentTracker(onChange SegmentChangeFunc, queues map[string]*QueueManager) *segmentTracker {
	st := &segmentTracker{
		queueHashToCurrentSegment: make(map[string]int, len(queues)),
		onChange:                  onChange,
	}

	currentSegmentMetric = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "current_segment",
		Help:      "Current segment being read by the remote write storage.",
	})

	for hash, q := range queues {
		st.trackSegmentChangesForQueue(hash, q)
	}

	return st
}

func (st *segmentTracker) updateChangeFunc(f SegmentChangeFunc) {
	st.mux.Lock()
	defer st.mux.Unlock()
	st.onChange = f
}

func (st *segmentTracker) updateQueues(newQueues map[string]*QueueManager) {
	st.mux.Lock()
	defer st.mux.Unlock()

	updated := make(map[string]int, len(newQueues))
	for hash, newQueue := range newQueues {
		if existingQueue, ok := st.queueHashToCurrentSegment[hash]; ok {
			updated[hash] = existingQueue
		} else {
			st.trackSegmentChangesForQueue(hash, newQueue)
		}
	}

	st.queueHashToCurrentSegment = updated
}

func (st *segmentTracker) trackSegmentChangesForQueue(hash string, q *QueueManager) {
	st.queueHashToCurrentSegment[hash] = -1

	q.SetSegmentChangeFunc(func(currentSegment int) {
		st.mux.Lock()
		defer st.mux.Unlock()

		// TODO should we move the current segment backwards we happen to have a if a new queue that starts on an older segment?
		st.queueHashToCurrentSegment[hash] = currentSegment
		for _, qCurrentSeg := range st.queueHashToCurrentSegment {
			// A different queue is still behind, so there's no segment change to propagate
			if qCurrentSeg < currentSegment {
				return
			}
		}

		st.currentSegment = currentSegment
		currentSegmentMetric.Set(float64(currentSegment))
		st.onChange(currentSegment)
	})
}

type timestampTracker struct {
	writeStorage         *WriteStorage
	appendOptions        *storage.AppendOptions
	samples              int64
	exemplars            int64
	histograms           int64
	highestTimestamp     int64
	highestRecvTimestamp *maxTimestamp
}

func (t *timestampTracker) SetOptions(opts *storage.AppendOptions) {
	t.appendOptions = opts
}

// Append implements storage.Appender.
func (t *timestampTracker) Append(_ storage.SeriesRef, _ labels.Labels, ts int64, _ float64) (storage.SeriesRef, error) {
	t.samples++
	if ts > t.highestTimestamp {
		t.highestTimestamp = ts
	}
	return 0, nil
}

func (t *timestampTracker) AppendExemplar(_ storage.SeriesRef, _ labels.Labels, _ exemplar.Exemplar) (storage.SeriesRef, error) {
	t.exemplars++
	return 0, nil
}

func (t *timestampTracker) AppendHistogram(_ storage.SeriesRef, _ labels.Labels, ts int64, _ *histogram.Histogram, _ *histogram.FloatHistogram) (storage.SeriesRef, error) {
	t.histograms++
	if ts > t.highestTimestamp {
		t.highestTimestamp = ts
	}
	return 0, nil
}

func (t *timestampTracker) AppendCTZeroSample(_ storage.SeriesRef, _ labels.Labels, _, ct int64) (storage.SeriesRef, error) {
	t.samples++
	if ct > t.highestTimestamp {
		// Theoretically, we should never see a CT zero sample with a timestamp higher than the highest timestamp we've seen so far.
		// However, we're not going to enforce that here, as it is not the responsibility of the tracker to enforce this.
		t.highestTimestamp = ct
	}
	return 0, nil
}

func (t *timestampTracker) AppendHistogramCTZeroSample(_ storage.SeriesRef, _ labels.Labels, _, ct int64, _ *histogram.Histogram, _ *histogram.FloatHistogram) (storage.SeriesRef, error) {
	t.histograms++
	if ct > t.highestTimestamp {
		// Theoretically, we should never see a CT zero sample with a timestamp higher than the highest timestamp we've seen so far.
		// However, we're not going to enforce that here, as it is not the responsibility of the tracker to enforce this.
		t.highestTimestamp = ct
	}
	return 0, nil
}

func (t *timestampTracker) UpdateMetadata(_ storage.SeriesRef, _ labels.Labels, _ metadata.Metadata) (storage.SeriesRef, error) {
	// TODO: Add and increment a `metadata` field when we get around to wiring metadata in remote_write.
	// UpdateMetadata is no-op for remote write (where timestampTracker is being used) for now.
	return 0, nil
}

// Commit implements storage.Appender.
func (t *timestampTracker) Commit() error {
	t.writeStorage.samplesIn.incr(t.samples + t.exemplars + t.histograms)

	samplesIn.Add(float64(t.samples))
	exemplarsIn.Add(float64(t.exemplars))
	histogramsIn.Add(float64(t.histograms))
	t.highestRecvTimestamp.Set(float64(t.highestTimestamp / 1000))
	return nil
}

// Rollback implements storage.Appender.
func (*timestampTracker) Rollback() error {
	return nil
}
