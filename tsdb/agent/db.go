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

// PROTOTYPE: series-less agent DB. There is no global series table. Each WAL
// segment carries its own series records with segment-local refs, and the
// appender always returns SeriesRef 0. See PROTOTYPE_NOTES.md.

package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/prometheus/prometheus/tsdb/wlog"
	"github.com/prometheus/prometheus/util/compression"
)

const (
	sampleMetricTypeFloat     = "float"
	sampleMetricTypeHistogram = "histogram"

	// WAL page and record header sizes, mirrored from tsdb/wlog for batch size estimates.
	walPageSize         = 32 * 1024
	walRecordHeaderSize = 7

	// maxSamplesPerLog bounds how many float samples one rotation check covers.
	maxSamplesPerLog = 4096
)

var ErrUnsupported = errors.New("unsupported operation with WAL-only storage")

// Default values for options.
var (
	DefaultTruncateFrequency = 2 * time.Hour
	DefaultMinWALTime        = int64(5 * time.Minute / time.Millisecond)
	DefaultMaxWALTime        = int64(4 * time.Hour / time.Millisecond)
)

// Options of the WAL storage.
type Options struct {
	// Segments (wal files) max size.
	// WALSegmentSize <= 0, segment size is default size.
	// WALSegmentSize > 0, segment size is WALSegmentSize.
	WALSegmentSize int

	// WALCompression configures the compression type to use on records in the WAL.
	WALCompression compression.Type

	// StripeSize is unused in the series-less prototype.
	StripeSize int

	// TruncateFrequency determines how frequently to delete segments that every
	// remote_write destination has sent.
	TruncateFrequency time.Duration

	// MinWALTime and MaxWALTime are unused in the series-less prototype.
	MinWALTime, MaxWALTime int64

	// NoLockfile disables creation and consideration of a lock file.
	NoLockfile bool

	// OutOfOrderTimeWindow is unused in the series-less prototype.
	OutOfOrderTimeWindow int64

	// EnableSTAsZeroSample represents 'created-timestamp-zero-ingestion' feature flag.
	// If true, ST, if non-empty and earlier than sample timestamp, will be stored
	// as a zero sample before the actual sample.
	EnableSTAsZeroSample bool

	// EnableSTStorage determines whether agent DB should write a Start Timestamp (ST)
	// per sample to WAL.
	EnableSTStorage bool

	// CheckpointFromInMemorySeries and CheckpointBatchSize are unused in the
	// series-less prototype (there are no checkpoints).
	CheckpointFromInMemorySeries bool
	CheckpointBatchSize          int
}

// DefaultOptions used for the WAL storage. They are reasonable for setups using
// millisecond-precision timestamps.
func DefaultOptions() *Options {
	return &Options{
		WALSegmentSize:       wlog.DefaultSegmentSize,
		WALCompression:       compression.None,
		StripeSize:           tsdb.DefaultStripeSize,
		TruncateFrequency:    DefaultTruncateFrequency,
		MinWALTime:           DefaultMinWALTime,
		MaxWALTime:           DefaultMaxWALTime,
		NoLockfile:           false,
		OutOfOrderTimeWindow: 0,
	}
}

type dbMetrics struct {
	r prometheus.Registerer

	// numActiveSeries is the size of the current segment's series table.
	numActiveSeries        prometheus.Gauge
	totalAppendedSamples   *prometheus.CounterVec
	totalAppendedExemplars prometheus.Counter
	walTruncateDuration    prometheus.Summary
	segmentRotations       prometheus.Counter
	rotationMisestimates   prometheus.Counter
}

func newDBMetrics(r prometheus.Registerer) *dbMetrics {
	m := dbMetrics{r: r}
	m.numActiveSeries = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_agent_active_series",
		Help: "Number of series in the series table of the WAL segment being written",
	})

	m.totalAppendedSamples = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "prometheus_agent_samples_appended_total",
		Help: "Total number of samples appended to the storage",
	}, []string{"type"})

	m.totalAppendedExemplars = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_agent_exemplars_appended_total",
		Help: "Total number of exemplars appended to the storage",
	})

	m.walTruncateDuration = prometheus.NewSummary(prometheus.SummaryOpts{
		Name: "prometheus_agent_truncate_duration_seconds",
		Help: "Duration of WAL truncation.",
	})

	m.segmentRotations = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_agent_segment_rotations_total",
		Help: "Total number of WAL segment rotations triggered by the agent before a commit.",
	})

	m.rotationMisestimates = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_agent_segment_rotation_misestimates_total",
		Help: "Total number of commits where the WAL rotated on its own, so records may reference a series table from the previous segment.",
	})

	if r != nil {
		r.MustRegister(
			m.numActiveSeries,
			m.totalAppendedSamples,
			m.totalAppendedExemplars,
			m.walTruncateDuration,
			m.segmentRotations,
			m.rotationMisestimates,
		)
	}

	return &m
}

func (m *dbMetrics) Unregister() {
	if m.r == nil {
		return
	}
	cs := []prometheus.Collector{
		m.numActiveSeries,
		m.totalAppendedSamples,
		m.totalAppendedExemplars,
		m.walTruncateDuration,
		m.segmentRotations,
		m.rotationMisestimates,
	}
	for _, c := range cs {
		m.r.Unregister(c)
	}
}

// segEntry is one series in the current segment's table.
type segEntry struct {
	ref  chunks.HeadSeriesRef
	lset labels.Labels
}

// DB represents a WAL-only storage. It implements storage.DB.
type DB struct {
	mtx    sync.RWMutex
	logger *slog.Logger
	opts   *Options
	rs     *remote.Storage

	wal    *wlog.WL
	locker *tsdbutil.DirLocker

	appenderPool   sync.Pool
	appenderV2Pool sync.Pool
	bufPool        sync.Pool

	// segMu guards the series table of the segment being written. Commits are
	// serialized on it, so a batch never straddles a rotation.
	segMu   sync.Mutex
	table   map[uint64][]segEntry
	nextRef chunks.HeadSeriesRef

	donec chan struct{}
	stopc chan struct{}

	writeNotified wlog.WriteNotified

	metrics *dbMetrics
}

// Open returns a new agent.DB in the given directory.
func Open(l *slog.Logger, reg prometheus.Registerer, rs *remote.Storage, dir string, opts *Options) (*DB, error) {
	opts = validateOptions(opts)

	locker, err := tsdbutil.NewDirLocker(dir, "agent", l, reg)
	if err != nil {
		return nil, err
	}
	if !opts.NoLockfile {
		if err := locker.Lock(); err != nil {
			return nil, err
		}
	}

	// remote_write expects WAL to be stored in a "wal" subdirectory of the main storage.
	dir = filepath.Join(dir, "wal")

	w, err := wlog.NewSize(l, reg, dir, opts.WALSegmentSize, opts.WALCompression)
	if err != nil {
		return nil, fmt.Errorf("creating WAL: %w", err)
	}

	db := &DB{
		logger: l,
		opts:   opts,
		rs:     rs,

		wal:    w,
		locker: locker,

		table: make(map[uint64][]segEntry),

		donec: make(chan struct{}),
		stopc: make(chan struct{}),

		metrics: newDBMetrics(reg),
	}

	db.bufPool.New = func() any {
		return make([]byte, 0, 1024)
	}

	newBase := func() appenderBase {
		return appenderBase{
			DB:                     db,
			pendingSamples:         make([]record.RefSample, 0, 100),
			pendingHistograms:      make([]record.RefHistogramSample, 0, 100),
			pendingFloatHistograms: make([]record.RefFloatHistogramSample, 0, 100),
			pendingExamplars:       make([]record.RefExemplar, 0, 10),
		}
	}
	db.appenderPool.New = func() any {
		return &appender{appenderBase: newBase()}
	}
	db.appenderV2Pool.New = func() any {
		return &appenderV2{appenderBase: newBase()}
	}

	go db.run()
	return db, nil
}

// SetWriteNotified allows to set an instance to notify when a write happens.
// It must be used during initialization. It is not safe to use it during execution.
func (db *DB) SetWriteNotified(wn wlog.WriteNotified) {
	db.writeNotified = wn
}

func validateOptions(opts *Options) *Options {
	if opts == nil {
		opts = DefaultOptions()
	}
	if opts.WALSegmentSize <= 0 {
		opts.WALSegmentSize = wlog.DefaultSegmentSize
	}

	if opts.WALCompression == "" {
		opts.WALCompression = compression.None
	}

	if opts.TruncateFrequency <= 0 {
		opts.TruncateFrequency = DefaultTruncateFrequency
	}
	return opts
}

func (db *DB) run() {
	defer close(db.donec)

	for {
		select {
		case <-db.stopc:
			return
		case <-time.After(db.opts.TruncateFrequency):
			if err := db.truncate(); err != nil {
				db.logger.Warn("failed to truncate WAL", "err", err)
			}
		}
	}
}

// truncate deletes the segments that every remote_write destination has sent.
// The segment being written is never deleted.
func (db *DB) truncate() error {
	return db.truncateSegments(db.rs.LowestSentSegment())
}

// truncateSegments deletes segments up to and including sent.
func (db *DB) truncateSegments(sent int) error {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	start := time.Now()
	active, _ := db.wal.SegmentAndOffset()
	// Keep everything from sent+1 on. A destination that has sent nothing yields -1.
	keepFrom := min(sent+1, active)
	if keepFrom <= 0 {
		return nil
	}
	if err := db.wal.Truncate(keepFrom); err != nil {
		return fmt.Errorf("truncate segments: %w", err)
	}
	db.metrics.walTruncateDuration.Observe(time.Since(start).Seconds())
	db.logger.Debug("WAL truncated", "keepFrom", keepFrom, "duration", time.Since(start))
	return nil
}

// StartTime implements the Storage interface.
func (*DB) StartTime() (int64, error) {
	return int64(model.Latest), nil
}

// Querier implements the Storage interface.
func (*DB) Querier(int64, int64) (storage.Querier, error) {
	return nil, ErrUnsupported
}

// ChunkQuerier implements the Storage interface.
func (*DB) ChunkQuerier(int64, int64) (storage.ChunkQuerier, error) {
	return nil, ErrUnsupported
}

// ExemplarQuerier implements the Storage interface.
func (*DB) ExemplarQuerier(context.Context) (storage.ExemplarQuerier, error) {
	return nil, ErrUnsupported
}

// Appender implements storage.Storage.
func (db *DB) Appender(context.Context) storage.Appender {
	return db.appenderPool.Get().(storage.Appender)
}

// Close implements the Storage interface.
func (db *DB) Close() error {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	close(db.stopc)
	<-db.donec

	db.metrics.Unregister()

	return errors.Join(db.locker.Release(), db.wal.Close())
}

// pendingSeries identifies the series of a pending record until Commit
// resolves it to a segment-local ref.
type pendingSeries struct {
	lset labels.Labels
	hash uint64
}

type appenderBase struct {
	*DB

	pendingSamples         []record.RefSample
	pendingHistograms      []record.RefHistogramSample
	pendingFloatHistograms []record.RefFloatHistogramSample
	pendingExamplars       []record.RefExemplar

	// Series of the pending records above, same indexes.
	sampleSeries         []pendingSeries
	histogramSeries      []pendingSeries
	floatHistogramSeries []pendingSeries
	exemplarSeries       []pendingSeries

	// Scratch for the series records of one log call.
	seriesBuf []record.RefSeries
}

type appender struct {
	appenderBase

	hints *storage.AppendOptions
}

func (a *appender) SetOptions(opts *storage.AppendOptions) {
	a.hints = opts
}

// checkLabels validates l the way the TSDB head appender does and returns the
// cleaned labels and their hash.
func checkLabels(l labels.Labels) (pendingSeries, error) {
	l = l.WithoutEmpty()
	if l.IsEmpty() {
		return pendingSeries{}, fmt.Errorf("empty labelset: %w", tsdb.ErrInvalidSample)
	}
	if lbl, dup := l.HasDuplicateLabelNames(); dup {
		return pendingSeries{}, fmt.Errorf(`label name "%s" is not unique: %w`, lbl, tsdb.ErrInvalidSample)
	}
	return pendingSeries{lset: l, hash: l.Hash()}, nil
}

func (a *appender) Append(_ storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	ps, err := checkLabels(l)
	if err != nil {
		return 0, err
	}
	a.addSample(ps, 0, t, v)
	return 0, nil
}

func (a *appenderBase) addSample(ps pendingSeries, st, t int64, v float64) {
	a.pendingSamples = append(a.pendingSamples, record.RefSample{ST: st, T: t, V: v})
	a.sampleSeries = append(a.sampleSeries, ps)
	a.metrics.totalAppendedSamples.WithLabelValues(sampleMetricTypeFloat).Inc()
}

func (a *appenderBase) addHistogram(ps pendingSeries, st, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram) {
	switch {
	case h != nil:
		a.pendingHistograms = append(a.pendingHistograms, record.RefHistogramSample{ST: st, T: t, H: h})
		a.histogramSeries = append(a.histogramSeries, ps)
	case fh != nil:
		a.pendingFloatHistograms = append(a.pendingFloatHistograms, record.RefFloatHistogramSample{ST: st, T: t, FH: fh})
		a.floatHistogramSeries = append(a.floatHistogramSeries, ps)
	default:
		return
	}
	a.metrics.totalAppendedSamples.WithLabelValues(sampleMetricTypeHistogram).Inc()
}

// The prototype has no series lookup by ref, so exemplars are resolved by the
// labels passed by the caller and the latest-exemplar dedupe is gone.
func (a *appender) AppendExemplar(_ storage.SeriesRef, l labels.Labels, e exemplar.Exemplar) (storage.SeriesRef, error) {
	ps, err := checkLabels(l)
	if err != nil {
		return 0, err
	}
	// Ensure no empty labels have gotten through.
	e.Labels = e.Labels.WithoutEmpty()
	if err := a.validateExemplar(e); err != nil {
		return 0, err
	}
	a.addExemplar(ps, e)
	return 0, nil
}

func (a *appenderBase) addExemplar(ps pendingSeries, e exemplar.Exemplar) {
	a.pendingExamplars = append(a.pendingExamplars, record.RefExemplar{T: e.Ts, V: e.Value, Labels: e.Labels})
	a.exemplarSeries = append(a.exemplarSeries, ps)
	a.metrics.totalAppendedExemplars.Inc()
}

func (*appenderBase) validateExemplar(e exemplar.Exemplar) error {
	if lbl, dup := e.Labels.HasDuplicateLabelNames(); dup {
		return fmt.Errorf(`label name "%s" is not unique: %w`, lbl, tsdb.ErrInvalidExemplar)
	}

	// Exemplar label length does not include chars involved in text rendering such as quotes
	// equals sign, or commas. See definition of const ExemplarMaxLabelLength.
	labelSetLen := 0
	return e.Labels.Validate(func(l labels.Label) error {
		labelSetLen += utf8.RuneCountInString(l.Name)
		labelSetLen += utf8.RuneCountInString(l.Value)
		if labelSetLen > exemplar.ExemplarMaxLabelSetLength {
			return storage.ErrExemplarLabelLength
		}
		return nil
	})
}

func (a *appender) AppendHistogram(_ storage.SeriesRef, l labels.Labels, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	if h != nil {
		if err := h.Validate(); err != nil {
			return 0, err
		}
	}
	if fh != nil {
		if err := fh.Validate(); err != nil {
			return 0, err
		}
	}
	ps, err := checkLabels(l)
	if err != nil {
		return 0, err
	}
	a.addHistogram(ps, 0, t, h, fh)
	return 0, nil
}

func (*appender) UpdateMetadata(storage.SeriesRef, labels.Labels, metadata.Metadata) (storage.SeriesRef, error) {
	// TODO: Wire metadata in the Agent's appender.
	return 0, nil
}

func (a *appender) AppendHistogramSTZeroSample(_ storage.SeriesRef, l labels.Labels, t, st int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	if h != nil {
		if err := h.Validate(); err != nil {
			return 0, err
		}
	}
	if fh != nil {
		if err := fh.Validate(); err != nil {
			return 0, err
		}
	}
	if st >= t {
		return 0, storage.ErrSTNewerThanSample
	}
	ps, err := checkLabels(l)
	if err != nil {
		return 0, err
	}
	switch {
	case h != nil:
		a.addHistogram(ps, 0, st, &histogram.Histogram{}, nil)
	case fh != nil:
		a.addHistogram(ps, 0, st, nil, &histogram.FloatHistogram{})
	}
	return 0, nil
}

func (a *appender) AppendSTZeroSample(_ storage.SeriesRef, l labels.Labels, t, st int64) (storage.SeriesRef, error) {
	if st >= t {
		return 0, storage.ErrSTNewerThanSample
	}
	ps, err := checkLabels(l)
	if err != nil {
		return 0, err
	}
	a.addSample(ps, 0, st, 0)
	return 0, nil
}

// Commit submits the collected samples and purges the batch.
func (a *appender) Commit() error {
	defer a.appenderPool.Put(a)
	return a.commit()
}

func (a *appender) Rollback() error {
	defer a.appenderPool.Put(a)
	return a.rollback()
}

func (a *appenderBase) commit() error {
	if err := a.log(); err != nil {
		a.clearData()
		return err
	}

	a.clearData()

	if a.writeNotified != nil {
		a.writeNotified.Notify()
	}
	return nil
}

// log writes all pending data to the WAL. Float samples go in chunks of
// maxSamplesPerLog, and everything else rides with the last chunk. Each chunk
// checks for a rotation before it resolves refs, so its series records and
// samples always land in the same segment.
func (a *appenderBase) log() error {
	a.mtx.RLock()
	defer a.mtx.RUnlock()

	a.segMu.Lock()
	defer a.segMu.Unlock()

	n := len(a.pendingSamples)
	for start := 0; ; start += maxSamplesPerLog {
		end := min(start+maxSamplesPerLog, n)
		last := end == n
		if err := a.logChunk(start, end, last); err != nil {
			return err
		}
		if last {
			return nil
		}
	}
}

func (a *appenderBase) logChunk(start, end int, withRest bool) error {
	samples := a.pendingSamples[start:end]
	sampleSeries := a.sampleSeries[start:end]
	var (
		hists   []record.RefHistogramSample
		fhists  []record.RefFloatHistogramSample
		exs     []record.RefExemplar
		hSeries []pendingSeries
		fSeries []pendingSeries
		eSeries []pendingSeries
	)
	if withRest {
		hists, fhists, exs = a.pendingHistograms, a.pendingFloatHistograms, a.pendingExamplars
		hSeries, fSeries, eSeries = a.histogramSeries, a.floatHistogramSeries, a.exemplarSeries
	}
	if len(samples)+len(hists)+len(fhists)+len(exs) == 0 {
		return nil
	}

	seg, off := a.wal.SegmentAndOffset()
	if est := estimateBytes(samples, sampleSeries, hists, hSeries, fhists, fSeries, exs, eSeries); off > 0 && off+est > a.opts.WALSegmentSize {
		if err := a.rotateLocked(); err != nil {
			return err
		}
		seg, _ = a.wal.SegmentAndOffset()
	}

	a.seriesBuf = a.seriesBuf[:0]
	for i := range samples {
		samples[i].Ref = a.refLocked(sampleSeries[i])
	}
	for i := range hists {
		hists[i].Ref = a.refLocked(hSeries[i])
	}
	for i := range fhists {
		fhists[i].Ref = a.refLocked(fSeries[i])
	}
	for i := range exs {
		exs[i].Ref = a.refLocked(eSeries[i])
	}
	a.metrics.numActiveSeries.Set(float64(a.tableLenLocked()))

	if err := a.writeRecords(samples, hists, fhists, exs); err != nil {
		return err
	}

	if after, _ := a.wal.SegmentAndOffset(); after != seg {
		// The WAL rotated on its own, so some records may reference a table
		// that lives in the previous segment.
		a.metrics.rotationMisestimates.Inc()
		a.logger.Warn("WAL rotated inside a commit, batch size estimate was too low", "from", seg, "to", after)
		a.resetTableLocked()
	}
	return nil
}

func (a *appenderBase) writeRecords(samples []record.RefSample, hists []record.RefHistogramSample, fhists []record.RefFloatHistogramSample, exs []record.RefExemplar) error {
	encoder := record.Encoder{EnableSTStorage: a.opts.EnableSTStorage}
	buf := a.bufPool.Get().([]byte)
	defer func() {
		a.bufPool.Put(buf[:0]) //nolint:staticcheck
	}()

	if len(a.seriesBuf) > 0 {
		buf = encoder.Series(a.seriesBuf, buf)
		if err := a.wal.Log(buf); err != nil {
			return err
		}
		buf = buf[:0]
	}

	if len(samples) > 0 {
		buf = encoder.Samples(samples, buf)
		if err := a.wal.Log(buf); err != nil {
			return err
		}
		buf = buf[:0]
	}

	if len(hists) > 0 {
		var customBucketsHistograms []record.RefHistogramSample
		buf, customBucketsHistograms = encoder.HistogramSamples(hists, buf)
		if len(buf) > 0 {
			if err := a.wal.Log(buf); err != nil {
				return err
			}
			buf = buf[:0]
		}
		if len(customBucketsHistograms) > 0 {
			buf = encoder.CustomBucketsHistogramSamples(customBucketsHistograms, nil)
			if err := a.wal.Log(buf); err != nil {
				return err
			}
			buf = buf[:0]
		}
	}

	if len(fhists) > 0 {
		var customBucketsFloatHistograms []record.RefFloatHistogramSample
		buf, customBucketsFloatHistograms = encoder.FloatHistogramSamples(fhists, buf)
		if len(buf) > 0 {
			if err := a.wal.Log(buf); err != nil {
				return err
			}
			buf = buf[:0]
		}
		if len(customBucketsFloatHistograms) > 0 {
			buf = encoder.CustomBucketsFloatHistogramSamples(customBucketsFloatHistograms, nil)
			if err := a.wal.Log(buf); err != nil {
				return err
			}
			buf = buf[:0]
		}
	}

	if len(exs) > 0 {
		buf = encoder.Exemplars(exs, buf)
		if err := a.wal.Log(buf); err != nil {
			return err
		}
		buf = buf[:0]
	}
	return nil
}

// refLocked returns the segment-local ref for ps, adding it to the table and to
// the series records of the current log call when it is new to this segment.
// Must be called with segMu held.
func (a *appenderBase) refLocked(ps pendingSeries) chunks.HeadSeriesRef {
	for _, e := range a.table[ps.hash] {
		if labels.Equal(e.lset, ps.lset) {
			return e.ref
		}
	}
	a.nextRef++
	a.table[ps.hash] = append(a.table[ps.hash], segEntry{ref: a.nextRef, lset: ps.lset})
	a.seriesBuf = append(a.seriesBuf, record.RefSeries{Ref: a.nextRef, Labels: ps.lset})
	return a.nextRef
}

func (db *DB) tableLenLocked() int {
	n := 0
	for _, es := range db.table {
		n += len(es)
	}
	return n
}

func (db *DB) resetTableLocked() {
	db.table = make(map[uint64][]segEntry)
	db.nextRef = 0
	db.metrics.numActiveSeries.Set(0)
}

// rotateLocked starts a new WAL segment with an empty series table. Series that
// are still being scraped are re-emitted by the next commit that touches them.
// Must be called with segMu held.
func (db *DB) rotateLocked() error {
	if _, err := db.wal.NextSegmentSync(); err != nil {
		return fmt.Errorf("next segment: %w", err)
	}
	db.resetTableLocked()
	db.metrics.segmentRotations.Inc()
	return nil
}

// clearData clears all pending data.
func (a *appenderBase) clearData() {
	a.pendingSamples = a.pendingSamples[:0]
	a.pendingHistograms = a.pendingHistograms[:0]
	a.pendingFloatHistograms = a.pendingFloatHistograms[:0]
	a.pendingExamplars = a.pendingExamplars[:0]
	clear(a.sampleSeries)
	clear(a.histogramSeries)
	clear(a.floatHistogramSeries)
	clear(a.exemplarSeries)
	a.sampleSeries = a.sampleSeries[:0]
	a.histogramSeries = a.histogramSeries[:0]
	a.floatHistogramSeries = a.floatHistogramSeries[:0]
	a.exemplarSeries = a.exemplarSeries[:0]
	clear(a.seriesBuf)
	a.seriesBuf = a.seriesBuf[:0]
}

// Series are only created at commit, so there is nothing to keep on rollback.
func (a *appenderBase) rollback() error {
	a.clearData()
	return nil
}

// estimateBytes is an upper bound on the WAL bytes a chunk needs, including
// series records for every series and one page of slack.
func estimateBytes(
	samples []record.RefSample, sSeries []pendingSeries,
	hists []record.RefHistogramSample, hSeries []pendingSeries,
	fhists []record.RefFloatHistogramSample, fSeries []pendingSeries,
	exs []record.RefExemplar, eSeries []pendingSeries,
) int {
	est := walPageSize
	seriesBytes := func(ps []pendingSeries) int {
		n := 0
		for _, p := range ps {
			n += 8
			p.lset.Range(func(l labels.Label) {
				n += len(l.Name) + len(l.Value) + 4
			})
		}
		return n
	}
	est += seriesBytes(sSeries) + seriesBytes(hSeries) + seriesBytes(fSeries) + seriesBytes(eSeries)
	est += 40 * len(samples)
	for _, h := range hists {
		est += 64 + 16*(len(h.H.PositiveSpans)+len(h.H.NegativeSpans)+len(h.H.PositiveBuckets)+len(h.H.NegativeBuckets)+len(h.H.CustomValues))
	}
	for _, h := range fhists {
		est += 64 + 16*(len(h.FH.PositiveSpans)+len(h.FH.NegativeSpans)+len(h.FH.PositiveBuckets)+len(h.FH.NegativeBuckets)+len(h.FH.CustomValues))
	}
	for _, e := range exs {
		est += 32
		e.Labels.Range(func(l labels.Label) {
			est += len(l.Name) + len(l.Value) + 4
		})
	}
	// Record headers: one per page fragment, plus a handful of records.
	est += (est/walPageSize + 8) * walRecordHeaderSize
	return est
}
