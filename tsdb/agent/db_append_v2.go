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
	"context"
	"fmt"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
)

// AppenderV2 implements storage.AppenderV2.
func (db *DB) AppenderV2(context.Context) storage.AppenderV2 {
	return db.appenderV2Pool.Get().(storage.AppenderV2)
}

type appenderV2 struct {
	appenderBase
}

// Append appends pending sample to agent's DB. It always returns SeriesRef 0.
// TODO: Wire metadata in the Agent's appender.
func (a *appenderV2) Append(_ storage.SeriesRef, ls labels.Labels, st, t int64, v float64, h *histogram.Histogram, fh *histogram.FloatHistogram, opts storage.AOptions) (storage.SeriesRef, error) {
	var (
		// Avoid shadowing err variables for reliability.
		valErr  error
		isStale bool
	)
	// Fail fast on incorrect histograms.
	switch {
	case fh != nil:
		valErr = fh.Validate()
	case h != nil:
		valErr = h.Validate()
	}
	if valErr != nil {
		return 0, valErr
	}

	ps, err := checkLabels(ls)
	if err != nil {
		return 0, err
	}

	if a.opts.EnableSTAsZeroSample && st != 0 {
		a.bestEffortAppendSTZeroSample(ps, st, t, h, fh)
	}

	switch {
	case fh != nil:
		isStale = value.IsStaleNaN(fh.Sum)
		a.addHistogram(ps, st, t, nil, fh)
	case h != nil:
		isStale = value.IsStaleNaN(h.Sum)
		a.addHistogram(ps, st, t, h, nil)
	default:
		isStale = value.IsStaleNaN(v)
		a.addSample(ps, st, t, v)
	}
	if isStale {
		// For stale values we never attempt to process metadata/exemplars, claim the success.
		return 0, nil
	}

	// Append exemplars if any and if storage was configured for it.
	if len(opts.Exemplars) > 0 {
		// Currently only exemplars can return partial errors.
		return 0, a.appendExemplars(ps, opts.Exemplars)
	}
	return 0, nil
}

// AppendExemplars implements storage.ExemplarAppenderV2. The prototype has no
// series lookup by ref, so the series is identified by ls alone.
func (a *appenderV2) AppendExemplars(_ storage.SeriesRef, ls labels.Labels, exemplars []exemplar.Exemplar) (storage.SeriesRef, error) {
	ps, err := checkLabels(ls)
	if err != nil {
		return 0, fmt.Errorf("invalid series labels when trying to add exemplars: %w", err)
	}
	if len(exemplars) == 0 {
		return 0, nil
	}
	return 0, a.appendExemplars(ps, exemplars)
}

var _ storage.ExemplarAppenderV2 = &appenderV2{}

func (a *appenderV2) Commit() error {
	defer a.appenderV2Pool.Put(a)
	return a.commit()
}

func (a *appenderV2) Rollback() error {
	defer a.appenderV2Pool.Put(a)
	return a.rollback()
}

func (a *appenderV2) appendExemplars(ps pendingSeries, exemplars []exemplar.Exemplar) error {
	var errs []error
	for _, e := range exemplars {
		// Ensure no empty labels have gotten through.
		e.Labels = e.Labels.WithoutEmpty()

		if err := a.validateExemplar(e); err != nil {
			// Return partial errors.
			errs = append(errs, err)
			continue
		}
		a.addExemplar(ps, e)
	}
	if len(errs) > 0 {
		return &storage.AppendPartialError{ExemplarErrors: errs}
	}
	return nil
}

// NOTE(bwplotka): This feature might be deprecated and removed once PROM-60
// is implemented.
//
// ST is an experimental feature, we don't fail the append on errors, just debug log.
func (a *appenderV2) bestEffortAppendSTZeroSample(ps pendingSeries, st, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram) {
	if st >= t {
		a.logger.Debug("Error when appending ST", "series", ps.lset.String(), "st", st, "t", t, "err", storage.ErrSTNewerThanSample)
		return
	}

	switch {
	case fh != nil:
		a.addHistogram(ps, 0, st, nil, &histogram.FloatHistogram{
			// The STZeroSample represents a counter reset by definition.
			CounterResetHint: histogram.CounterReset,
			// Replicate other fields to avoid needless chunk creation.
			Schema:        fh.Schema,
			ZeroThreshold: fh.ZeroThreshold,
			CustomValues:  fh.CustomValues,
		})
	case h != nil:
		a.addHistogram(ps, 0, st, &histogram.Histogram{
			// The STZeroSample represents a counter reset by definition.
			CounterResetHint: histogram.CounterReset,
			// Replicate other fields to avoid needless chunk creation.
			Schema:        h.Schema,
			ZeroThreshold: h.ZeroThreshold,
			CustomValues:  h.CustomValues,
		}, nil)
	default:
		a.addSample(ps, 0, st, 0)
	}
}
