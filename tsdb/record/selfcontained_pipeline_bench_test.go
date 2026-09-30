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

package record

import (
	"math"
	"math/bits"
	"testing"

	"github.com/cespare/xxhash/v2"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/util/compression"
)

// End-to-end cost of one commit, from the agent's appender to compressed
// remote write requests: encode, WAL record compression, queue-side decode and
// request building. Requests hold benchBatch samples, the max_samples_per_send
// default. Everything is single-threaded and single-shard; shard hashes are
// computed so their cost is included.

const benchBatch = 2000

var benchSep = []byte{0xff}

type reqSink struct {
	wire   int
	hashes uint64
}

// requestBuilder batches series into requests and compresses them.
type requestBuilder struct {
	v2      bool
	st      writev2.SymbolsTable
	ts1     []prompb.TimeSeries
	ts2     []writev2.TimeSeries
	n       int
	marshal []byte
	cbuf    compression.EncodeBuffer
	sink    *reqSink
}

func newRequestBuilder(v2 bool, sink *reqSink) *requestBuilder {
	return &requestBuilder{
		v2:   v2,
		st:   writev2.NewSymbolTable(),
		ts1:  make([]prompb.TimeSeries, benchBatch),
		ts2:  make([]writev2.TimeSeries, benchBatch),
		cbuf: compression.NewSyncEncodeBuffer(),
		sink: sink,
	}
}

func (r *requestBuilder) addLabels(lset labels.Labels, t int64, v float64) {
	if r.v2 {
		ts := &r.ts2[r.n]
		ts.LabelsRefs = r.st.SymbolizeLabels(lset, ts.LabelsRefs[:0])
		ts.Samples = append(ts.Samples[:0], writev2.Sample{Value: v, Timestamp: t})
	} else {
		ts := &r.ts1[r.n]
		ts.Labels = prompb.FromLabels(lset, ts.Labels[:0])
		ts.Samples = append(ts.Samples[:0], prompb.Sample{Value: v, Timestamp: t})
	}
	r.next()
}

func (r *requestBuilder) next() {
	r.n++
	if r.n == benchBatch {
		r.flush()
	}
}

func (r *requestBuilder) flush() {
	if r.n == 0 {
		return
	}
	var size int
	if r.v2 {
		req := writev2.Request{Symbols: r.st.Symbols(), Timeseries: r.ts2[:r.n]}
		size = req.Size()
		r.marshal = growBytes(r.marshal, size)
		if _, err := req.MarshalToSizedBuffer(r.marshal[:size]); err != nil {
			panic(err)
		}
		r.st.Reset()
	} else {
		req := prompb.WriteRequest{Timeseries: r.ts1[:r.n]}
		size = req.Size()
		r.marshal = growBytes(r.marshal, size)
		if _, err := req.MarshalToSizedBuffer(r.marshal[:size]); err != nil {
			panic(err)
		}
	}
	c, err := compression.Encode(compression.Snappy, r.marshal[:size], r.cbuf)
	if err != nil {
		panic(err)
	}
	r.sink.wire += len(c)
	r.n = 0
}

func growBytes(b []byte, n int) []byte {
	if cap(b) < n {
		return make([]byte, n)
	}
	return b[:n]
}

// transcoder builds requests straight from a symbols record, without
// building labels.Labels. Record symbols map to request symbols through a
// generation-tagged remap, reset whenever the request or the record changes.
type transcoder struct {
	r     *requestBuilder
	syms  []string
	remap []uint32
	gen   []uint32
	cur   uint32
	h     xxhash.Digest
}

func (x *transcoder) sym(i uint32) uint32 {
	if x.gen[i] == x.cur {
		return x.remap[i]
	}
	ref := x.r.st.Symbolize(x.syms[i])
	x.remap[i], x.gen[i] = ref, x.cur
	return ref
}

func (x *transcoder) record(rec []byte) {
	d := encoding.Decbuf{B: rec}
	d.Byte()
	x.syms = x.syms[:0]
	for n := d.Uvarint(); n > 0; n-- {
		x.syms = append(x.syms, yoloString(d.UvarintBytes()))
	}
	if cap(x.gen) < len(x.syms) {
		x.remap = make([]uint32, len(x.syms))
		x.gen = make([]uint32, len(x.syms))
	}
	x.remap, x.gen = x.remap[:len(x.syms)], x.gen[:len(x.syms)]
	x.cur++ // Indexes now refer to a different record.
	baseT := d.Be64int64()
	for n := d.Uvarint(); n > 0; n-- {
		nl := d.Uvarint()
		x.h.Reset()
		var ts1 *prompb.TimeSeries
		var ts2 *writev2.TimeSeries
		if x.r.v2 {
			ts2 = &x.r.ts2[x.r.n]
			ts2.LabelsRefs = ts2.LabelsRefs[:0]
		} else {
			ts1 = &x.r.ts1[x.r.n]
			ts1.Labels = ts1.Labels[:0]
		}
		for ; nl > 0; nl-- {
			ni, vi := d.Uvarint32(), d.Uvarint32()
			// Shard routing needs a stable per-series hash, not labels.Hash itself.
			_, _ = x.h.WriteString(x.syms[ni])
			_, _ = x.h.Write(benchSep)
			_, _ = x.h.WriteString(x.syms[vi])
			_, _ = x.h.Write(benchSep)
			if x.r.v2 {
				ts2.LabelsRefs = append(ts2.LabelsRefs, x.sym(ni), x.sym(vi))
			} else {
				ts1.Labels = append(ts1.Labels, prompb.Label{Name: x.syms[ni], Value: x.syms[vi]})
			}
		}
		x.r.sink.hashes += x.h.Sum64()
		t := baseT + d.Varint64()
		v := math.Float64frombits(d.Be64())
		if x.r.v2 {
			ts2.Samples = append(ts2.Samples[:0], writev2.Sample{Value: v, Timestamp: t})
		} else {
			ts1.Samples = append(ts1.Samples[:0], prompb.Sample{Value: v, Timestamp: t})
		}
		if x.r.n+1 == benchBatch {
			x.r.next()
			x.cur++ // New request, new symbol table.
			continue
		}
		x.r.next()
	}
}

func BenchmarkSelfContainedPipeline(b *testing.B) {
	for _, shape := range []string{"node-exporter", "grafana", "ksm"} {
		series := loadBenchShape(b, shape)
		samples := make([]RefSample, len(series))
		table := make(map[chunks.HeadSeriesRef]labels.Labels, len(series))
		for i, s := range series {
			samples[i] = RefSample{Ref: chunks.HeadSeriesRef(i + 1), T: benchBaseT, V: s.v}
			table[chunks.HeadSeriesRef(i+1)] = s.lset
		}
		for _, v2 := range []bool{false, true} {
			proto := "prw1"
			if v2 {
				proto = "prw2"
			}
			run := func(path string, fn func(r *requestBuilder) int) {
				b.Run(shape+"/"+proto+"/"+path, func(b *testing.B) {
					sink := &reqSink{}
					r := newRequestBuilder(v2, sink)
					var wal int
					b.ReportAllocs()
					for b.Loop() {
						sink.wire = 0
						wal = fn(r)
						r.flush()
					}
					reportPerSample(b, len(series), wal)
					b.ReportMetric(float64(sink.wire)/float64(len(series)), "wireB/sample")
				})
			}

			// No WAL: labels go straight from the appender into requests, like vmagent.
			run("vm-like", func(r *requestBuilder) int {
				for _, s := range series {
					r.sink.hashes += s.lset.Hash()
					r.addLabels(s.lset, benchBaseT, s.v)
				}
				return 0
			})

			var (
				enc  Encoder
				dec  = NewDecoder(nil, nil)
				cenc = compression.NewSyncEncodeBuffer()
				cdec = compression.NewSyncDecodeBuffer()
				buf  []byte
				dbuf []byte
				rs   []RefSample
			)
			walRoundTripWith := func(ct compression.Type, rec []byte) (int, []byte) {
				c, err := compression.Encode(ct, rec, cenc)
				if err != nil {
					b.Fatal(err)
				}
				n := len(c)
				raw, err := compression.Decode(ct, c, cdec)
				if err != nil {
					b.Fatal(err)
				}
				dbuf = append(dbuf[:0], raw...)
				return n, dbuf
			}
			walRoundTrip := func(rec []byte) (int, []byte) { return walRoundTripWith(compression.Snappy, rec) }

			// Upstream: ref-only samples, labels from the queue's ref table.
			run("upstream", func(r *requestBuilder) int {
				buf = enc.Samples(samples, buf[:0])
				n, raw := walRoundTrip(buf)
				var err error
				rs, err = dec.Samples(raw, rs[:0])
				if err != nil {
					b.Fatal(err)
				}
				for _, s := range rs {
					r.sink.hashes += uint64(s.Ref) // Upstream shards by ref.
					r.addLabels(table[s.Ref], s.T, s.V)
				}
				return n
			})

			syms := map[string]uint32{}
			var refs []uint32
			sb := labels.NewScratchBuilder(16)
			var dsyms []string
			var out []benchDecoded

			// Self-contained, decoded into labels.Labels, then the usual request path.
			run("decode", func(r *requestBuilder) int {
				buf, refs = encodeSymbolsRecord(series, syms, refs, buf[:0])
				n, raw := walRoundTrip(buf)
				dsyms, out = decodeSymbolsRecord(raw, &sb, dsyms, out)
				for _, s := range out {
					r.sink.hashes += s.lset.Hash()
					r.addLabels(s.lset, s.t, s.v)
				}
				return n
			})

			// Self-contained, transcoded straight into requests.
			x := &transcoder{}
			run("transcode", func(r *requestBuilder) int {
				x.r = r
				buf, refs = encodeSymbolsRecord(series, syms, refs, buf[:0])
				n, raw := walRoundTrip(buf)
				x.record(raw)
				return n
			})

			// Self-contained prev-delta records, transcoded, with each WAL compression.
			var pd prevDeltaEncoder
			px := &prevDeltaTranscoder{}
			for _, ct := range []compression.Type{compression.Snappy, compression.Zstd} {
				run("prevdelta-transcode/"+ct, func(r *requestBuilder) int {
					px.r = r
					buf = pd.encode(series, buf[:0])
					n, raw := walRoundTripWith(ct, buf)
					px.record(raw)
					return n
				})
			}
		}
	}
}

// prevDeltaTranscoder builds requests straight from a prev-delta record. A
// label equal to the previous series' reuses its string, and for PRW 2.0 its
// request symbol ref too while both series are in the same request.
type prevDeltaTranscoder struct {
	r                   *requestBuilder
	prev, cur           []labels.Label
	prevRefs, curRefs   []uint32
	prevHash, curHash   []uint64 // Per-label hashes, reused for labels equal to the previous series'.
	prevReq, requestGen uint32
}

func (x *prevDeltaTranscoder) record(rec []byte) {
	d := encoding.Decbuf{B: rec}
	d.Byte()
	baseT := d.Be64int64()
	x.prev = x.prev[:0]
	for n := d.Uvarint(); n > 0; n-- {
		nl := d.Uvarint()
		mask := d.Uvarint64()
		x.cur, x.curRefs, x.curHash = x.cur[:0], x.curRefs[:0], x.curHash[:0]
		sameReq := x.prevReq == x.requestGen
		var h uint64
		for i := range nl {
			var l labels.Label
			eq := i < 64 && mask&(1<<i) != 0
			if eq {
				l = x.prev[i]
			} else {
				l.Name = yoloString(d.UvarintBytes())
				l.Value = yoloString(d.UvarintBytes())
			}
			x.cur = append(x.cur, l)
			var lh uint64
			if eq {
				lh = x.prevHash[i]
			} else {
				lh = xxhash.Sum64String(l.Name) ^ bits.RotateLeft64(xxhash.Sum64String(l.Value), 31)
			}
			x.curHash = append(x.curHash, lh)
			// Order-dependent combine, so the result is a function of the label set.
			h = bits.RotateLeft64(h, 7) ^ (lh * 0x9e3779b97f4a7c15)
			if x.r.v2 {
				if eq && sameReq {
					x.curRefs = append(x.curRefs, x.prevRefs[2*i], x.prevRefs[2*i+1])
				} else {
					x.curRefs = append(x.curRefs, x.r.st.Symbolize(l.Name), x.r.st.Symbolize(l.Value))
				}
			}
		}
		x.r.sink.hashes += h
		t := baseT + d.Varint64()
		v := math.Float64frombits(d.Be64())
		if x.r.v2 {
			ts := &x.r.ts2[x.r.n]
			ts.LabelsRefs = append(ts.LabelsRefs[:0], x.curRefs...)
			ts.Samples = append(ts.Samples[:0], writev2.Sample{Value: v, Timestamp: t})
		} else {
			ts := &x.r.ts1[x.r.n]
			ts.Labels = ts.Labels[:0]
			for _, l := range x.cur {
				ts.Labels = append(ts.Labels, prompb.Label{Name: l.Name, Value: l.Value})
			}
			ts.Samples = append(ts.Samples[:0], prompb.Sample{Value: v, Timestamp: t})
		}
		x.prevReq = x.requestGen
		if x.r.n+1 == benchBatch {
			x.requestGen++ // The flush resets the request symbol table.
		}
		x.r.next()
		x.prev, x.cur = x.cur, x.prev
		x.prevRefs, x.curRefs = x.curRefs, x.prevRefs
		x.prevHash, x.curHash = x.curHash, x.prevHash
	}
	x.requestGen++ // The caller may flush after the record.
}
