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
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/textparse"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/util/compression"
)

// Benchmarks for self-contained WAL records, where labels travel with samples
// instead of being referenced by a series ref. One record is one commit
// (one scrape). Inputs are exposition dumps in $WAL_BENCH_SHAPES/<shape>.txt.

const benchBaseT = int64(1790000000000)

type benchSeries struct {
	lset labels.Labels
	v    float64
}

type benchDecoded struct {
	lset labels.Labels
	t    int64
	v    float64
}

func loadBenchShape(b *testing.B, name string) []benchSeries {
	dir := os.Getenv("WAL_BENCH_SHAPES")
	if dir == "" {
		b.Skip("WAL_BENCH_SHAPES not set")
	}
	buf, err := os.ReadFile(filepath.Join(dir, name+".txt"))
	require.NoError(b, err)
	p := textparse.NewPromParser(buf, labels.NewSymbolTable(), false)
	var out []benchSeries
	for {
		e, err := p.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(b, err)
		if e != textparse.EntrySeries {
			continue
		}
		var l labels.Labels
		p.Labels(&l)
		_, _, v := p.Series()
		out = append(out, benchSeries{lset: l, v: v})
	}
	return out
}

// Format 3: one symbol table per record, series as symbol refs.
const benchSymbolsType Type = 200

func encodeSymbolsRecord(series []benchSeries, syms map[string]uint32, refs []uint32, b []byte) ([]byte, []uint32) {
	clear(syms)
	refs = refs[:0]
	var symList []string
	sym := func(s string) uint32 {
		if i, ok := syms[s]; ok {
			return i
		}
		i := uint32(len(symList))
		syms[s] = i
		symList = append(symList, s)
		return i
	}
	for _, s := range series {
		refs = append(refs, uint32(s.lset.Len()))
		s.lset.Range(func(l labels.Label) {
			refs = append(refs, sym(l.Name), sym(l.Value))
		})
	}
	e := encoding.Encbuf{B: b[:0]}
	e.PutByte(byte(benchSymbolsType))
	e.PutUvarint(len(symList))
	for _, s := range symList {
		e.PutUvarintStr(s)
	}
	e.PutBE64int64(benchBaseT)
	e.PutUvarint(len(series))
	i := 0
	for _, s := range series {
		n := int(refs[i])
		i++
		e.PutUvarint(n)
		for j := 0; j < 2*n; j++ {
			e.PutUvarint32(refs[i])
			i++
		}
		e.PutVarint64(0) // All samples in a scrape share its timestamp.
		e.PutBE64(math.Float64bits(s.v))
	}
	return e.Get(), refs
}

func decodeSymbolsRecord(rec []byte, b *labels.ScratchBuilder, syms []string, out []benchDecoded) ([]string, []benchDecoded) {
	d := encoding.Decbuf{B: rec}
	d.Byte()
	syms = syms[:0]
	for n := d.Uvarint(); n > 0; n-- {
		syms = append(syms, yoloString(d.UvarintBytes()))
	}
	baseT := d.Be64int64()
	out = out[:0]
	for n := d.Uvarint(); n > 0; n-- {
		b.Reset()
		for l := d.Uvarint(); l > 0; l-- {
			name, value := syms[d.Uvarint32()], syms[d.Uvarint32()]
			b.Add(name, value)
		}
		t := baseT + d.Varint64()
		v := math.Float64frombits(d.Be64())
		out = append(out, benchDecoded{lset: b.Labels(), t: t, v: v})
	}
	return syms, out
}

// Format 4: labels inline per series, no symbols.
const benchInlineType Type = 201

func encodeInlineRecord(series []benchSeries, b []byte) []byte {
	e := encoding.Encbuf{B: b[:0]}
	e.PutByte(byte(benchInlineType))
	e.PutBE64int64(benchBaseT)
	e.PutUvarint(len(series))
	for _, s := range series {
		e.PutUvarint(s.lset.Len())
		s.lset.Range(func(l labels.Label) {
			e.PutUvarintStr(l.Name)
			e.PutUvarintStr(l.Value)
		})
		e.PutVarint64(0)
		e.PutBE64(math.Float64bits(s.v))
	}
	return e.Get()
}

func decodeInlineRecord(rec []byte, b *labels.ScratchBuilder, out []benchDecoded) []benchDecoded {
	d := encoding.Decbuf{B: rec}
	d.Byte()
	baseT := d.Be64int64()
	out = out[:0]
	for n := d.Uvarint(); n > 0; n-- {
		b.Reset()
		for l := d.Uvarint(); l > 0; l-- {
			name := yoloString(d.UvarintBytes())
			value := yoloString(d.UvarintBytes())
			b.Add(name, value)
		}
		t := baseT + d.Varint64()
		v := math.Float64frombits(d.Be64())
		out = append(out, benchDecoded{lset: b.Labels(), t: t, v: v})
	}
	return out
}

var benchShapes = []string{"node-exporter", "cadvisor", "grafana"}

func reportPerSample(b *testing.B, n, recBytes int) {
	b.ReportMetric(float64(recBytes)/float64(n), "B/sample")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(n), "ns/sample")
}

// BenchmarkSelfContainedEncode measures commit-side encoding plus record
// compression. "ref" is upstream: a ref-only samples record, with series
// records assumed written once and amortized away. "ref-series" is the same
// commit with its series record re-emitted, the per-segment prototype at K=1.
func BenchmarkSelfContainedEncode(b *testing.B) {
	for _, shape := range benchShapes {
		series := loadBenchShape(b, shape)
		samples := make([]RefSample, len(series))
		refSeries := make([]RefSeries, len(series))
		for i, s := range series {
			samples[i] = RefSample{Ref: chunks.HeadSeriesRef(i + 1), T: benchBaseT, V: s.v}
			refSeries[i] = RefSeries{Ref: chunks.HeadSeriesRef(i + 1), Labels: s.lset}
		}
		for _, ct := range compression.Types() {
			cbuf := compression.NewSyncEncodeBuffer()
			var buf, buf2 []byte
			// compressed returns the WAL bytes for one record; the WAL compresses each record on its own.
			compressed := func(rec []byte) int {
				c, err := compression.Encode(ct, rec, cbuf)
				if err != nil {
					b.Fatal(err)
				}
				return len(c)
			}
			run := func(name string, encode func() int) {
				b.Run(shape+"/"+name+"/"+ct, func(b *testing.B) {
					var size int
					b.ReportAllocs()
					for b.Loop() {
						size = encode()
					}
					reportPerSample(b, len(series), size)
				})
			}
			var enc Encoder
			run("ref", func() int {
				buf = enc.Samples(samples, buf[:0])
				return compressed(buf)
			})
			run("ref-series", func() int {
				buf2 = enc.Series(refSeries, buf2[:0])
				n := compressed(buf2)
				buf = enc.Samples(samples, buf[:0])
				return n + compressed(buf)
			})
			syms := map[string]uint32{}
			var refs []uint32
			run("symbols", func() int {
				buf, refs = encodeSymbolsRecord(series, syms, refs, buf[:0])
				return compressed(buf)
			})
			run("inline", func() int {
				buf = encodeInlineRecord(series, buf[:0])
				return compressed(buf)
			})
		}
	}
}

// BenchmarkSelfContainedDecode measures queue-side decompression and decoding
// into labels plus sample, which is what the queue holds today. "ref" looks
// labels up in a ref-to-labels map the queue keeps, which self-contained
// records don't need.
func BenchmarkSelfContainedDecode(b *testing.B) {
	for _, shape := range benchShapes {
		series := loadBenchShape(b, shape)
		samples := make([]RefSample, len(series))
		table := make(map[chunks.HeadSeriesRef]labels.Labels, len(series))
		for i, s := range series {
			samples[i] = RefSample{Ref: chunks.HeadSeriesRef(i + 1), T: benchBaseT, V: s.v}
			table[chunks.HeadSeriesRef(i+1)] = s.lset
		}
		var enc Encoder
		recs := map[string][]byte{
			"ref":     enc.Samples(samples, nil),
			"symbols": func() []byte { r, _ := encodeSymbolsRecord(series, map[string]uint32{}, nil, nil); return r }(),
			"inline":  encodeInlineRecord(series, nil),
		}
		for _, ct := range compression.Types() {
			cenc := compression.NewSyncEncodeBuffer()
			cdec := compression.NewSyncDecodeBuffer()
			for _, name := range []string{"ref", "symbols", "inline"} {
				c, err := compression.Encode(ct, recs[name], cenc)
				require.NoError(b, err)
				c = append([]byte(nil), c...)
				b.Run(shape+"/"+name+"/"+ct, func(b *testing.B) {
					dec := NewDecoder(nil, nil)
					sb := labels.NewScratchBuilder(16)
					var (
						dbuf []byte
						out  []benchDecoded
						syms []string
						rs   []RefSample
					)
					b.ReportAllocs()
					for b.Loop() {
						raw, err := compression.Decode(ct, c, cdec)
						if err != nil {
							b.Fatal(err)
						}
						dbuf = append(dbuf[:0], raw...)
						switch name {
						case "ref":
							rs, err = dec.Samples(dbuf, rs[:0])
							if err != nil {
								b.Fatal(err)
							}
							out = out[:0]
							for _, s := range rs {
								out = append(out, benchDecoded{lset: table[s.Ref], t: s.T, v: s.V})
							}
						case "symbols":
							syms, out = decodeSymbolsRecord(dbuf, &sb, syms, out)
						case "inline":
							out = decodeInlineRecord(dbuf, &sb, out)
						}
					}
					if len(out) != len(series) {
						b.Fatalf("decoded %d series, want %d", len(out), len(series))
					}
					for i := range out {
						if !labels.Equal(out[i].lset, series[i].lset) {
							b.Fatalf("series %d: got %s, want %s", i, out[i].lset, series[i].lset)
						}
					}
					reportPerSample(b, len(series), len(c))
				})
			}
		}
	}
}

// BenchmarkSelfContainedScale measures the symbols format across commit sizes,
// and a large commit split into fixed-size records. Reports encode plus decode.
func BenchmarkSelfContainedScale(b *testing.B) {
	node := loadBenchShape(b, "node-exporter")
	ksm := loadBenchShape(b, "ksm")
	cases := []struct {
		name   string
		series []benchSeries
		chunk  int // Series per record, 0 for one record per commit.
	}{
		{"commit=100", node[:100], 0},
		{"commit=1.7k", node, 0},
		{"commit=4.2k", loadBenchShape(b, "grafana"), 0},
		{"commit=108k", ksm, 0},
		{"commit=108k/record=16k", ksm, 16000},
		{"commit=108k/record=4k", ksm, 4000},
		{"commit=108k/record=1k", ksm, 1000},
	}
	for _, c := range cases {
		for _, ct := range []compression.Type{compression.Snappy, compression.Zstd} {
			b.Run(c.name+"/"+ct, func(b *testing.B) {
				chunk := c.chunk
				if chunk == 0 {
					chunk = len(c.series)
				}
				cenc := compression.NewSyncEncodeBuffer()
				cdec := compression.NewSyncDecodeBuffer()
				syms := map[string]uint32{}
				sb := labels.NewScratchBuilder(16)
				var (
					refs  []uint32
					buf   []byte
					dbuf  []byte
					dsyms []string
					out   []benchDecoded
					size  int
				)
				b.ReportAllocs()
				for b.Loop() {
					size = 0
					for i := 0; i < len(c.series); i += chunk {
						part := c.series[i:min(i+chunk, len(c.series))]
						buf, refs = encodeSymbolsRecord(part, syms, refs, buf[:0])
						comp, err := compression.Encode(ct, buf, cenc)
						if err != nil {
							b.Fatal(err)
						}
						size += len(comp)
						raw, err := compression.Decode(ct, comp, cdec)
						if err != nil {
							b.Fatal(err)
						}
						dbuf = append(dbuf[:0], raw...)
						dsyms, out = decodeSymbolsRecord(dbuf, &sb, dsyms, out)
					}
				}
				reportPerSample(b, len(c.series), size)
			})
		}
	}
}

// Format 5: each series stores a bitmask of labels equal to the same position
// in the previous series, and only the differing labels inline. Series in one
// scrape mostly share job, instance and metric name with their neighbour, so
// this needs string compares instead of symbol hashing.
const benchPrevDeltaType Type = 202

type prevDeltaEncoder struct {
	prev, cur []labels.Label
}

func (p *prevDeltaEncoder) encode(series []benchSeries, b []byte) []byte {
	e := encoding.Encbuf{B: b[:0]}
	e.PutByte(byte(benchPrevDeltaType))
	e.PutBE64int64(benchBaseT)
	e.PutUvarint(len(series))
	p.prev = p.prev[:0]
	for _, s := range series {
		p.cur = p.cur[:0]
		s.lset.Range(func(l labels.Label) { p.cur = append(p.cur, l) })
		var mask uint64
		for i := 0; i < len(p.cur) && i < len(p.prev) && i < 64; i++ {
			if p.cur[i] == p.prev[i] {
				mask |= 1 << i
			}
		}
		e.PutUvarint(len(p.cur))
		e.PutUvarint64(mask)
		for i, l := range p.cur {
			if i < 64 && mask&(1<<i) != 0 {
				continue
			}
			e.PutUvarintStr(l.Name)
			e.PutUvarintStr(l.Value)
		}
		e.PutVarint64(0)
		e.PutBE64(math.Float64bits(s.v))
		p.prev, p.cur = p.cur, p.prev
	}
	return e.Get()
}

func decodePrevDeltaRecord(rec []byte, b *labels.ScratchBuilder, prev, cur []labels.Label, out []benchDecoded) ([]labels.Label, []labels.Label, []benchDecoded) {
	d := encoding.Decbuf{B: rec}
	d.Byte()
	baseT := d.Be64int64()
	out = out[:0]
	prev = prev[:0]
	for n := d.Uvarint(); n > 0; n-- {
		nl := d.Uvarint()
		mask := d.Uvarint64()
		cur = cur[:0]
		b.Reset()
		for i := range nl {
			var l labels.Label
			if i < 64 && mask&(1<<i) != 0 {
				l = prev[i]
			} else {
				l.Name = yoloString(d.UvarintBytes())
				l.Value = yoloString(d.UvarintBytes())
			}
			cur = append(cur, l)
			b.Add(l.Name, l.Value)
		}
		t := baseT + d.Varint64()
		v := math.Float64frombits(d.Be64())
		out = append(out, benchDecoded{lset: b.Labels(), t: t, v: v})
		prev, cur = cur, prev
	}
	return prev, cur, out
}

func BenchmarkSelfContainedPrevDelta(b *testing.B) {
	for _, shape := range []string{"node-exporter", "cadvisor", "grafana", "ksm"} {
		series := loadBenchShape(b, shape)
		for _, ct := range []compression.Type{compression.Snappy, compression.Zstd} {
			cenc := compression.NewSyncEncodeBuffer()
			cdec := compression.NewSyncDecodeBuffer()
			var p prevDeltaEncoder
			var buf []byte
			b.Run(shape+"/encode/"+ct, func(b *testing.B) {
				var size int
				b.ReportAllocs()
				for b.Loop() {
					buf = p.encode(series, buf[:0])
					c, err := compression.Encode(ct, buf, cenc)
					if err != nil {
						b.Fatal(err)
					}
					size = len(c)
				}
				reportPerSample(b, len(series), size)
			})
			rec := p.encode(series, nil)
			c, err := compression.Encode(ct, rec, cenc)
			require.NoError(b, err)
			c = append([]byte(nil), c...)
			b.Run(shape+"/decode/"+ct, func(b *testing.B) {
				sb := labels.NewScratchBuilder(16)
				var (
					dbuf      []byte
					prev, cur []labels.Label
					out       []benchDecoded
				)
				b.ReportAllocs()
				for b.Loop() {
					raw, err := compression.Decode(ct, c, cdec)
					if err != nil {
						b.Fatal(err)
					}
					dbuf = append(dbuf[:0], raw...)
					prev, cur, out = decodePrevDeltaRecord(dbuf, &sb, prev, cur, out)
				}
				for i := range out {
					if !labels.Equal(out[i].lset, series[i].lset) {
						b.Fatalf("series %d: got %s, want %s", i, out[i].lset, series[i].lset)
					}
				}
				reportPerSample(b, len(series), len(c))
			})
		}
	}
}

// BenchmarkSelfContainedPrevDeltaSplit measures a large commit split into
// fixed-size prev-delta records, each compressed on its own.
func BenchmarkSelfContainedPrevDeltaSplit(b *testing.B) {
	ksm := loadBenchShape(b, "ksm")
	for _, chunk := range []int{len(ksm), 16000, 4000, 1000} {
		for _, ct := range []compression.Type{compression.Snappy, compression.Zstd} {
			b.Run("record="+strconv.Itoa(chunk)+"/"+ct, func(b *testing.B) {
				cenc := compression.NewSyncEncodeBuffer()
				var p prevDeltaEncoder
				var buf []byte
				var size int
				for b.Loop() {
					size = 0
					for i := 0; i < len(ksm); i += chunk {
						buf = p.encode(ksm[i:min(i+chunk, len(ksm))], buf[:0])
						c, err := compression.Encode(ct, buf, cenc)
						if err != nil {
							b.Fatal(err)
						}
						size += len(c)
					}
				}
				reportPerSample(b, len(ksm), size)
			})
		}
	}
}
