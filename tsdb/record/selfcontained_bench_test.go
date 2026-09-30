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
