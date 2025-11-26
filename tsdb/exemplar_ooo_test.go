// Copyright 2024 The Prometheus Authors
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

package tsdb

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

func TestAddExemplar_OutOfOrder_Basic(t *testing.T) {
	exs, err := NewCircularExemplarStorage(10, eMetrics)
	require.NoError(t, err)
	es := exs.(*CircularExemplarStorage)

	l := labels.FromStrings("service", "test")

	require.NoError(t, es.AddExemplar(l, exemplar.Exemplar{
		Labels: labels.FromStrings("trace_id", "trace1"),
		Value:  1.0,
		Ts:     10,
	}))

	err = es.AddExemplar(l, exemplar.Exemplar{
		Labels: labels.FromStrings("trace_id", "trace2"),
		Value:  2.0,
		Ts:     5,
	})
	require.NoError(t, err)

	require.NoError(t, es.AddExemplar(l, exemplar.Exemplar{
		Labels: labels.FromStrings("trace_id", "trace3"),
		Value:  3.0,
		Ts:     15,
	}))

	m := labels.MustNewMatcher(labels.MatchEqual, "service", "test")
	ret, err := es.Select(0, 20, []*labels.Matcher{m})
	require.NoError(t, err)
	require.Len(t, ret, 1)
	require.Len(t, ret[0].Exemplars, 3)

	require.Equal(t, int64(5), ret[0].Exemplars[0].Ts)
	require.Equal(t, int64(10), ret[0].Exemplars[1].Ts)
	require.Equal(t, int64(15), ret[0].Exemplars[2].Ts)
}

func TestAddExemplar_OutOfOrder_MultipleInsertions(t *testing.T) {
	exs, err := NewCircularExemplarStorage(20, eMetrics)
	require.NoError(t, err)
	es := exs.(*CircularExemplarStorage)

	l := labels.FromStrings("service", "test")
	timestamps := []int64{50, 20, 80, 10, 30, 60, 40, 70}

	for i, ts := range timestamps {
		e := exemplar.Exemplar{
			Labels: labels.FromStrings("trace_id", "trace"+strconv.Itoa(i)),
			Value:  float64(ts),
			Ts:     ts,
		}
		err := es.AddExemplar(l, e)
		require.NoError(t, err, "ts=%d", ts)
	}

	m := labels.MustNewMatcher(labels.MatchEqual, "service", "test")
	ret, err := es.Select(0, 100, []*labels.Matcher{m})
	require.NoError(t, err)
	require.Len(t, ret, 1)
	require.Len(t, ret[0].Exemplars, 8)

	expectedOrder := []int64{10, 20, 30, 40, 50, 60, 70, 80}
	for i, expected := range expectedOrder {
		require.Equal(t, expected, ret[0].Exemplars[i].Ts)
	}
}

func TestAddExemplar_OutOfOrder_OTLPBatches(t *testing.T) {
	exs, err := NewCircularExemplarStorage(50, eMetrics)
	require.NoError(t, err)
	es := exs.(*CircularExemplarStorage)

	l := labels.FromStrings("service", "otlp-service", "instance", "pod-1")

	batch1 := []int64{100, 101, 102, 103, 104}
	for _, ts := range batch1 {
		require.NoError(t, es.AddExemplar(l, exemplar.Exemplar{
			Labels: labels.FromStrings("trace_id", "batch1_"+strconv.FormatInt(ts, 10)),
			Value:  float64(ts),
			Ts:     ts,
		}))
	}

	batch2 := []int64{95, 96, 97, 98, 99}
	for _, ts := range batch2 {
		err := es.AddExemplar(l, exemplar.Exemplar{
			Labels: labels.FromStrings("trace_id", "batch2_"+strconv.FormatInt(ts, 10)),
			Value:  float64(ts),
			Ts:     ts,
		})
		require.NoError(t, err, "delayed batch ts=%d", ts)
	}

	batch3 := []int64{105, 106, 107, 108, 109}
	for _, ts := range batch3 {
		require.NoError(t, es.AddExemplar(l, exemplar.Exemplar{
			Labels: labels.FromStrings("trace_id", "batch3_"+strconv.FormatInt(ts, 10)),
			Value:  float64(ts),
			Ts:     ts,
		}))
	}

	m := labels.MustNewMatcher(labels.MatchEqual, "service", "otlp-service")
	ret, err := es.Select(0, 200, []*labels.Matcher{m})
	require.NoError(t, err)
	require.Len(t, ret, 1)
	require.Len(t, ret[0].Exemplars, 15)

	for i := 0; i < len(ret[0].Exemplars); i++ {
		expectedTs := int64(95 + i)
		require.Equal(t, expectedTs, ret[0].Exemplars[i].Ts)
	}
}

func TestAddExemplar_OutOfOrder_BufferWrap(t *testing.T) {
	exs, err := NewCircularExemplarStorage(5, eMetrics)
	require.NoError(t, err)
	es := exs.(*CircularExemplarStorage)

	l := labels.FromStrings("service", "test")

	for i := 1; i <= 5; i++ {
		require.NoError(t, es.AddExemplar(l, exemplar.Exemplar{
			Labels: labels.FromStrings("trace_id", "trace"+strconv.Itoa(i)),
			Value:  float64(i * 10),
			Ts:     int64(i * 10),
		}))
	}

	require.NoError(t, es.AddExemplar(l, exemplar.Exemplar{
		Labels: labels.FromStrings("trace_id", "trace6"),
		Value:  60.0,
		Ts:     60,
	}))

	err = es.AddExemplar(l, exemplar.Exemplar{
		Labels: labels.FromStrings("trace_id", "trace_ooo"),
		Value:  35.0,
		Ts:     35,
	})
	require.NoError(t, err)

	m := labels.MustNewMatcher(labels.MatchEqual, "service", "test")
	ret, err := es.Select(0, 100, []*labels.Matcher{m})
	require.NoError(t, err)
	require.Len(t, ret, 1)
	require.Len(t, ret[0].Exemplars, 5)

	for i := 1; i < len(ret[0].Exemplars); i++ {
		require.Less(t, ret[0].Exemplars[i-1].Ts, ret[0].Exemplars[i].Ts)
	}

	for _, ex := range ret[0].Exemplars {
		require.NotEqual(t, int64(20), ex.Ts)
	}
}

func TestValidateExemplar_OutOfOrder_ShouldAccept(t *testing.T) {
	exs, err := NewCircularExemplarStorage(10, eMetrics)
	require.NoError(t, err)
	es := exs.(*CircularExemplarStorage)

	l := labels.FromStrings("service", "test")

	require.NoError(t, es.AddExemplar(l, exemplar.Exemplar{
		Labels: labels.FromStrings("trace_id", "trace1"),
		Value:  1.0,
		Ts:     10,
	}))

	err = es.ValidateExemplar(l, exemplar.Exemplar{
		Labels: labels.FromStrings("trace_id", "trace2"),
		Value:  2.0,
		Ts:     5,
	})
	require.NoError(t, err)
	require.NotEqual(t, storage.ErrOutOfOrderExemplar, err)
}
