package loadgen

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRueidisGeneratorStats(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/stats" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]int64{
			"qps": 1234, "workers": 5,
			"sets_ok": 100, "sets_fail": 1,
			"gets_ok": 99, "gets_fail": 0,
			"mismatch": 0,
		})
	}))
	t.Cleanup(srv.Close)

	g := &RueidisGenerator{baseURL: srv.URL, http: srv.Client()}

	s, err := g.Stats()
	require.NoError(t, err)
	assert.EqualValues(t, 1234, s.QPS)
	assert.EqualValues(t, 100, s.SetsOK)
	assert.EqualValues(t, 0, s.Mismatch)

}

func TestRueidisStatsDeltaSince(t *testing.T) {
	before := RueidisStats{SetsOK: 10, SetsFail: 1, GetsOK: 20, GetsFail: 2, Mismatch: 3}
	after := RueidisStats{
		QPS: 200, Workers: 4,
		SetsOK: 15, SetsFail: 2, GetsOK: 27, GetsFail: 4, Mismatch: 3,
		LastErr: "GET timeout",
	}

	delta, err := after.DeltaSince(before)
	require.NoError(t, err)
	assert.Equal(t, RueidisStats{
		QPS: 200, Workers: 4,
		SetsOK: 5, SetsFail: 1, GetsOK: 7, GetsFail: 2,
		LastErr: "GET timeout",
	}, delta)
	assert.EqualValues(t, 15, delta.CompletedOps())
	assert.InDelta(t, float64(12)/15, delta.SuccessRate(), 1e-9)
}

func TestRueidisStatsDeltaSinceRejectsReset(t *testing.T) {
	_, err := (RueidisStats{SetsOK: 9}).DeltaSince(RueidisStats{SetsOK: 10})
	require.ErrorContains(t, err, "sets_ok reset between snapshots")
}
