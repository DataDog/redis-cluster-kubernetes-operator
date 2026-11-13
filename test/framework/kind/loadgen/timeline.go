// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package loadgen

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// verifyHTTPTimeout bounds GET /verify, which reads back every acknowledged key.
const verifyHTTPTimeout = 10 * time.Minute

// TimelineRow mirrors one scheduled second of the pod's open-loop timeline.
type TimelineRow struct {
	Unix   int64             `json:"t"`
	OK     int64             `json:"ok"`
	Fail   int64             `json:"fail"`
	P50Ms  float64           `json:"p50_ms"`
	P99Ms  float64           `json:"p99_ms"`
	MaxMs  float64           `json:"max_ms"`
	Errors map[string]string `json:"errors,omitempty"`
}

// VerifyResult mirrors the pod's read-back of acknowledged SETs.
type VerifyResult struct {
	Checked  int      `json:"checked"`
	Missing  int      `json:"missing"`
	Mismatch int      `json:"mismatch"`
	Errors   int      `json:"errors"`
	Samples  []string `json:"samples,omitempty"`
}

// Lost is the number of acknowledged writes that did not read back intact.
func (v VerifyResult) Lost() int { return v.Missing + v.Mismatch }

// Timeline fetches the open-loop per-second timeline.
func (g *RueidisGenerator) Timeline() ([]TimelineRow, error) {
	var rows []TimelineRow
	return rows, g.getJSON(g.http, "/timeline", &rows)
}

// Pause stops issuing load and waits for in-flight requests; the control
// surface stays up for Verify.
func (g *RueidisGenerator) Pause() error {
	return g.getJSON(g.http, "/pause", nil)
}

// Verify reads back every acknowledged SET. Call Pause first.
func (g *RueidisGenerator) Verify() (VerifyResult, error) {
	var res VerifyResult
	return res, g.getJSON(&http.Client{Timeout: verifyHTTPTimeout}, "/verify", &res)
}

func (g *RueidisGenerator) getJSON(c *http.Client, path string, out any) error {
	resp, err := c.Get(g.baseURL + path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s: %s", path, resp.Status, readErrorBody(resp.Body))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
