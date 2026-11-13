// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	rckind "github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/kind"
)

func TestWriteTableMedianAndWorst(t *testing.T) {
	run := func(unavailable int64, errRate float64) rckind.MeasureReport {
		return rckind.MeasureReport{Scenario: "PrimaryDelete", Primaries: 3, TargetPrimaries: 3,
			UnavailableSeconds: unavailable, ErrorRate: errRate}
	}
	var out strings.Builder
	writeTable(&out, []rckind.MeasureReport{run(4, 0.01), run(2, 0.02), run(9, 0.05)})
	assert.Contains(t, out.String(), "| PrimaryDelete | 3 → 3 | 3 | 4 / 9 | 2.00% / 5.00% |")
}
