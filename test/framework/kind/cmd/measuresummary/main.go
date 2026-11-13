// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

// Measuresummary prints a markdown table of measure-lane reports: median and
// worst run per scenario and primary count.
//
//	go run ./test/framework/kind/cmd/measuresummary .local/measure
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"

	rckind "github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/kind"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: measuresummary REPORT_DIR")
	}
	reports, err := readReports(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	writeTable(os.Stdout, reports)
}

func readReports(dir string) ([]rckind.MeasureReport, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return nil, err
	}
	reports := make([]rckind.MeasureReport, 0, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var r rckind.MeasureReport
		if err := json.Unmarshal(data, &r); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		reports = append(reports, r)
	}
	return reports, nil
}

type groupKey struct {
	scenario  string
	primaries int32
}

func writeTable(w io.Writer, reports []rckind.MeasureReport) {
	groups := map[groupKey][]rckind.MeasureReport{}
	var keys []groupKey
	for _, r := range reports {
		k := groupKey{r.Scenario, r.Primaries}
		if _, ok := groups[k]; !ok {
			keys = append(keys, k)
		}
		groups[k] = append(groups[k], r)
	}
	slices.SortFunc(keys, func(a, b groupKey) int {
		if a.scenario != b.scenario {
			if a.scenario < b.scenario {
				return -1
			}
			return 1
		}
		return int(a.primaries - b.primaries)
	})

	fmt.Fprintln(w, "| Scenario | Primaries | Runs | Unavailable s (median / worst) | Error rate (median / worst) | p99 ms baseline → fault (median) | Max ms (worst) | Acked writes lost |")
	fmt.Fprintln(w, "| --- | ---: | ---: | --- | --- | --- | ---: | ---: |")
	for _, k := range keys {
		g := groups[k]
		unavailable := pick(g, func(r rckind.MeasureReport) float64 { return float64(r.UnavailableSeconds) })
		errRate := pick(g, func(r rckind.MeasureReport) float64 { return 100 * r.ErrorRate })
		baseP99 := pick(g, func(r rckind.MeasureReport) float64 { return r.BaselineP99Ms })
		faultP99 := pick(g, func(r rckind.MeasureReport) float64 { return r.FaultP99Ms })
		maxMs := pick(g, func(r rckind.MeasureReport) float64 { return r.FaultMaxMs })
		lost := pick(g, func(r rckind.MeasureReport) float64 { return float64(r.AckedLost) })
		fmt.Fprintf(w, "| %s | %d → %d | %d | %.0f / %.0f | %.2f%% / %.2f%% | %.1f → %.1f | %.0f | %.0f |\n",
			k.scenario, k.primaries, g[0].TargetPrimaries, len(g),
			unavailable.median, unavailable.worst, errRate.median, errRate.worst,
			baseP99.median, faultP99.median, maxMs.worst, lost.worst)
	}
}

type stat struct{ median, worst float64 }

// pick returns the median and maximum of one field across runs.
func pick(runs []rckind.MeasureReport, field func(rckind.MeasureReport) float64) stat {
	values := make([]float64, len(runs))
	for i, r := range runs {
		values[i] = field(r)
	}
	slices.Sort(values)
	return stat{median: values[len(values)/2], worst: values[len(values)-1]}
}
