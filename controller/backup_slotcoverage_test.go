package controller

import "testing"

func TestParseSlotRange(t *testing.T) {
	cases := []struct {
		in     string
		lo, hi int
		ok     bool
	}{
		{"0-5460", 0, 5460, true},
		{"100-100", 100, 100, true},
		{" 0-16383 ", 0, 16383, true},
		{"5461", 0, 0, false},    // no dash
		{"abc-def", 0, 0, false}, // non-numeric
		{"10-5", 0, 0, false},    // first > last
		{"", 0, 0, false},
		{"0-", 0, 0, false}, // empty hi
		{"-5", 0, 0, false}, // empty lo
	}
	for _, c := range cases {
		lo, hi, ok := parseSlotRange(c.in)
		if ok != c.ok || (ok && (lo != c.lo || hi != c.hi)) {
			t.Errorf("parseSlotRange(%q) = (%d,%d,%v), want (%d,%d,%v)", c.in, lo, hi, ok, c.lo, c.hi, c.ok)
		}
	}
}

// TestSlotCoverageIsComplete guards fix #6's trust gate: the manifest's SlotCoverage is
// only used to re-own slots when it covers all 16384 slots exactly once. Anything
// suspect (gap, overlap, out-of-range, malformed) must fall back to a keys-only restore.
func TestSlotCoverageIsComplete(t *testing.T) {
	cases := []struct {
		name     string
		coverage map[int]string
		want     bool
	}{
		{"full single", map[int]string{0: "0-16383"}, true},
		{"full split", map[int]string{0: "0-5460", 1: "5461-10922", 2: "10923-16383"}, true},
		{"full multi-range value", map[int]string{0: "0-5460,5461-10922", 1: "10923-16383"}, true},
		{"gap", map[int]string{0: "0-5460", 1: "5462-16383"}, false}, // 5461 missing
		{"empty map", map[int]string{}, false},
		{"overlap but full", map[int]string{0: "0-10000", 1: "9000-16383"}, false},
		{"overlap partial", map[int]string{0: "0-10000", 1: "9000-15000"}, false},
		{"out of range", map[int]string{0: "0-20000"}, false},
		{"malformed range", map[int]string{0: "0-16383", 1: "bad"}, false},
		{"partial coverage", map[int]string{0: "0-100"}, false},
	}
	for _, c := range cases {
		if got := slotCoverageIsComplete(c.coverage); got != c.want {
			t.Errorf("%s: slotCoverageIsComplete = %v, want %v", c.name, got, c.want)
		}
	}
}
