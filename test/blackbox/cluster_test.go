package blackbox

import "testing"

func TestClusterParserRejectsDuplicateAndInvalidSlots(t *testing.T) {
	t.Parallel()
	nodes := parseClusterNodes("a 10.0.0.1:6379@16379 master - 0 0 1 connected 0-10\n" +
		"b 10.0.0.2:6379@16379 master - 0 0 2 connected 10-20")
	if _, err := slotCoverage(nodes); err == nil {
		t.Fatal("duplicate slot ownership was accepted")
	}
	if _, _, err := parseSlotRange("16384"); err == nil {
		t.Fatal("out-of-range slot was accepted")
	}
}
