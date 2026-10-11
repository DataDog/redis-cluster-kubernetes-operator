package property

import (
	"fmt"

	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"pgregory.net/rapid"
)

// STSPodName generates pod names matching a StatefulSet's `<prefix>-<index>` shape.
// Used by §8.5 (GetPodIndex vs parseIndex cross-function agreement).
func STSPodName() *rapid.Generator[string] {
	return rapid.Custom(func(t *rapid.T) string {
		prefix := rapid.StringMatching(`[a-z][a-z0-9-]{2,30}`).Draw(t, "prefix")
		idx := rapid.IntRange(0, 999).Draw(t, "idx")
		return fmt.Sprintf("%s-%d", prefix, idx)
	})
}

// SlotRange generates a [first,last] hash-slot range bounded by
// Redis Cluster's [0, constants.TotalSlots).
func SlotRange() *rapid.Generator[[2]int] {
	maxSlot := constants.TotalSlots - 1
	return rapid.Custom(func(t *rapid.T) [2]int {
		first := rapid.IntRange(0, maxSlot).Draw(t, "first")
		last := rapid.IntRange(first, maxSlot).Draw(t, "last")
		return [2]int{first, last}
	})
}
