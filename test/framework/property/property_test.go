package property_test

import (
	"strings"
	"testing"

	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/property"
	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

func TestSTSPodName_AlwaysHasIndexSuffix(t *testing.T) {
	property.Check(t, func(rt *rapid.T) {
		name := property.STSPodName().Draw(rt, "name")
		idx := strings.LastIndex(name, "-")
		if idx < 0 || idx == len(name)-1 {
			rt.Fatalf("generated name %q has no `-<index>` suffix", name)
		}
	})
}

func TestSlotRange_FirstNotGreaterThanLast(t *testing.T) {
	property.Check(t, func(rt *rapid.T) {
		r := property.SlotRange().Draw(rt, "range")
		assert.LessOrEqual(t, r[0], r[1])
		assert.GreaterOrEqual(t, r[0], 0)
		assert.LessOrEqual(t, r[1], constants.TotalSlots-1)
	})
}
