package bluegreen

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFailoverModeForAttempts(t *testing.T) {
	tests := []struct {
		attempts         int32
		expectedMode     string
		expectedForce    bool
		expectedTakeover bool
	}{
		{0, "soft", false, false},
		{1, "soft", false, false},
		{2, "FORCE", true, false},
		{3, "FORCE", true, false},
		{4, "TAKEOVER", false, true},
		{5, "TAKEOVER", false, true},
		{6, "TAKEOVER", false, true},
		{7, "TAKEOVER", false, true},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("attempt_%d", tt.attempts), func(t *testing.T) {
			force, takeover, mode := failoverModeForAttempts(tt.attempts)
			assert.Equal(t, tt.expectedMode, mode)
			assert.Equal(t, tt.expectedForce, force)
			assert.Equal(t, tt.expectedTakeover, takeover)
			assert.False(t, force && takeover, "Redis failover modes must be mutually exclusive")
		})
	}
}
