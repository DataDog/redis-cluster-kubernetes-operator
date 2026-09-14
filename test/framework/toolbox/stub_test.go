package toolbox_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/toolbox"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func exec(t *testing.T, s *toolbox.Stub, cmd ...string) (int, string, string, error) {
	t.Helper()
	return s.Exec(context.Background(), "toolbox-pod", "toolbox", cmd, logr.Discard())
}

func bashLc(redisCli string) []string {
	return []string{"bash", "-lc", "REDIS_AUTH=...; redis-cli $REDIS_AUTH -h 10.0.0.1 -p 6379 " + redisCli}
}

func TestStub_OnCommand_MatcherDispatch(t *testing.T) {
	s := toolbox.New().OnCommand("first-arg-is-foo",
		func(_, _ string, cmd []string) bool { return len(cmd) > 0 && cmd[0] == "foo" },
		toolbox.Stdout("matched"),
	)

	rc, out, _, err := exec(t, s, "foo", "bar")
	require.NoError(t, err)
	assert.Equal(t, 0, rc)
	assert.Equal(t, "matched", out)
}

func TestStub_OnRedisCommand_MatchesBashLcInvocation(t *testing.T) {
	canned := "abc123 10.0.0.1:6379@16379 myself,master - 0 0 0 connected 0-16383\n"
	s := toolbox.New().OnRedisCommand("CLUSTER NODES", toolbox.Stdout(canned))

	rc, out, _, err := s.Exec(context.Background(), "toolbox-pod", "toolbox", bashLc("CLUSTER NODES"), logr.Discard())
	require.NoError(t, err)
	assert.Equal(t, 0, rc)
	assert.Equal(t, canned, out)
}

func TestStub_OnRedisCommand_CaseInsensitive(t *testing.T) {
	s := toolbox.New().OnRedisCommand("cluster nodes", toolbox.Stdout("ok"))

	_, out, _, err := s.Exec(context.Background(), "p", "c", bashLc("CLUSTER NODES"), logr.Discard())
	require.NoError(t, err)
	assert.Equal(t, "ok", out)
}

func TestStub_OnS3Command(t *testing.T) {
	cases := []struct {
		name string
		cmd  string
	}{
		{"bare", "aws s3 cp s3://bucket/key /tmp/file"},
		{"wrapped", "$(command -v aws 2>/dev/null || echo /usr/local/aws-cli/v2/current/bin/aws) s3 cp /tmp/file s3://bucket/key"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := toolbox.New().OnS3Command("cp", toolbox.OK())
			rc, _, _, err := s.Exec(context.Background(), "tb", "tb", []string{"bash", "-lc", tc.cmd}, logr.Discard())
			require.NoError(t, err)
			assert.Equal(t, 0, rc)
		})
	}
}

func TestStub_NoMatch_DefaultReturns127(t *testing.T) {
	s := toolbox.New()

	rc, _, stderr, err := exec(t, s, "unknown", "command")
	require.NoError(t, err, "non-strict stub should not error on unmatched")
	assert.Equal(t, 127, rc)
	assert.Contains(t, stderr, "no rule matched")
}

func TestStub_Strict_NoMatch_ReturnsError(t *testing.T) {
	s := toolbox.New().Strict()

	_, _, _, err := exec(t, s, "unknown")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no rule matched")
}

func TestStub_FailNext_FiresOnceThenClears(t *testing.T) {
	transient := errors.New("kubectl exec EOF")
	s := toolbox.New().
		OnCommand("any", func(_, _ string, _ []string) bool { return true }, toolbox.OK()).
		FailNext(transient)

	// First call: should return the injected error, not match the rule.
	_, _, _, err := exec(t, s, "anything")
	require.ErrorIs(t, err, transient)

	// Second call: FailNext cleared, rule takes over.
	rc, _, _, err := exec(t, s, "anything")
	require.NoError(t, err)
	assert.Equal(t, 0, rc)
}

func TestStub_DelayNext_Blocks(t *testing.T) {
	s := toolbox.New().
		OnCommand("any", func(_, _ string, _ []string) bool { return true }, toolbox.OK()).
		DelayNext(75 * time.Millisecond)

	start := time.Now()
	_, _, _, err := exec(t, s, "anything")
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.GreaterOrEqual(t, elapsed, 70*time.Millisecond, "DelayNext should have made Exec block ~75ms")
}

func TestStub_DelayNext_RespectsContextCancel(t *testing.T) {
	s := toolbox.New().
		OnCommand("any", func(_, _ string, _ []string) bool { return true }, toolbox.OK()).
		DelayNext(2 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, _, _, err := s.Exec(ctx, "tb", "tb", []string{"anything"}, logr.Discard())
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, elapsed, 500*time.Millisecond, "Exec should have returned promptly on ctx.Done()")
}

func TestStub_Calls_RecordsInvocationsInOrder(t *testing.T) {
	s := toolbox.New().
		OnCommand("any", func(_, _ string, _ []string) bool { return true }, toolbox.OK())

	_, _, _, _ = exec(t, s, "first")
	_, _, _, _ = exec(t, s, "second", "arg")
	_, _, _, _ = exec(t, s, "third")

	calls := s.Calls()
	require.Len(t, calls, 3)
	assert.Equal(t, []string{"first"}, calls[0].Command)
	assert.Equal(t, []string{"second", "arg"}, calls[1].Command)
	assert.Equal(t, []string{"third"}, calls[2].Command)
}

func TestStub_Calls_DefensiveCopy(t *testing.T) {
	s := toolbox.New().
		OnCommand("any", func(_, _ string, _ []string) bool { return true }, toolbox.OK())

	cmd := []string{"original"}
	_, _, _, _ = s.Exec(context.Background(), "tb", "tb", cmd, logr.Discard())
	cmd[0] = "mutated-after-call" // simulate caller reusing slice

	calls := s.Calls()
	require.Len(t, calls, 1)
	assert.Equal(t, "original", calls[0].Command[0], "stub must defensively copy the command slice on input")
}

func TestStub_Calls_SnapshotIsImmutable(t *testing.T) {
	s := toolbox.New().
		OnCommand("any", func(_, _ string, _ []string) bool { return true }, toolbox.OK())
	_, _, _, _ = s.Exec(context.Background(), "tb", "tb", []string{"original"}, logr.Discard())

	snap := s.Calls()
	require.Len(t, snap, 1)
	snap[0].Command[0] = "mutated-by-test"

	again := s.Calls()
	require.Len(t, again, 1)
	assert.Equal(t, "original", again[0].Command[0], "Calls() must deep-copy Command so the snapshot can't write back")
}

func TestStub_Reset_ClearsRulesAndCalls_PreservesStrict(t *testing.T) {
	s := toolbox.New().Strict().
		OnCommand("any", func(_, _ string, _ []string) bool { return true }, toolbox.OK())
	_, _, _, _ = exec(t, s, "warmup")
	require.Len(t, s.Calls(), 1)

	s.Reset()

	assert.Empty(t, s.Calls(), "Reset should clear recorded calls")
	_, _, _, err := exec(t, s, "anything")
	require.Error(t, err, "rules should be cleared but strict-mode preserved")
	assert.Contains(t, err.Error(), "no rule matched")
}

func TestStub_MostRecentRuleWins(t *testing.T) {
	s := toolbox.New().
		OnCommand("first", func(_, _ string, _ []string) bool { return true }, toolbox.Stdout("first")).
		OnCommand("second", func(_, _ string, _ []string) bool { return true }, toolbox.Stdout("second"))

	_, out, _, err := exec(t, s, "x")
	require.NoError(t, err)
	assert.Equal(t, "second", out, "more recent matching rule should win")
}

func TestStub_ConcurrentCalls_RaceSafe(t *testing.T) {
	// Run with `go test -race` to catch data races. Without -race this is
	// still a useful smoke check that nothing deadlocks.
	s := toolbox.New().
		OnCommand("any", func(_, _ string, _ []string) bool { return true }, toolbox.OK())

	const goroutines = 32
	const callsPerGoroutine = 50
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < callsPerGoroutine; i++ {
				_, _, _, _ = exec(t, s, "concurrent")
			}
		}()
	}
	wg.Wait()

	assert.Len(t, s.Calls(), goroutines*callsPerGoroutine)
}
