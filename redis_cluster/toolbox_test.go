// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package redis_cluster

import (
	"context"
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics"
	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/toolbox"
	"github.com/DataDog/redis-cluster-kubernetes-operator/timeouts"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ PodExecutor = (*toolbox.Stub)(nil)

const (
	testPrimaryID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testReplicaID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

type unknownCredentialProvider struct{}

func (unknownCredentialProvider) credentialProvider() {}

func newToolboxWithStub(stub *toolbox.Stub) *Toolbox {
	return &Toolbox{
		kubeExec:           stub,
		toolboxPod:         &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "toolbox-pod"}},
		ctx:                context.Background(),
		RedisPort:          6379,
		Log:                logr.Discard(),
		Metrics:            metrics.CreateNoop(),
		CredentialProvider: NoAuthentication{},
	}
}

// Covers IPv4 and IPv6 at the argv-only execution boundary.
func TestToolbox_RedisArgvBoundary(t *testing.T) {
	cases := []struct {
		name string
		ip   string
	}{
		{name: "ipv4", ip: "10.0.0.1"},
		{name: "ipv6", ip: "2001:db8::1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := toolbox.New().OnCommand("any", func(_, _ string, _ []string) bool { return true }, toolbox.OK())
			tb := newToolboxWithStub(stub)

			_, _, _, err := tb.ClusterNodes(context.Background(), tc.ip)
			require.NoError(t, err)

			calls := stub.Calls()
			require.Len(t, calls, 1)
			assert.Equal(t, []string{"redis-cli", "-e", "-h", tc.ip, "-p", "6379", "CLUSTER", "NODES"}, calls[0].Command)
			assert.NotContains(t, calls[0].Command, "bash")
		})
	}
}

func TestToolboxAppendOnlyEnabled(t *testing.T) {
	for _, test := range []struct {
		name    string
		output  string
		enabled bool
	}{
		{name: "enabled", output: "appendonly\nyes\n", enabled: true},
		{name: "disabled", output: "appendonly\nno\n", enabled: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			stub := toolbox.New().OnCommand(
				"appendonly",
				func(_, _ string, command []string) bool {
					return strings.Contains(strings.Join(command, " "), "CONFIG GET appendonly")
				},
				toolbox.Stdout(test.output),
			)
			tb := newToolboxWithStub(stub)
			enabled, err := tb.AppendOnlyEnabled(context.Background(), &ClusterNode{IP: "10.0.0.1"})
			require.NoError(t, err)
			assert.Equal(t, test.enabled, enabled)
		})
	}
}

func TestToolbox_MountedSecretAuthNeverPlacesCredentialsInExecRequest(t *testing.T) {
	stub := toolbox.New().OnCommand("any", func(_, _ string, _ []string) bool { return true }, toolbox.OK())
	tb := newToolboxWithStub(stub)
	tb.CredentialProvider = MountedSecretAuthentication{}

	_, _, _, err := tb.ClusterInfo(context.Background(), "10.0.0.1")
	require.NoError(t, err)

	calls := stub.Calls()
	require.Len(t, calls, 1)
	assert.Equal(t, []string{mountedSecretRedisCLI, "-e", "-h", "10.0.0.1", "-p", "6379", "CLUSTER", "INFO"}, calls[0].Command)
	joined := strings.Join(calls[0].Command, " ")
	assert.NotContains(t, joined, "username")
	assert.NotContains(t, joined, "password")
	assert.NotContains(t, joined, "REDISCLI_AUTH")
}

func TestToolbox_MountedSecretAuthDoesNotLeakCredentialOnExecError(t *testing.T) {
	const password = "s3cr3t;$(touch /tmp/pwned)"
	stub := toolbox.New().FailNext(errors.New("transport failed"))
	tb := newToolboxWithStub(stub)
	tb.CredentialProvider = MountedSecretAuthentication{}

	_, _, _, err := tb.ClusterInfo(context.Background(), "10.0.0.1")
	require.EqualError(t, err, "transport failed")
	require.Len(t, stub.Calls(), 1)
	assert.NotContains(t, strings.Join(stub.Calls()[0].Command, " "), password)
	assert.NotContains(t, err.Error(), password)
}

func TestCredentialProviderAllowlistFailsClosed(t *testing.T) {
	providers := []CredentialProvider{
		nil,
		(*NoAuthentication)(nil),
		(*MountedSecretAuthentication)(nil),
		unknownCredentialProvider{},
	}
	for _, provider := range providers {
		_, err := redisCLIExecutable(provider)
		require.Error(t, err, "provider %T must be rejected", provider)
	}
	for provider, want := range map[CredentialProvider]string{
		NoAuthentication{}:            "redis-cli",
		MountedSecretAuthentication{}: mountedSecretRedisCLI,
	} {
		got, err := redisCLIExecutable(provider)
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}
}

func TestMountedSecretHelperAndVolumeUseFixedPathsAndMode(t *testing.T) {
	helper, err := os.ReadFile("../files/redis-operator-redis-cli")
	require.NoError(t, err)
	deployment, err := os.ReadFile("../templates/deployment-toolbox.yaml")
	require.NoError(t, err)
	configMap, err := os.ReadFile("../templates/configmap-toolbox.yaml")
	require.NoError(t, err)

	for _, want := range []string{
		`auth_dir=/var/run/redis-operator/auth`,
		`username=$(cat "$auth_dir/username")`,
		`REDISCLI_AUTH=$(cat "$auth_dir/password")`,
		`exec redis-cli --user "$username" --no-auth-warning "$@"`,
	} {
		assert.Contains(t, string(helper), want)
	}
	for _, want := range []string{
		"defaultMode: 0555",
		"defaultMode: 0440",
		"fsGroup: 999",
		"mountPath: /usr/local/bin/redis-operator-redis-cli",
		"mountPath: /var/run/redis-operator/auth",
		"path: username",
		"path: password",
	} {
		assert.Contains(t, string(deployment), want)
	}
	assert.Contains(t, string(configMap), `name: {{ include "redis-cluster-kubernetes-operator.redisAuthHelperName" . }}`)
	assert.Equal(t, 1, strings.Count(string(configMap), "immutable: true"))
	assert.Contains(t, string(deployment), `name: {{ include "redis-cluster-kubernetes-operator.redisAuthHelperName" . }}`)
	assert.Contains(t, string(deployment), `command: ["sleep"]`)
	_, err = os.Stat("../files/.bashrc-toolbox")
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestMountedSecretHelperDoesNotEvaluateCredentialMetacharacters(t *testing.T) {
	helper, err := os.ReadFile("../files/redis-operator-redis-cli")
	require.NoError(t, err)

	tempDir := t.TempDir()
	authDir := filepath.Join(tempDir, "auth")
	require.NoError(t, os.Mkdir(authDir, 0o700))
	marker := filepath.Join(tempDir, "credential-was-evaluated")
	username := `operator;$(touch ` + marker + `)`
	password := `p@ss word;&$(touch ` + marker + `)`
	require.NoError(t, os.WriteFile(filepath.Join(authDir, "username"), []byte(username), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(authDir, "password"), []byte(password), 0o600))

	binDir := filepath.Join(tempDir, "bin")
	require.NoError(t, os.Mkdir(binDir, 0o700))
	fakeRedisCLI := filepath.Join(binDir, "redis-cli")
	require.NoError(t, os.WriteFile(fakeRedisCLI, []byte("#!/bin/sh\nset -eu\nprintf '%s' \"$REDISCLI_AUTH\" > \"$CAPTURED_PASSWORD\"\nprintf '%s\\n' \"$@\" > \"$CAPTURED_ARGS\"\n"), 0o700))

	quotedAuthDir := "'" + strings.ReplaceAll(authDir, "'", `'"'"'`) + "'"
	testHelper := strings.Replace(string(helper), "auth_dir=/var/run/redis-operator/auth", "auth_dir="+quotedAuthDir, 1)
	require.NotEqual(t, string(helper), testHelper)
	helperPath := filepath.Join(tempDir, "redis-operator-redis-cli")
	require.NoError(t, os.WriteFile(helperPath, []byte(testHelper), 0o700))

	capturedPassword := filepath.Join(tempDir, "password")
	capturedArgs := filepath.Join(tempDir, "args")
	cmd := exec.Command(helperPath, "-h", "10.0.0.1", "PING")
	cmd.Env = append(os.Environ(),
		"PATH="+binDir+":"+os.Getenv("PATH"),
		"CAPTURED_PASSWORD="+capturedPassword,
		"CAPTURED_ARGS="+capturedArgs,
	)
	require.NoError(t, cmd.Run())

	gotPassword, err := os.ReadFile(capturedPassword)
	require.NoError(t, err)
	assert.Equal(t, password, string(gotPassword))
	gotArgs, err := os.ReadFile(capturedArgs)
	require.NoError(t, err)
	assert.Equal(t, "--user\n"+username+"\n--no-auth-warning\n-h\n10.0.0.1\nPING\n", string(gotArgs))
	_, err = os.Stat(marker)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestToolbox_RejectsShellMetacharacters(t *testing.T) {
	stub := toolbox.New()
	tb := newToolboxWithStub(stub)

	_, _, _, err := tb.ClusterNodes(context.Background(), "10.0.0.1; touch /tmp/pwned")
	require.Error(t, err)
	assert.Empty(t, stub.Calls())
	require.Error(t, tb.ReplicateOnIP("10.0.0.1", "abc;shutdown"))
	require.Error(t, tb.AddSlot(&ClusterNode{IP: "10.0.0.1"}, 16384))
	require.Error(t, tb.AddSlots(&ClusterNode{IP: "10.0.0.1"}, 12, 3))
	assert.Empty(t, stub.Calls())
	cluster := &Cluster{
		RedisCluster: &cachev1.RedisCluster{},
		RedisPort:    6379,
		Nodes:        map[NodeRole][]*ClusterNode{PrimaryNode: {{IP: "10.0.0.1"}}},
	}
	require.Error(t, tb.RebalanceWeighted(cluster, []NodeWeight{{NodeID: "abc;shutdown", Weight: 1}}, false))
	assert.Empty(t, stub.Calls())
}

func TestToolbox_NodeIDsRequireExactlyFortyHexCharacters(t *testing.T) {
	for _, invalid := range []string{"", "abc", strings.Repeat("a", 39), strings.Repeat("a", 41), strings.Repeat("g", 40), strings.Repeat("a", 39) + ";"} {
		require.Error(t, validateNodeID(invalid), invalid)
	}
	require.NoError(t, validateNodeID(testPrimaryID))
	require.NoError(t, validateNodeID(strings.ToUpper(testPrimaryID)))
}

func TestToolbox_PublicNodeOperationsRejectNilWithoutExec(t *testing.T) {
	stub := toolbox.New()
	tb := newToolboxWithStub(stub)
	require.Error(t, tb.ResetNode(nil, false))
	require.Error(t, tb.AddReplica(nil, &ClusterNode{IP: "10.0.0.2"}, 6379, testPrimaryID))
	require.Error(t, tb.AddPrimary(&ClusterNode{IP: "10.0.0.1"}, nil, 6379))
	require.Error(t, tb.Failover(nil, false, false))
	require.Error(t, tb.Replicates(nil, &ClusterNode{ID: testPrimaryID}))
	require.Error(t, tb.Replicates(&ClusterNode{IP: "10.0.0.1"}, nil))
	require.Error(t, tb.AddSlot(nil, 1))
	require.Error(t, tb.AddSlots(nil, 1, 2))
	_, _, _, err := tb.FlushAll(nil)
	require.Error(t, err)
	_, _, _, err = tb.PersistenceInfo(nil)
	require.Error(t, err)
	_, _, _, err = tb.LastSave(nil)
	require.Error(t, err)
	_, _, _, err = tb.BGSave(nil)
	require.Error(t, err)
	_, _, _, err = tb.ShutdownNoSave(nil)
	require.Error(t, err)
	require.Error(t, tb.CreateCluster(nil))
	require.Error(t, tb.MeetCluster(nil))
	_, _, err = tb.ForgetNodeFromAll(nil, nil)
	require.Error(t, err)
	require.Error(t, tb.Rebalance(nil, false))
	require.Error(t, tb.RebalanceWeighted(nil, nil, false))
	require.Error(t, tb.FixCluster(nil))
	assert.Empty(t, stub.Calls())
}

func TestToolbox_NilReceiverFailsClosed(t *testing.T) {
	var tb *Toolbox
	_, _, _, err := tb.ClusterInfo(context.Background(), "10.0.0.1")
	require.Error(t, err)
	require.Error(t, tb.AddSlot(&ClusterNode{IP: "10.0.0.1"}, 1))
	require.Error(t, tb.Failover(&ClusterNode{IP: "10.0.0.1"}, false, false))
}

func TestToolbox_TypedOperationsPreserveRedisArgv(t *testing.T) {
	tests := []struct {
		name string
		run  func(*Toolbox) error
		want []string
	}{
		{name: "meet", run: func(tb *Toolbox) error { return tb.Meet("10.0.0.1", "10.0.0.2", 6379) }, want: []string{"CLUSTER", "MEET", "10.0.0.2", "6379"}},
		{name: "failover", run: func(tb *Toolbox) error {
			return tb.Failover(&ClusterNode{IP: "10.0.0.2", ID: testReplicaID}, true, false)
		}, want: []string{"CLUSTER", "FAILOVER", "FORCE"}},
		{name: "replicate", run: func(tb *Toolbox) error {
			return tb.Replicates(&ClusterNode{IP: "10.0.0.2"}, &ClusterNode{ID: testPrimaryID})
		}, want: []string{"CLUSTER", "REPLICATE", testPrimaryID}},
		{name: "slot", run: func(tb *Toolbox) error { return tb.AddSlots(&ClusterNode{IP: "10.0.0.1"}, 1, 20) }, want: []string{"CLUSTER", "ADDSLOTSRANGE", "1", "20"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stub := toolbox.New().OnCommand("ok", func(_, _ string, _ []string) bool { return true }, toolbox.Stdout("OK\n"))
			tb := newToolboxWithStub(stub)
			require.NoError(t, tc.run(tb))
			require.Len(t, stub.Calls(), 1)
			assert.Equal(t, tc.want, stub.Calls()[0].Command[len(stub.Calls()[0].Command)-len(tc.want):])
		})
	}
}

func TestToolbox_CreateAndRebalanceUseFixedClusterArguments(t *testing.T) {
	stub := toolbox.New().OnCommand("ok", func(_, _ string, _ []string) bool { return true }, toolbox.Stdout("OK\n"))
	tb := newToolboxWithStub(stub)
	cluster := &Cluster{
		RedisCluster: &cachev1.RedisCluster{Spec: cachev1.RedisClusterSpec{Replicas: 1}},
		RedisPort:    6379,
		AllNodes:     []*ClusterNode{{IP: "10.0.0.1"}, {IP: "10.0.0.2"}},
		Nodes:        map[NodeRole][]*ClusterNode{PrimaryNode: {{IP: "10.0.0.1"}}},
	}
	require.NoError(t, tb.CreateCluster(cluster))
	require.NoError(t, tb.RebalanceWeighted(cluster, []NodeWeight{{NodeID: testPrimaryID, Weight: 0}, {NodeID: testReplicaID, Weight: 1.5}}, true))
	calls := stub.Calls()
	require.Len(t, calls, 2)
	assert.Equal(t, []string{"redis-cli", "-e", "--cluster", "create", "10.0.0.1:6379", "10.0.0.2:6379", "--cluster-replicas", "1", "--cluster-yes"}, calls[0].Command)
	assert.Equal(t, []string{"redis-cli", "-e", "--cluster", "rebalance", "10.0.0.1:6379", "--cluster-use-empty-masters", "--cluster-weight", testPrimaryID + "=0", testReplicaID + "=1.5"}, calls[1].Command)
}

func TestToolbox_CreateMeetAndQueryUseSelectedAuthenticationMode(t *testing.T) {
	providers := []struct {
		name       string
		provider   CredentialProvider
		executable string
	}{
		{name: "no-auth", provider: NoAuthentication{}, executable: "redis-cli"},
		{name: "mounted-secret", provider: MountedSecretAuthentication{}, executable: mountedSecretRedisCLI},
	}
	for _, tc := range providers {
		t.Run(tc.name, func(t *testing.T) {
			stub := toolbox.New().OnCommand("ok", func(_, _ string, _ []string) bool { return true }, toolbox.Stdout("OK\n"))
			tb := newToolboxWithStub(stub)
			tb.CredentialProvider = tc.provider
			cluster := &Cluster{
				RedisCluster: &cachev1.RedisCluster{},
				RedisPort:    6379,
				AllNodes:     []*ClusterNode{{IP: "10.0.0.1"}, {IP: "10.0.0.2"}},
			}
			require.NoError(t, tb.CreateCluster(cluster))
			require.NoError(t, tb.Meet("10.0.0.1", "10.0.0.2", 6379))
			_, _, _, err := tb.ClusterInfo(context.Background(), "10.0.0.1")
			require.NoError(t, err)
			for _, call := range stub.Calls() {
				require.NotEmpty(t, call.Command)
				assert.Equal(t, tc.executable, call.Command[0])
				assert.NotContains(t, call.Command, "bash")
			}
		})
	}
}

func TestToolbox_RebalanceAndFixRejectInvalidEntrypointsWithoutExec(t *testing.T) {
	for _, cluster := range []*Cluster{
		nil,
		{RedisPort: 0, Nodes: map[NodeRole][]*ClusterNode{PrimaryNode: {{IP: "10.0.0.1"}}}},
		{RedisPort: 6379, Nodes: map[NodeRole][]*ClusterNode{PrimaryNode: {{IP: "10.0.0.1;shutdown"}}}},
		{RedisPort: 6379, RedisCluster: &cachev1.RedisCluster{}, Toolbox: &Toolbox{Namespace: "invalid;namespace"}},
	} {
		stub := toolbox.New()
		tb := newToolboxWithStub(stub)
		require.Error(t, tb.Rebalance(cluster, false))
		require.Error(t, tb.RebalanceWeighted(cluster, []NodeWeight{{NodeID: testPrimaryID, Weight: 1}}, false))
		require.Error(t, tb.FixCluster(cluster))
		assert.Empty(t, stub.Calls())
	}
}

func TestToolbox_RebalanceWeightsRejectInvalidValuesWithoutExec(t *testing.T) {
	cluster := &Cluster{RedisPort: 6379, Nodes: map[NodeRole][]*ClusterNode{PrimaryNode: {{IP: "10.0.0.1"}}}}
	for _, weight := range []NodeWeight{
		{NodeID: "short", Weight: 1},
		{NodeID: testPrimaryID, Weight: -1},
		{NodeID: testPrimaryID, Weight: math.Inf(1)},
		{NodeID: testPrimaryID, Weight: math.NaN()},
	} {
		stub := toolbox.New()
		tb := newToolboxWithStub(stub)
		require.Error(t, tb.RebalanceWeighted(cluster, []NodeWeight{weight}, false))
		assert.Empty(t, stub.Calls())
	}
}

func TestToolbox_SetReplicaMigrationAttemptsEveryNode(t *testing.T) {
	stub := toolbox.New().OnCommand(
		"config-set",
		func(_, _ string, command []string) bool {
			return strings.Contains(strings.Join(command, " "), "CONFIG SET cluster-allow-replica-migration no")
		},
		toolbox.Stdout("OK\n"),
	)
	tb := newToolboxWithStub(stub)

	err := tb.SetReplicaMigration([]*ClusterNode{
		{PodName: "redis-0", IP: "10.0.0.1"},
		{PodName: "redis-1", IP: "10.0.0.2"},
	}, false)
	require.NoError(t, err)

	calls := stub.Calls()
	require.Len(t, calls, 2)
	assert.Equal(t, []string{"redis-cli", "-e", "-h", "10.0.0.1", "-p", "6379", "CONFIG", "SET", "cluster-allow-replica-migration", "no"}, calls[0].Command)
	assert.Equal(t, []string{"redis-cli", "-e", "-h", "10.0.0.2", "-p", "6379", "CONFIG", "SET", "cluster-allow-replica-migration", "no"}, calls[1].Command)
}

func TestToolbox_SetReplicaMigrationRequiresEveryNodeToAcknowledge(t *testing.T) {
	stub := toolbox.New().OnCommand(
		"config-set",
		func(_, _ string, _ []string) bool { return true },
		toolbox.Stdout("ERR simulated failure\n"),
	)
	tb := newToolboxWithStub(stub)

	err := tb.SetReplicaMigration([]*ClusterNode{
		{PodName: "redis-0", IP: "10.0.0.1"},
		{PodName: "redis-1", IP: "10.0.0.2"},
	}, true)
	require.EqualError(t, err, "setting cluster-allow-replica-migration=yes succeeded on 0 of 2 nodes")
	assert.Len(t, stub.Calls(), 2)
}

func TestToolbox_SetReplicaMigrationAttemptsRemainingNodesAfterExecFailure(t *testing.T) {
	stub := toolbox.New().
		OnCommand("ok", func(_, _ string, _ []string) bool { return true }, toolbox.Stdout("OK\n")).
		FailNext(errors.New("first node unavailable"))
	tb := newToolboxWithStub(stub)

	err := tb.SetReplicaMigration([]*ClusterNode{{IP: "10.0.0.1"}, {IP: "10.0.0.2"}}, false)
	require.ErrorContains(t, err, "first node unavailable")
	assert.Len(t, stub.Calls(), 2)
}

func TestToolbox_SetReplicaMigrationRejectsMissingIPBeforeExecuting(t *testing.T) {
	stub := toolbox.New()
	tb := newToolboxWithStub(stub)

	err := tb.SetReplicaMigration([]*ClusterNode{{PodName: "redis-0"}}, false)
	require.EqualError(t, err, "cannot set replica migration on a Redis node without an IP")
	assert.Empty(t, stub.Calls())
}

func TestToolbox_ExecMethodsUseCallerContext(t *testing.T) {
	stub := toolbox.New().DelayNext(time.Hour)
	tb := newToolboxWithStub(stub)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, _, err := tb.ClusterNodes(ctx, "10.0.0.1")
	require.ErrorIs(t, err, context.Canceled)

	stub.DelayNext(time.Hour)
	_, err = tb.GetReplicationInfoContext(ctx, "10.0.0.1")
	require.ErrorIs(t, err, context.Canceled)

	stub.DelayNext(time.Hour)
	_, err = tb.GetNodeIDByIPContext(ctx, "10.0.0.1")
	require.ErrorIs(t, err, context.Canceled)

	stub.DelayNext(time.Hour)
	_, err = tb.GetNodeIDByAddressContext(ctx, "redis-0.redis.default.svc.cluster.local")
	require.ErrorIs(t, err, context.Canceled)

}

func TestToolbox_GetNodeIDByAddressUsesHostname(t *testing.T) {
	stub := toolbox.New().OnRedisCommand("CLUSTER MYID", toolbox.Stdout(testPrimaryID+"\n"))
	tb := newToolboxWithStub(stub)

	nodeID, err := tb.GetNodeIDByAddressContext(
		context.Background(),
		"redis-cluster-test-6.redis-cluster-test.default.svc",
	)
	require.NoError(t, err)
	assert.Equal(t, testPrimaryID, nodeID)

	calls := stub.Calls()
	require.Len(t, calls, 1)
	assert.Contains(t, strings.Join(calls[0].Command, " "),
		"-h redis-cluster-test-6.redis-cluster-test.default.svc")
}

func TestClusterViewBudgetScalesAndBoundsEachNode(t *testing.T) {
	assert.Equal(t, 35*time.Second, clusterViewTimeout(6))
	assert.Equal(t, 65*time.Second, clusterViewTimeout(12))
}

func TestToolbox_MeetClusterStopsWhenContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stub := toolbox.New().DelayNext(time.Hour)
	tb := newToolboxWithStub(stub)
	tb.ctx = ctx
	tb.Metrics = metrics.CreateNoop()
	cluster := &Cluster{
		AllNodes: []*ClusterNode{
			{IP: "10.0.0.1"},
			{IP: "10.0.0.2"},
		},
		Nodes:     map[NodeRole][]*ClusterNode{PrimaryNode: nil},
		RedisPort: 6379,
		Timeouts: &timeouts.Config{
			PollInterval:     2 * time.Second,
			OperationTimeout: 4 * time.Second,
		},
	}

	errCh := make(chan error, 1)
	go func() { errCh <- tb.MeetCluster(cluster) }()
	require.Eventually(t, func() bool { return len(stub.Calls()) > 0 }, time.Second, 10*time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("MeetCluster did not stop after parent cancellation")
	}
}

func TestToolbox_MeetClusterOperationTimeoutCancelsActiveCommand(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stub := toolbox.New().DelayNext(time.Hour)
	tb := newToolboxWithStub(stub)
	tb.ctx = ctx
	tb.Metrics = metrics.CreateNoop()
	cluster := &Cluster{
		AllNodes: []*ClusterNode{
			{IP: "10.0.0.1"},
			{IP: "10.0.0.2"},
		},
		Nodes:     map[NodeRole][]*ClusterNode{PrimaryNode: nil},
		RedisPort: 6379,
		Timeouts: &timeouts.Config{
			PollInterval:     10 * time.Millisecond,
			OperationTimeout: 100 * time.Millisecond,
		},
	}

	errCh := make(chan error, 1)
	go func() { errCh <- tb.MeetCluster(cluster) }()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("MeetCluster operation timeout did not cancel the active command")
	}
}

func TestToolboxRunObjectStoreCommandKeepsPayloadOutOfCommand(t *testing.T) {
	stub := toolbox.New().OnCommand("upload", func(_, _ string, cmd []string) bool {
		return len(cmd) > 0 && cmd[0] == "aws"
	}, toolbox.OK())
	tb := newToolboxWithStub(stub)
	payload := []byte(`{"secret":";$(touch /tmp/pwned)"}`)

	_, _, _, err := tb.RunObjectStoreCommand(context.Background(), &ClusterNode{PodName: "redis-0"}, constants.BackupContainerName, ObjectStoreAWS, []string{"s3", "cp", "-", "s3://bucket/manifest.json"}, payload)
	require.NoError(t, err)
	calls := stub.Calls()
	require.Len(t, calls, 1)
	assert.Equal(t, payload, calls[0].Stdin)
	assert.NotContains(t, strings.Join(calls[0].Command, " "), string(payload))
}

func TestToolboxRunBackupCommandSupportsCustomFixedArgv(t *testing.T) {
	stub := toolbox.New().OnCommand("custom", func(_, _ string, cmd []string) bool {
		return len(cmd) > 0 && cmd[0] == "rclone"
	}, toolbox.OK())
	tb := newToolboxWithStub(stub)
	payload := []byte("manifest")
	command := []string{"rclone", "copyto", "-", "remote:backup/manifest.json"}

	_, _, _, err := tb.RunBackupCommand(context.Background(), &ClusterNode{PodName: "redis-0"}, constants.BackupContainerName, command, payload)
	require.NoError(t, err)
	require.Len(t, stub.Calls(), 1)
	assert.Equal(t, command, stub.Calls()[0].Command)
	assert.Equal(t, payload, stub.Calls()[0].Stdin)
}

func TestToolboxRunObjectStoreCommandRejectsUnknownExecutionTargets(t *testing.T) {
	stub := toolbox.New().OnCommand("all", func(_, _ string, _ []string) bool { return true }, toolbox.OK())
	tb := newToolboxWithStub(stub)
	node := &ClusterNode{PodName: "redis-0"}

	_, _, _, err := tb.RunObjectStoreCommand(context.Background(), node, constants.BackupContainerName, ObjectStoreCLI(255), nil, nil)
	require.ErrorContains(t, err, "unsupported object-store CLI")
	_, _, _, err = tb.RunObjectStoreCommand(context.Background(), node, "operator", ObjectStoreAWS, nil, nil)
	require.ErrorContains(t, err, "unsupported object-store container")
	_, _, _, err = tb.RunObjectStoreCommand(context.Background(), nil, constants.BackupContainerName, ObjectStoreAWS, nil, nil)
	require.ErrorContains(t, err, "Redis pod name is required")
	assert.Empty(t, stub.Calls())
}
