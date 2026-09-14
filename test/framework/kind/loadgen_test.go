package kind

import (
	"fmt"
	"strings"
	"testing"

	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPreferredRedisIP(t *testing.T) {
	ready := corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue}
	pods := []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:            "blank",
				Labels:          map[string]string{constants.BlueGreenRoleLabel: constants.BlueGreenRoleActive},
				OwnerReferences: []metav1.OwnerReference{{Kind: "StatefulSet", Name: "green"}},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning, PodIP: "10.0.0.1", Conditions: []corev1.PodCondition{ready},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:            "serving",
				Labels:          map[string]string{constants.BlueGreenRoleLabel: constants.BlueGreenRoleActive},
				OwnerReferences: []metav1.OwnerReference{{Kind: "StatefulSet", Name: "blue"}},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning, PodIP: "10.0.0.2", Conditions: []corev1.PodCondition{ready},
			},
		},
	}

	require.Equal(t, "10.0.0.2", preferredRedisIP(pods, "blue"))
	require.Equal(t, "10.0.0.1", preferredRedisIP(pods, ""), "fall back to a serving pod without active StatefulSet status")
	delete(pods[1].Labels, constants.BlueGreenRoleLabel)
	require.Equal(t, "10.0.0.1", preferredRedisIP(pods, "missing"))
}

func TestCountMGetMatches(t *testing.T) {
	keys := []string{"k0", "k1", "k2"}
	written := map[string]string{
		"k0": "v0",
		"k1": "v1",
		"k2": "v2",
	}

	tests := []struct {
		name    string
		out     string
		matched int
		wantErr string
	}{
		{name: "all match", out: `["v0","v1","v2"]`, matched: 3},
		{name: "missing and changed", out: `["v0",null,"other"]`, matched: 1},
		{name: "slot redirect", out: "-> Redirected to slot [1] located at 10.0.0.2:6379\n[\"v0\",\"v1\",\"v2\"]", matched: 3},
		{name: "invalid JSON", out: `not-json`, wantErr: "decode JSON"},
		{name: "wrong value count", out: `["v0","v1"]`, wantErr: "got 2 values for 3 keys"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matched, err := countMGetMatches(tt.out, keys, written)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.matched, matched)
		})
	}
}

func TestBuildMGetCommandSortsKeysAndPlacesOptionsBeforeCommand(t *testing.T) {
	written := map[string]string{
		"k2": "v2",
		"k0": "v0",
		"k1": "v1",
	}

	keys, args := buildMGetCommand(written)

	require.Equal(t, []string{"k0", "k1", "k2"}, keys)
	require.Equal(t, []string{"-2", "--json", "-c", "MGET", "k0", "k1", "k2"}, args)
}

func TestSplitRedisStringKVs(t *testing.T) {
	kvs := make([]redisStringKV, 100)
	for i := range kvs {
		kvs[i] = redisStringKV{Key: fmt.Sprintf("k%d", i), Value: fmt.Sprintf("v%d", i)}
	}

	require.Nil(t, splitRedisStringKVs(nil))
	require.Equal(t, []int{1}, redisStringBatchLengths(kvs[:1]))
	require.Equal(t, []int{10}, redisStringBatchLengths(kvs[:10]))
	require.Equal(t, []int{10, 1}, redisStringBatchLengths(kvs[:11]))
	require.Equal(t, []int{10, 10, 1}, redisStringBatchLengths(kvs[:21]))
	require.Equal(t, []int{10, 10, 10, 10, 10, 10, 10, 10, 10, 10}, redisStringBatchLengths(kvs))

	flattened := make([]redisStringKV, 0, len(kvs))
	for _, batch := range splitRedisStringKVs(kvs) {
		flattened = append(flattened, batch...)
	}
	require.Equal(t, kvs, flattened, "batching must preserve key order")
}

func TestBuildRedisStringBatchScriptWritesThenVerifiesEachKey(t *testing.T) {
	r := &redisExec{redisPort: 6379, password: "p'ass"}
	kvs := []redisStringKV{
		{Key: "key 0", Value: "value'0"},
		{Key: "key:$1", Value: ""},
	}

	script := r.buildRedisStringBatchScript("10.0.0.2", kvs, true)

	require.Contains(t, script, "export REDISCLI_AUTH='p'\\''ass';")
	require.Contains(t, script, "redis_cli=(redis-cli --user toolbox --no-auth-warning -h '10.0.0.2' -p 6379 -2 --json -c -e)")
	require.NotContains(t, script, "--pipe")
	require.NotContains(t, script, "MSET")
	require.NotContains(t, script, "MGET")
	require.Contains(t, script, "got=\"${got##*$'\\n'}\"")
	require.Contains(t, script, shellQuote(`""`), "empty string must remain distinct from Redis null")

	set0 := strings.Index(script, "SET "+shellQuote(kvs[0].Key)+" "+shellQuote(kvs[0].Value))
	get0 := strings.Index(script, "GET "+shellQuote(kvs[0].Key))
	set1 := strings.Index(script, "SET "+shellQuote(kvs[1].Key)+" "+shellQuote(kvs[1].Value))
	get1 := strings.Index(script, "GET "+shellQuote(kvs[1].Key))
	require.GreaterOrEqual(t, set0, 0)
	require.Greater(t, get0, set0)
	require.Greater(t, set1, get0)
	require.Greater(t, get1, set1)
	require.Contains(t, script, "SET failed for key")
	require.Contains(t, script, "GET failed for key")
	require.Contains(t, script, "want %s, got %s")
}

func TestBuildRedisStringBatchScriptVerifyOnly(t *testing.T) {
	r := &redisExec{redisPort: 6380}
	kvs := []redisStringKV{{Key: "key\n1", Value: "value\n1"}}

	script := r.buildRedisStringBatchScript("10.0.0.3", kvs, false)

	require.NotContains(t, script, " SET ")
	require.Contains(t, script, "GET "+shellQuote(kvs[0].Key))
	expectedJSON := `"value\n1"`
	require.Contains(t, script, shellQuote(expectedJSON))
}

func redisStringBatchLengths(kvs []redisStringKV) []int {
	batches := splitRedisStringKVs(kvs)
	lengths := make([]int, len(batches))
	for i := range batches {
		lengths[i] = len(batches[i])
	}
	return lengths
}
