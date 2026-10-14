// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package kind

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/kube"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The toolbox loadgen drives Redis through redis-cli -c inside the cluster.

const (
	// loadgenStartupTimeout bounds the toolbox readiness wait.
	loadgenStartupTimeout = 2 * time.Minute
	// loadgenKeyPrefix namespaces every key this loadgen writes.
	loadgenKeyPrefix = "loadgen"
	// loadgenHashTag pins all keys to one hash slot so a single SET/GET batch is
	// MOVED-free; the cluster still routes the batch through the owning primary.
	loadgenHashTag = "lg"
	// execTimeout bounds a single redis-cli exec against the toolbox.
	execTimeout = 30 * time.Second
	// redisStringBatchSize bounds keys checked by one toolbox exec.
	redisStringBatchSize = 10
)

type redisStringKV struct {
	Key   string
	Value string
}

func sequentialStringKVs(prefix string, count int) []redisStringKV {
	kvs := make([]redisStringKV, count)
	for i := range kvs {
		kvs[i] = redisStringKV{Key: fmt.Sprintf("%s:%d", prefix, i), Value: strconv.Itoa(i)}
	}
	return kvs
}

// Loadgen is the data-loss workload handle a test holds. The toolbox-backed
// implementation below satisfies it; a rueidis pod impl can replace it later
// without changing call sites.
type Loadgen interface {
	// SeedData writes the configured string key set and records expected values.
	SeedData(ctx context.Context) error
	// VerifyData reads every seeded string key back; returns (matched, total, error).
	VerifyData(ctx context.Context) (matched, total int, err error)
	// SeedTypedData writes hash/set/list keys and records expected values.
	SeedTypedData(ctx context.Context) error
	// VerifyTypedData reads every seeded typed key back; returns (matched, total, error).
	VerifyTypedData(ctx context.Context) (matched, total int, err error)
	// ClusterHealthy reports cluster_state=ok with all TotalSlots assigned, as
	// seen from inside the cluster — the gate that makes a Verify trustworthy.
	ClusterHealthy(ctx context.Context) (bool, error)
}

// RequireSeedDataIntact verifies the expected string keys.
func RequireSeedDataIntact(t *testing.T, loadgen Loadgen, expected int) {
	t.Helper()
	require.NoError(t, CheckSeedDataIntact(t.Context(), loadgen, expected))
}

// CheckSeedDataIntact verifies the expected string keys.
func CheckSeedDataIntact(ctx context.Context, loadgen Loadgen, expected int) error {
	return checkLoadgenData(ctx, "string", expected, loadgen.VerifyData)
}

// RequireTypedDataIntact verifies the expected typed keys.
func RequireTypedDataIntact(t *testing.T, loadgen Loadgen, expected int) {
	t.Helper()
	require.NoError(t, CheckTypedDataIntact(t.Context(), loadgen, expected))
}

// CheckTypedDataIntact verifies the expected typed keys.
func CheckTypedDataIntact(ctx context.Context, loadgen Loadgen, expected int) error {
	return checkLoadgenData(ctx, "typed", expected, loadgen.VerifyTypedData)
}

func checkLoadgenData(
	ctx context.Context,
	kind string,
	expected int,
	verify func(context.Context) (matched, total int, err error),
) error {
	matched, total, err := verify(ctx)
	if err != nil {
		return fmt.Errorf("verify %s data: %w", kind, err)
	}
	if total != expected {
		return fmt.Errorf("%s key count: want %d, got %d", kind, expected, total)
	}
	if matched != total {
		return fmt.Errorf("lost %d/%d %s keys", total-matched, total, kind)
	}
	return nil
}

// redisExec runs redis-cli through the toolbox jump pod (no local server, so
// cluster-mode targets a real redis pod IP). Shared by the loadgen + Suite probes.
type redisExec struct {
	exec       *kube.Exec
	client     client.Client
	namespace  string
	password   string
	redisPort  int
	toolboxPod string
	log        logr.Logger
}

// newRedisExec connects a redis-cli exec handle to the namespace's toolbox pod,
// waiting for the pod to be Running+Ready.
func newRedisExec(cfg *rest.Config, c client.Client, namespace, password string) (*redisExec, error) {
	log := logr.Discard()
	exec, err := kube.NewKubeExec(cfg, namespace, log)
	if err != nil {
		return nil, fmt.Errorf("create toolbox exec: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), loadgenStartupTimeout)
	defer cancel()
	pod, err := waitForToolboxPod(ctx, c, namespace)
	if err != nil {
		return nil, fmt.Errorf("toolbox pod not ready: %w", err)
	}
	return &redisExec{
		exec:       exec,
		client:     c,
		namespace:  namespace,
		password:   password,
		redisPort:  redisPort,
		toolboxPod: pod,
		log:        log,
	}, nil
}

// anyRedisIP returns a Running+Ready redis pod IP to aim cluster-mode redis-cli
// at. During blue-green it prefers the recorded active StatefulSet.
func (r *redisExec) anyRedisIP(ctx context.Context) (string, error) {
	pods := &corev1.PodList{}
	if err := r.client.List(ctx, pods, client.InNamespace(r.namespace),
		client.HasLabels{constants.LabelRedisClusterName}); err != nil {
		return "", err
	}
	active := r.activeStatefulSet(ctx, pods.Items)
	if ip := preferredRedisIP(pods.Items, active); ip != "" {
		return ip, nil
	}
	return "", fmt.Errorf("no ready redis pod (label %s) in namespace %s", constants.LabelRedisClusterName, r.namespace)
}

func (r *redisExec) activeStatefulSet(ctx context.Context, pods []corev1.Pod) string {
	for i := range pods {
		name := pods[i].Labels[constants.LabelRedisClusterName]
		if name == "" {
			continue
		}
		rc := &cachev1.RedisCluster{}
		if err := r.client.Get(ctx, types.NamespacedName{Namespace: r.namespace, Name: name}, rc); err != nil {
			continue
		}
		if rc.Status.ActiveClusterName != "" {
			return rc.Status.ActiveClusterName
		}
		return rc.Spec.StatefulsetTemplate.Name
	}
	return ""
}

func preferredRedisIP(pods []corev1.Pod, activeStatefulSet string) string {
	serving := ""
	fallback := ""
	for i := range pods {
		pod := &pods[i]
		if !podRunningReady(*pod) || pod.Status.PodIP == "" {
			continue
		}
		if ownedByStatefulSet(pod, activeStatefulSet) {
			return pod.Status.PodIP
		}
		if serving == "" && constants.IsRoleActive(pod.Labels) {
			serving = pod.Status.PodIP
		}
		if fallback == "" {
			fallback = pod.Status.PodIP
		}
	}
	if serving != "" {
		return serving
	}
	return fallback
}

func ownedByStatefulSet(pod *corev1.Pod, name string) bool {
	if name == "" {
		return false
	}
	for _, owner := range pod.OwnerReferences {
		if owner.Kind == "StatefulSet" && owner.Name == name {
			return true
		}
	}
	return false
}

// Command runs cluster-mode (-c) redis-cli against a ready redis pod, following MOVED.
func (r *redisExec) Command(ctx context.Context, args ...string) (string, error) {
	ip, err := r.anyRedisIP(ctx)
	if err != nil {
		return "", err
	}
	cmd := fmt.Sprintf("%sredis-cli %s -h %s -p %d -c %s",
		r.authPrefix(), r.authFlags(), ip, r.redisPort, quoteArgs(args))
	out, err := r.sh(ctx, cmd)
	return strings.TrimRight(out, "\r\n"), err
}

// CommandAtIP runs redis-cli against a specific node IP (no -c), for INFO
// replication / per-replica offsets / CLUSTER RESET on a target node.
func (r *redisExec) CommandAtIP(ctx context.Context, ip string, args ...string) (string, error) {
	cmd := fmt.Sprintf("%sredis-cli %s -h %s -p %d %s",
		r.authPrefix(), r.authFlags(), ip, r.redisPort, quoteArgs(args))
	out, err := r.sh(ctx, cmd)
	return strings.TrimRight(out, "\r\n"), err
}

// writeAndVerifyStrings writes and reads each key through cluster routing.
func (r *redisExec) writeAndVerifyStrings(ctx context.Context, kvs []redisStringKV) error {
	return r.runStringBatches(ctx, kvs, true)
}

// verifyStrings checks each key through cluster routing.
func (r *redisExec) verifyStrings(ctx context.Context, kvs []redisStringKV) error {
	return r.runStringBatches(ctx, kvs, false)
}

func (r *redisExec) runStringBatches(ctx context.Context, kvs []redisStringKV, write bool) error {
	batches := splitRedisStringKVs(kvs)
	if len(batches) == 0 {
		return nil
	}
	ip, err := r.anyRedisIP(ctx)
	if err != nil {
		return fmt.Errorf("select Redis node for string batches: %w", err)
	}
	for batchIndex, batch := range batches {
		out, err := r.sh(ctx, r.buildRedisStringBatchScript(ip, batch, write))
		if err != nil {
			return fmt.Errorf("run string batch %d: %w (stdout=%s)", batchIndex, err, strings.TrimSpace(out))
		}
	}
	return nil
}

func splitRedisStringKVs(kvs []redisStringKV) [][]redisStringKV {
	if len(kvs) == 0 {
		return nil
	}
	batches := make([][]redisStringKV, 0, (len(kvs)+redisStringBatchSize-1)/redisStringBatchSize)
	for start := 0; start < len(kvs); start += redisStringBatchSize {
		end := min(start+redisStringBatchSize, len(kvs))
		batches = append(batches, kvs[start:end])
	}
	return batches
}

func (r *redisExec) buildRedisStringBatchScript(ip string, kvs []redisStringKV, write bool) string {
	var script strings.Builder
	script.WriteString("set -uo pipefail\n")
	script.WriteString(r.authPrefix())
	fmt.Fprintf(&script, "redis_cli=(redis-cli %s -h %s -p %d -2 --json -c -e)\n",
		r.authFlags(), shellQuote(ip), r.redisPort)
	for _, kv := range kvs {
		key := shellQuote(kv.Key)
		if write {
			fmt.Fprintf(&script, "if ! \"${redis_cli[@]}\" SET %s %s >/dev/null; then\n", key, shellQuote(kv.Value))
			fmt.Fprintf(&script, "  printf 'SET failed for key %%s\\n' %s >&2\n", key)
			script.WriteString("  exit 1\nfi\n")
		}
		expectedJSON, _ := json.Marshal(kv.Value) // strings are always JSON-marshalable
		fmt.Fprintf(&script, "if ! got=\"$(\"${redis_cli[@]}\" GET %s)\"; then\n", key)
		fmt.Fprintf(&script, "  printf 'GET failed for key %%s\\n' %s >&2\n", key)
		script.WriteString("  exit 1\nfi\n")
		// Keep only the value after a MOVED redirect notice.
		script.WriteString("got=\"${got##*$'\\n'}\"\n")
		fmt.Fprintf(&script, "if [[ \"$got\" != %s ]]; then\n", shellQuote(string(expectedJSON)))
		fmt.Fprintf(&script, "  printf 'GET %%s: want %%s, got %%s\\n' %s %s \"$got\" >&2\n",
			key, shellQuote(string(expectedJSON)))
		script.WriteString("  exit 1\nfi\n")
	}
	return script.String()
}

// ClusterNodes returns `CLUSTER NODES` text from inside the cluster.
func (r *redisExec) ClusterNodes(ctx context.Context) (string, error) {
	return r.Command(ctx, "CLUSTER", "NODES")
}

// ClusterInfo returns `CLUSTER INFO` text from inside the cluster.
func (r *redisExec) ClusterInfo(ctx context.Context) (string, error) {
	return r.Command(ctx, "CLUSTER", "INFO")
}

// ExecOnPod runs an arbitrary argv in a pod container (CLUSTER RESET HARD on a
// specific node, aws-cli on the backup sidecar) and returns rc/stdout/stderr.
func (r *redisExec) ExecOnPod(ctx context.Context, pod, container string, argv []string) (int, string, string, error) {
	ctx, cancel := context.WithTimeout(ctx, execTimeout)
	defer cancel()
	return r.exec.Exec(ctx, pod, container, argv, r.log)
}

// pipeAt feeds newline-delimited commands to `redis-cli --pipe` against a
// specific node via a quoted here-doc. --pipe does NOT follow MOVED, so the
// caller must target the node that owns the keys' slot.
func (r *redisExec) pipeAt(ctx context.Context, ip, script string) (string, error) {
	cmd := fmt.Sprintf("%sredis-cli %s -h %s -p %d --pipe <<'LOADGEN_EOF'\n%s\nLOADGEN_EOF",
		r.authPrefix(), r.authFlags(), ip, r.redisPort, script)
	return r.sh(ctx, cmd)
}

// sh runs one shell command on the toolbox pod and returns stdout, erroring on
// a non-zero exit so a silent redis failure can't masquerade as data loss.
func (r *redisExec) sh(ctx context.Context, command string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, execTimeout)
	defer cancel()
	rc, stdout, stderr, err := r.exec.Exec(ctx, r.toolboxPod, constants.ToolboxContainerName,
		[]string{"bash", "-lc", command}, r.log)
	if err != nil {
		return stdout, err
	}
	if rc != 0 {
		return stdout, fmt.Errorf("redis-cli exit %d: %s", rc, strings.TrimSpace(stderr))
	}
	return stdout, nil
}

// authPrefix exports REDISCLI_AUTH when a toolbox password is set.
func (r *redisExec) authPrefix() string {
	if r.password == "" {
		return ""
	}
	return fmt.Sprintf("export REDISCLI_AUTH=%s; ", shellQuote(r.password))
}

// authFlags selects the toolbox ACL user when a password is set.
func (r *redisExec) authFlags() string {
	if r.password == "" {
		return ""
	}
	return "--user toolbox --no-auth-warning"
}

// toolboxLoadgen drives Redis through the operator's toolbox pod via redis-cli -c.
type toolboxLoadgen struct {
	*redisExec
	seedKeys int
	written  map[string]string // string key → expected value
}

var _ Loadgen = (*toolboxLoadgen)(nil)

// newToolboxLoadgen wraps an existing exec handle as a data-path loadgen, so a
// Suite can share its one redisExec with the workload.
func newToolboxLoadgen(re *redisExec, seedKeys int) *toolboxLoadgen {
	return &toolboxLoadgen{
		redisExec: re,
		seedKeys:  seedKeys,
		written:   make(map[string]string, seedKeys),
	}
}

// SeedData writes seedKeys hash-tagged keys via one pipelined redis-cli -c call.
func (l *toolboxLoadgen) SeedData(ctx context.Context) error {
	var script strings.Builder
	for i := 0; i < l.seedKeys; i++ {
		key := l.key(i)
		val := fmt.Sprintf("v-%d", i)
		l.written[key] = val
		fmt.Fprintf(&script, "SET %s %s\n", key, val)
	}
	ip, err := l.seedOwnerIP(ctx)
	if err != nil {
		return fmt.Errorf("find seed slot owner: %w", err)
	}
	out, err := l.pipeAt(ctx, ip, script.String())
	if err != nil {
		return fmt.Errorf("seed pipe: %w (out=%s)", err, out)
	}
	return nil
}

// seedOwnerIP returns the IP of the primary owning the loadgen's hash slot; all
// seeded keys share that slot, so --pipe (no MOVED) can target it directly.
func (l *toolboxLoadgen) seedOwnerIP(ctx context.Context) (string, error) {
	viewerIP, err := l.anyRedisIP(ctx)
	if err != nil {
		return "", err
	}
	slotStr, err := l.CommandAtIP(ctx, viewerIP, "CLUSTER", "KEYSLOT", l.key(0))
	if err != nil {
		return "", err
	}
	slot, err := strconv.Atoi(strings.TrimSpace(slotStr))
	if err != nil {
		return "", fmt.Errorf("parse keyslot %q: %w", slotStr, err)
	}
	// Query CLUSTER NODES from an explicit viewer so parseClusterNodesFrom can
	// back-fill the self row's empty IP (KIND runs redis without --cluster-announce-ip);
	// otherwise the queried primary's own row reports IP="" and never matches.
	nodes, err := l.CommandAtIP(ctx, viewerIP, "CLUSTER", "NODES")
	if err != nil {
		return "", err
	}
	for _, n := range parseClusterNodesFrom(nodes, viewerIP) {
		if n.IsMaster && !n.Failed && n.OwnsSlot(slot) {
			return n.IP, nil
		}
	}
	return "", fmt.Errorf("no live primary owns slot %d", slot)
}

// VerifyData checks seeded values with a slot-local MGET.
func (l *toolboxLoadgen) VerifyData(ctx context.Context) (int, int, error) {
	total := len(l.written)
	if total == 0 {
		return 0, 0, nil
	}

	keys, args := buildMGetCommand(l.written)
	ip, err := l.seedOwnerIP(ctx)
	if err != nil {
		return 0, total, fmt.Errorf("find verify slot owner: %w", err)
	}
	out, err := l.CommandAtIP(ctx, ip, args...)
	if err != nil {
		return 0, total, fmt.Errorf("verify MGET: %w", err)
	}
	matched, err := countMGetMatches(out, keys, l.written)
	if err != nil {
		return matched, total, fmt.Errorf("verify MGET response: %w", err)
	}
	return matched, total, nil
}

func buildMGetCommand(written map[string]string) ([]string, []string) {
	keys := make([]string, 0, len(written))
	for key := range written {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	args := make([]string, 0, len(keys)+4)
	args = append(args, "-2", "--json", "-c", "MGET")
	args = append(args, keys...)
	return keys, args
}

// countMGetMatches counts expected values in a JSON MGET response.
func countMGetMatches(out string, keys []string, written map[string]string) (int, error) {
	// Decode the final line after any MOVED notice.
	out = strings.TrimSpace(out)
	if lastNewline := strings.LastIndexByte(out, '\n'); lastNewline >= 0 {
		out = strings.TrimSpace(out[lastNewline+1:])
	}
	var values []*string
	if err := json.Unmarshal([]byte(out), &values); err != nil {
		return 0, fmt.Errorf("decode JSON: %w", err)
	}
	if len(values) != len(keys) {
		return 0, fmt.Errorf("got %d values for %d keys", len(values), len(keys))
	}
	matched := 0
	for i, value := range values {
		if value != nil && *value == written[keys[i]] {
			matched++
		}
	}
	return matched, nil
}

// SeedTypedData writes a hash/set/list key per index (hash-tagged) and records
// the expected scalar VerifyTypedData reads back — proving non-string keyspaces
// survive a rollout, not just strings.
func (l *toolboxLoadgen) SeedTypedData(ctx context.Context) error {
	var script strings.Builder
	for i := 0; i < l.seedKeys; i++ {
		hk, sk, lk := l.typedKeys(i)
		val := fmt.Sprintf("v-%d", i)
		fmt.Fprintf(&script, "HSET %s f %s\n", hk, val)
		fmt.Fprintf(&script, "SADD %s %s\n", sk, val)
		fmt.Fprintf(&script, "RPUSH %s %s\n", lk, val)
	}
	ip, err := l.seedOwnerIP(ctx)
	if err != nil {
		return fmt.Errorf("find seed slot owner: %w", err)
	}
	out, err := l.pipeAt(ctx, ip, script.String())
	if err != nil {
		return fmt.Errorf("seed typed pipe: %w (out=%s)", err, out)
	}
	return nil
}

// VerifyTypedData reads each seeded typed key back via the matching read command
// and counts how many still hold their expected scalar.
func (l *toolboxLoadgen) VerifyTypedData(ctx context.Context) (int, int, error) {
	matched := 0
	total := 3 * l.seedKeys
	for i := 0; i < l.seedKeys; i++ {
		hk, sk, lk := l.typedKeys(i)
		want := fmt.Sprintf("v-%d", i)
		if got, err := l.Command(ctx, "HGET", hk, "f"); err != nil {
			return matched, total, fmt.Errorf("verify HGET %s: %w", hk, err)
		} else if got == want {
			matched++
		}
		if got, err := l.Command(ctx, "SISMEMBER", sk, want); err != nil {
			return matched, total, fmt.Errorf("verify SISMEMBER %s: %w", sk, err)
		} else if strings.TrimSpace(got) == "1" {
			matched++
		}
		if got, err := l.Command(ctx, "LINDEX", lk, "0"); err != nil {
			return matched, total, fmt.Errorf("verify LINDEX %s: %w", lk, err)
		} else if got == want {
			matched++
		}
	}
	return matched, total, nil
}

// ClusterHealthy reports cluster_state=ok with all TotalSlots assigned, queried
// from inside the cluster through the toolbox (CLUSTER INFO + CLUSTER NODES).
func (l *toolboxLoadgen) ClusterHealthy(ctx context.Context) (bool, error) {
	viewerIP, err := l.anyRedisIP(ctx)
	if err != nil {
		return false, err
	}
	info, err := l.CommandAtIP(ctx, viewerIP, "CLUSTER", "INFO")
	if err != nil {
		return false, err
	}
	if infoField(info, "cluster_state") != "ok" {
		return false, nil
	}
	nodes, err := l.CommandAtIP(ctx, viewerIP, "CLUSTER", "NODES")
	if err != nil {
		return false, err
	}
	return sumPrimarySlotCoverage(ParseClusterNodes(nodes)) == constants.TotalSlots, nil
}

// key returns the i-th seeded string key, hash-tagged so all keys share one slot.
func (l *toolboxLoadgen) key(i int) string {
	return fmt.Sprintf("%s:{%s}:%d", loadgenKeyPrefix, loadgenHashTag, i)
}

// typedKeys returns the i-th hash/set/list keys, all hash-tagged to one slot.
func (l *toolboxLoadgen) typedKeys(i int) (hash, set, list string) {
	return fmt.Sprintf("%s:h:{%s}:%d", loadgenKeyPrefix, loadgenHashTag, i),
		fmt.Sprintf("%s:s:{%s}:%d", loadgenKeyPrefix, loadgenHashTag, i),
		fmt.Sprintf("%s:l:{%s}:%d", loadgenKeyPrefix, loadgenHashTag, i)
}

// waitForToolboxPod blocks until the operator's toolbox pod is Running+Ready.
func waitForToolboxPod(ctx context.Context, c client.Client, ns string) (string, error) {
	var last string
	for {
		pods := &corev1.PodList{}
		if err := c.List(ctx, pods, client.InNamespace(ns), client.MatchingLabels{
			"app":     constants.ToolboxContainerName,
			"service": constants.ToolboxContainerName,
		}); err == nil {
			for i := range pods.Items {
				if podRunningReady(pods.Items[i]) {
					return pods.Items[i].Name, nil
				}
				last = pods.Items[i].Name
			}
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("toolbox pod not Ready (last=%q): %w", last, ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

// podRunningReady reports Running + Ready=True.
func podRunningReady(pod corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// shellQuote single-quotes s for safe inclusion in a bash -lc command.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// quoteArgs single-quotes each redis-cli arg and joins them with spaces.
func quoteArgs(args []string) string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = shellQuote(a)
	}
	return strings.Join(out, " ")
}
