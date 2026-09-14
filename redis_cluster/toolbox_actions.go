package redis_cluster

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/utils"
	"k8s.io/apimachinery/pkg/util/wait"
)

// SetReplicaMigration enables or disables Redis' autonomous replica migration
// on every supplied node. Every node is attempted even if an earlier command fails. The
// operation succeeds only when every node acknowledges the requested setting.
func (t *Toolbox) SetReplicaMigration(nodes []*ClusterNode, enabled bool) error {
	if err := t.validate(); err != nil {
		return err
	}
	if len(nodes) == 0 {
		return nil
	}

	value := "no"
	if enabled {
		value = "yes"
	}

	for _, node := range nodes {
		if node == nil || validateAddress(node.IP) != nil {
			return fmt.Errorf("cannot set replica migration on a Redis node without an IP")
		}
	}

	t.countOperation("set_replica_migration")
	okCount := 0
	var firstErr error
	for _, node := range nodes {
		rc, stdout, stderr, err := t.runRedis(t.ctx, node.IP, t.RedisPort,
			"CONFIG", "SET", "cluster-allow-replica-migration", value)
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("failed to set cluster-allow-replica-migration=%s: %w", value, err)
		}
		if rc != 0 && firstErr == nil {
			firstErr = fmt.Errorf("setting cluster-allow-replica-migration=%s exited with code %d: %s", value, rc, strings.TrimSpace(stderr))
		}
		if err == nil && rc == 0 && utils.ContainsLine(stdout, "OK") {
			okCount++
		}
	}
	if firstErr != nil {
		return firstErr
	}
	if okCount != len(nodes) {
		return fmt.Errorf(
			"setting cluster-allow-replica-migration=%s succeeded on %d of %d nodes",
			value,
			okCount,
			len(nodes),
		)
	}
	return nil
}

// ForgetNodeFromIPs sends CLUSTER FORGET to specific IPs.
// Returns success count and list of failed IPs.
func (t *Toolbox) ForgetNodeFromIPs(ips []string, port int, zombieID string) (success int, failedIPs []string) {
	if err := t.validate(); err != nil {
		return 0, append([]string(nil), ips...)
	}
	if validatePort(port) != nil || validateNodeID(zombieID) != nil {
		return 0, append([]string(nil), ips...)
	}
	if len(ips) == 0 {
		return
	}
	for _, ip := range ips {
		rc, stdout, _, err := t.runRedis(t.ctx, ip, port, "CLUSTER", "FORGET", zombieID)
		if err == nil && rc == 0 && utils.ContainsLine(stdout, "OK") {
			success++
		} else {
			failedIPs = append(failedIPs, ip)
		}
	}
	return
}

// ForgetNodeFromAll sends CLUSTER FORGET to all primaries and replicas.
// It returns the success count, failed IPs, and validation/setup errors.
func (t *Toolbox) ForgetNodeFromAll(cluster *Cluster, zombie *ClusterNode) (success int, failedIPs []string, err error) {
	if err := t.validate(); err != nil {
		return 0, nil, err
	}
	if cluster == nil || zombie == nil {
		return 0, nil, errors.New("Redis cluster and zombie node are required")
	}
	if err := validateNodeID(zombie.ID); err != nil {
		return 0, nil, err
	}
	var ips []string
	for _, n := range cluster.AllNodes {
		if n == nil {
			continue
		}
		if n.RoleIs(PrimaryNode, ReplicaNode) && n.ID != zombie.ID && n.IP != "" {
			ips = append(ips, n.IP)
		}
	}
	success, failedIPs = t.ForgetNodeFromIPs(ips, cluster.RedisPort, zombie.ID)
	return success, failedIPs, nil
}

// ResetNode resets a node.
func (t *Toolbox) ResetNode(clusterNode *ClusterNode, hard bool) error {
	if err := t.validate(); err != nil {
		return err
	}
	if err := validateClusterNode(clusterNode); err != nil {
		return err
	}
	t.countOperation("reset_node")
	mode := "SOFT"
	if hard {
		mode = "HARD"
	}
	rc, stdout, _, err := t.runRedis(t.ctx, clusterNode.IP, t.RedisPort, "CLUSTER", "RESET", mode)
	if err == nil {
		if rc != 0 {
			err = fmt.Errorf("command exit code was %d", rc)
		} else if !utils.ContainsLine(stdout, constants.OKForgetNode) {
			err = fmt.Errorf("output does not indicate success: %s", stdout)
		}
	}
	return err
}

// AddReplica adds a replica node to a specific master.
// masterID specifies which primary the new node should replicate.
// This prevents the reset loop where Redis auto-assigns to a primary that already has enough replicas.
func (t *Toolbox) AddReplica(clusterNode *ClusterNode, nodeToAdd *ClusterNode, port int, masterID string) error {
	if err := t.validate(); err != nil {
		return err
	}
	if err := validateClusterNode(clusterNode); err != nil {
		return err
	}
	if err := validateClusterNode(nodeToAdd); err != nil {
		return err
	}
	t.countOperation("add_replica")
	var rc int
	var stdout, stderr string
	var err error
	if err = validateAddress(clusterNode.IP); err != nil {
		return err
	}
	if err = validateAddress(nodeToAdd.IP); err != nil {
		return err
	}
	if err = validatePort(port); err != nil {
		return err
	}
	args := []string{"--cluster", "add-node", netAddress(nodeToAdd.IP, port), netAddress(clusterNode.IP, port), "--cluster-slave"}
	if masterID != "" {
		if err = validateNodeID(masterID); err != nil {
			return err
		}
		args = append(args, "--cluster-master-id", masterID)
	}
	rc, stdout, stderr, err = t.runRedisCluster(t.ctx, args...)
	if err == nil {
		if rc != 0 {
			err = fmt.Errorf("command exit code was %d, stdout=%s, stderr=%s", rc, strings.TrimSpace(stdout), strings.TrimSpace(stderr))
		} else if !utils.ContainsLine(stdout, constants.OKAddNode) {
			err = fmt.Errorf("output does not indicate success: %s", stdout)
		}
	}
	return err
}

// AddPrimary adds a primary node.
func (t *Toolbox) AddPrimary(clusterNode *ClusterNode, nodeToAdd *ClusterNode, port int) error {
	if err := t.validate(); err != nil {
		return err
	}
	if err := validateClusterNode(clusterNode); err != nil {
		return err
	}
	if err := validateClusterNode(nodeToAdd); err != nil {
		return err
	}
	t.countOperation("add_primary")
	if err := validateAddress(clusterNode.IP); err != nil {
		return err
	}
	if err := validateAddress(nodeToAdd.IP); err != nil {
		return err
	}
	if err := validatePort(port); err != nil {
		return err
	}
	rc, stdout, _, err := t.runRedisCluster(t.ctx, "--cluster", "add-node", netAddress(nodeToAdd.IP, port), netAddress(clusterNode.IP, port))
	if err == nil {
		if rc != 0 {
			err = fmt.Errorf("command exit code was %d", rc)
		} else if !utils.ContainsLine(stdout, constants.OKAddNode) {
			err = fmt.Errorf("output does not indicate success: %s", stdout)
		}
	}
	return err
}

// Meet introduces two nodes to each other via CLUSTER MEET.
// The 'from' node will be told to meet the 'to' node.
func (t *Toolbox) Meet(fromIP, toIP string, port int) error {
	if err := t.validate(); err != nil {
		return err
	}
	return t.meet(t.ctx, fromIP, toIP, port)
}

func (t *Toolbox) meet(ctx context.Context, fromIP, toIP string, port int) error {
	if err := t.validate(); err != nil {
		return err
	}
	t.countOperation("meet")
	if err := validateAddress(toIP); err != nil {
		return err
	}
	rc, _, _, err := t.runRedis(ctx, fromIP, port, "CLUSTER", "MEET", toIP, strconv.Itoa(port))
	if err == nil && rc != 0 {
		err = fmt.Errorf("command exit code was %d", rc)
	}
	if err != nil {
		return utils.WrapErr(err, "failed to meet")
	}
	return nil
}

// MeetCluster creates a cluster from restored nodes.
func (t *Toolbox) MeetCluster(cluster *Cluster) error {
	if err := t.validate(); err != nil {
		return err
	}
	if cluster == nil || cluster.Timeouts == nil {
		return errors.New("Redis cluster and timeouts are required")
	}
	t.countOperation("meet_cluster")
	if len(cluster.Nodes) < 1 {
		// nothing to do
		return nil
	}
	if len(cluster.AllNodes) == 0 {
		return errors.New("Redis cluster has no nodes to meet")
	}

	// func to introduce all nodes in the cluster
	// clusters do not need to meet reciprocally, but all nodes must meet other nodes once

	// meet each node with the next one (and the last one with the first one)
	meetAll := func(ctx context.Context) bool {
		for i := range cluster.AllNodes {
			if cluster.AllNodes[i] == nil || cluster.AllNodes[(i+1)%len(cluster.AllNodes)] == nil {
				return false
			}
			fromIP := cluster.AllNodes[i].IP
			toIP := cluster.AllNodes[(i+1)%len(cluster.AllNodes)].IP
			if err := t.meet(ctx, fromIP, toIP, cluster.RedisPort); err != nil {
				t.Log.Error(err, "Failed to meet node", "from", fromIP, "to", toIP)
				return false
			}
		}
		return true
	}

	// Restored pods can take time to accept CLUSTER MEET. Bind the polling loop
	// to the reconcile context so manager shutdown does not wait for the full
	// operation timeout.
	err := wait.PollUntilContextTimeout(
		t.ctx,
		cluster.Timeouts.PollInterval,
		cluster.Timeouts.OperationTimeout,
		true,
		func(ctx context.Context) (bool, error) { return meetAll(ctx), nil },
	)
	if err != nil {
		if ctxErr := t.ctx.Err(); ctxErr != nil {
			return fmt.Errorf("meeting restored cluster nodes canceled: %w", ctxErr)
		}
		t.Log.Info("Failed to introduce all cluster nodes")
		t.countBadSituation("could_not_meet_peers")
		return fmt.Errorf(
			"failed to meet all pods within %v: %w",
			cluster.Timeouts.OperationTimeout,
			err,
		)
	}

	return nil
}

// CreateCluster creates a cluster from new nodes.
func (t *Toolbox) CreateCluster(cluster *Cluster) error {
	if err := t.validate(); err != nil {
		return err
	}
	if cluster == nil || cluster.RedisCluster == nil {
		return errors.New("Redis cluster is required")
	}
	t.countOperation("create_cluster")
	replicas := int(cluster.RedisCluster.Spec.Replicas)
	if replicas < 0 {
		return errors.New("Redis replica count must not be negative")
	}
	args := []string{"--cluster", "create"}
	for _, node := range cluster.AllNodes {
		if node == nil {
			return errors.New("Redis cluster node is required")
		}
		if err := validateAddress(node.IP); err != nil {
			return err
		}
		args = append(args, netAddress(node.IP, cluster.RedisPort))
	}
	if err := validatePort(cluster.RedisPort); err != nil {
		return err
	}
	args = append(args, "--cluster-replicas", strconv.Itoa(replicas), "--cluster-yes")
	rc, stdout, stderr, err := t.runRedisCluster(t.ctx, args...)
	if err == nil && rc != 0 {
		err = fmt.Errorf("command exit code was %d, stdout=%s, stderr=%s", rc, strings.TrimSpace(stdout), strings.TrimSpace(stderr))
	}
	return err
}

func rebalanceEntryPoint(cluster *Cluster) (string, error) {
	if cluster == nil {
		return "", errors.New("Redis cluster is required")
	}
	if err := validatePort(cluster.RedisPort); err != nil {
		return "", err
	}
	for _, node := range cluster.Nodes[PrimaryNode] {
		if node == nil {
			return "", errors.New("Redis primary node is required")
		}
		if node.IP != "" {
			if err := validateAddress(node.IP); err != nil {
				return "", err
			}
			return netAddress(node.IP, cluster.RedisPort), nil
		}
	}
	if cluster.RedisCluster == nil || cluster.Toolbox == nil {
		return "", errors.New("Redis cluster metadata and toolbox are required")
	}
	address := cluster.RedisCluster.Name + "." + cluster.Toolbox.Namespace
	if err := validateAddress(address); err != nil {
		return "", err
	}
	return netAddress(address, cluster.RedisPort), nil
}

// Rebalance rebalances the cluster, optionally using non-empty masters.
func (t *Toolbox) Rebalance(cluster *Cluster, useEmptyMasters bool) error {
	if err := t.validate(); err != nil {
		return err
	}
	t.countOperation("rebalance")
	entryPoint, err := rebalanceEntryPoint(cluster)
	if err != nil {
		return err
	}
	args := []string{"--cluster", "rebalance", entryPoint}
	if useEmptyMasters {
		args = append(args, "--cluster-use-empty-masters")
	}
	rc, _, _, err := t.runRedisCluster(t.ctx, args...)
	if err == nil && rc != 0 {
		err = fmt.Errorf("command exit code was %d", rc)
	}
	return err
}

// NodeWeight assigns a nonnegative rebalance weight to one Redis node.
type NodeWeight struct {
	NodeID string
	Weight float64
}

// RebalanceWeighted rebalances the cluster with typed weights, optionally using empty masters.
func (t *Toolbox) RebalanceWeighted(cluster *Cluster, weights []NodeWeight, useEmptyMasters bool) error {
	if err := t.validate(); err != nil {
		return err
	}
	t.countOperation("rebalance_weighted")
	entryPoint, err := rebalanceEntryPoint(cluster)
	if err != nil {
		return err
	}
	args := []string{"--cluster", "rebalance", entryPoint}
	if useEmptyMasters {
		args = append(args, "--cluster-use-empty-masters")
	}
	args = append(args, "--cluster-weight")
	for _, weight := range weights {
		if validateNodeID(weight.NodeID) != nil || weight.Weight < 0 || math.IsInf(weight.Weight, 0) || math.IsNaN(weight.Weight) {
			return fmt.Errorf("invalid Redis cluster weight for node %q", weight.NodeID)
		}
		args = append(args, weight.NodeID+"="+strconv.FormatFloat(weight.Weight, 'g', -1, 64))
	}
	rc, _, _, err := t.runRedisCluster(t.ctx, args...)
	if err == nil && rc != 0 {
		err = fmt.Errorf("command exit code was %d", rc)
	}
	return err
}

// FixCluster repairs transient open slots left by interrupted resharding.
func (t *Toolbox) FixCluster(cluster *Cluster) error {
	if err := t.validate(); err != nil {
		return err
	}
	t.countOperation("fix_cluster")
	entryPoint, err := rebalanceEntryPoint(cluster)
	if err != nil {
		return err
	}
	rc, stdout, stderr, err := t.runRedisCluster(t.ctx, "--cluster", "fix", entryPoint, "--cluster-yes")
	if err == nil && rc != 0 {
		err = fmt.Errorf("command exit code was %d, stdout=%s, stderr=%s", rc, strings.TrimSpace(stdout), strings.TrimSpace(stderr))
	}
	return err
}

// Failover fails over a replica.
func (t *Toolbox) Failover(replicaNode *ClusterNode, force bool, takeover bool) error {
	if err := t.validate(); err != nil {
		return err
	}
	return t.FailoverContext(t.ctx, replicaNode, force, takeover)
}

// FailoverContext fails over a replica with a caller-owned deadline.
func (t *Toolbox) FailoverContext(ctx context.Context, replicaNode *ClusterNode, force bool, takeover bool) error {
	if err := t.validate(); err != nil {
		return err
	}
	if err := validateClusterNode(replicaNode); err != nil {
		return err
	}
	if force && takeover {
		return errors.New("Redis failover mode cannot be both force and takeover")
	}
	start := time.Now()
	t.countOperation("failover")

	opt := ""
	if force {
		opt = "FORCE" // no prior handshake with master, but wants overall cluster agreement (clean Failover)
	}
	if takeover {
		opt = "TAKEOVER" // bypass cluster agreement entirely (hard Failover)
	}

	t.Log.Info("Initiating CLUSTER FAILOVER",
		"replicaNodeID", replicaNode.ID,
		"replicaPod", replicaNode.PodName,
		"mode", opt)

	// note than an OK response does *not* mean that the Failover actually happened as the cluster may disagree
	args := []string{"CLUSTER", "FAILOVER"}
	if opt != "" {
		args = append(args, opt)
	}
	rc, stdout, _, err := t.runRedis(ctx, replicaNode.IP, t.RedisPort, args...)

	duration := time.Since(start).Milliseconds()

	if err == nil {
		if rc != 0 {
			err = fmt.Errorf("command exit code was %d", rc)
		} else if !utils.ContainsLine(stdout, "OK") {
			err = fmt.Errorf("output does not indicate success: %s", stdout)
		}
	}

	if err != nil {
		t.Log.Error(err, "CLUSTER FAILOVER command failed",
			"replicaNodeID", replicaNode.ID,
			"mode", opt,
			"duration_ms", duration,
			"exitCode", rc)
	} else {
		t.Log.Info("CLUSTER FAILOVER command accepted",
			"replicaNodeID", replicaNode.ID,
			"mode", opt,
			"duration_ms", duration)
	}

	return err
}

// Replicates instructs a node which should already be a replicate, to replicate another node.
func (t *Toolbox) Replicates(replicaNode *ClusterNode, primaryNode *ClusterNode) error {
	if err := t.validate(); err != nil {
		return err
	}
	if err := validateClusterNode(replicaNode); err != nil {
		return err
	}
	if primaryNode == nil {
		return errors.New("Redis primary node is required")
	}
	t.countOperation("replicate")
	if err := validateNodeID(primaryNode.ID); err != nil {
		return err
	}
	rc, stdout, stderr, err := t.runRedis(t.ctx, replicaNode.IP, t.RedisPort, "CLUSTER", "REPLICATE", primaryNode.ID)
	if err == nil {
		if rc != 0 {
			err = fmt.Errorf("command exit code was %d, stdout=%s, stderr=%s", rc, strings.TrimSpace(stdout), strings.TrimSpace(stderr))
		} else if !utils.ContainsLine(stdout, "OK") {
			err = fmt.Errorf("output does not indicate success: %s", stdout)
		}
	}
	return err
}

// AddSlot adds one slot to a primary node.
func (t *Toolbox) AddSlot(node *ClusterNode, slot int) error {
	if err := t.validate(); err != nil {
		return err
	}
	if err := validateClusterNode(node); err != nil {
		return err
	}
	t.countOperation("add_slot")
	if err := validateSlot(slot); err != nil {
		return err
	}
	rc, stdout, _, err := t.runRedis(t.ctx, node.IP, t.RedisPort, "CLUSTER", "ADDSLOTS", strconv.Itoa(slot))
	if err == nil {
		if rc != 0 {
			err = fmt.Errorf("command exit code was %d", rc)
		} else if !utils.ContainsLine(stdout, "OK") {
			err = fmt.Errorf("output does not indicate success: %s", stdout)
		}
	}
	return err
}

// AddSlots adds a range of slots to a primary node.
func (t *Toolbox) AddSlots(node *ClusterNode, firstSlot int, lastSlot int) error {
	if err := t.validate(); err != nil {
		return err
	}
	if err := validateClusterNode(node); err != nil {
		return err
	}
	t.countOperation("add_slots")
	if err := validateSlot(firstSlot); err != nil {
		return err
	}
	if err := validateSlot(lastSlot); err != nil {
		return err
	}
	if firstSlot > lastSlot {
		return fmt.Errorf("invalid Redis slot range %d-%d", firstSlot, lastSlot)
	}
	rc, stdout, _, err := t.runRedis(t.ctx, node.IP, t.RedisPort, "CLUSTER", "ADDSLOTSRANGE", strconv.Itoa(firstSlot), strconv.Itoa(lastSlot))
	if err == nil {
		if rc != 0 {
			err = fmt.Errorf("command exit code was %d", rc)
		} else if !utils.ContainsLine(stdout, "OK") {
			err = fmt.Errorf("output does not indicate success: %s", stdout)
		}
	}
	return err
}

// GetReplicationInfo returns the INFO replication output for a node identified by IP.
func (t *Toolbox) GetReplicationInfo(ip string) (string, error) {
	if err := t.validate(); err != nil {
		return "", err
	}
	return t.GetReplicationInfoContext(t.ctx, ip)
}

// GetReplicationInfoContext returns INFO replication with a caller-owned deadline.
func (t *Toolbox) GetReplicationInfoContext(ctx context.Context, ip string) (string, error) {
	if err := t.validate(); err != nil {
		return "", err
	}
	rc, stdout, _, err := t.runRedis(ctx, ip, t.RedisPort, "INFO", "replication")
	if err == nil && rc != 0 {
		err = fmt.Errorf("command exit code was %d", rc)
	}
	return stdout, err
}

// GetNodeIDByIP returns the Redis node ID of a node at the given IP by parsing CLUSTER MYID.
func (t *Toolbox) GetNodeIDByIP(ip string) (string, error) {
	if err := t.validate(); err != nil {
		return "", err
	}
	return t.GetNodeIDByIPContext(t.ctx, ip)
}

// GetNodeIDByIPContext returns CLUSTER MYID with a caller-owned deadline.
func (t *Toolbox) GetNodeIDByIPContext(ctx context.Context, ip string) (string, error) {
	if err := t.validate(); err != nil {
		return "", err
	}
	return t.GetNodeIDByAddressContext(ctx, ip)
}

// GetNodeIDByAddressContext returns CLUSTER MYID from an IP address or trusted
// DNS hostname with a caller-owned deadline.
func (t *Toolbox) GetNodeIDByAddressContext(ctx context.Context, address string) (string, error) {
	if err := t.validate(); err != nil {
		return "", err
	}
	rc, stdout, _, err := t.runRedis(ctx, address, t.RedisPort, "CLUSTER", "MYID")
	if err == nil && rc != 0 {
		err = fmt.Errorf("command exit code was %d", rc)
	}
	if err != nil {
		return "", err
	}
	id := strings.TrimSpace(stdout)
	if err := validateNodeID(id); err != nil {
		return "", fmt.Errorf("CLUSTER MYID returned an invalid node ID: %w", err)
	}
	return id, nil
}

// ReplicateOnIP tells a node (by IP) to CLUSTER REPLICATE a primary.
func (t *Toolbox) ReplicateOnIP(replicaIP string, primaryID string) error {
	if err := t.validate(); err != nil {
		return err
	}
	t.countOperation("replicate_ip")
	if err := validateNodeID(primaryID); err != nil {
		return err
	}
	rc, stdout, stderr, err := t.runRedis(t.ctx, replicaIP, t.RedisPort, "CLUSTER", "REPLICATE", primaryID)
	if err == nil {
		if rc != 0 {
			err = fmt.Errorf("command exit code was %d, stdout=%s, stderr=%s", rc, strings.TrimSpace(stdout), strings.TrimSpace(stderr))
		} else if !utils.ContainsLine(stdout, "OK") {
			err = fmt.Errorf("output does not indicate success: %s", stdout)
		}
	}
	return err
}

// DBSize returns the number of keys in the node's keyspace via redis-cli DBSIZE.
// Used to refuse a destructive CLUSTER RESET HARD on a node that still holds data.
func (t *Toolbox) DBSize(ip string) (int, error) {
	if err := t.validate(); err != nil {
		return 0, err
	}
	rc, stdout, _, err := t.runRedis(t.ctx, ip, t.RedisPort, "DBSIZE")
	if err == nil && rc != 0 {
		err = fmt.Errorf("command exit code was %d", rc)
	}
	if err != nil {
		return 0, err
	}
	n, perr := strconv.Atoi(strings.TrimSpace(stdout))
	if perr != nil {
		return 0, fmt.Errorf("failed to parse DBSIZE output %q: %w", strings.TrimSpace(stdout), perr)
	}
	return n, nil
}

// PingIP checks if a Redis node at the given IP is responding.
func (t *Toolbox) PingIP(ip string) bool {
	if err := t.validate(); err != nil {
		return false
	}
	rc, stdout, _, err := t.runRedis(t.ctx, ip, t.RedisPort, "PING")
	return err == nil && rc == 0 && strings.Contains(stdout, "PONG")
}

func netAddress(address string, port int) string {
	if strings.Contains(address, ":") {
		return "[" + address + "]:" + strconv.Itoa(port)
	}
	return address + ":" + strconv.Itoa(port)
}

// ClusterNodes returns CLUSTER NODES output for an address.
func (t *Toolbox) ClusterNodes(ctx context.Context, address string) (int, string, string, error) {
	if err := t.validate(); err != nil {
		return 0, "", "", err
	}
	return t.runRedis(ctx, address, t.RedisPort, "CLUSTER", "NODES")
}

// ClusterInfo returns CLUSTER INFO output for an address.
func (t *Toolbox) ClusterInfo(ctx context.Context, address string) (int, string, string, error) {
	if err := t.validate(); err != nil {
		return 0, "", "", err
	}
	return t.runRedis(ctx, address, t.RedisPort, "CLUSTER", "INFO")
}

// ConfigureReplicaMigration changes the setting on one address.
func (t *Toolbox) ConfigureReplicaMigration(ctx context.Context, address string, enabled bool) (int, string, string, error) {
	if err := t.validate(); err != nil {
		return 0, "", "", err
	}
	value := "no"
	if enabled {
		value = "yes"
	}
	return t.runRedis(ctx, address, t.RedisPort, "CONFIG", "SET", "cluster-allow-replica-migration", value)
}

// ClientPause pauses clients for a validated duration.
func (t *Toolbox) ClientPause(ctx context.Context, address string, duration time.Duration) (int, string, string, error) {
	if err := t.validate(); err != nil {
		return 0, "", "", err
	}
	if duration <= 0 {
		return 0, "", "", fmt.Errorf("invalid client pause duration %s", duration)
	}
	return t.runRedis(ctx, address, t.RedisPort, "CLIENT", "PAUSE", strconv.FormatInt(duration.Milliseconds(), 10), "ALL")
}

func (t *Toolbox) ClientUnpause(ctx context.Context, address string) (int, string, string, error) {
	if err := t.validate(); err != nil {
		return 0, "", "", err
	}
	return t.runRedis(ctx, address, t.RedisPort, "CLIENT", "UNPAUSE")
}

func (t *Toolbox) ClientKillLocal(ctx context.Context, address string) (int, string, string, error) {
	if err := t.validate(); err != nil {
		return 0, "", "", err
	}
	if err := validateAddress(address); err != nil {
		return 0, "", "", err
	}
	return t.runRedis(ctx, address, t.RedisPort, "CLIENT", "KILL", "LADDR", netAddress(address, t.RedisPort))
}

func (t *Toolbox) MeetAddress(ctx context.Context, from, to string, port int) (int, string, string, error) {
	if err := t.validate(); err != nil {
		return 0, "", "", err
	}
	if err := validateAddress(to); err != nil {
		return 0, "", "", err
	}
	return t.runRedis(ctx, from, port, "CLUSTER", "MEET", to, strconv.Itoa(port))
}

func (t *Toolbox) FlushAll(node *ClusterNode) (int, string, string, error) {
	if err := t.validate(); err != nil {
		return 0, "", "", err
	}
	if err := validateClusterNode(node); err != nil {
		return 0, "", "", err
	}
	return t.runRedis(t.ctx, node.IP, t.RedisPort, "FLUSHALL")
}

func (t *Toolbox) PersistenceInfo(node *ClusterNode) (int, string, string, error) {
	if err := t.validate(); err != nil {
		return 0, "", "", err
	}
	if err := validateClusterNode(node); err != nil {
		return 0, "", "", err
	}
	return t.runRedis(t.ctx, node.IP, t.RedisPort, "INFO", "persistence")
}

func (t *Toolbox) LastSave(node *ClusterNode) (int, string, string, error) {
	if err := t.validate(); err != nil {
		return 0, "", "", err
	}
	if err := validateClusterNode(node); err != nil {
		return 0, "", "", err
	}
	return t.runRedis(t.ctx, node.IP, t.RedisPort, "LASTSAVE")
}

func (t *Toolbox) BGSave(node *ClusterNode) (int, string, string, error) {
	if err := t.validate(); err != nil {
		return 0, "", "", err
	}
	if err := validateClusterNode(node); err != nil {
		return 0, "", "", err
	}
	return t.runRedis(t.ctx, node.IP, t.RedisPort, "BGSAVE")
}

func (t *Toolbox) ShutdownNoSave(node *ClusterNode) (int, string, string, error) {
	if err := t.validate(); err != nil {
		return 0, "", "", err
	}
	if err := validateClusterNode(node); err != nil {
		return 0, "", "", err
	}
	return t.runRedis(t.ctx, node.IP, t.RedisPort, "SHUTDOWN", "NOSAVE")
}

func (t *Toolbox) countOperation(name string) {
	if t != nil && t.Metrics != nil {
		_ = t.Metrics.CountToolboxOperation(name)
	}
}

func (t *Toolbox) countBadSituation(name string) {
	if t != nil && t.Metrics != nil {
		_ = t.Metrics.CountBadSituation(name)
	}
}
