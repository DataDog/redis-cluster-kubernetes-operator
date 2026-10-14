// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package controller

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	corev1 "k8s.io/api/core/v1"
)

const (
	redisServiceNameEnv                 = "REDIS_SERVICE_NAME"
	kubernetesServiceDNSSuffix          = "svc"
	primaryEndpointProbeTimeout         = 3 * time.Second
	primaryEndpointSuccessInterval      = 2 * time.Second
	primaryEndpointRequiredSuccesses    = 3
	primaryEndpointNotReadyMetricReason = "reason:"
	primaryEndpointNotReadyBadSituation = "primary_endpoint_not_ready"
)

type primaryEndpointReadinessReason string

const (
	primaryEndpointReasonIPEndpoint                primaryEndpointReadinessReason = "ip_endpoint"
	primaryEndpointReasonAnnouncedHostnameMismatch primaryEndpointReadinessReason = "announced_hostname_mismatch"
	primaryEndpointReasonProbeError                primaryEndpointReadinessReason = "probe_error"
	primaryEndpointReasonResolvedIPMismatch        primaryEndpointReadinessReason = "resolved_ip_mismatch"
	primaryEndpointReasonNodeIDMismatch            primaryEndpointReadinessReason = "node_id_mismatch"
	primaryEndpointReasonStabilizing               primaryEndpointReadinessReason = "stabilizing"
	primaryEndpointReasonReady                     primaryEndpointReadinessReason = "ready"
)

type primaryEndpointProbeResult struct {
	ResolvedIPs []string
	NodeID      string
}

type primaryEndpointProber interface {
	Probe(
		ctx context.Context,
		cluster *redis_cluster.Cluster,
		hostname string,
	) (primaryEndpointProbeResult, error)
}

// PrimaryEndpointResolver derives the Redis client hostname that must pass the
// identity stability gate. The portable default uses Kubernetes Service DNS;
// private environments may inject a different resolver.
type PrimaryEndpointResolver interface {
	ResolveHostname(
		cluster *redis_cluster.Cluster,
		node *redis_cluster.ClusterNode,
		pod *corev1.Pod,
	) (string, bool)
}

// KubernetesServiceDNSResolver derives <pod>.<service>.<namespace>.svc.
type KubernetesServiceDNSResolver struct{}

func (KubernetesServiceDNSResolver) ResolveHostname(
	cluster *redis_cluster.Cluster,
	node *redis_cluster.ClusterNode,
	pod *corev1.Pod,
) (string, bool) {
	return expectedRedisPodHostname(cluster, node, pod)
}

type lookupIPAddrResolver interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
}

type redisPrimaryEndpointProber struct {
	resolver lookupIPAddrResolver
}

func (p redisPrimaryEndpointProber) Probe(
	ctx context.Context,
	cluster *redis_cluster.Cluster,
	hostname string,
) (primaryEndpointProbeResult, error) {
	resolver := p.resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}

	lookupCtx, lookupCancel := context.WithTimeout(ctx, primaryEndpointProbeTimeout)
	addresses, err := resolver.LookupIPAddr(lookupCtx, hostname)
	lookupCancel()
	if err != nil {
		return primaryEndpointProbeResult{}, fmt.Errorf("resolve %s: %w", hostname, err)
	}

	result := primaryEndpointProbeResult{
		ResolvedIPs: uniqueSortedIPStrings(addresses),
	}
	if len(result.ResolvedIPs) == 0 {
		return result, nil
	}
	if cluster.Toolbox == nil {
		return result, fmt.Errorf("probe Redis identity at %s: toolbox is unavailable", hostname)
	}

	identityCtx, identityCancel := context.WithTimeout(ctx, primaryEndpointProbeTimeout)
	result.NodeID, err = cluster.Toolbox.GetNodeIDByAddressContext(identityCtx, hostname)
	identityCancel()
	if err != nil {
		return result, fmt.Errorf("probe Redis identity at %s: %w", hostname, err)
	}
	return result, nil
}

func uniqueSortedIPStrings(addresses []net.IPAddr) []string {
	unique := make(map[string]struct{}, len(addresses))
	for _, address := range addresses {
		parsed, ok := canonicalIP(address.IP.String())
		if ok {
			unique[parsed] = struct{}{}
		}
	}
	result := make([]string, 0, len(unique))
	for address := range unique {
		result = append(result, address)
	}
	sort.Strings(result)
	return result
}

type primaryEndpointObservation struct {
	identity    string
	successes   int
	lastSuccess time.Time
}

type primaryEndpointStability struct {
	Successes  int
	Ready      bool
	RetryAfter time.Duration
}

// primaryEndpointReadinessTracker is process-local by design. Losing its state
// during an operator restart is fail-safe: the pod must pass the DNS gate again.
type primaryEndpointReadinessTracker struct {
	mu           sync.Mutex
	observations map[string]primaryEndpointObservation
}

func (t *primaryEndpointReadinessTracker) Observe(
	podKey string,
	identity string,
	success bool,
	now time.Time,
) primaryEndpointStability {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.observations == nil {
		t.observations = make(map[string]primaryEndpointObservation)
	}
	if !success {
		delete(t.observations, podKey)
		return primaryEndpointStability{RetryAfter: primaryEndpointSuccessInterval}
	}

	observation, found := t.observations[podKey]
	if !found || observation.identity != identity || now.Before(observation.lastSuccess) {
		t.observations[podKey] = primaryEndpointObservation{
			identity:    identity,
			successes:   1,
			lastSuccess: now,
		}
		return primaryEndpointStability{
			Successes:  1,
			RetryAfter: primaryEndpointSuccessInterval,
		}
	}
	if observation.successes >= primaryEndpointRequiredSuccesses {
		return primaryEndpointStability{
			Successes: observation.successes,
			Ready:     true,
		}
	}

	elapsed := now.Sub(observation.lastSuccess)
	if elapsed < primaryEndpointSuccessInterval {
		return primaryEndpointStability{
			Successes:  observation.successes,
			RetryAfter: primaryEndpointSuccessInterval - elapsed,
		}
	}

	observation.successes++
	observation.lastSuccess = now
	t.observations[podKey] = observation
	return primaryEndpointStability{
		Successes:  observation.successes,
		Ready:      observation.successes >= primaryEndpointRequiredSuccesses,
		RetryAfter: primaryEndpointSuccessInterval,
	}
}

func (t *primaryEndpointReadinessTracker) Reset(podKey string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.observations, podKey)
}

type primaryEndpointReadinessStatus struct {
	Hostname          string
	AnnouncedHostname string
	ExpectedIPs       []string
	ResolvedIPs       []string
	ExpectedNodeID    string
	ObservedNodeID    string
	Successes         int
	RetryAfter        time.Duration
	Reason            primaryEndpointReadinessReason
	ProbeErr          error
}

func (r *RedisClusterReconciler) primaryEndpointReady(
	ctx context.Context,
	cluster *redis_cluster.Cluster,
	primary *redis_cluster.ClusterNode,
	pod *corev1.Pod,
) (bool, primaryEndpointReadinessStatus) {
	resolver := r.PrimaryEndpointResolver
	if optionalProviderIsNil(resolver) {
		resolver = KubernetesServiceDNSResolver{}
	}
	hostname, hostnameEndpointsEnabled := resolver.ResolveHostname(cluster, primary, pod)
	status := primaryEndpointReadinessStatus{
		Hostname:          hostname,
		AnnouncedHostname: primary.Hostname,
		ExpectedIPs:       expectedPodIPs(pod),
		ExpectedNodeID:    primary.ID,
	}
	if !hostnameEndpointsEnabled {
		r.endpointTracker().Reset(primaryEndpointPodKey(pod))
		status.Reason = primaryEndpointReasonIPEndpoint
		return true, status
	}

	probeSucceeded := false
	if !strings.EqualFold(strings.TrimSuffix(primary.Hostname, "."), hostname) {
		status.Reason = primaryEndpointReasonAnnouncedHostnameMismatch
	} else {
		prober := r.primaryEndpointProber
		if prober == nil {
			prober = redisPrimaryEndpointProber{}
		}
		result, err := prober.Probe(ctx, cluster, hostname)
		status.ResolvedIPs = result.ResolvedIPs
		status.ObservedNodeID = result.NodeID
		status.ProbeErr = err
		switch {
		case err != nil:
			status.Reason = primaryEndpointReasonProbeError
		case !resolvedIPsMatchPod(result.ResolvedIPs, pod):
			status.Reason = primaryEndpointReasonResolvedIPMismatch
		case result.NodeID != primary.ID:
			status.Reason = primaryEndpointReasonNodeIDMismatch
		default:
			status.Reason = primaryEndpointReasonStabilizing
			probeSucceeded = true
		}
	}

	stability := r.endpointTracker().Observe(
		primaryEndpointPodKey(pod),
		primaryEndpointIdentity(primary, pod, hostname),
		probeSucceeded,
		r.endpointNow(),
	)
	status.Successes = stability.Successes
	status.RetryAfter = stability.RetryAfter
	if stability.Ready {
		status.Reason = primaryEndpointReasonReady
	}
	return stability.Ready, status
}

func (r *RedisClusterReconciler) resetPrimaryEndpointReadiness(pod *corev1.Pod) {
	if pod != nil {
		r.endpointTracker().Reset(primaryEndpointPodKey(pod))
	}
}

func (r *RedisClusterReconciler) endpointTracker() *primaryEndpointReadinessTracker {
	if r.primaryEndpointReadiness == nil {
		r.primaryEndpointReadiness = &primaryEndpointReadinessTracker{}
	}
	return r.primaryEndpointReadiness
}

func (r *RedisClusterReconciler) endpointNow() time.Time {
	if r.primaryEndpointNow != nil {
		return r.primaryEndpointNow()
	}
	return time.Now()
}

func expectedRedisPodHostname(
	cluster *redis_cluster.Cluster,
	primary *redis_cluster.ClusterNode,
	pod *corev1.Pod,
) (string, bool) {
	serviceName, _ := redisServiceNameFromPod(pod)
	if serviceName == "" && cluster.StatefulSet != nil {
		serviceName = cluster.StatefulSet.Spec.ServiceName
	}
	if serviceName == "" {
		serviceName = cluster.RedisCluster.Name
	}
	return fmt.Sprintf(
		"%s.%s.%s.%s",
		pod.Name,
		serviceName,
		pod.Namespace,
		kubernetesServiceDNSSuffix,
	), true
}

func redisServiceNameFromPod(pod *corev1.Pod) (string, bool) {
	for _, container := range pod.Spec.Containers {
		if container.Name != constants.RedisContainerName {
			continue
		}
		for _, env := range container.Env {
			if env.Name == redisServiceNameEnv {
				return env.Value, true
			}
		}
	}
	return "", false
}

func primaryEndpointPodKey(pod *corev1.Pod) string {
	return pod.Namespace + "/" + pod.Name
}

func primaryEndpointIdentity(
	primary *redis_cluster.ClusterNode,
	pod *corev1.Pod,
	hostname string,
) string {
	return strings.Join([]string{
		string(pod.UID),
		pod.Status.PodIP,
		hostname,
		primary.ID,
	}, "\x00")
}

func expectedPodIPs(pod *corev1.Pod) []string {
	unique := make(map[string]struct{}, len(pod.Status.PodIPs)+1)
	if parsed, ok := canonicalIP(pod.Status.PodIP); ok {
		unique[parsed] = struct{}{}
	}
	for _, podIP := range pod.Status.PodIPs {
		if parsed, ok := canonicalIP(podIP.IP); ok {
			unique[parsed] = struct{}{}
		}
	}
	result := make([]string, 0, len(unique))
	for address := range unique {
		result = append(result, address)
	}
	sort.Strings(result)
	return result
}

func resolvedIPsMatchPod(resolvedIPs []string, pod *corev1.Pod) bool {
	expected := expectedPodIPs(pod)
	if len(expected) == 0 || len(resolvedIPs) == 0 {
		return false
	}

	expectedSet := make(map[string]struct{}, len(expected))
	for _, address := range expected {
		expectedSet[address] = struct{}{}
	}
	primaryIP, primaryOK := canonicalIP(pod.Status.PodIP)
	primaryFound := false
	for _, resolvedIP := range resolvedIPs {
		address, ok := canonicalIP(resolvedIP)
		if !ok {
			return false
		}
		if _, ok := expectedSet[address]; !ok {
			return false
		}
		if primaryOK && address == primaryIP {
			primaryFound = true
		}
	}
	return primaryOK && primaryFound
}

func canonicalIP(value string) (string, bool) {
	address, err := netip.ParseAddr(value)
	if err != nil {
		return "", false
	}
	return address.Unmap().String(), true
}
