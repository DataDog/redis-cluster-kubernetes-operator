package redis_cluster

import (
	"context"
	"crypto/sha256"
	"fmt"
	"maps"
	"regexp"
	"sort"
	"strings"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics/counters"
	"github.com/DataDog/redis-cluster-kubernetes-operator/utils"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	requiredPodListLabelNames = []string{"app", "redis_cluster_name"}
)

const redisServiceNameEnv = "REDIS_SERVICE_NAME"

// BlueGreenPeerServiceName returns the private headless Service used for Redis
// node identity and gossip during a rollout. It is deliberately distinct from
// the client-facing Service, including when the alternating StatefulSet name is
// the original cluster name.
func BlueGreenPeerServiceName(statefulSetName string) string {
	const suffix = "-peer"
	if len(statefulSetName)+len(suffix) <= 63 {
		return statefulSetName + suffix
	}
	sum := sha256.Sum256([]byte(statefulSetName))
	prefix := strings.TrimRight(statefulSetName[:49], "-")
	return fmt.Sprintf("%s-%x%s", prefix, sum[:4], suffix)
}

func effectiveStatefulSetName(redisCluster *cachev1.RedisCluster) string {
	if redisCluster.Status.ActiveClusterName != "" {
		return redisCluster.Status.ActiveClusterName
	}
	if redisCluster.Spec.StatefulsetTemplate.Name != "" {
		return redisCluster.Spec.StatefulsetTemplate.Name
	}
	return redisCluster.Name
}

// ForCluster specializes a ClusterClient for a specified Redis cluster.
func (cc *ClusterClient) ForCluster(name types.NamespacedName) ClusterClient {
	return ClusterClient{
		Client:             cc,
		ClusterScheme:      cc.ClusterScheme,
		RestConfig:         cc.RestConfig,
		Namespace:          cc.Namespace,
		Log:                cc.Log.WithValues("redis-cluster", name),
		Metrics:            cc.Metrics.WithRedisCluster(name.Name),
		CredentialProvider: cc.CredentialProvider,
	}
}

// GetOrCreateStatefulSet gets the StatefulSet resource or creates it if it does not exist yet.
func (cc *ClusterClient) GetOrCreateStatefulSet(ctx context.Context, redisCluster *cachev1.RedisCluster) (*appsv1.StatefulSet, error) {
	nsn := types.NamespacedName{
		Name:      effectiveStatefulSetName(redisCluster),
		Namespace: redisCluster.Namespace,
	}
	sts := &appsv1.StatefulSet{}
	if err := cc.Get(ctx, nsn, sts); err != nil {
		if errors.IsNotFound(err) {
			sts, err = cc.createNewStatefulSet(ctx, redisCluster)
			if err != nil {
				return nil, utils.WrapErr(err, "failed to create the StatefulSet resource")
			}
			return sts, nil
		}
		cc.Log.Error(err, "Failed to get the StatefulSet resource")
		return nil, utils.WrapErr(err, "failed to get the StatefulSet resource")
	}
	cc.Log.Info("Got the StatefulSet resource")
	return sts, nil
}

// GetStatefulSet gets the StatefulSet resource.
func (cc *ClusterClient) GetStatefulSet(ctx context.Context, redisCluster *cachev1.RedisCluster) (*appsv1.StatefulSet, error) {
	nsn := types.NamespacedName{
		Name:      effectiveStatefulSetName(redisCluster),
		Namespace: redisCluster.Namespace,
	}
	sts := &appsv1.StatefulSet{}
	if err := cc.Get(ctx, nsn, sts); err != nil {
		cc.Log.Error(err, "Failed to get the StatefulSet resource")
		return nil, utils.WrapErr(err, "failed to get the StatefulSet resource")
	}
	cc.Log.Info("Got the StatefulSet resource")
	return sts, nil
}

// GetStatefulSetByName gets a StatefulSet resource by namespace and name.
// Returns nil without error if the StatefulSet does not exist.
func (cc *ClusterClient) GetStatefulSetByName(ctx context.Context, namespace, name string) (*appsv1.StatefulSet, error) {
	nsn := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}
	sts := &appsv1.StatefulSet{}
	if err := cc.Get(ctx, nsn, sts); err != nil {
		if errors.IsNotFound(err) {
			cc.Log.Info("StatefulSet not found", "namespace", namespace, "name", name)
			return nil, nil
		}
		cc.Log.Error(err, "Failed to get StatefulSet", "namespace", namespace, "name", name)
		return nil, utils.WrapErr(err, "failed to get StatefulSet")
	}
	return sts, nil
}

// createNewStatefulSet creates a new Redis cluster StatefulSet resource.
func (cc *ClusterClient) createNewStatefulSet(ctx context.Context, redisCluster *cachev1.RedisCluster) (*appsv1.StatefulSet, error) {
	// Define a new statefulSet
	sts, err := cc.getStatefulSetForRedisCluster(redisCluster)
	if err != nil {
		cc.Log.Error(err, "Failed to get the StatefulSet template")
		return nil, utils.WrapErr(err, "failed to get the StatefulSet template")
	}
	cc.Log.Info("Create a new StatefulSet", "statefulSetNamespace", sts.Namespace, "statefulSetName", sts.Name)
	if redisCluster.Status.BlueGreen.Phase == "" {
		constants.ActivateRole(sts.Spec.Template.Labels)
	}
	if err := cc.Create(ctx, sts); err != nil {
		cc.Log.Error(err, "Failed to create a new StatefulSet", "statefulSetNamespace", sts.Namespace, "statefulSetName", sts.Name)
		return nil, utils.WrapErr(err, "failed to create a new StatefulSet")
	}
	cc.Log.Info("Created the StatefulSet")
	_ = cc.Metrics.Count(counters.StatefulSetCreated)
	return sts, nil
}

// UpdateStatefulSet updates the Redis cluster StatefulSet resource.
func (cc *ClusterClient) UpdateStatefulSet(ctx context.Context, redisCluster *cachev1.RedisCluster) error {
	// Get the updated sts definition
	newStatefulSet, err := cc.getStatefulSetForRedisCluster(redisCluster)
	if err != nil {
		cc.Log.Error(err, "Failed to get the StatefulSet template")
		return utils.WrapErr(err, "failed to get the StatefulSet template")
	}
	if err := cc.Update(ctx, newStatefulSet); err != nil {
		cc.Log.Error(err, "Failed to update the StatefulSet", "statefulSetNamespace", newStatefulSet.Namespace, "statefulSetName", newStatefulSet.Name)
		return utils.WrapErr(err, "failed to update the StatefulSet")
	}
	cc.Log.Info("Updated the statefulSet")
	_ = cc.Metrics.Count(counters.StatefulSetUpdated)
	return nil
}

func (cc *ClusterClient) ScaleStatefulSet(ctx context.Context, sts *appsv1.StatefulSet, replicas int32) error {
	cc.Log.Info("Scaling StatefulSet", "name", sts.Name, "from", *sts.Spec.Replicas, "to", replicas)
	sts.Spec.Replicas = &replicas
	if err := cc.Update(ctx, sts); err != nil {
		cc.Log.Error(err, "Failed to scale StatefulSet", "name", sts.Name)
		return utils.WrapErr(err, "failed to scale StatefulSet")
	}
	cc.Log.Info("Scaled StatefulSet", "name", sts.Name, "replicas", replicas)
	_ = cc.Metrics.Count(counters.StatefulSetUpdated)
	return nil
}

// getStatefulSetForRedisCluster prepares a StatefulSet for a Redis cluster (for get/create).
func (cc *ClusterClient) getStatefulSetForRedisCluster(redisCluster *cachev1.RedisCluster) (*appsv1.StatefulSet, error) {
	// using a statefulSet that is part of the custom resource definition
	// this way, the statefulSet can be templated by helm
	sts := redisCluster.Spec.StatefulsetTemplate.DeepCopy()
	if sts.Name == "" {
		sts.Name = redisCluster.Name
	}
	sts.Namespace = redisCluster.Namespace
	replicas := redisCluster.EffectivePodCount()
	sts.Spec.Replicas = &replicas
	if sts.Labels == nil {
		sts.Labels = make(map[string]string)
	}
	// The compatibility CRD schema prunes metadata from the embedded
	// StatefulSet. chart_version is also present on the parent resource, so
	// restore it when materializing the workload.
	if chartVersion, ok := redisCluster.Labels["chart_version"]; ok {
		sts.Labels["chart_version"] = chartVersion
	}
	// The compatibility CRD's embedded PodTemplate metadata can be pruned by
	// structural-schema API servers. A StatefulSet must repeat every selector
	// label on its pod template, so reconstruct that required identity.
	if sts.Spec.Template.Labels == nil {
		sts.Spec.Template.Labels = make(map[string]string)
	}
	if sts.Spec.Selector != nil {
		maps.Copy(sts.Spec.Template.Labels, sts.Spec.Selector.MatchLabels)
	}
	if err := restorePrunedVolumeClaimTemplateName(sts); err != nil {
		return nil, err
	}

	// Set redis-cluster instance as the owner and controller
	if err := ctrl.SetControllerReference(redisCluster, sts, cc.ClusterScheme); err != nil {
		cc.Log.Error(err, "Failed to set the StatefulSet's controller reference")
		return nil, utils.WrapErr(err, "failed to set the StatefulSet's controller reference")
	}
	return sts, nil
}

// restorePrunedVolumeClaimTemplateName repairs the single-claim case supported
// by the compatibility CRD. Structural-schema API servers prune ObjectMeta
// fields from embedded PVC templates, but retain the corresponding pod mount.
func restorePrunedVolumeClaimTemplateName(sts *appsv1.StatefulSet) error {
	unnamedClaim := -1
	usedNames := make(map[string]struct{}, len(sts.Spec.Template.Spec.Volumes)+len(sts.Spec.VolumeClaimTemplates))
	for _, volume := range sts.Spec.Template.Spec.Volumes {
		usedNames[volume.Name] = struct{}{}
	}
	for index := range sts.Spec.VolumeClaimTemplates {
		name := sts.Spec.VolumeClaimTemplates[index].Name
		if name != "" {
			usedNames[name] = struct{}{}
			continue
		}
		if unnamedClaim != -1 {
			return fmt.Errorf("cannot restore more than one pruned volume claim template name")
		}
		unnamedClaim = index
	}
	if unnamedClaim == -1 {
		return nil
	}

	candidates := make(map[string]struct{})
	for _, container := range append(sts.Spec.Template.Spec.InitContainers, sts.Spec.Template.Spec.Containers...) {
		for _, mount := range container.VolumeMounts {
			if _, used := usedNames[mount.Name]; !used {
				candidates[mount.Name] = struct{}{}
			}
		}
		for _, device := range container.VolumeDevices {
			if _, used := usedNames[device.Name]; !used {
				candidates[device.Name] = struct{}{}
			}
		}
	}
	if len(candidates) != 1 {
		return fmt.Errorf("cannot unambiguously restore pruned volume claim template name: found %d candidate pod volumes", len(candidates))
	}
	for name := range candidates {
		sts.Spec.VolumeClaimTemplates[unnamedClaim].Name = name
	}
	return nil
}

// TryReadyStatefulSet verifies that the StatefulSet is ready, or can be marked as ready
func (cc *ClusterClient) TryReadyStatefulSet(ctx context.Context, redisCluster *cachev1.RedisCluster, redisStatefulSet *appsv1.StatefulSet) (bool, error) {

	// NOTE: ready is only used during cluster bootstrap and NEVER goes back to false
	statefulSetIsReady := redisCluster.Status.StatefulSetReady
	if statefulSetIsReady {
		return true, nil
	}

	// StatefulSet.Role.
	//   Replicas            = from the spec, how many pods it should have
	//   AvailableReplicas   = pods ready for enough time (avoid flapping)
	//   UnavailableReplicas = replicas - available
	//   UpdatedReplicas     = reachable from deployment, matching the spec template
	//   ReadyReplicas       = reachable from deployment (but maybe not already available)

	expectedPodCount := int(redisCluster.EffectivePodCount())

	if expectedPodCount != int(redisStatefulSet.Status.Replicas) {
		// This is expected during scaling or initial deployment - not an error
		cc.Log.Info("Waiting for StatefulSet to reach expected size",
			"expected", expectedPodCount,
			"current", redisStatefulSet.Status.Replicas)
		return false, nil
	}
	if expectedPodCount != int(*&redisStatefulSet.Status.AvailableReplicas) {
		cc.Log.Info("Waiting for pods to become available",
			"expected", expectedPodCount,
			"available", redisStatefulSet.Status.AvailableReplicas)
		return false, nil
	}

	// test that all pods required by the statefulSet are up and running
	podsAreReady, err := cc.testStatefulSetPods(ctx, redisCluster, expectedPodCount)
	if err != nil {
		cc.Log.Error(err, "Failed to test the StatefulSet pods")
		return false, utils.WrapErr(err, "failed to test the StatefulSet pods")
	}

	return podsAreReady, nil
}

// testStatefulSetPods tests that all pods required by the statefulSet are up and running.
func (cc *ClusterClient) testStatefulSetPods(ctx context.Context, redisCluster *cachev1.RedisCluster, expectedPodCount int) (bool, error) {

	// actually verify the pods - assuming AvailableReplicas cannot be trusted

	pods, err := cc.GetRedisClusterPods(ctx, redisCluster)
	if err != nil {
		return false, err
	}
	if len(pods.Items) != expectedPodCount {
		cc.Log.Error(fmt.Errorf("expected != pod count"), "Internal error", "expected", expectedPodCount, "podCount", pods.Size())
		return false, nil
	}
	ready := true
	for _, pod := range pods.Items {
		if pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" {
			ready = false
			break
		}
	}
	if !ready {
		cc.Log.Error(fmt.Errorf("pod count is OK but some pods don't have an IP?"), "Internal error")
	}
	return ready, nil
}

// getPodListLabels gets the list of StatefulSet labels key/vals to use for listing the StatefulSet pods.
func getPodListLabels(labels map[string]string) (map[string]string, error) {
	filtered := make(map[string]string)
	for _, key := range requiredPodListLabelNames {
		if v, ok := labels[key]; ok {
			filtered[key] = v
		} else {
			return nil, fmt.Errorf("label key (%s) was not found", key)
		}
	}
	// chart_name narrows legacy pod queries when it survives CRD persistence,
	// but embedded ObjectMeta fields can be pruned by the API server.
	if chartName, ok := labels["chart_name"]; ok {
		filtered["chart_name"] = chartName
	}
	return filtered, nil
}

// getPods gets the list of pods in the StatefulSet.
func (cc *ClusterClient) getPods(ctx context.Context, namespace string, clusterName string, templateLabels map[string]string) (*corev1.PodList, error) {
	labels, err := getPodListLabels(templateLabels)
	if err != nil {
		cc.Log.Error(err, "failed to get the pod list labels")
		return nil, err
	}

	podList := &corev1.PodList{}
	listOpts := []client.ListOption{
		client.InNamespace(namespace),
		client.MatchingLabels(labels),
	}
	if err := cc.List(ctx, podList, listOpts...); err != nil {
		cc.Log.Error(err, "failed to list pods", "namespace", namespace, "clusterName", clusterName)
		return nil, err
	}

	return podList, nil
}

// GetRedisClusterPods gets the pods owned by the StatefulSet.
func (cc *ClusterClient) GetRedisClusterPods(ctx context.Context, redisCluster *cachev1.RedisCluster) (*corev1.PodList, error) {
	labels := maps.Clone(redisCluster.Spec.StatefulsetTemplate.Labels)
	if labels == nil {
		labels = make(map[string]string)
	}
	if redisCluster.Spec.StatefulsetTemplate.Spec.Selector != nil {
		maps.Copy(labels, redisCluster.Spec.StatefulsetTemplate.Spec.Selector.MatchLabels)
	}
	return cc.getPods(ctx, redisCluster.Namespace, redisCluster.Name, labels)
}

// GetBluePods returns pods belonging to the serving (blue) StatefulSet.
// Uses name-prefix matching. When one STS name is a prefix of the other
// (e.g. "base" vs "base-a"), we check the longer prefix first so that
// "base-a-0" is never misclassified as a "base-" pod.
func (cc *ClusterClient) GetBluePods(ctx context.Context, redisCluster *cachev1.RedisCluster) (*corev1.PodList, error) {
	servingName := redisCluster.Status.ActiveClusterName
	if servingName == "" {
		servingName = redisCluster.Spec.StatefulsetTemplate.Name
	}
	greenName := constants.AlternateSTSName(servingName)
	allPods, err := cc.GetRedisClusterPods(ctx, redisCluster)
	if err != nil {
		return nil, err
	}
	filtered := &corev1.PodList{}
	bluePrefix := servingName + "-"
	greenPrefix := greenName + "-"
	for _, pod := range allPods.Items {
		if constants.MatchesSTSPrefix(pod.Name, bluePrefix, greenPrefix) {
			filtered.Items = append(filtered.Items, pod)
		}
	}
	return filtered, nil
}

// GetGreenOnlyCluster builds a Cluster object using only green pods (excludes blue pods).
// Used during late BG phases (FormingCluster, ScalingDownBlue, TearingDown) where blue
// pods are being terminated and would cause kubectl exec hangs in mergeRedisNodes.
func (cc *ClusterClient) GetGreenOnlyCluster(ctx context.Context, redisCluster *cachev1.RedisCluster, statefulSet *appsv1.StatefulSet) (*Cluster, error) {
	log := cc.Log.WithName("cluster")

	greenPods, err := cc.GetGreenPods(ctx, redisCluster)
	if err != nil {
		return nil, utils.WrapErr(err, "failed to get green pods")
	}
	log.Info("Got green-only pods", "count", len(greenPods))

	if len(greenPods) == 0 {
		return nil, fmt.Errorf("no green pods found; check activeClusterName %q",
			redisCluster.Status.ActiveClusterName)
	}

	redisPort, err := cc.getRedisPort(ctx, redisCluster)
	if err != nil {
		return nil, utils.WrapErr(err, "failed to get the Redis port")
	}

	toolbox, err := cc.GetToolbox(ctx, redisCluster.Namespace, redisPort)
	if err != nil {
		return nil, utils.WrapErr(err, "failed to get toolbox")
	}

	nodes, err := podsToClusterNodes(greenPods)
	if err != nil {
		return nil, utils.WrapErr(err, "failed to convert green pods to nodes")
	}

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].PodIndex < nodes[j].PodIndex
	})

	nodes, err = mergeRedisNodes(toolbox, nodes)
	if err != nil {
		return nil, utils.WrapErr(err, "failed to populate Redis details for green pods")
	}

	groupedNodes := groupNodes(nodes)
	var node *ClusterNode
	if len(groupedNodes[PrimaryNode]) > 0 {
		node = groupedNodes[PrimaryNode][0]
	} else if len(groupedNodes[ReplicaNode]) > 0 {
		node = groupedNodes[ReplicaNode][0]
	}

	return &Cluster{
		RedisCluster: redisCluster,
		StatefulSet:  statefulSet,
		Toolbox:      toolbox,
		RedisPort:    redisPort,
		RedisVersion: parseRedisVersionFromSTS(statefulSet),
		AllNodes:     nodes,
		Nodes:        groupedNodes,
		Node:         node,
	}, nil
}

// GetBlueOnlyCluster builds a Cluster object using only blue pods (excludes green pods).
// Used during blue/green deployment phases where green pods may be offline.
func (cc *ClusterClient) GetBlueOnlyCluster(ctx context.Context, redisCluster *cachev1.RedisCluster, statefulSet *appsv1.StatefulSet) (*Cluster, error) {
	log := cc.Log.WithName("cluster")

	podList, err := cc.GetBluePods(ctx, redisCluster)
	if err != nil {
		return nil, utils.WrapErr(err, "failed to get blue pods")
	}
	log.Info("Got blue-only pods", "count", len(podList.Items))

	if len(podList.Items) == 0 {
		return nil, fmt.Errorf("no blue pods found for serving STS %q; check activeClusterName",
			redisCluster.Status.ActiveClusterName)
	}

	redisPort, err := cc.getRedisPort(ctx, redisCluster)
	if err != nil {
		return nil, utils.WrapErr(err, "failed to get the Redis port")
	}

	toolbox, err := cc.GetToolbox(ctx, redisCluster.Namespace, redisPort)
	if err != nil {
		return nil, utils.WrapErr(err, "failed to get toolbox")
	}

	nodes, err := podsToClusterNodes(podList.Items)
	if err != nil {
		return nil, utils.WrapErr(err, "failed to convert pods to nodes")
	}

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].PodIndex < nodes[j].PodIndex
	})

	nodes, err = mergeRedisNodes(toolbox, nodes)
	if err != nil {
		return nil, utils.WrapErr(err, "failed to populate Redis details")
	}

	groupedNodes := groupNodes(nodes)
	var node *ClusterNode
	if len(groupedNodes[PrimaryNode]) > 0 {
		node = groupedNodes[PrimaryNode][0]
	} else if len(groupedNodes[ReplicaNode]) > 0 {
		node = groupedNodes[ReplicaNode][0]
	}

	return &Cluster{
		RedisCluster: redisCluster,
		StatefulSet:  statefulSet,
		Toolbox:      toolbox,
		RedisPort:    redisPort,
		RedisVersion: parseRedisVersion(statefulSet.Spec.Template.Spec.Containers[0].Image),
		AllNodes:     nodes,
		Nodes:        groupedNodes,
		Node:         node,
	}, nil
}

// CreateGreenStatefulSet creates a green StatefulSet for blue/green deployment by cloning the blue template.
// It derives the green name from activeClusterName using the [base, base-a] toggle.
//
// IMPORTANT: The STS selector does NOT include the role label. This ensures:
// - STS ownership is stable (based on app + cluster_name only)
// - Role labels can be changed freely without breaking ownership
// - After BG completes, pods keep their owner reference
func (cc *ClusterClient) CreateGreenStatefulSet(ctx context.Context, redisCluster *cachev1.RedisCluster) (*appsv1.StatefulSet, string, error) {
	greenSTS := redisCluster.Spec.StatefulsetTemplate.DeepCopy()
	if err := restorePrunedVolumeClaimTemplateName(greenSTS); err != nil {
		return nil, "", err
	}
	replicas := redisCluster.EffectivePodCount()
	greenSTS.Spec.Replicas = &replicas
	activeName := redisCluster.Status.ActiveClusterName
	if activeName == "" {
		activeName = greenSTS.Name
	}
	greenName := constants.AlternateSTSName(activeName)

	// Change name
	greenSTS.Name = greenName
	greenSTS.Namespace = redisCluster.Namespace

	// DO NOT add role label to selector — keep selector stable for ownership.
	if greenSTS.Spec.Template.Labels == nil {
		greenSTS.Spec.Template.Labels = make(map[string]string)
	}
	constants.DeactivateRole(greenSTS.Spec.Template.Labels)
	greenSTS.Spec.Template.Labels[constants.BlueGreenStatefulSetLabel] = greenName

	// Redis peer identity must not depend on the client-facing Service. That
	// Service is restricted to role=active pods during a rollout, while this
	// rollout-specific Service keeps green node hostnames resolvable before they
	// are safe to receive client traffic.
	peerServiceName := BlueGreenPeerServiceName(greenName)
	greenSTS.Spec.ServiceName = peerServiceName
	for containerIndex := range greenSTS.Spec.Template.Spec.Containers {
		for envIndex := range greenSTS.Spec.Template.Spec.Containers[containerIndex].Env {
			env := &greenSTS.Spec.Template.Spec.Containers[containerIndex].Env[envIndex]
			if env.Name == redisServiceNameEnv {
				env.Value = peerServiceName
				env.ValueFrom = nil
			}
		}
	}

	// Clear resource metadata for creation
	greenSTS.ResourceVersion = ""
	greenSTS.UID = ""
	greenSTS.Generation = 0
	greenSTS.CreationTimestamp = metav1.Time{}
	greenSTS.Status = appsv1.StatefulSetStatus{}

	// Set owner reference to the RedisCluster
	if err := ctrl.SetControllerReference(redisCluster, greenSTS, cc.ClusterScheme); err != nil {
		return nil, greenName, utils.WrapErr(err, "failed to set green StatefulSet controller reference")
	}

	// Auto-delete PVCs when StatefulSet is deleted (K8s 1.27+).
	// This avoids RBAC issues and orphaned PVCs during blue-green teardown.
	greenSTS.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
		WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
		WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
	}

	cc.Log.Info("Creating green StatefulSet", "name", greenName, "replicas", *greenSTS.Spec.Replicas)
	if err := cc.Create(ctx, greenSTS); err != nil {
		if errors.IsAlreadyExists(err) {
			cc.Log.Info("Green StatefulSet already exists", "name", greenName)
			existing := &appsv1.StatefulSet{}
			if getErr := cc.Get(ctx, types.NamespacedName{Name: greenName, Namespace: redisCluster.Namespace}, existing); getErr != nil {
				return nil, greenName, utils.WrapErr(getErr, "failed to get existing green StatefulSet")
			}
			if existing.Spec.Replicas != nil && *existing.Spec.Replicas == 0 {
				cc.Log.Info("Stale green STS has 0 replicas, deleting for clean start", "name", greenName)
				if delErr := cc.Delete(ctx, existing); delErr != nil {
					return nil, greenName, utils.WrapErr(delErr, "failed to delete stale green StatefulSet")
				}
				return nil, greenName, fmt.Errorf("deleted stale green STS %s, requeueing", greenName)
			}
			return existing, greenName, nil
		}
		return nil, greenName, utils.WrapErr(err, "failed to create green StatefulSet")
	}
	cc.Log.Info("Created green StatefulSet", "name", greenName)
	_ = cc.Metrics.Count(counters.BlueGreenStarted)
	return greenSTS, greenName, nil
}

// CreateGreenHeadlessService creates a private peer-discovery Service for the
// green StatefulSet. Client traffic continues to use the main Service.
func (cc *ClusterClient) CreateGreenHeadlessService(ctx context.Context, redisCluster *cachev1.RedisCluster, greenSTSName string, redisPort int) error {
	appLabel := redisCluster.Spec.StatefulsetTemplate.Spec.Template.Labels[constants.LabelApp]
	peerServiceName := BlueGreenPeerServiceName(greenSTSName)
	greenSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      peerServiceName,
			Namespace: redisCluster.Namespace,
			Labels: map[string]string{
				constants.LabelApp:              appLabel,
				constants.LabelRedisClusterName: redisCluster.Name,
			},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector: map[string]string{
				constants.LabelApp:                  appLabel,
				constants.LabelRedisClusterName:     redisCluster.Name,
				constants.BlueGreenStatefulSetLabel: greenSTSName,
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "redis-server",
					Port:       int32(redisPort),
					TargetPort: intstr.FromInt(redisPort),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       "redis-gossip",
					Port:       int32(redisPort + 10000),
					TargetPort: intstr.FromInt(redisPort + 10000),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}

	if err := ctrl.SetControllerReference(redisCluster, greenSvc, cc.ClusterScheme); err != nil {
		return utils.WrapErr(err, "failed to set green service controller reference")
	}

	cc.Log.Info("Creating green headless service", "name", peerServiceName,
		"annotations", greenSvc.Annotations, "selector", greenSvc.Spec.Selector)
	if err := cc.Create(ctx, greenSvc); err != nil {
		if errors.IsAlreadyExists(err) {
			existing := &corev1.Service{}
			if getErr := cc.Get(ctx, types.NamespacedName{Name: peerServiceName, Namespace: redisCluster.Namespace}, existing); getErr != nil {
				return utils.WrapErr(getErr, "failed to get existing green headless service")
			}
			if !maps.Equal(existing.Spec.Selector, greenSvc.Spec.Selector) {
				existing.Spec.Selector = maps.Clone(greenSvc.Spec.Selector)
				if updateErr := cc.Update(ctx, existing); updateErr != nil {
					return utils.WrapErr(updateErr, "failed to repair green headless service selector")
				}
				cc.Log.Info("Repaired green headless service selector", "name", peerServiceName)
			} else {
				cc.Log.Info("Green headless service already exists", "name", peerServiceName)
			}
			return nil
		}
		return utils.WrapErr(err, "failed to create green headless service")
	}
	cc.Log.Info("Created green headless service", "name", peerServiceName,
		"annotations", greenSvc.Annotations)
	return nil
}

// GetGreenStatefulSet gets the green StatefulSet by name.
func (cc *ClusterClient) GetGreenStatefulSet(ctx context.Context, namespace string, greenSTSName string) (*appsv1.StatefulSet, error) {
	sts := &appsv1.StatefulSet{}
	if err := cc.Get(ctx, types.NamespacedName{Name: greenSTSName, Namespace: namespace}, sts); err != nil {
		return nil, utils.WrapErr(err, "failed to get green StatefulSet")
	}
	return sts, nil
}

// GetGreenPods returns pods belonging to the new (green) StatefulSet.
// Derives the green STS name from activeClusterName using [base, base-a] toggle,
// then filters by pod name prefix.
func (cc *ClusterClient) GetGreenPods(ctx context.Context, redisCluster *cachev1.RedisCluster) ([]corev1.Pod, error) {
	activeName := redisCluster.Status.ActiveClusterName
	if activeName == "" {
		activeName = redisCluster.Spec.StatefulsetTemplate.Name
	}
	greenName := constants.AlternateSTSName(activeName)
	allPods, err := cc.GetRedisClusterPods(ctx, redisCluster)
	if err != nil {
		return nil, utils.WrapErr(err, "failed to list pods")
	}
	var greenPods []corev1.Pod
	greenPrefix := greenName + "-"
	bluePrefix := activeName + "-"
	for _, pod := range allPods.Items {
		if constants.MatchesSTSPrefix(pod.Name, greenPrefix, bluePrefix) {
			greenPods = append(greenPods, pod)
		}
	}
	return greenPods, nil
}

// GetActivePods lists pods with role=active label (post-BG steady state).
func (cc *ClusterClient) GetActivePods(ctx context.Context, namespace string) ([]corev1.Pod, error) {
	podList := &corev1.PodList{}
	listOpts := []client.ListOption{
		client.InNamespace(namespace),
		client.MatchingLabels(map[string]string{
			constants.BlueGreenRoleLabel: constants.BlueGreenRoleActive,
		}),
	}
	if err := cc.List(ctx, podList, listOpts...); err != nil {
		return nil, utils.WrapErr(err, "failed to list active pods")
	}
	return podList.Items, nil
}

// DeleteGreenResources deletes the green StatefulSet and headless service (cleanup/rollback).
func (cc *ClusterClient) DeleteGreenResources(ctx context.Context, namespace string, greenSTSName string) error {
	// Delete green StatefulSet
	sts := &appsv1.StatefulSet{}
	if err := cc.Get(ctx, types.NamespacedName{Name: greenSTSName, Namespace: namespace}, sts); err == nil {
		cc.Log.Info("Deleting green StatefulSet", "name", greenSTSName)
		if err := cc.Delete(ctx, sts); err != nil && !errors.IsNotFound(err) {
			return utils.WrapErr(err, "failed to delete green StatefulSet")
		}
	}

	// Delete green PVCs to prevent stale nodes.conf on next BG.
	// Without this, pods restart with old cluster config → standalone "other" state.
	// Use regex to avoid matching blue PVCs when names share a prefix
	// (e.g. "redis-cluster-richa" vs "redis-cluster-richa-alt").
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := cc.List(ctx, pvcList, client.InNamespace(namespace)); err == nil {
		greenPVCPattern, _ := regexp.Compile(`^data-` + regexp.QuoteMeta(greenSTSName) + `-\d`)
		if greenPVCPattern != nil {
			deleted := 0
			for i := range pvcList.Items {
				if greenPVCPattern.MatchString(pvcList.Items[i].Name) {
					cc.Log.Info("Deleting green PVC during rollback", "pvc", pvcList.Items[i].Name)
					if err := cc.Delete(ctx, &pvcList.Items[i]); err != nil && !errors.IsNotFound(err) {
						cc.Log.Error(err, "Failed to delete green PVC (non-fatal)", "pvc", pvcList.Items[i].Name)
					} else {
						deleted++
					}
				}
			}
			if deleted > 0 {
				cc.Log.Info("Deleted green PVCs during rollback", "count", deleted)
			}
		}
	}

	// Delete green headless service (only if operator-created, i.e. has ownerReferences)
	svc := &corev1.Service{}
	peerServiceName := BlueGreenPeerServiceName(greenSTSName)
	if err := cc.Get(ctx, types.NamespacedName{Name: peerServiceName, Namespace: namespace}, svc); err == nil {
		if len(svc.OwnerReferences) > 0 {
			cc.Log.Info("Deleting green headless service (operator-owned)", "name", peerServiceName)
			if err := cc.Delete(ctx, svc); err != nil && !errors.IsNotFound(err) {
				return utils.WrapErr(err, "failed to delete green headless service")
			}
		} else {
			cc.Log.Info("Skipping service deletion — no ownerReferences", "name", peerServiceName)
		}
	}

	return nil
}

// DeleteStatefulSet deletes a StatefulSet by name.
// Uses Background propagation policy to return immediately without waiting for pods to terminate.
func (cc *ClusterClient) DeleteStatefulSet(ctx context.Context, namespace string, name string) error {
	sts := &appsv1.StatefulSet{}
	if err := cc.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, sts); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return utils.WrapErr(err, "failed to get StatefulSet for deletion")
	}
	cc.Log.Info("Deleting StatefulSet", "name", name)

	// Background propagation: API returns immediately, garbage collector cleans up async.
	// This avoids context deadline exceeded when K8s API is slow.
	deletePolicy := metav1.DeletePropagationBackground
	if err := cc.Delete(ctx, sts, &client.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	}); err != nil && !errors.IsNotFound(err) {
		return utils.WrapErr(err, "failed to delete StatefulSet")
	}
	return nil
}

// UpdateServiceSelector updates the selector of a Service to route traffic to a different set of pods.
func (cc *ClusterClient) UpdateServiceSelector(ctx context.Context, namespace string, serviceName string, newSelector map[string]string) error {
	svc := &corev1.Service{}
	if err := cc.Get(ctx, types.NamespacedName{Name: serviceName, Namespace: namespace}, svc); err != nil {
		return utils.WrapErr(err, "failed to get service for DNS switch")
	}

	cc.Log.Info("Updating service selector for DNS switch",
		"service", serviceName,
		"oldSelector", svc.Spec.Selector,
		"newSelector", newSelector)

	svc.Spec.Selector = newSelector
	if err := cc.Update(ctx, svc); err != nil {
		return utils.WrapErr(err, "failed to update service selector")
	}
	cc.Log.Info("Updated service selector", "service", serviceName)
	return nil
}

// getRedisPort gets the Redis port from the Service resource.
func (cc *ClusterClient) getRedisPort(ctx context.Context, redisCluster *cachev1.RedisCluster) (int, error) {

	svc := &corev1.Service{}
	mainName := types.NamespacedName{Name: redisCluster.Name, Namespace: redisCluster.Namespace}
	altName := types.NamespacedName{Name: redisCluster.Name + "-alt", Namespace: redisCluster.Namespace}

	var found bool
	if err := cc.Get(ctx, mainName, svc); err == nil {
		found = true
	} else {
		cc.Log.Info("Main service not found, trying alt service", "main", mainName.Name, "alt", altName.Name)
		if err := cc.Get(ctx, altName, svc); err == nil {
			found = true
		}
	}

	redisPort := 6379
	if found {
		for _, port := range svc.Spec.Ports {
			if port.Name == "redis-server" {
				redisPort = int(port.Port)
				break
			}
		}
	} else {
		cc.Log.Info("No service found, using default Redis port", "port", redisPort)
	}

	return redisPort, nil
}
