package redis_cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"

	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/kube"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics"
	"github.com/DataDog/redis-cluster-kubernetes-operator/utils"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const mountedSecretRedisCLI = "/usr/local/bin/redis-operator-redis-cli"

var (
	hostnameRE = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$`)
	nodeIDRE   = regexp.MustCompile(`^[A-Fa-f0-9]{40}$`)
)

// CredentialProvider selects a fixed redis-cli launcher. Implementations must
// never return credential material: secret-backed authentication is performed
// inside the toolbox pod by reading mounted files.
type CredentialProvider interface {
	credentialProvider()
}

// NoAuthentication invokes redis-cli directly and is the default.
type NoAuthentication struct{}

func (NoAuthentication) credentialProvider() {}

// MountedSecretAuthentication uses the fixed in-pod helper. Credential values
// therefore never cross the Kubernetes exec boundary.
type MountedSecretAuthentication struct{}

func (MountedSecretAuthentication) credentialProvider() {}

// PodExecutor abstracts fixed-argv Kubernetes exec calls. Production code
// passes a *kube.Exec; tests pass a stub.
type PodExecutor interface {
	Exec(ctx context.Context, podName, containerName string, command []string, log logr.Logger) (rc int, stdout, stderr string, err error)
	ExecWithInput(ctx context.Context, podName, containerName string, command []string, input []byte, log logr.Logger) (rc int, stdout, stderr string, err error)
}

type Toolbox struct {
	client.Client
	kubeExec           PodExecutor
	toolboxPod         *corev1.Pod
	ctx                context.Context
	Namespace          string
	RedisPort          int
	Log                logr.Logger
	Metrics            metrics.Metrics
	CredentialProvider CredentialProvider
}

// Context returns the lifecycle context used by controller operations.
func (t *Toolbox) Context() context.Context {
	if t == nil || t.ctx == nil {
		return context.Background()
	}
	return t.ctx
}

// GetToolbox find the Toolbox pod and returns a Toolbox object.
func (cc *ClusterClient) GetToolbox(ctx context.Context, namespace string, redisPort int) (*Toolbox, error) {

	log := cc.Log.WithName("toolbox")

	podList := &corev1.PodList{}
	listOpts := []client.ListOption{
		client.InNamespace(namespace),
		client.MatchingLabels(map[string]string{"app": constants.ToolboxContainerName, "service": constants.ToolboxContainerName}),
	}
	if err := cc.List(ctx, podList, listOpts...); err != nil {
		log.Error(err, "Failed to get the Toolbox pod", "Namespace", namespace)
		return nil, err
	}

	if len(podList.Items) == 0 {
		return nil, errors.New("could not find the Toolbox")
	} else if len(podList.Items) != 1 {
		log.Info("Found multiple toolboxes, using the first one", "toolboxCount", len(podList.Items))
	}

	toolboxPod := &podList.Items[0]

	log.Info("Got the Toolbox pod", "podName", toolboxPod.Name, "podIP", toolboxPod.Status.PodIP)

	kubeExec, err := kube.NewKubeExec(cc.RestConfig, namespace, log)
	if err != nil {
		err = utils.WrapErr(err, "Failed to create the k8 commands executor")
		log.Error(err, "Failed to create the Toolbox")
		return nil, err
	}
	if _, err := redisCLIExecutable(cc.CredentialProvider); err != nil {
		return nil, err
	}

	return &Toolbox{
		Client:             cc,
		Namespace:          namespace,
		toolboxPod:         toolboxPod,
		kubeExec:           kubeExec,
		ctx:                ctx,
		Log:                log,
		RedisPort:          redisPort,
		Metrics:            cc.Metrics,
		CredentialProvider: cc.CredentialProvider,
	}, nil
}

func validateAddress(address string) error {
	if address == "" || len(address) > 253 {
		return errors.New("invalid Redis address")
	}
	if net.ParseIP(address) != nil {
		return nil
	}
	if !hostnameRE.MatchString(address) || strings.Contains(address, "..") {
		return fmt.Errorf("invalid Redis address %q", address)
	}
	for _, label := range strings.Split(address, ".") {
		if len(label) == 0 || len(label) > 63 || strings.HasPrefix(label, "-") || strings.HasSuffix(label, "-") {
			return fmt.Errorf("invalid Redis address %q", address)
		}
	}
	return nil
}

func validatePort(port int) error {
	if port < 1 || port > 65535 {
		return fmt.Errorf("invalid Redis port %d", port)
	}
	return nil
}

func validateNodeID(id string) error {
	if !nodeIDRE.MatchString(id) {
		return fmt.Errorf("invalid Redis node ID %q", id)
	}
	return nil
}

func validateSlot(slot int) error {
	if slot < 0 || slot >= constants.TotalSlots {
		return fmt.Errorf("invalid Redis slot %d", slot)
	}
	return nil
}

func (t *Toolbox) validate() error {
	if t == nil || t.kubeExec == nil || t.toolboxPod == nil {
		return errors.New("toolbox executor is not initialized")
	}
	if t.ctx == nil {
		return errors.New("toolbox context is required")
	}
	return nil
}

func validateClusterNode(node *ClusterNode) error {
	if node == nil {
		return errors.New("Redis cluster node is required")
	}
	return validateAddress(node.IP)
}

func redisCLIExecutable(provider CredentialProvider) (string, error) {
	switch provider.(type) {
	case NoAuthentication:
		return "redis-cli", nil
	case MountedSecretAuthentication:
		return mountedSecretRedisCLI, nil
	case nil:
		return "", errors.New("Redis credential provider is required")
	default:
		return "", fmt.Errorf("unsupported Redis credential provider %T", provider)
	}
}

// runRedis is the only toolbox Redis execution boundary. Callers provide
// already-typed operation arguments, never shell or printf command text.
func (t *Toolbox) runRedis(ctx context.Context, address string, port int, args ...string) (int, string, string, error) {
	if err := t.validate(); err != nil {
		return 0, "", "", err
	}
	if ctx == nil {
		return 0, "", "", errors.New("Redis operation context is required")
	}
	if err := validateAddress(address); err != nil {
		return 0, "", "", err
	}
	if err := validatePort(port); err != nil {
		return 0, "", "", err
	}
	executable, err := redisCLIExecutable(t.CredentialProvider)
	if err != nil {
		return 0, "", "", err
	}
	command := []string{executable, "-e", "-h", address, "-p", strconv.Itoa(port)}
	command = append(command, args...)
	return t.kubeExec.Exec(ctx, t.toolboxPod.Name, constants.ToolboxContainerName, command, t.Log)
}

func (t *Toolbox) runRedisCluster(ctx context.Context, args ...string) (int, string, string, error) {
	if err := t.validate(); err != nil {
		return 0, "", "", err
	}
	if ctx == nil {
		return 0, "", "", errors.New("Redis operation context is required")
	}
	executable, err := redisCLIExecutable(t.CredentialProvider)
	if err != nil {
		return 0, "", "", err
	}
	command := append([]string{executable, "-e"}, args...)
	return t.kubeExec.Exec(ctx, t.toolboxPod.Name, constants.ToolboxContainerName, command, t.Log)
}

// ObjectStoreCLI identifies one supported object-store command-line client.
type ObjectStoreCLI uint8

const (
	ObjectStoreAWS ObjectStoreCLI = iota + 1
	ObjectStoreGCloud
	ObjectStoreAzure
)

func objectStoreCommand(cli ObjectStoreCLI, args []string) ([]string, error) {
	var prefix []string
	switch cli {
	case ObjectStoreAWS:
		prefix = []string{"aws"}
	case ObjectStoreGCloud:
		prefix = []string{"gcloud"}
	case ObjectStoreAzure:
		prefix = []string{"env", "AZURE_CONFIG_DIR=/tmp/.azure", "az"}
	default:
		return nil, fmt.Errorf("unsupported object-store CLI %d", cli)
	}
	return append(prefix, args...), nil
}

// RunBackupCommand transports one fixed argv command with optional opaque
// stdin. It does not add shell wrapping. Providers are trusted in-process
// extensions; execution remains limited to the Redis and backup containers.
func (t *Toolbox) RunBackupCommand(ctx context.Context, node *ClusterNode, containerName string, command []string, input []byte) (int, string, string, error) {
	if err := t.validate(); err != nil {
		return 0, "", "", err
	}
	if ctx == nil {
		return 0, "", "", errors.New("exec context is required")
	}
	if node == nil || node.PodName == "" {
		return 0, "", "", errors.New("Redis pod name is required")
	}
	if containerName != constants.RedisContainerName && containerName != constants.BackupContainerName {
		return 0, "", "", errors.New("unsupported object-store container")
	}
	if len(command) == 0 {
		return 0, "", "", errors.New("backup command is required")
	}
	return t.kubeExec.ExecWithInput(ctx, node.PodName, containerName, command, input, t.Log)
}

// RunObjectStoreCommand transports fixed-argv object-store commands with
// optional opaque stdin. It permits only the public backup CLIs and fixed
// Redis/backup containers.
func (t *Toolbox) RunObjectStoreCommand(ctx context.Context, node *ClusterNode, containerName string, cli ObjectStoreCLI, args []string, input []byte) (int, string, string, error) {
	if err := t.validate(); err != nil {
		return 0, "", "", err
	}
	if ctx == nil {
		return 0, "", "", errors.New("exec context is required")
	}
	if node == nil || node.PodName == "" {
		return 0, "", "", errors.New("Redis pod name is required")
	}
	if containerName != constants.RedisContainerName && containerName != constants.BackupContainerName {
		return 0, "", "", errors.New("unsupported object-store container")
	}
	command, err := objectStoreCommand(cli, args)
	if err != nil {
		return 0, "", "", err
	}
	var inputCopy []byte
	if input != nil {
		inputCopy = make([]byte, len(input))
		copy(inputCopy, input)
	}
	return t.kubeExec.ExecWithInput(ctx, node.PodName, containerName, command, inputCopy, t.Log)
}
