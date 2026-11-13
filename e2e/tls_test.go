// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//go:build kind_e2e

package e2e

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	rckind "github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/kind"
	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/redisbuilder"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	clientTLSSecretName = "redis-cluster-tester-tls"
	clientTLSKeyPrefix  = "tls"
	clientTLSKeyCount   = 64
	clientTLSCertTTL    = 24 * time.Hour
)

// TestE2E_ClientTLS verifies client-only TLS: clients use the TLS port and are
// redirected to TLS ports, while the operator keeps the plaintext port.
func TestE2E_ClientTLS(t *testing.T) {
	s := rckind.NewSuite(t)
	createClientTLSSecret(t, s, clientTLSSecretName)
	s.CreateCluster(t, rckind.NewRedisCluster(clusterName, s.Namespace,
		redisbuilder.WithClientTLS(clientTLSSecretName)), clusterReadyTimeout)
	s.RequireReadyStatus(t, clusterName, clusterReadyTimeout)
	s.RequireTopology(t, clusterName, redisbuilder.DefaultPrimaries,
		redisbuilder.DefaultReplicas, clusterReadyTimeout)
	s.RequireSlotCoverage(t, clusterName, topologySettleTimeout)

	pods := s.RedisPods(t, clusterName)
	for _, pod := range pods {
		require.Equal(t, "PONG", tlsRedisCLI(t, s, pod, "PING"), "TLS PING on %s", pod)
	}

	// Redis picks the advertised port per connection: TLS clients must only see
	// TLS ports, plaintext clients (the operator) only plaintext ports.
	require.Equal(t, []int{redisbuilder.TLSPort}, clusterNodesPorts(t, tlsRedisCLI(t, s, pods[0], "CLUSTER", "NODES")))
	plainNodes := s.RedisCommandAtIP(t, podIP(t, s, pods[0]), "CLUSTER", "NODES")
	require.Equal(t, []int{redisbuilder.RedisPort}, clusterNodesPorts(t, plainNodes))

	// Keys spread across every shard, so -c must follow MOVED to TLS ports.
	script := fmt.Sprintf(`for i in $(seq 0 %d); do %s -c SET %s:$i $i >/dev/null || exit 1; done`,
		clientTLSKeyCount-1, tlsRedisCLIPrefix(), clientTLSKeyPrefix)
	code, stdout, stderr, err := s.ExecOnPod(s.Ctx, pods[0], constants.RedisContainerName, []string{"sh", "-c", script})
	require.NoError(t, err)
	require.Zero(t, code, "TLS cluster writes failed: stdout=%s stderr=%s", stdout, stderr)
	s.RequireStringData(t, clientTLSKeyPrefix, clientTLSKeyCount, topologySettleTimeout)
}

func tlsRedisCLIPrefix() string {
	return fmt.Sprintf("redis-cli --tls --cacert %s/%s -h 127.0.0.1 -p %d",
		redisbuilder.TLSMountPath, redisbuilder.TLSCAKey, redisbuilder.TLSPort)
}

// tlsRedisCLI runs redis-cli over the pod's own TLS listener.
func tlsRedisCLI(t *testing.T, s *rckind.Suite, pod string, args ...string) string {
	t.Helper()
	argv := append(strings.Fields(tlsRedisCLIPrefix()), args...)
	code, stdout, stderr, err := s.ExecOnPod(s.Ctx, pod, constants.RedisContainerName, argv)
	require.NoError(t, err)
	require.Zero(t, code, "redis-cli %v on %s: %s", args, pod, stderr)
	return strings.TrimSpace(stdout)
}

func podIP(t *testing.T, s *rckind.Suite, name string) string {
	t.Helper()
	pod := &corev1.Pod{}
	require.NoError(t, s.Client.Get(s.Ctx, types.NamespacedName{Namespace: s.Namespace, Name: name}, pod))
	require.NotEmpty(t, pod.Status.PodIP)
	return pod.Status.PodIP
}

// clusterNodesPorts returns the distinct client ports in CLUSTER NODES output
// (field 2 is ip:port@bus[,hostname]).
func clusterNodesPorts(t *testing.T, nodes string) []int {
	t.Helper()
	seen := map[int]bool{}
	var ports []int
	for _, line := range strings.Split(strings.TrimSpace(nodes), "\n") {
		fields := strings.Fields(line)
		require.GreaterOrEqual(t, len(fields), 2, "CLUSTER NODES line %q", line)
		addr := strings.SplitN(fields[1], "@", 2)[0]
		port, err := strconv.Atoi(addr[strings.LastIndex(addr, ":")+1:])
		require.NoError(t, err, "CLUSTER NODES address %q", fields[1])
		if !seen[port] {
			seen[port] = true
			ports = append(ports, port)
		}
	}
	return ports
}

// createClientTLSSecret writes a cert-manager-shaped Secret (tls.crt, tls.key,
// ca.crt) signed by a throwaway CA.
func createClientTLSSecret(t *testing.T, s *rckind.Suite, name string) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	now := time.Now()
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "redis-cluster-e2e-ca"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(clientTLSCertTTL),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	serviceDNS := fmt.Sprintf("%s.%s.svc", clusterName, s.Namespace)
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: serviceDNS},
		DNSNames:     []string{serviceDNS, "*." + serviceDNS, "localhost"},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(clientTLSCertTTL),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caCert, &serverKey.PublicKey, caKey)
	require.NoError(t, err)
	serverKeyDER, err := x509.MarshalECPrivateKey(serverKey)
	require.NoError(t, err)

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: s.Namespace},
		Type:       corev1.SecretTypeTLS,
		Data: map[string][]byte{
			corev1.TLSCertKey:       pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}),
			corev1.TLSPrivateKeyKey: pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: serverKeyDER}),
			redisbuilder.TLSCAKey:   pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		},
	}
	require.NoError(t, s.Client.Create(s.Ctx, secret))
}
