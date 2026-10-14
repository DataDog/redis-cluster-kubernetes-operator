// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeBackupManifestRejectsCorruptOrAmbiguousJSON(t *testing.T) {
	manifest := validBackupManifestForTest()
	valid, err := json.Marshal(manifest)
	require.NoError(t, err)

	for name, data := range map[string][]byte{
		"corrupt":        []byte(`{"version":`),
		"unknown field":  append(append([]byte(nil), valid[:len(valid)-1]...), []byte(`,"signature":"invented"}`)...),
		"trailing value": append(append([]byte(nil), valid...), []byte(` {}`)...),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := decodeBackupManifest(data)
			require.Error(t, err)
		})
	}
}

func TestValidateBackupManifestRejectsMissingManifest(t *testing.T) {
	_, err := validateBackupManifest(nil, "target/latest")
	require.Error(t, err)
}

func TestValidateBackupManifestAcceptsCrossClusterRestore(t *testing.T) {
	manifest := validBackupManifestForTest()
	manifest.ClusterName = "source-cluster"

	artifacts, err := validateBackupManifest(&manifest, "destination-restore/latest")

	require.NoError(t, err)
	require.Len(t, artifacts, 3)
	assert.Equal(t, "0/dump.rdb", artifacts[0].ObjectKey)
}

func TestValidateBackupManifestFailsClosed(t *testing.T) {
	tests := map[string]func(*BackupManifest){
		"unsupported version": func(m *BackupManifest) { m.Version = "v2" },
		"recorded failure":    func(m *BackupManifest) { m.FailedNodes = []int{2} },
		"missing primary":     func(m *BackupManifest) { m.Artifacts = m.Artifacts[2:] },
		"duplicate ordinal":   func(m *BackupManifest) { m.Artifacts[2].Ordinal = 0 },
		"path escaping":       func(m *BackupManifest) { m.Artifacts[0].ObjectKey = "../dump.rdb" },
		"absolute path":       func(m *BackupManifest) { m.Artifacts[0].ObjectKey = "/dump.rdb" },
		"wrong object key":    func(m *BackupManifest) { m.Artifacts[0].ObjectKey = "1/dump.rdb" },
		"invalid digest":      func(m *BackupManifest) { m.Artifacts[0].SHA256 = "not-a-digest" },
		"empty artifact":      func(m *BackupManifest) { m.Artifacts[0].SizeBytes = 0 },
		"missing slots": func(m *BackupManifest) {
			m.Artifacts[0].PrimarySlotRanges = []BackupSlotRange{{First: 0, Last: 5000}}
		},
		"overlapping slots": func(m *BackupManifest) {
			m.Artifacts[2].PrimarySlotRanges = []BackupSlotRange{{First: 5000, Last: 10922}}
		},
		"replica owns slots": func(m *BackupManifest) {
			m.Artifacts[4].PrimarySlotRanges = []BackupSlotRange{{First: 0, Last: 0}}
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			manifest := validBackupManifestForTest()
			mutate(&manifest)
			_, err := validateBackupManifest(&manifest, "target/latest")
			require.Error(t, err)
		})
	}
}

func TestValidateStagedArtifactRejectsDigestAndSizeMismatch(t *testing.T) {
	artifact := validBackupManifestForTest().Artifacts[0]

	err := validateStagedArtifact(artifact, artifact.SizeBytes+1, artifact.SHA256)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "size")

	err = validateStagedArtifact(artifact, artifact.SizeBytes, digestForTest("different"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "digest")
}

func TestManifestCoverageUsesPrimaryArtifactInventory(t *testing.T) {
	manifest := validBackupManifestForTest()
	coverage, err := manifestSlotCoverage(&manifest)
	require.NoError(t, err)
	assert.Equal(t, map[int]string{0: "0-5460", 1: "5461-16383"}, coverage)
}

func validBackupManifestForTest() BackupManifest {
	return BackupManifest{
		Version:     backupManifestVersion,
		Timestamp:   1,
		ClusterName: "source",
		NodeCount:   3,
		Primaries:   2,
		BackupPath:  "source/in-progress",
		Artifacts: []BackupArtifact{
			{Kind: backupArtifactRDB, Role: redis_cluster.PrimaryNode.String(), Ordinal: 0, ObjectKey: "0/dump.rdb", SHA256: digestForTest("zero"), SizeBytes: 4, PrimarySlotRanges: []BackupSlotRange{{First: 0, Last: 5460}}},
			{Kind: backupArtifactNodes, Role: redis_cluster.PrimaryNode.String(), Ordinal: 0, ObjectKey: "0/nodes.conf", SHA256: digestForTest("nodes-zero"), SizeBytes: 10},
			{Kind: backupArtifactRDB, Role: redis_cluster.PrimaryNode.String(), Ordinal: 1, ObjectKey: "1/dump.rdb", SHA256: digestForTest("one"), SizeBytes: 3, PrimarySlotRanges: []BackupSlotRange{{First: 5461, Last: 16383}}},
			{Kind: backupArtifactNodes, Role: redis_cluster.PrimaryNode.String(), Ordinal: 1, ObjectKey: "1/nodes.conf", SHA256: digestForTest("nodes-one"), SizeBytes: 9},
			{Kind: backupArtifactRDB, Role: redis_cluster.ReplicaNode.String(), Ordinal: 2, ObjectKey: "2/dump.rdb", SHA256: digestForTest("two"), SizeBytes: 3},
			{Kind: backupArtifactNodes, Role: redis_cluster.ReplicaNode.String(), Ordinal: 2, ObjectKey: "2/nodes.conf", SHA256: digestForTest("nodes-two"), SizeBytes: 9},
		},
	}
}

func digestForTest(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
