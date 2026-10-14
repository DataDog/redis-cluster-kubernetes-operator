// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package redis_cluster

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/toolbox"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToolboxBackupFileOperationsUseFixedArgv(t *testing.T) {
	stub := toolbox.New().OnCommand("all", func(_, _ string, _ []string) bool { return true }, toolbox.OK())
	tb := newToolboxWithStub(stub)
	node := &ClusterNode{PodName: "redis-cluster-sample-0"}

	require.NoError(t, tb.PrepareNodesConfBackup(node))
	require.NoError(t, tb.PrepareDumpRDBBackup(node))
	require.NoError(t, tb.ActivateStagedDump(node))
	require.NoError(t, tb.RemoveBackupFiles(node, BackupFileNodesConfBackup, BackupFileDumpRDBBackup, BackupFileStagedDump, BackupFileStagedNodesConf))

	calls := stub.Calls()
	require.Len(t, calls, 6)
	assert.Equal(t, []string{"cp", "--", "/mnt/redis/nodes.conf", "/mnt/redis/nodes.conf.backup"}, calls[0].Command)
	assert.Equal(t, []string{"sed", "-E", "-i", `s/([0-9]+\.){3}[0-9]+/0.42.42.42/g`, "/mnt/redis/nodes.conf.backup"}, calls[1].Command)
	assert.Equal(t, []string{"cp", "--", "/mnt/redis/dump.rdb", "/mnt/redis/dump.rdb.backup"}, calls[2].Command)
	assert.Equal(t, []string{"chmod", "a+rw", "--", "/mnt/redis/dump.rdb.restore"}, calls[3].Command)
	assert.Equal(t, []string{"mv", "--", "/mnt/redis/dump.rdb.restore", "/mnt/redis/dump.rdb"}, calls[4].Command)
	assert.Equal(t, []string{"rm", "-f", "--", "/mnt/redis/nodes.conf.backup", "/mnt/redis/dump.rdb.backup", "/mnt/redis/dump.rdb.restore", "/mnt/redis/nodes.conf.restore"}, calls[5].Command)
	for _, call := range calls {
		assert.Equal(t, node.PodName, call.PodName)
		assert.Equal(t, constants.RedisContainerName, call.ContainerName)
		joined := strings.Join(call.Command, " ")
		assert.NotContains(t, joined, "bash")
		assert.NotContains(t, joined, "sh -c")
		assert.NotContains(t, call.Command, "-c")
	}
}

func TestToolboxBackupFileMetadataUsesSeparateFixedCommands(t *testing.T) {
	const digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	stub := toolbox.New().
		OnCommand("hash", func(_, _ string, command []string) bool { return command[0] == "sha256sum" }, toolbox.Stdout(digest+"  /mnt/redis/dump.rdb\n")).
		OnCommand("size", func(_, _ string, command []string) bool { return command[0] == "stat" }, toolbox.Stdout("4096\n"))
	tb := newToolboxWithStub(stub)

	size, gotDigest, err := tb.BackupFileMetadata(context.Background(), &ClusterNode{PodName: "redis-cluster-sample-0"}, BackupFileDumpRDB)
	require.NoError(t, err)
	assert.Equal(t, int64(4096), size)
	assert.Equal(t, digest, gotDigest)
	assert.Equal(t, []string{"sha256sum", "--", "/mnt/redis/dump.rdb"}, stub.Calls()[0].Command)
	assert.Equal(t, []string{"stat", "-c", "%s", "--", "/mnt/redis/dump.rdb"}, stub.Calls()[1].Command)
}

func TestToolboxBackupFileOperationsFailClosed(t *testing.T) {
	node := &ClusterNode{PodName: "redis-cluster-sample-0"}

	stub := toolbox.New()
	tb := newToolboxWithStub(stub)
	_, _, err := tb.BackupFileMetadata(context.Background(), node, BackupFile(255))
	require.Error(t, err)
	assert.Empty(t, stub.Calls())
	require.Error(t, tb.RemoveBackupFiles(node, BackupFile(255)))
	assert.Empty(t, stub.Calls())

	stub = toolbox.New().FailNext(errors.New("exec failed"))
	tb = newToolboxWithStub(stub)
	require.ErrorContains(t, tb.PrepareNodesConfBackup(node), "exec failed")
	require.Len(t, stub.Calls(), 1)
}

func TestToolboxBackupFileMetadataHonorsCancellation(t *testing.T) {
	stub := toolbox.New().
		OnCommand("all", func(_, _ string, _ []string) bool { return true }, toolbox.OK()).
		DelayNext(time.Second)
	tb := newToolboxWithStub(stub)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := tb.BackupFileMetadata(ctx, &ClusterNode{PodName: "redis-cluster-sample-0"}, BackupFileDumpRDB)
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, stub.Calls(), 1)
}

func TestToolboxPrepareNodesConfBackupReportsSanitizeAndCleanupFailures(t *testing.T) {
	stub := toolbox.New().
		OnCommand("copy", func(_, _ string, command []string) bool { return command[0] == "cp" }, toolbox.OK()).
		OnCommand("sanitize", func(_, _ string, command []string) bool { return command[0] == "sed" }, toolbox.Failure(2, "sanitize failed")).
		OnCommand("cleanup", func(_, _ string, command []string) bool { return command[0] == "rm" }, toolbox.Failure(3, "cleanup failed"))
	tb := newToolboxWithStub(stub)

	err := tb.PrepareNodesConfBackup(&ClusterNode{PodName: "redis-cluster-sample-0"})
	require.ErrorContains(t, err, "sanitize failed")
	require.ErrorContains(t, err, "cleanup failed")
	require.Len(t, stub.Calls(), 3)
}

func TestToolboxActivateStagedDumpStopsOnMoveFailure(t *testing.T) {
	stub := toolbox.New().
		OnCommand("permissions", func(_, _ string, command []string) bool { return command[0] == "chmod" }, toolbox.OK()).
		OnCommand("move", func(_, _ string, command []string) bool { return command[0] == "mv" }, toolbox.Failure(2, "move failed"))
	tb := newToolboxWithStub(stub)

	require.ErrorContains(t, tb.ActivateStagedDump(&ClusterNode{PodName: "redis-cluster-sample-0"}), "move failed")
	require.Len(t, stub.Calls(), 2)
}

func TestToolboxActivateStagedDumpReportsPermissionFailure(t *testing.T) {
	stub := toolbox.New().
		OnCommand("permissions", func(_, _ string, command []string) bool { return command[0] == "chmod" }, toolbox.Failure(2, "chmod failed"))
	tb := newToolboxWithStub(stub)

	require.ErrorContains(t, tb.ActivateStagedDump(&ClusterNode{PodName: "redis-cluster-sample-0"}), "chmod failed")
	require.Len(t, stub.Calls(), 1)
}
