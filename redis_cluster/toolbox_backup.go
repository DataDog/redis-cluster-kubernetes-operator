// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package redis_cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
)

const redisDataDirectory = "/mnt/redis"

const nodesConfSanitizer = `s/([0-9]+\.){3}[0-9]+/0.42.42.42/g`

const (
	dumpRDBPath         = redisDataDirectory + "/dump.rdb"
	dumpRDBBackupPath   = redisDataDirectory + "/dump.rdb.backup"
	nodesConfPath       = redisDataDirectory + "/nodes.conf"
	nodesConfBackupPath = redisDataDirectory + "/nodes.conf.backup"
	stagedDumpPath      = redisDataDirectory + "/dump.rdb.restore"
	stagedNodesConfPath = redisDataDirectory + "/nodes.conf.restore"
)

// BackupFile identifies one operator-owned file at a fixed path in a Redis pod.
// It deliberately cannot carry an arbitrary filesystem path.
type BackupFile uint8

const (
	BackupFileDumpRDB BackupFile = iota + 1
	BackupFileDumpRDBBackup
	BackupFileNodesConfBackup
	BackupFileStagedDump
	BackupFileStagedNodesConf
)

// Path returns the fixed local path associated with a backup file identity.
// Unknown identities fail closed rather than being converted to a path.
func (file BackupFile) Path() (string, error) {
	switch file {
	case BackupFileDumpRDB:
		return dumpRDBPath, nil
	case BackupFileDumpRDBBackup:
		return dumpRDBBackupPath, nil
	case BackupFileNodesConfBackup:
		return nodesConfBackupPath, nil
	case BackupFileStagedDump:
		return stagedDumpPath, nil
	case BackupFileStagedNodesConf:
		return stagedNodesConfPath, nil
	default:
		return "", fmt.Errorf("unsupported backup file %d", file)
	}
}

// PrepareDumpRDBBackup snapshots dump.rdb to an operator-owned fixed path so
// hashing and upload always read the same immutable file.
func (t *Toolbox) PrepareDumpRDBBackup(node *ClusterNode) error {
	_, err := t.execBackupFileCommand(t.Context(), node, []string{"cp", "--", dumpRDBPath, dumpRDBBackupPath})
	return err
}

func (t *Toolbox) execBackupFileCommand(ctx context.Context, node *ClusterNode, command []string) (string, error) {
	if err := t.validate(); err != nil {
		return "", err
	}
	if ctx == nil {
		return "", errors.New("backup file operation context is required")
	}
	if node == nil || node.PodName == "" {
		return "", errors.New("Redis pod name is required")
	}
	if len(command) == 0 {
		return "", errors.New("backup file command is required")
	}
	rc, stdout, stderr, err := t.kubeExec.Exec(ctx, node.PodName, constants.RedisContainerName, command, t.Log)
	if err != nil {
		return "", fmt.Errorf("%s failed: %w", command[0], err)
	}
	if rc != 0 {
		return "", fmt.Errorf("%s failed with exit code %d: %s", command[0], rc, strings.TrimSpace(stderr))
	}
	return stdout, nil
}

// PrepareNodesConfBackup copies nodes.conf and sanitizes IPv4 addresses in the
// copy using fixed executable arguments.
func (t *Toolbox) PrepareNodesConfBackup(node *ClusterNode) error {
	if _, err := t.execBackupFileCommand(t.Context(), node, []string{"cp", "--", nodesConfPath, nodesConfBackupPath}); err != nil {
		return err
	}
	if _, err := t.execBackupFileCommand(t.Context(), node, []string{"sed", "-E", "-i", nodesConfSanitizer, nodesConfBackupPath}); err != nil {
		return errors.Join(err, t.RemoveBackupFiles(node, BackupFileNodesConfBackup))
	}
	return nil
}

// BackupFileMetadata returns the size and lowercase SHA-256 digest for a known
// backup file. Hash and size are separate fixed commands, so no shell is used.
func (t *Toolbox) BackupFileMetadata(ctx context.Context, node *ClusterNode, file BackupFile) (int64, string, error) {
	path, err := file.Path()
	if err != nil {
		return 0, "", err
	}
	hashOutput, err := t.execBackupFileCommand(ctx, node, []string{"sha256sum", "--", path})
	if err != nil {
		return 0, "", err
	}
	fields := strings.Fields(hashOutput)
	if len(fields) == 0 {
		return 0, "", errors.New("unexpected SHA-256 output")
	}
	digest := strings.ToLower(fields[0])
	if decoded, decodeErr := hex.DecodeString(digest); decodeErr != nil || len(decoded) != sha256.Size {
		return 0, "", errors.New("invalid SHA-256 output")
	}

	sizeOutput, err := t.execBackupFileCommand(ctx, node, []string{"stat", "-c", "%s", "--", path})
	if err != nil {
		return 0, "", err
	}
	size, err := strconv.ParseInt(strings.TrimSpace(sizeOutput), 10, 64)
	if err != nil || size <= 0 {
		return 0, "", errors.New("invalid size output")
	}
	return size, digest, nil
}

// ActivateStagedDump replaces dump.rdb with the validated staged artifact and
// restores the permissions expected by the Redis container.
func (t *Toolbox) ActivateStagedDump(node *ClusterNode) error {
	if _, err := t.execBackupFileCommand(t.Context(), node, []string{"chmod", "a+rw", "--", stagedDumpPath}); err != nil {
		return err
	}
	_, err := t.execBackupFileCommand(t.Context(), node, []string{"mv", "--", stagedDumpPath, dumpRDBPath})
	return err
}

// RemoveBackupFiles removes only paths represented by the closed BackupFile
// set. Unknown values fail before an exec request is created.
func (t *Toolbox) RemoveBackupFiles(node *ClusterNode, files ...BackupFile) error {
	if len(files) == 0 {
		return nil
	}
	command := []string{"rm", "-f", "--"}
	for _, file := range files {
		path, err := file.Path()
		if err != nil {
			return err
		}
		command = append(command, path)
	}
	_, err := t.execBackupFileCommand(t.Context(), node, command)
	return err
}
