package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type commandCall struct {
	argv  []string
	stdin []byte
}

type recordingBackendExecutor struct {
	calls     []commandCall
	responses []BackendCommandResult
	errors    []error
}

func (e *recordingBackendExecutor) Execute(_ context.Context, argv []string, input []byte) (BackendCommandResult, error) {
	e.calls = append(e.calls, commandCall{argv: append([]string(nil), argv...), stdin: append([]byte(nil), input...)})
	i := len(e.calls) - 1
	var result BackendCommandResult
	var err error
	if i < len(e.responses) {
		result = e.responses[i]
	}
	if i < len(e.errors) {
		err = e.errors[i]
	}
	return result, err
}

func TestNewBackupBackendValidatesProviderNames(t *testing.T) {
	tests := []struct {
		kind, name string
		valid      bool
	}{
		{"s3", "valid-bucket.1", true}, {"s3", "192.168.1.1", false}, {"s3", "xn--reserved", false}, {"s3", "amzn-s3-demo-name", false}, {"s3", "bad..dots", false},
		{"gcs", "valid_bucket.1", true}, {"gcs", "g00gle-name", false}, {"gcs", "goog-name", false}, {"gcs", "192.168.1.1", false},
		{"azure", "valid-container", true}, {"azure", "bad--container", false}, {"azure", "Uppercase", false},
		{"unknown", "valid-name", false},
	}
	for _, tc := range tests {
		t.Run(tc.kind+"/"+tc.name, func(t *testing.T) {
			_, err := newBackupBackend(tc.kind, tc.name)
			assert.Equal(t, tc.valid, err == nil)
		})
	}
}

func TestAllBackendsUseFixedArgvWithoutShell(t *testing.T) {
	backends := []struct{ kind, name, executable string }{{"s3", "my-bucket", "aws"}, {"gcs", "my-bucket", "gcloud"}, {"azure", "my-container", "env"}}
	for _, tc := range backends {
		t.Run(tc.kind, func(t *testing.T) {
			backend, err := newBackupBackend(tc.kind, tc.name)
			require.NoError(t, err)
			exec := &recordingBackendExecutor{responses: []BackendCommandResult{{}, {}, {}, {}, {}, {Stdout: "cluster/latest/object"}, {}, {}, {}, {}, {}, {}}}
			store := backend.Bind(exec)
			ctx := context.Background()
			require.NoError(t, store.UploadFile(ctx, "/mnt/redis/dump.rdb", "cluster/0/dump.rdb"))
			require.NoError(t, store.DownloadFile(ctx, "cluster/0/dump.rdb", "/mnt/redis/dump.rdb"))
			require.NoError(t, store.CopyPrefix(ctx, "cluster/latest", "cluster/previous"))
			require.NoError(t, store.DeletePrefix(ctx, "cluster/previous"))
			for _, call := range exec.calls {
				require.NotEmpty(t, call.argv)
				assert.Equal(t, tc.executable, call.argv[0])
				assert.NotContains(t, call.argv, "sh")
				assert.NotContains(t, call.argv, "bash")
				assert.NotContains(t, call.argv, "-c")
				assert.NotContains(t, strings.Join(call.argv, " "), "; true")
			}
		})
	}
}

func TestRemotePathsRejectedBeforeExecForEveryBackend(t *testing.T) {
	invalid := []string{"", "/absolute", "../escape", "a/../b", "a//b", "-option", "a/-option", "s3://bucket/key", "s3:key", `a\b`, "a;touch", "a$(id)", "a b", "a\u00a0b", "a\nb"}
	for _, kind := range []string{"s3", "gcs", "azure"} {
		name := "my-bucket"
		if kind == "azure" {
			name = "my-container"
		}
		backend, err := newBackupBackend(kind, name)
		require.NoError(t, err)
		for _, value := range invalid {
			t.Run(kind+"/"+value, func(t *testing.T) {
				exec := &recordingBackendExecutor{}
				err := backend.Bind(exec).UploadFile(context.Background(), "/mnt/redis/dump.rdb", value)
				require.Error(t, err)
				assert.Empty(t, exec.calls)
			})
		}
	}
}

func TestLocalPathsRejectOptionAndInjectionBeforeExec(t *testing.T) {
	backend, err := newBackupBackend("s3", "my-bucket")
	require.NoError(t, err)
	for _, value := range []string{"-file", "relative/file", "/mnt/../etc/passwd", "/mnt/redis/a;id", "/mnt/redis/-option"} {
		exec := &recordingBackendExecutor{}
		err := backend.Bind(exec).UploadFile(context.Background(), value, "cluster/dump.rdb")
		require.Error(t, err)
		assert.Empty(t, exec.calls)
	}
}

func TestBackendErrorClassificationIsFailClosed(t *testing.T) {
	tests := []struct {
		stderr  string
		execErr error
		kind    BackendErrorKind
	}{
		{"AccessDenied: object not found", nil, BackendAuthentication},
		{"dial tcp: connection timed out", nil, BackendNetwork},
		{"NoSuchKey", nil, BackendNotFound},
		{"unexpected provider failure", nil, BackendFailure},
		{"", errors.New("untyped executor failure"), BackendFailure},
		{"", &backendCommandTransportError{Kind: BackendAuthentication}, BackendAuthentication},
		{"", &backendCommandTransportError{Kind: BackendNetwork}, BackendNetwork},
		{"", &backendCommandTransportError{Kind: BackendFailure}, BackendFailure},
	}
	for _, tc := range tests {
		err := classifyBackendFailure(backendS3, "exists", BackendCommandResult{ExitCode: 1, Stderr: tc.stderr}, tc.execErr)
		var backendErr *BackendError
		require.ErrorAs(t, err, &backendErr)
		assert.Equal(t, tc.kind, backendErr.Kind)
		if tc.stderr != "" {
			assert.NotContains(t, err.Error(), tc.stderr)
		}
	}
}

func TestBackendErrorClassificationRecognizesNativeProviderCodes(t *testing.T) {
	tests := []struct {
		provider backendProvider
		stderr   string
		kind     BackendErrorKind
	}{
		{backendS3, "InvalidAccessKeyId", BackendAuthentication},
		{backendS3, "ExpiredToken", BackendAuthentication},
		{backendGCS, "invalid_grant", BackendAuthentication},
		{backendAzure, "AuthorizationPermissionMismatch", BackendAuthentication},
		{backendS3, "Could not connect to endpoint URL", BackendNetwork},
		{backendS3, "NoSuchKey", BackendNotFound},
		{backendGCS, "No URLs matched", BackendNotFound},
		{backendAzure, "BlobNotFound", BackendNotFound},
		{backendGCS, "BlobNotFound", BackendFailure},
	}
	for _, tc := range tests {
		err := classifyBackendFailure(tc.provider, "exists", BackendCommandResult{ExitCode: 1, Stderr: tc.stderr}, nil)
		var backendErr *BackendError
		require.ErrorAs(t, err, &backendErr)
		assert.Equal(t, tc.kind, backendErr.Kind)
	}
}

func TestAzureCopyPrefixUsesSynchronousBlobCopiesWithoutMovingSource(t *testing.T) {
	backend, err := newBackupBackend("azure", "my-container")
	require.NoError(t, err)
	exec := &recordingBackendExecutor{responses: []BackendCommandResult{{}, {Stdout: "cluster/latest/0/dump.rdb\ncluster/latest/manifest.json"}, {}, {}}}
	require.NoError(t, backend.Bind(exec).CopyPrefix(context.Background(), "cluster/latest", "cluster/previous"))
	require.Len(t, exec.calls, 4)
	for _, call := range exec.calls {
		joined := strings.Join(call.argv, " ")
		assert.NotContains(t, joined, " storage fs ")
		assert.NotContains(t, joined, " directory move ")
	}
	assert.Contains(t, strings.Join(exec.calls[2].argv, " "), "storage blob copy start")
	assert.Contains(t, exec.calls[2].argv, "cluster/previous/0/dump.rdb")
	assert.Contains(t, exec.calls[2].argv, "--requires-sync")
	assert.Contains(t, strings.Join(exec.calls[3].argv, " "), "storage blob copy start")
	assert.Contains(t, exec.calls[3].argv, "cluster/previous/manifest.json")
}

func TestS3FailureCannotBeSwallowed(t *testing.T) {
	backend, err := newBackupBackend("s3", "my-bucket")
	require.NoError(t, err)
	exec := &recordingBackendExecutor{responses: []BackendCommandResult{{ExitCode: 23, Stderr: "AccessDenied"}}}
	err = backend.Bind(exec).CopyPrefix(context.Background(), "cluster/latest", "cluster/previous")
	require.Error(t, err)
	var backendErr *BackendError
	require.ErrorAs(t, err, &backendErr)
	assert.Equal(t, BackendAuthentication, backendErr.Kind)
	assert.Equal(t, []string{"aws", "s3", "cp", "s3://my-bucket/cluster/latest/", "s3://my-bucket/cluster/previous/", "--recursive", "--quiet"}, exec.calls[0].argv)
}

func TestManifestBytesTravelOnlyOnStdin(t *testing.T) {
	payload := []byte(`{"value":";$(touch /tmp/pwned)","password":"secret"}`)
	for _, tc := range []struct{ kind, name string }{{"s3", "my-bucket"}, {"gcs", "my-bucket"}, {"azure", "my-container"}} {
		backend, err := newBackupBackend(tc.kind, tc.name)
		require.NoError(t, err)
		exec := &recordingBackendExecutor{}
		require.NoError(t, backend.Bind(exec).UploadBytes(context.Background(), payload, "cluster/latest/manifest.json"))
		last := exec.calls[len(exec.calls)-1]
		assert.Equal(t, payload, last.stdin)
		assert.NotContains(t, strings.Join(last.argv, " "), string(payload))
	}
}

func TestExistsDistinguishesAbsenceFromProviderFailure(t *testing.T) {
	backend, err := newBackupBackend("s3", "my-bucket")
	require.NoError(t, err)
	for _, tc := range []struct {
		name, stdout, stderr string
		rc                   int
		exists               bool
		kind                 BackendErrorKind
	}{
		{"absent", "0", "", 0, false, ""}, {"present", "1", "", 0, true, ""},
		{"auth", "", "AccessDenied", 1, false, BackendAuthentication}, {"network", "", "connection timed out", 1, false, BackendNetwork}, {"backend", "", "oops", 1, false, BackendFailure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec := &recordingBackendExecutor{responses: []BackendCommandResult{{ExitCode: tc.rc, Stdout: tc.stdout, Stderr: tc.stderr}}}
			exists, err := backend.Bind(exec).Exists(context.Background(), "cluster/latest")
			assert.Equal(t, tc.exists, exists)
			if tc.kind == "" {
				require.NoError(t, err)
			} else {
				var backendErr *BackendError
				require.ErrorAs(t, err, &backendErr)
				assert.Equal(t, tc.kind, backendErr.Kind)
			}
		})
	}
}

func TestGCSExistsEscapesPrefixAsRegex(t *testing.T) {
	backend, err := newBackupBackend("gcs", "my-bucket")
	require.NoError(t, err)
	exec := &recordingBackendExecutor{responses: []BackendCommandResult{{Stdout: "cluster.v1/latest/object"}}}
	exists, err := backend.Bind(exec).Exists(context.Background(), "cluster.v1/latest")
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Contains(t, exec.calls[0].argv, `--filter=name~^cluster\.v1/latest/`)
}

func TestExistsParityAcrossBackends(t *testing.T) {
	backends := []struct{ kind, name string }{{"s3", "my-bucket"}, {"gcs", "my-bucket"}, {"azure", "my-container"}}
	cases := []struct {
		name, stdout, stderr string
		rc                   int
		exists               bool
		kind                 BackendErrorKind
	}{
		{"absent", "", "", 0, false, ""}, {"present", "object", "", 0, true, ""},
		{"auth", "", "AccessDenied", 1, false, BackendAuthentication},
		{"network", "", "connection timed out", 1, false, BackendNetwork},
		{"backend", "", "provider failed", 1, false, BackendFailure},
	}
	for _, backendCase := range backends {
		backend, err := newBackupBackend(backendCase.kind, backendCase.name)
		require.NoError(t, err)
		for _, tc := range cases {
			t.Run(backendCase.kind+"/"+tc.name, func(t *testing.T) {
				stdout := tc.stdout
				if backendCase.kind == "s3" && tc.rc == 0 {
					if tc.exists {
						stdout = "1"
					} else {
						stdout = "0"
					}
				}
				responses := []BackendCommandResult{{ExitCode: tc.rc, Stdout: stdout, Stderr: tc.stderr}}
				if backendCase.kind == "azure" {
					responses = append([]BackendCommandResult{{}}, responses...)
				}
				exec := &recordingBackendExecutor{responses: responses}
				exists, err := backend.Bind(exec).Exists(context.Background(), "cluster/latest")
				assert.Equal(t, tc.exists, exists)
				if tc.kind == "" {
					require.NoError(t, err)
				} else {
					var backendErr *BackendError
					require.ErrorAs(t, err, &backendErr)
					assert.Equal(t, tc.kind, backendErr.Kind)
				}
			})
		}
	}
}

type failEveryOperationExecutor struct{ calls []commandCall }

func (e *failEveryOperationExecutor) Execute(_ context.Context, argv []string, input []byte) (BackendCommandResult, error) {
	e.calls = append(e.calls, commandCall{argv: append([]string(nil), argv...), stdin: append([]byte(nil), input...)})
	if len(argv) >= 4 && argv[0] == "env" && argv[2] == "az" && argv[3] == "login" {
		return BackendCommandResult{}, nil
	}
	return BackendCommandResult{ExitCode: 31, Stderr: "provider failed"}, nil
}

func TestEveryTypedOperationPropagatesProviderFailureForEveryBackend(t *testing.T) {
	for _, tc := range []struct{ kind, name string }{{"s3", "my-bucket"}, {"gcs", "my-bucket"}, {"azure", "my-container"}} {
		t.Run(tc.kind, func(t *testing.T) {
			backend, err := newBackupBackend(tc.kind, tc.name)
			require.NoError(t, err)
			exec := &failEveryOperationExecutor{}
			store := backend.Bind(exec)
			ctx := context.Background()
			operations := []func() error{
				func() error { return store.UploadFile(ctx, "/mnt/redis/dump.rdb", "cluster/dump.rdb") },
				func() error { return store.DownloadFile(ctx, "cluster/dump.rdb", "/mnt/redis/dump.rdb") },
				func() error { return store.UploadBytes(ctx, []byte("manifest"), "cluster/manifest.json") },
				func() error { _, err := store.DownloadBytes(ctx, "cluster/manifest.json"); return err },
				func() error { return store.CopyPrefix(ctx, "cluster/latest", "cluster/previous") },
				func() error { return store.DeletePrefix(ctx, "cluster/previous") },
				func() error { _, err := store.Exists(ctx, "cluster/latest"); return err },
			}
			for i, operation := range operations {
				assert.Error(t, operation(), "operation %d swallowed provider failure", i)
			}
		})
	}
}
