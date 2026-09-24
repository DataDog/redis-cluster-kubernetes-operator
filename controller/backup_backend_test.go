package controller

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

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

type stubBackupBackend struct{}

func (stubBackupBackend) Bind(BackendCommandExecutor) BackupStore { return &promotionStore{} }

var _ BackupProvider = stubBackupBackend{}

var defaultBackupBackends = NewBackupBackendRegistry()

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
			_, err := defaultBackupBackends.NewBackupBackend(tc.kind, tc.name)
			assert.Equal(t, tc.valid, err == nil)
		})
	}
}

func TestBackupBackendRegistrySupportsCustomProviders(t *testing.T) {
	registry := NewBackupBackendRegistry()
	var gotLocation string
	require.NoError(t, registry.Register("custom", func(location string) (BackupProvider, error) {
		gotLocation = location
		return stubBackupBackend{}, nil
	}))

	backend, err := registry.NewBackupBackend("custom", "tenant-backups")
	require.NoError(t, err)
	assert.IsType(t, stubBackupBackend{}, backend)
	assert.Equal(t, "tenant-backups", gotLocation)

	for _, tc := range []struct {
		provider string
		location string
		want     BackupProvider
	}{
		{provider: "", location: "my-bucket", want: &s3Backend{}},
		{provider: "s3", location: "my-bucket", want: &s3Backend{}},
		{provider: "gcs", location: "my-bucket", want: &gcsBackend{}},
		{provider: "azure", location: "my-container", want: &azureBackend{}},
	} {
		t.Run("built-in/"+tc.provider, func(t *testing.T) {
			backend, err := registry.NewBackupBackend(tc.provider, tc.location)
			require.NoError(t, err)
			assert.IsType(t, tc.want, backend)
		})
	}

	_, err = registry.NewBackupBackend("missing", "tenant-backups")
	require.ErrorContains(t, err, `unknown backup provider "missing"`)
}

func TestBackupBackendRegistryRejectsInvalidRegistrations(t *testing.T) {
	registry := NewBackupBackendRegistry()
	provider := BackupBackendProviderFunc(func(string) (BackupProvider, error) { return stubBackupBackend{}, nil })

	require.Error(t, registry.Register("", provider))
	require.Error(t, registry.Register("UPPERCASE", provider))
	require.Error(t, registry.Register("custom", nil))
	require.NoError(t, registry.Register("custom", provider))
	require.Error(t, registry.Register("custom", provider))
}

func TestBackupBackendRegistryRejectsConstructorFailures(t *testing.T) {
	registry := NewBackupBackendRegistry()
	wantErr := errors.New("construction failed")
	require.NoError(t, registry.Register("failure", func(string) (BackupProvider, error) { return nil, wantErr }))
	require.NoError(t, registry.Register("nil", func(string) (BackupProvider, error) { return nil, nil }))
	var typedNil *s3Backend
	require.NoError(t, registry.Register("typed-nil", func(string) (BackupProvider, error) { return typedNil, nil }))

	_, err := registry.NewBackupBackend("failure", "location")
	require.ErrorIs(t, err, wantErr)
	_, err = registry.NewBackupBackend("nil", "location")
	require.ErrorContains(t, err, "returned a nil backend")
	_, err = registry.NewBackupBackend("typed-nil", "location")
	require.ErrorContains(t, err, "returned a nil backend")
}

func TestDefaultBackupBackendRegistryUsesAWSForEmptyProvider(t *testing.T) {
	backend, err := defaultBackupBackends.NewBackupBackend("", "my-bucket")
	require.NoError(t, err)
	assert.IsType(t, &s3Backend{}, backend)
}

func TestAllBackendsUseFixedArgvWithoutShell(t *testing.T) {
	backends := []struct{ kind, name, executable string }{{"s3", "my-bucket", "aws"}, {"gcs", "my-bucket", "gcloud"}, {"azure", "my-container", "env"}}
	for _, tc := range backends {
		t.Run(tc.kind, func(t *testing.T) {
			backend, err := defaultBackupBackends.NewBackupBackend(tc.kind, tc.name)
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
		backend, err := defaultBackupBackends.NewBackupBackend(kind, name)
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
	backend, err := defaultBackupBackends.NewBackupBackend("s3", "my-bucket")
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
		{"", &BackendError{Kind: BackendAuthentication, Operation: "execute"}, BackendAuthentication},
		{"", &BackendError{Kind: BackendNetwork, Operation: "execute"}, BackendNetwork},
		{"", &BackendError{Kind: BackendFailure, Operation: "execute"}, BackendFailure},
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
	backend, err := defaultBackupBackends.NewBackupBackend("azure", "my-container")
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

func TestAzureDownloadBytesWritesBlobToStdoutWithoutMetadata(t *testing.T) {
	backend, err := defaultBackupBackends.NewBackupBackend("azure", "my-container")
	require.NoError(t, err)
	payload := []byte(`{"redisCluster":"example"}`)
	exec := &recordingBackendExecutor{responses: []BackendCommandResult{{}, {Stdout: string(payload)}}}

	got, err := backend.Bind(exec).DownloadBytes(context.Background(), "cluster/latest/manifest.json")
	require.NoError(t, err)
	assert.Equal(t, payload, got)
	require.Len(t, exec.calls, 2)
	assert.Equal(t, []string{
		"env", "AZURE_CONFIG_DIR=/tmp/.azure", "az", "storage", "blob", "download",
		"--auth-mode", "login", "--overwrite", "true", "--container-name", "my-container",
		"--name", "cluster/latest/manifest.json", "--file", "/dev/stdout", "--output", "none",
		"--only-show-errors",
	}, exec.calls[1].argv)
}

type blockingAzureExecutor struct {
	mu          sync.Mutex
	calls       int
	blockOnCall int
	blocked     chan struct{}
	release     chan struct{}
	firstCall   chan struct{}
	firstOnce   sync.Once
	responses   []BackendCommandResult
}

func (e *blockingAzureExecutor) Execute(_ context.Context, _ []string, _ []byte) (BackendCommandResult, error) {
	e.firstOnce.Do(func() {
		if e.firstCall != nil {
			close(e.firstCall)
		}
	})

	e.mu.Lock()
	e.calls++
	call := e.calls
	var response BackendCommandResult
	if call <= len(e.responses) {
		response = e.responses[call-1]
	}
	e.mu.Unlock()

	if call == e.blockOnCall {
		close(e.blocked)
		<-e.release
	}
	return response, nil
}

func TestAzureBackendSerializesAuthenticatedCommandSequences(t *testing.T) {
	tests := []struct {
		name      string
		responses []BackendCommandResult
		operation func(BackupStore) error
	}{
		{
			name:      "copy prefix",
			responses: []BackendCommandResult{{}, {Stdout: "cluster/latest/object"}, {}},
			operation: func(store BackupStore) error {
				return store.CopyPrefix(context.Background(), "cluster/latest", "cluster/previous")
			},
		},
		{
			name:      "delete prefix",
			responses: []BackendCommandResult{{}, {Stdout: "cluster/previous/object"}, {}},
			operation: func(store BackupStore) error {
				return store.DeletePrefix(context.Background(), "cluster/previous")
			},
		},
		{
			name:      "exists",
			responses: []BackendCommandResult{{}, {Stdout: "cluster/latest/object"}},
			operation: func(store BackupStore) error {
				_, err := store.Exists(context.Background(), "cluster/latest")
				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backend, err := NewAzureBackupBackend("my-container")
			require.NoError(t, err)

			blocked := make(chan struct{})
			release := make(chan struct{})
			firstExec := &blockingAzureExecutor{
				blockOnCall: 2,
				blocked:     blocked,
				release:     release,
				responses:   tc.responses,
			}
			secondCalled := make(chan struct{})
			secondExec := &blockingAzureExecutor{
				firstCall: secondCalled,
				responses: []BackendCommandResult{{}, {Stdout: "cluster/latest/object"}},
			}

			firstResult := make(chan error, 1)
			go func() { firstResult <- tc.operation(backend.Bind(firstExec)) }()
			<-blocked

			secondStarted := make(chan struct{})
			secondResult := make(chan error, 1)
			go func() {
				close(secondStarted)
				_, err := backend.Bind(secondExec).Exists(context.Background(), "cluster/latest")
				secondResult <- err
			}()
			<-secondStarted

			overlapped := false
			select {
			case <-secondCalled:
				overlapped = true
			case <-time.After(100 * time.Millisecond):
			}

			close(release)
			require.NoError(t, <-firstResult)
			require.NoError(t, <-secondResult)
			assert.False(t, overlapped, "a second Azure command started while an authenticated sequence was still running")
		})
	}
}

func TestS3FailureCannotBeSwallowed(t *testing.T) {
	backend, err := defaultBackupBackends.NewBackupBackend("s3", "my-bucket")
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
		backend, err := defaultBackupBackends.NewBackupBackend(tc.kind, tc.name)
		require.NoError(t, err)
		exec := &recordingBackendExecutor{}
		require.NoError(t, backend.Bind(exec).UploadBytes(context.Background(), payload, "cluster/latest/manifest.json"))
		last := exec.calls[len(exec.calls)-1]
		assert.Equal(t, payload, last.stdin)
		assert.NotContains(t, strings.Join(last.argv, " "), string(payload))
	}
}

func TestExistsDistinguishesAbsenceFromProviderFailure(t *testing.T) {
	backend, err := defaultBackupBackends.NewBackupBackend("s3", "my-bucket")
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
	backend, err := defaultBackupBackends.NewBackupBackend("gcs", "my-bucket")
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
		backend, err := defaultBackupBackends.NewBackupBackend(backendCase.kind, backendCase.name)
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
			backend, err := defaultBackupBackends.NewBackupBackend(tc.kind, tc.name)
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
