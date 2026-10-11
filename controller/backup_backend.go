package controller

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

type BackendErrorKind string
type backendProvider string

const (
	BackendNotFound       BackendErrorKind = "not_found"
	BackendAuthentication BackendErrorKind = "authentication"
	BackendNetwork        BackendErrorKind = "network"
	BackendFailure        BackendErrorKind = "backend"

	backendS3    backendProvider = "s3"
	backendGCS   backendProvider = "gcs"
	backendAzure backendProvider = "azure"
)

// BackendError excludes provider output because it can contain credentials or signed URLs.
type BackendError struct {
	Kind      BackendErrorKind
	Operation string
	ExitCode  int
}

func (e *BackendError) Error() string {
	if e.ExitCode != 0 {
		return fmt.Sprintf("object-store %s failed (%s, exit code %d)", e.Operation, e.Kind, e.ExitCode)
	}
	return fmt.Sprintf("object-store %s failed (%s)", e.Operation, e.Kind)
}

func IsBackendNotFound(err error) bool {
	var backendErr *BackendError
	return errors.As(err, &backendErr) && backendErr.Kind == BackendNotFound
}

type BackendCommandResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
}

type backendCommandTransportError struct{ Kind BackendErrorKind }

func (e *backendCommandTransportError) Error() string {
	return "object-store command transport failed (" + string(e.Kind) + ")"
}

// BackendCommandExecutor is pre-bound to one pod and container. Input is sent
// only over stdin and must never be inserted into argv, request URLs, or logs.
type BackendCommandExecutor interface {
	Execute(context.Context, []string, []byte) (BackendCommandResult, error)
}

type BackupBackend interface {
	Bind(BackendCommandExecutor) ObjectStore
	URI(string) (string, error)
}

// ObjectStore is the typed contract shared with U5.
type ObjectStore interface {
	UploadFile(context.Context, string, string) error
	DownloadFile(context.Context, string, string) error
	UploadBytes(context.Context, []byte, string) error
	DownloadBytes(context.Context, string) ([]byte, error)
	CopyPrefix(context.Context, string, string) error
	DeletePrefix(context.Context, string) error
	Exists(context.Context, string) (bool, error)
}

func newBackupBackend(kind, bucket string) (BackupBackend, error) {
	switch kind {
	case "", "s3":
		if err := validateS3Bucket(bucket); err != nil {
			return nil, err
		}
		return &s3Backend{bucket: bucket}, nil
	case "gcs":
		if err := validateGCSBucket(bucket); err != nil {
			return nil, err
		}
		return &gcsBackend{bucket: bucket}, nil
	case "azure":
		if err := validateAzureContainer(bucket); err != nil {
			return nil, err
		}
		return &azureBackend{container: bucket}, nil
	default:
		return nil, fmt.Errorf("unknown backup backend %q", kind)
	}
}

var (
	s3BucketRE    = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
	gcsBucketRE   = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*[a-z0-9]$`)
	azureBucketRE = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)
	unsafePathRE  = regexp.MustCompile("[;&|$><`'\"*?\\[\\]{}()!]")
)

func validateS3Bucket(name string) error {
	if len(name) < 3 || len(name) > 63 || !s3BucketRE.MatchString(name) || strings.Contains(name, "..") || net.ParseIP(name) != nil ||
		strings.HasPrefix(name, "xn--") || strings.HasPrefix(name, "sthree-") || strings.HasPrefix(name, "amzn-s3-demo-") ||
		strings.HasSuffix(name, "-s3alias") || strings.HasSuffix(name, "--ol-s3") || strings.HasSuffix(name, ".mrap") ||
		strings.HasSuffix(name, "--x-s3") || strings.HasSuffix(name, "--table-s3") {
		return errors.New("invalid S3 bucket name")
	}
	return nil
}

func validateGCSBucket(name string) error {
	if len(name) < 3 || len(name) > 222 || !gcsBucketRE.MatchString(name) || strings.Contains(name, "..") || net.ParseIP(name) != nil ||
		strings.HasPrefix(name, "goog") || strings.Contains(name, "google") || strings.Contains(name, "g00gle") {
		return errors.New("invalid GCS bucket name")
	}
	for _, component := range strings.Split(name, ".") {
		if len(component) == 0 || len(component) > 63 {
			return errors.New("invalid GCS bucket name")
		}
	}
	return nil
}

func validateAzureContainer(name string) error {
	if len(name) < 3 || len(name) > 63 || !azureBucketRE.MatchString(name) || strings.Contains(name, "--") {
		return errors.New("invalid Azure container name")
	}
	return nil
}

func validateRemotePath(value string) error {
	if value == "" || len(value) > 1024 || strings.HasPrefix(value, "/") || strings.HasPrefix(value, "-") || strings.Contains(value, ":") ||
		strings.Contains(value, `\`) || unsafePathRE.MatchString(value) || path.Clean(value) != value {
		return errors.New("invalid object-store relative path")
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return errors.New("invalid object-store relative path")
		}
	}
	for _, segment := range strings.Split(value, "/") {
		if segment == "" || segment == "." || segment == ".." || strings.HasPrefix(segment, "-") {
			return errors.New("invalid object-store relative path")
		}
	}
	return nil
}

func validateLocalPath(value string) error {
	if value == "" || !strings.HasPrefix(value, "/") || path.Clean(value) != value || strings.Contains(value, `\`) || unsafePathRE.MatchString(value) {
		return errors.New("invalid local backup path")
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return errors.New("invalid local backup path")
		}
	}
	for _, segment := range strings.Split(strings.TrimPrefix(value, "/"), "/") {
		if segment == "" || segment == "." || segment == ".." || strings.HasPrefix(segment, "-") {
			return errors.New("invalid local backup path")
		}
	}
	return nil
}

func classifyBackendFailure(provider backendProvider, operation string, result BackendCommandResult, execErr error) error {
	if execErr != nil {
		kind := BackendFailure
		var transportErr *backendCommandTransportError
		if errors.As(execErr, &transportErr) {
			kind = transportErr.Kind
		}
		return &BackendError{Kind: kind, Operation: operation}
	}
	output := strings.ToLower(result.Stderr + "\n" + result.Stdout)
	kind := BackendFailure
	switch {
	case strings.Contains(output, "accessdenied"), strings.Contains(output, "unauthorized"), strings.Contains(output, "authentication"), strings.Contains(output, "credentials"), strings.Contains(output, "permission denied"), strings.Contains(output, "forbidden"),
		strings.Contains(output, "authorizationpermissionmismatch"), strings.Contains(output, "invalidauthenticationinfo"), strings.Contains(output, "invalidaccesskeyid"), strings.Contains(output, "signaturedoesnotmatch"), strings.Contains(output, "expiredtoken"), strings.Contains(output, "invalid_grant"):
		kind = BackendAuthentication
	case strings.Contains(output, "timeout"), strings.Contains(output, "timed out"), strings.Contains(output, "connection"), strings.Contains(output, "network"), strings.Contains(output, "dns"), strings.Contains(output, "resolve host"),
		strings.Contains(output, "could not connect to endpoint url"), strings.Contains(output, "name or service not known"), strings.Contains(output, "temporary failure in name resolution"):
		kind = BackendNetwork
	case provider == backendS3 && (strings.Contains(output, "nosuchkey") || strings.Contains(output, "specified key does not exist")):
		kind = BackendNotFound
	case provider == backendGCS && (strings.Contains(output, "matched no objects") || strings.Contains(output, "no urls matched")):
		kind = BackendNotFound
	case provider == backendAzure && (strings.Contains(output, "blobnotfound") || strings.Contains(output, "pathnotfound")):
		kind = BackendNotFound
	}
	return &BackendError{Kind: kind, Operation: operation, ExitCode: result.ExitCode}
}

func runCommand(ctx context.Context, provider backendProvider, executor BackendCommandExecutor, operation string, command []string, input []byte) (BackendCommandResult, error) {
	if ctx == nil {
		return BackendCommandResult{}, &BackendError{Kind: BackendFailure, Operation: operation}
	}
	if executor == nil {
		return BackendCommandResult{}, &BackendError{Kind: BackendFailure, Operation: operation}
	}
	var inputCopy []byte
	if input != nil {
		inputCopy = make([]byte, len(input))
		copy(inputCopy, input)
	}
	result, err := executor.Execute(ctx, append([]string(nil), command...), inputCopy)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return result, err
	}
	if err != nil || result.ExitCode != 0 {
		return result, classifyBackendFailure(provider, operation, result, err)
	}
	return result, nil
}

type s3Backend struct{ bucket string }
type s3Store struct {
	backend  *s3Backend
	executor BackendCommandExecutor
}

func (b *s3Backend) Bind(e BackendCommandExecutor) ObjectStore {
	return &s3Store{backend: b, executor: e}
}
func (b *s3Backend) URI(p string) (string, error) {
	if err := validateRemotePath(p); err != nil {
		return "", err
	}
	return "s3://" + b.bucket + "/" + p, nil
}
func (s *s3Store) uri(p string) (string, error) { return s.backend.URI(p) }
func (s *s3Store) UploadFile(ctx context.Context, local, remote string) error {
	if err := validateLocalPath(local); err != nil {
		return err
	}
	uri, err := s.uri(remote)
	if err != nil {
		return err
	}
	_, err = runCommand(ctx, backendS3, s.executor, "upload", []string{"aws", "s3", "cp", local, uri}, nil)
	return err
}
func (s *s3Store) DownloadFile(ctx context.Context, remote, local string) error {
	if err := validateLocalPath(local); err != nil {
		return err
	}
	uri, err := s.uri(remote)
	if err != nil {
		return err
	}
	_, err = runCommand(ctx, backendS3, s.executor, "download", []string{"aws", "s3", "cp", uri, local}, nil)
	return err
}
func (s *s3Store) UploadBytes(ctx context.Context, data []byte, remote string) error {
	uri, err := s.uri(remote)
	if err != nil {
		return err
	}
	_, err = runCommand(ctx, backendS3, s.executor, "upload", []string{"aws", "s3", "cp", "-", uri}, data)
	return err
}
func (s *s3Store) DownloadBytes(ctx context.Context, remote string) ([]byte, error) {
	uri, err := s.uri(remote)
	if err != nil {
		return nil, err
	}
	result, err := runCommand(ctx, backendS3, s.executor, "download", []string{"aws", "s3", "cp", uri, "-"}, nil)
	return []byte(result.Stdout), err
}
func (s *s3Store) CopyPrefix(ctx context.Context, src, dst string) error {
	srcURI, err := s.uri(src)
	if err != nil {
		return err
	}
	dstURI, err := s.uri(dst)
	if err != nil {
		return err
	}
	_, err = runCommand(ctx, backendS3, s.executor, "copy", []string{"aws", "s3", "cp", srcURI + "/", dstURI + "/", "--recursive", "--quiet"}, nil)
	return err
}
func (s *s3Store) DeletePrefix(ctx context.Context, p string) error {
	uri, err := s.uri(p)
	if err != nil {
		return err
	}
	_, err = runCommand(ctx, backendS3, s.executor, "delete", []string{"aws", "s3", "rm", uri + "/", "--recursive", "--quiet"}, nil)
	return err
}
func (s *s3Store) Exists(ctx context.Context, p string) (bool, error) {
	if err := validateRemotePath(p); err != nil {
		return false, err
	}
	result, err := runCommand(ctx, backendS3, s.executor, "exists", []string{"aws", "s3api", "list-objects-v2", "--bucket", s.backend.bucket, "--prefix", p + "/", "--max-keys", "1", "--query", "KeyCount", "--output", "text"}, nil)
	if err != nil {
		return false, err
	}
	n, convErr := strconv.Atoi(strings.TrimSpace(result.Stdout))
	if convErr != nil {
		return false, &BackendError{Kind: BackendFailure, Operation: "exists"}
	}
	return n > 0, nil
}

type gcsBackend struct{ bucket string }
type gcsStore struct {
	backend  *gcsBackend
	executor BackendCommandExecutor
}

func (b *gcsBackend) Bind(e BackendCommandExecutor) ObjectStore {
	return &gcsStore{backend: b, executor: e}
}
func (b *gcsBackend) URI(p string) (string, error) {
	if err := validateRemotePath(p); err != nil {
		return "", err
	}
	return "gs://" + b.bucket + "/" + p, nil
}
func (s *gcsStore) uri(p string) (string, error) { return s.backend.URI(p) }
func (s *gcsStore) UploadFile(ctx context.Context, local, remote string) error {
	if err := validateLocalPath(local); err != nil {
		return err
	}
	uri, err := s.uri(remote)
	if err != nil {
		return err
	}
	_, err = runCommand(ctx, backendGCS, s.executor, "upload", []string{"gcloud", "storage", "cp", local, uri}, nil)
	return err
}
func (s *gcsStore) DownloadFile(ctx context.Context, remote, local string) error {
	if err := validateLocalPath(local); err != nil {
		return err
	}
	uri, err := s.uri(remote)
	if err != nil {
		return err
	}
	_, err = runCommand(ctx, backendGCS, s.executor, "download", []string{"gcloud", "storage", "cp", uri, local}, nil)
	return err
}
func (s *gcsStore) UploadBytes(ctx context.Context, data []byte, remote string) error {
	uri, err := s.uri(remote)
	if err != nil {
		return err
	}
	_, err = runCommand(ctx, backendGCS, s.executor, "upload", []string{"gcloud", "storage", "cp", "-", uri}, data)
	return err
}
func (s *gcsStore) DownloadBytes(ctx context.Context, remote string) ([]byte, error) {
	uri, err := s.uri(remote)
	if err != nil {
		return nil, err
	}
	result, err := runCommand(ctx, backendGCS, s.executor, "download", []string{"gcloud", "storage", "cp", uri, "-"}, nil)
	return []byte(result.Stdout), err
}
func (s *gcsStore) CopyPrefix(ctx context.Context, src, dst string) error {
	srcURI, err := s.uri(src)
	if err != nil {
		return err
	}
	dstURI, err := s.uri(dst)
	if err != nil {
		return err
	}
	_, err = runCommand(ctx, backendGCS, s.executor, "copy", []string{"gcloud", "storage", "rsync", srcURI, dstURI, "--recursive"}, nil)
	return err
}
func (s *gcsStore) DeletePrefix(ctx context.Context, p string) error {
	uri, err := s.uri(p)
	if err != nil {
		return err
	}
	_, err = runCommand(ctx, backendGCS, s.executor, "delete", []string{"gcloud", "storage", "rm", uri + "/", "--recursive"}, nil)
	return err
}
func (s *gcsStore) Exists(ctx context.Context, p string) (bool, error) {
	if err := validateRemotePath(p); err != nil {
		return false, err
	}
	filter := "--filter=name~^" + regexp.QuoteMeta(p+"/")
	result, err := runCommand(ctx, backendGCS, s.executor, "exists", []string{"gcloud", "storage", "objects", "list", "gs://" + s.backend.bucket, filter, "--limit=1", "--format=value(name)"}, nil)
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(result.Stdout) != "", nil
}

type azureBackend struct{ container string }
type azureStore struct {
	backend  *azureBackend
	executor BackendCommandExecutor
}

func (b *azureBackend) Bind(e BackendCommandExecutor) ObjectStore {
	return &azureStore{backend: b, executor: e}
}
func (b *azureBackend) URI(p string) (string, error) {
	if err := validateRemotePath(p); err != nil {
		return "", err
	}
	return "az://" + b.container + "/" + p, nil
}
func (s *azureStore) login(ctx context.Context) error {
	_, err := runCommand(ctx, backendAzure, s.executor, "authenticate", []string{"env", "AZURE_CONFIG_DIR=/tmp/.azure", "az", "login", "--identity", "--allow-no-subscriptions", "--output", "none"}, nil)
	return err
}
func (s *azureStore) run(ctx context.Context, operation string, args []string, input []byte) (BackendCommandResult, error) {
	return runCommand(ctx, backendAzure, s.executor, operation, append([]string{"env", "AZURE_CONFIG_DIR=/tmp/.azure", "az"}, args...), input)
}
func (s *azureStore) runAuthenticated(ctx context.Context, operation string, args []string, input []byte) (BackendCommandResult, error) {
	if err := s.login(ctx); err != nil {
		return BackendCommandResult{}, err
	}
	return s.run(ctx, operation, args, input)
}
func (s *azureStore) UploadFile(ctx context.Context, local, remote string) error {
	if err := validateLocalPath(local); err != nil {
		return err
	}
	if err := validateRemotePath(remote); err != nil {
		return err
	}
	_, err := s.runAuthenticated(ctx, "upload", []string{"storage", "blob", "upload", "--auth-mode", "login", "--overwrite", "true", "--container-name", s.backend.container, "--name", remote, "--file", local, "--only-show-errors"}, nil)
	return err
}
func (s *azureStore) DownloadFile(ctx context.Context, remote, local string) error {
	if err := validateRemotePath(remote); err != nil {
		return err
	}
	if err := validateLocalPath(local); err != nil {
		return err
	}
	_, err := s.runAuthenticated(ctx, "download", []string{"storage", "blob", "download", "--auth-mode", "login", "--overwrite", "true", "--container-name", s.backend.container, "--name", remote, "--file", local, "--only-show-errors"}, nil)
	return err
}
func (s *azureStore) UploadBytes(ctx context.Context, data []byte, remote string) error {
	if err := validateRemotePath(remote); err != nil {
		return err
	}
	_, err := s.runAuthenticated(ctx, "upload", []string{"storage", "blob", "upload", "--auth-mode", "login", "--overwrite", "true", "--container-name", s.backend.container, "--name", remote, "--data", "@-", "--only-show-errors"}, data)
	return err
}
func (s *azureStore) DownloadBytes(ctx context.Context, remote string) ([]byte, error) {
	if err := validateRemotePath(remote); err != nil {
		return nil, err
	}
	result, err := s.runAuthenticated(ctx, "download", []string{"storage", "blob", "download", "--auth-mode", "login", "--container-name", s.backend.container, "--name", remote, "--only-show-errors"}, nil)
	return []byte(result.Stdout), err
}
func (s *azureStore) DeletePrefix(ctx context.Context, p string) error {
	if err := validateRemotePath(p); err != nil {
		return err
	}
	if err := s.login(ctx); err != nil {
		return err
	}
	result, err := s.run(ctx, "delete", []string{"storage", "blob", "list", "--auth-mode", "login", "--container-name", s.backend.container, "--prefix", p + "/", "--query", "[].name", "--output", "tsv", "--only-show-errors"}, nil)
	if err != nil {
		return err
	}
	for _, name := range strings.Split(strings.TrimSpace(result.Stdout), "\n") {
		name = strings.TrimSuffix(name, "\r")
		if name == "" {
			continue
		}
		if err := validateRemotePath(name); err != nil {
			return &BackendError{Kind: BackendFailure, Operation: "delete"}
		}
		if _, err := s.run(ctx, "delete", []string{"storage", "blob", "delete", "--auth-mode", "login", "--container-name", s.backend.container, "--name", name, "--only-show-errors"}, nil); err != nil {
			return err
		}
	}
	return nil
}
func (s *azureStore) CopyPrefix(ctx context.Context, src, dst string) error {
	if err := validateRemotePath(src); err != nil {
		return err
	}
	if err := validateRemotePath(dst); err != nil {
		return err
	}
	if err := s.login(ctx); err != nil {
		return err
	}
	result, err := s.run(ctx, "copy", []string{"storage", "blob", "list", "--auth-mode", "login", "--container-name", s.backend.container, "--prefix", src + "/", "--query", "[].name", "--output", "tsv", "--only-show-errors"}, nil)
	if err != nil {
		return err
	}
	if strings.TrimSpace(result.Stdout) == "" {
		return &BackendError{Kind: BackendNotFound, Operation: "copy"}
	}
	for _, name := range strings.Split(strings.TrimSpace(result.Stdout), "\n") {
		name = strings.TrimSuffix(name, "\r")
		if err := validateRemotePath(name); err != nil || !strings.HasPrefix(name, src+"/") {
			return &BackendError{Kind: BackendFailure, Operation: "copy"}
		}
		destination := dst + "/" + strings.TrimPrefix(name, src+"/")
		if err := validateRemotePath(destination); err != nil {
			return &BackendError{Kind: BackendFailure, Operation: "copy"}
		}
		if _, err := s.run(ctx, "copy", []string{"storage", "blob", "copy", "start", "--auth-mode", "login", "--destination-container", s.backend.container, "--destination-blob", destination, "--source-container", s.backend.container, "--source-blob", name, "--requires-sync", "true", "--only-show-errors"}, nil); err != nil {
			return err
		}
	}
	return nil
}
func (s *azureStore) Exists(ctx context.Context, p string) (bool, error) {
	if err := validateRemotePath(p); err != nil {
		return false, err
	}
	result, err := s.runAuthenticated(ctx, "exists", []string{"storage", "blob", "list", "--auth-mode", "login", "--container-name", s.backend.container, "--prefix", p + "/", "--num-results", "1", "--query", "[0].name", "--output", "tsv", "--only-show-errors"}, nil)
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(result.Stdout) != "", nil
}
