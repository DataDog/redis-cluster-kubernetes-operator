package kube

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"regexp"
	"strings"

	"github.com/DataDog/redis-cluster-kubernetes-operator/utils"
	"github.com/go-logr/logr"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	exec2 "k8s.io/client-go/util/exec"
)

// Defense-in-depth redaction for legacy or accidental REDISCLI_AUTH command
// arguments. The supported mounted-Secret design never sends credentials over
// exec, but sanitizing both plain and URL-encoded forms prevents a future
// caller regression from leaking them through debug or SPDY error messages.
var (
	// Plaintext forms inside command strings.
	redisAuthPlainQuotedRE = regexp.MustCompile(`REDISCLI_AUTH="[^"]*"`)
	redisAuthPlainRE       = regexp.MustCompile(`REDISCLI_AUTH=[^\s"&]+`)
	// URL-encoded forms inside apiserver URLs.
	redisAuthURLEncQuotedRE = regexp.MustCompile(`REDISCLI_AUTH%3D%22[^%]*%22`)
	redisAuthURLEncRE       = regexp.MustCompile(`REDISCLI_AUTH%3D[^%&\s]+`)
)

type ExecErrorKind string

const (
	ExecAuthentication ExecErrorKind = "authentication"
	ExecNetwork        ExecErrorKind = "network"
	ExecBackend        ExecErrorKind = "backend"
)

// ExecError carries only a sanitized failure and its transport category.
type ExecError struct {
	Kind  ExecErrorKind
	cause error
}

func (e *ExecError) Error() string { return e.cause.Error() }
func (e *ExecError) Unwrap() error { return e.cause }

func newExecError(err error) *ExecError {
	sanitized := errors.New(redactRedisAuth(err.Error()))
	message := strings.ToLower(sanitized.Error())
	kind := ExecBackend
	switch {
	case strings.Contains(message, "unauthorized"), strings.Contains(message, "forbidden"), strings.Contains(message, "status code 401"), strings.Contains(message, "status code 403"):
		kind = ExecAuthentication
	case strings.Contains(message, "timeout"), strings.Contains(message, "timed out"), strings.Contains(message, "connection"), strings.Contains(message, "dial "), strings.Contains(message, "dns"), strings.Contains(message, "resolve host"), strings.Contains(message, "unexpected eof"):
		kind = ExecNetwork
	}
	return &ExecError{Kind: kind, cause: sanitized}
}

func redactRedisAuth(s string) string {
	s = redisAuthPlainQuotedRE.ReplaceAllString(s, `REDISCLI_AUTH="REDACTED"`)
	s = redisAuthPlainRE.ReplaceAllString(s, `REDISCLI_AUTH=REDACTED`)
	s = redisAuthURLEncQuotedRE.ReplaceAllString(s, `REDISCLI_AUTH%3D%22REDACTED%22`)
	s = redisAuthURLEncRE.ReplaceAllString(s, `REDISCLI_AUTH%3DREDACTED`)
	return s
}

func redactCommand(command []string) []string {
	out := make([]string, len(command))
	for i, c := range command {
		out[i] = redactRedisAuth(c)
	}
	return out
}

func execStreamFailure(log logr.Logger, err error, _ string, _ string) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	execErr := newExecError(err)
	log.Error(execErr, "Failed exec.Stream", "err", reflect.TypeOf(err))
	return utils.WrapErr(execErr, "failed exec.Stream")
}

// Exec provides support for executing k8s commands.
type Exec struct {
	config    *rest.Config
	clientSet *kubernetes.Clientset
	namespace string
}

// NewKubeExec creates a new Exec instance.
func NewKubeExec(config *rest.Config, namespace string, log logr.Logger) (*Exec, error) {

	// create the clientSet
	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Error(err, "failed to create a clientSet using the config")
		return nil, err
	}

	return &Exec{
		config:    config,
		clientSet: clientSet,
		namespace: namespace,
	}, nil
}

// ExecNC executes a command on a specific pod and wait for the command's output.
func (c *Exec) ExecNC(ctx context.Context, podName string, command []string, log logr.Logger) (int, string, string, error) {
	return c.exec(ctx, podName, "", command, nil, log, true)
}

// Exec executes a command on a specific pod and container and wait for the command's output.
func (c *Exec) Exec(ctx context.Context, podName string, containerName string, command []string, log logr.Logger) (int, string, string, error) {
	return c.exec(ctx, podName, containerName, command, nil, log, true)
}

// ExecWithInput executes a fixed argv command and transports payload only over
// stdin. Arguments are summarized rather than logged, and the payload is never
// included in command logging or the exec request URL.
func (c *Exec) ExecWithInput(ctx context.Context, podName, containerName string, command []string, input []byte, log logr.Logger) (int, string, string, error) {
	return c.exec(ctx, podName, containerName, command, input, log, false)
}

// Executes a command on a specific pod and optional container and wait for the command's output.
func (c *Exec) exec(ctx context.Context, podName string, containerName string, command []string, input []byte, log logr.Logger, logArguments bool) (int, string, string, error) {
	logExecCommand(log, podName, containerName, command, logArguments)

	req := c.clientSet.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(c.namespace).
		SubResource("exec")

	if containerName != "" {
		req = req.Param("container", containerName)
	}

	options := podExecOptions(command)
	if input != nil {
		options = podExecOptionsWithInput(command)
	}
	req.VersionedParams(options, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(c.config, "POST", req.URL())
	if err != nil {
		execErr := newExecError(err)
		log.Error(execErr, "Failed remotecommand.NewSPDYExecutor")
		return 0, "", "", utils.WrapErr(execErr, "failed remotecommand.NewSPDYExecutor")
	}

	var stdout, stderr strings.Builder
	var stdin io.Reader
	if input != nil {
		stdin = bytes.NewReader(input)
	}
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  stdin,
		Stdout: &stdout,
		Stderr: &stderr,
		Tty:    false,
	})

	strout := stdout.String()
	strerr := stderr.String()

	var codeExitError exec2.CodeExitError
	rc := 0
	if err != nil {
		if errors.As(err, &codeExitError) {
			rc = codeExitError.Code // an error is reported anytime rc != 0
		} else {
			// Captured output can contain streamed manifests or provider-sensitive
			// diagnostics. The failure helper deliberately ignores both streams.
			return 0, "", "", execStreamFailure(log, err, strout, strerr)
		}
	}

	// xlog.Debug("Executed command", "stdout", strout, "stderr", strerr, "rc", rc)
	return rc, strout, strerr, nil
}

func logExecCommand(log logr.Logger, podName, containerName string, command []string, logArguments bool) {
	xlog := utils.XLog(log)
	if logArguments {
		xlog.Debug("Execute command", "podName", podName, "containerName", containerName, "command", redactCommand(command))
		return
	}
	executable := ""
	argumentCount := 0
	if len(command) > 0 {
		executable = command[0]
		argumentCount = len(command) - 1
	}
	xlog.Debug("Execute command", "podName", podName, "containerName", containerName, "executable", executable, "argumentCount", argumentCount)
}

func podExecOptions(command []string) *v1.PodExecOptions {
	return &v1.PodExecOptions{
		Command: append([]string(nil), command...),
		Stdin:   false,
		Stdout:  true,
		Stderr:  true,
		TTY:     false,
	}
}

func podExecOptionsWithInput(command []string) *v1.PodExecOptions {
	options := podExecOptions(command)
	options.Stdin = true
	return options
}
