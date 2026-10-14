package kube

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-logr/logr/funcr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
)

func TestExecStreamFailureDoesNotLogCapturedOutput(t *testing.T) {
	const payload = `{"manifest":"sensitive"}`
	var logs strings.Builder
	log := funcr.New(func(prefix, args string) {
		logs.WriteString(prefix)
		logs.WriteString(args)
	}, funcr.Options{})
	err := execStreamFailure(log, errors.New("transport failed"), payload, "provider-secret", true)
	combined := logs.String() + err.Error()
	if strings.Contains(combined, payload) || strings.Contains(combined, "provider-secret") {
		t.Fatalf("captured exec output leaked through stream failure: %s", combined)
	}
}

func TestExecWithInputCommandLogOmitsProviderArguments(t *testing.T) {
	const credential = "https://storage.example/backup?signature=secret-token"
	var logs strings.Builder
	log := funcr.New(func(prefix, args string) {
		logs.WriteString(prefix)
		logs.WriteString(args)
	}, funcr.Options{Verbosity: 1})

	logExecCommand(log, "redis-0", "backup", []string{"rclone", "copyto", "-", credential}, false)
	combined := logs.String()
	if strings.Contains(combined, credential) || strings.Contains(combined, "secret-token") {
		t.Fatalf("provider argument leaked through command logging: %s", combined)
	}
	if !strings.Contains(combined, "rclone") || !strings.Contains(combined, "argumentCount") {
		t.Fatalf("safe command summary missing: %s", combined)
	}
}

func TestSensitiveExecTransportFailureOmitsDetails(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		failure   error
		kind      ExecErrorKind
	}{
		{
			name:      "SPDY setup",
			operation: "remotecommand.NewSPDYExecutor",
			failure:   errors.New("unable to upgrade connection for https://storage.example/backup?signature=spdy-secret: status code 403 Forbidden"),
			kind:      ExecAuthentication,
		},
		{
			name:      "stream",
			operation: "exec.Stream",
			failure:   errors.New("dial tcp for command rclone copyto - https://storage.example/backup?signature=stream-secret: connection timed out"),
			kind:      ExecNetwork,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var logs strings.Builder
			log := funcr.New(func(prefix, args string) {
				logs.WriteString(prefix)
				logs.WriteString(args)
			}, funcr.Options{})

			err := execTransportFailure(log, tc.failure, tc.operation, false)
			combined := logs.String() + err.Error()
			for _, secret := range []string{"spdy-secret", "stream-secret", "storage.example", "rclone copyto"} {
				if strings.Contains(combined, secret) {
					t.Fatalf("sensitive transport details leaked through failure: %s", combined)
				}
			}
			if !strings.Contains(combined, "exec transport error (details omitted)") {
				t.Fatalf("generic transport failure missing: %s", combined)
			}
			var execErr *ExecError
			if !errors.As(err, &execErr) {
				t.Fatalf("transport failure does not wrap ExecError: %v", err)
			}
			if execErr.Kind != tc.kind {
				t.Fatalf("got kind %q, want %q", execErr.Kind, tc.kind)
			}
		})
	}
}

func TestOrdinaryExecTransportFailureRetainsSanitizedDetails(t *testing.T) {
	const credential = "REDISCLI_AUTH=plain-secret"
	const diagnostic = "dial tcp: connection timed out"
	var logs strings.Builder
	log := funcr.New(func(prefix, args string) {
		logs.WriteString(prefix)
		logs.WriteString(args)
	}, funcr.Options{})

	err := execTransportFailure(log, errors.New(diagnostic+" "+credential), "exec.Stream", true)
	combined := logs.String() + err.Error()
	if !strings.Contains(combined, diagnostic) {
		t.Fatalf("ordinary transport diagnostic missing: %s", combined)
	}
	if strings.Contains(combined, "plain-secret") {
		t.Fatalf("Redis credential leaked through transport failure: %s", combined)
	}
	if !strings.Contains(combined, "REDISCLI_AUTH=REDACTED") {
		t.Fatalf("redacted credential marker missing: %s", combined)
	}
}

func TestExecErrorClassification(t *testing.T) {
	tests := []struct {
		message string
		kind    ExecErrorKind
	}{
		{"server returned status code 403 Forbidden", ExecAuthentication},
		{"dial tcp: connection timed out", ExecNetwork},
		{"SPDY protocol negotiation failed", ExecBackend},
	}
	for _, tc := range tests {
		t.Run(tc.message, func(t *testing.T) {
			err := newExecError(errors.New(tc.message))
			if err.Kind != tc.kind {
				t.Fatalf("got kind %q, want %q", err.Kind, tc.kind)
			}
		})
	}
}

func TestExecStreamFailurePreservesContextSentinels(t *testing.T) {
	for _, sentinel := range []error{context.Canceled, context.DeadlineExceeded} {
		for _, exposeDetails := range []bool{false, true} {
			original := fmt.Errorf("request URL contains secret-token: %w", sentinel)
			err := execTransportFailure(funcr.New(func(string, string) {}, funcr.Options{}), original, "exec.Stream", exposeDetails)
			if !errors.Is(err, sentinel) {
				t.Fatalf("stream failure lost context sentinel %v with exposeDetails=%t: %v", sentinel, exposeDetails, err)
			}
			if !exposeDetails && strings.Contains(err.Error(), "secret-token") {
				t.Fatalf("sensitive context failure leaked transport details: %v", err)
			}
		}
	}
}

func TestPodExecRequestSerializationContainsNoMountedSecretCredentials(t *testing.T) {
	secret := &corev1.Secret{Data: map[string][]byte{
		"username": []byte(`operator;$(touch /tmp/user-leak)`),
		"password": []byte(`p@ss word;&$(touch /tmp/password-leak)`),
	}}
	command := []string{
		"/usr/local/bin/redis-operator-redis-cli",
		"-h", "10.0.0.1", "-p", "6379", "CLUSTER", "INFO",
	}
	options := podExecOptions(command)
	values, err := scheme.ParameterCodec.EncodeParameters(options, corev1.SchemeGroupVersion)
	if err != nil {
		t.Fatal(err)
	}
	serialized := values.Encode()
	for key, credential := range secret.Data {
		if strings.Contains(strings.Join(command, "\x00"), string(credential)) {
			t.Fatalf("exec argv leaked the mounted Secret %s", key)
		}
		if strings.Contains(serialized, string(credential)) {
			t.Fatalf("PodExecOptions URL query leaked the mounted Secret %s: %s", key, serialized)
		}
	}
	got := values["command"]
	if strings.Join(got, "\x00") != strings.Join(command, "\x00") {
		t.Fatalf("PodExecOptions changed command arguments: got %#v want %#v", got, command)
	}
}

func TestPodExecOptionsWithInputEnablesStdinWithoutSerializingPayload(t *testing.T) {
	payload := []byte(`{"password":"p@ss;$(touch /tmp/pwned)"}`)
	options := podExecOptionsWithInput([]string{"aws", "s3", "cp", "-", "s3://bucket/manifest.json"})
	values, err := scheme.ParameterCodec.EncodeParameters(options, corev1.SchemeGroupVersion)
	if err != nil {
		t.Fatal(err)
	}
	if !options.Stdin {
		t.Fatal("stdin must be enabled for typed byte transport")
	}
	if strings.Contains(values.Encode(), string(payload)) {
		t.Fatal("stdin payload was serialized into the exec request URL")
	}
}
