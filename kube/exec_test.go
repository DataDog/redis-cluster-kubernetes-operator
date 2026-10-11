package kube

import (
	"context"
	"errors"
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
	err := execStreamFailure(log, errors.New("transport failed"), payload, "provider-secret")
	combined := logs.String() + err.Error()
	if strings.Contains(combined, payload) || strings.Contains(combined, "provider-secret") {
		t.Fatalf("captured exec output leaked through stream failure: %s", combined)
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
		err := execStreamFailure(funcr.New(func(string, string) {}, funcr.Options{}), sentinel, "partial manifest", "provider output")
		if !errors.Is(err, sentinel) {
			t.Fatalf("stream failure lost context sentinel %v: %v", sentinel, err)
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
