// Package toolbox stubs the operator's pod-exec interface for tests.
// OnCommand is the operator-agnostic core; OnRedisCommand and
// OnS3Command are convenience adapters. All methods are goroutine-safe.
package toolbox

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
)

// Response is what a matched rule returns to the caller.
type Response struct {
	RC     int
	Stdout string
	Stderr string
}

// OK is RC=0, empty stdout, empty stderr.
func OK() Response { return Response{RC: 0} }

// Stdout is RC=0 with the given stdout.
func Stdout(s string) Response { return Response{RC: 0, Stdout: s} }

// Failure is the given non-zero RC plus stderr.
func Failure(rc int, stderr string) Response { return Response{RC: rc, Stderr: stderr} }

// Matcher returns true when the given Exec call should match a rule.
type Matcher func(podName, containerName string, command []string) bool

// Call is one recorded Exec invocation.
type Call struct {
	PodName       string
	ContainerName string
	Command       []string
	Stdin         []byte
	At            time.Time
}

// Stub is a goroutine-safe pod-exec stub.
type Stub struct {
	mu        sync.Mutex
	rules     []rule
	failNext  error
	delayNext time.Duration
	calls     []Call
	strict    bool
}

type rule struct {
	name     string
	matcher  Matcher
	response Response
}

// New returns a Stub with no rules. Unmatched commands return RC=127 unless Strict is set.
func New() *Stub {
	return &Stub{}
}

// Strict makes unmatched Exec calls return an error instead of RC=127.
func (s *Stub) Strict() *Stub {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.strict = true
	return s
}

// OnCommand registers a matcher → response rule. Last matching rule wins.
func (s *Stub) OnCommand(name string, matcher Matcher, response Response) *Stub {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rules = append(s.rules, rule{name: name, matcher: matcher, response: response})
	return s
}

// OnRedisCommand matches a `redis-cli ... <verb> ...` invocation. Verb is case-insensitive.
func (s *Stub) OnRedisCommand(verb string, response Response) *Stub {
	re := regexp.MustCompile(`(?i)\bredis-cli\b[^\n]*?\b` + regexp.QuoteMeta(strings.ToLower(verb)) + `\b`)
	matcher := func(pod, container string, cmd []string) bool {
		joined := strings.ToLower(strings.Join(cmd, " "))
		return re.MatchString(joined)
	}
	return s.OnCommand("redis:"+verb, matcher, response)
}

// OnS3Command matches a fixed-argv `aws ... s3 <verb> ...` invocation.
func (s *Stub) OnS3Command(verb string, response Response) *Stub {
	re := regexp.MustCompile(`\baws\b[^\n]*?\bs3\s+` + regexp.QuoteMeta(verb) + `\b`)
	matcher := func(pod, container string, cmd []string) bool {
		joined := strings.Join(cmd, " ")
		return re.MatchString(joined)
	}
	return s.OnCommand("s3:"+verb, matcher, response)
}

// FailNext makes the next Exec call return err. Cleared after one call.
func (s *Stub) FailNext(err error) *Stub {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failNext = err
	return s
}

// DelayNext makes the next Exec call sleep d before responding. Honours ctx.Done(). Cleared after one call.
func (s *Stub) DelayNext(d time.Duration) *Stub {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delayNext = d
	return s
}

// Calls returns a snapshot of every Exec call in invocation order. Each
// Call's Command slice is deep-copied so test-side mutation cannot write
// back into the stub's internal state.
func (s *Stub) Calls() []Call {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Call, len(s.calls))
	for i, c := range s.calls {
		out[i] = c
		out[i].Command = append([]string(nil), c.Command...)
		out[i].Stdin = append([]byte(nil), c.Stdin...)
	}
	return out
}

// Reset clears rules, calls, and pending FailNext/DelayNext. Strict mode is preserved.
func (s *Stub) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rules = nil
	s.failNext = nil
	s.delayNext = 0
	s.calls = nil
}

// Exec records the call, applies any pending FailNext/DelayNext, and dispatches to the most-recently-added matching rule.
func (s *Stub) Exec(ctx context.Context, podName, containerName string, command []string, _ logr.Logger) (int, string, string, error) {
	return s.exec(ctx, podName, containerName, command, nil)
}

// ExecWithInput records opaque stdin separately from argv.
func (s *Stub) ExecWithInput(ctx context.Context, podName, containerName string, command []string, input []byte, _ logr.Logger) (int, string, string, error) {
	return s.exec(ctx, podName, containerName, command, input)
}

func (s *Stub) exec(ctx context.Context, podName, containerName string, command []string, input []byte) (int, string, string, error) {
	var inputCopy []byte
	if input != nil {
		inputCopy = make([]byte, len(input))
		copy(inputCopy, input)
	}
	s.mu.Lock()
	s.calls = append(s.calls, Call{
		PodName:       podName,
		ContainerName: containerName,
		Command:       append([]string(nil), command...),
		Stdin:         inputCopy,
		At:            time.Now(),
	})
	failNext := s.failNext
	s.failNext = nil
	delay := s.delayNext
	s.delayNext = 0
	rules := s.rules
	strict := s.strict
	s.mu.Unlock()

	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return 0, "", "", ctx.Err()
		}
	}
	if failNext != nil {
		return 0, "", "", failNext
	}

	for i := len(rules) - 1; i >= 0; i-- {
		if rules[i].matcher(podName, containerName, command) {
			r := rules[i].response
			return r.RC, r.Stdout, r.Stderr, nil
		}
	}

	if strict {
		return 0, "", "", fmt.Errorf("toolbox stub: no rule matched command %q on pod %q/%q", command, podName, containerName)
	}
	return 127, "", "command not found (toolbox stub: no rule matched)\n", nil
}
