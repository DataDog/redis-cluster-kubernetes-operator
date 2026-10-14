// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPublicSchemeDoesNotRegisterDatadogAutoscalingKinds(t *testing.T) {
	for groupVersionKind := range scheme.AllKnownTypes() {
		assert.NotEqual(t, "datadoghq.com", groupVersionKind.Group)
	}
}
