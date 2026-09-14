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
