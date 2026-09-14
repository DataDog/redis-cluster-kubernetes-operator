package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type failingListClient struct {
	client.Client
	err error
}

func (c failingListClient) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return c.err
}

func TestAutoscalingSafetyBlockerDoesNotExposePodListError(t *testing.T) {
	ctx := context.Background()
	reconciler, cluster, k8sClient := healthyAutoscalingFixture(t, 3)
	const sensitiveDetail = "forbidden /internal/path: database driver detail"
	reconciler.Client = failingListClient{
		Client: k8sClient,
		err:    errors.New(sensitiveDetail),
	}

	blocker := reconciler.autoscalingSafetyBlocker(ctx, cluster, true)

	assert.Equal(t, "could not inspect Redis pods", blocker)
	assert.NotContains(t, blocker, sensitiveDetail)
}
