package nfs

import (
	"context"
	"sync"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/filestore/public/api/protos"
	nfs_client "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

var endpointPickerCheckTimeout = 2 * time.Second
var endpointPickerCheckPeriod = 5 * time.Second

////////////////////////////////////////////////////////////////////////////////

// Implements pretty stupid algorithm for picking healthy endpoint.
// Should be superseded by Filestore public SDK discovery client once it is
// implemented.
type endpointPicker struct {
	endpoints        []string
	healthyEndpoints []string
	mutex            sync.Mutex
	markedAsHealthy  common.Cond
}

func newEndpointPicker(
	ctx context.Context,
	endpoints []string,
) *endpointPicker {

	p := &endpointPicker{
		endpoints: endpoints,
	}
	p.markedAsHealthy = common.NewCond(&p.mutex)

	go func() {
		ticker := time.NewTicker(endpointPickerCheckPeriod)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				for _, endpoint := range endpoints {
					p.checkHealth(ctx, endpoint)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return p
}

func (p *endpointPicker) checkHealth(ctx context.Context, endpoint string) {
	client, err := nfs_client.NewGrpcClient(
		&nfs_client.GrpcClientOpts{
			Endpoint: endpoint,
			// Credentials: not needed here
			Timeout: &endpointPickerCheckTimeout,
		},
		NewNfsClientLog(nfs_client.LOG_DEBUG),
	)
	if err != nil {
		p.markAsUnhealthy(ctx, endpoint)
		return
	}
	defer client.Close()

	logging.Debug(ctx, "pinging filestore endpoint %q", endpoint)

	_, err = client.Ping(ctx, &protos.TPingRequest{})
	if err != nil {
		p.markAsUnhealthy(ctx, endpoint)
		return
	}

	p.markAsHealthy(ctx, endpoint)
}

func (p *endpointPicker) markAsUnhealthy(
	ctx context.Context,
	endpoint string,
) {

	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.healthyEndpoints = common.Remove(p.healthyEndpoints, endpoint)
	logging.Info(ctx, "filestore endpoint %q marked as healthy", endpoint)
}

func (p *endpointPicker) markAsHealthy(ctx context.Context, endpoint string) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !common.Find(p.healthyEndpoints, endpoint) {
		p.healthyEndpoints = append(p.healthyEndpoints, endpoint)
		logging.Info(ctx, "filestore endpoint %q marked as unhealthy", endpoint)

		p.markedAsHealthy.Broadcast()
	}
}

func (p *endpointPicker) pickEndpoint(ctx context.Context) (string, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for len(p.healthyEndpoints) == 0 {
		logging.Info(
			ctx,
			"waiting for one of filestore endpoints to become ready",
		)
		err := p.markedAsHealthy.Wait(ctx)
		if err != nil {
			return "", err
		}
	}

	return common.RandomElement(p.healthyEndpoints), nil
}
