package nfs

import (
	"context"
	"sync"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/filestore/public/api/protos"
	nfs_client "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client"
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
	mutex            sync.RWMutex
}

func newEndpointPicker(
	ctx context.Context,
	endpoints []string,
) *endpointPicker {

	healthyEndpoints := make([]string, len(endpoints))
	copy(healthyEndpoints, endpoints)

	p := &endpointPicker{
		endpoints:        endpoints,
		healthyEndpoints: healthyEndpoints,
	}
	go func() {
		ticker := time.NewTicker(endpointPickerCheckPeriod)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				for _, endpoint := range endpoints {
					p.checkEndpoint(ctx, endpoint)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return p
}

func (p *endpointPicker) checkEndpoint(ctx context.Context, endpoint string) {
	client, err := nfs_client.NewGrpcEndpointClient(
		&nfs_client.GrpcClientOpts{
			Endpoint: endpoint,
			// Credentials: not needed here
			Timeout: &endpointPickerCheckTimeout,
		},
		NewNfsClientLog(nfs_client.LOG_DEBUG),
	)
	if err != nil {
		p.markAsUnhealthy(endpoint)
		return
	}
	defer client.Close()

	_, err = client.Ping(ctx, &protos.TPingRequest{})
	if err != nil {
		p.markAsUnhealthy(endpoint)
		return
	}

	p.markAsHealthy(endpoint)
}

func (p *endpointPicker) markAsUnhealthy(endpoint string) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.healthyEndpoints = common.Remove(p.healthyEndpoints, endpoint)
}

func (p *endpointPicker) markAsHealthy(endpoint string) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !common.Find(p.healthyEndpoints, endpoint) {
		p.healthyEndpoints = append(p.healthyEndpoints, endpoint)
	}
}

func (p *endpointPicker) pickEndpoint() string {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return common.RandomElement(p.healthyEndpoints)
}
