package facade

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/auth"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/tracing"
	"google.golang.org/grpc"
	grpc_codes "google.golang.org/grpc/codes"
	grpc_status "google.golang.org/grpc/status"
)

////////////////////////////////////////////////////////////////////////////////

type request struct {
	name       string
	permission string
}

////////////////////////////////////////////////////////////////////////////////

var (
	requests = []request{
		{name: "DiskService.Create", permission: "disk-manager.disks.create"},
		{name: "DiskService.Delete", permission: "disk-manager.disks.delete"},
		{name: "DiskService.Resize", permission: "disk-manager.disks.update"},
		{name: "DiskService.Alter", permission: "disk-manager.disks.update"},
		{name: "DiskService.Assign", permission: "disk-manager.disks.update"},
		{name: "DiskService.Unassign", permission: "disk-manager.disks.update"},
		{name: "DiskService.DescribeModel", permission: "disk-manager.disks.get"},
		{name: "DiskService.Stat", permission: "disk-manager.disks.get"},
		{name: "DiskService.Migrate", permission: "disk-manager.disks.update"},
		{name: "DiskService.SendMigrationSignal", permission: "disk-manager.disks.update"},
		{name: "DiskService.Describe", permission: "disk-manager.disks.get"},

		{name: "ImageService.Create", permission: "disk-manager.images.create"},
		{name: "ImageService.Update", permission: "disk-manager.images.update"},
		{name: "ImageService.Delete", permission: "disk-manager.images.delete"},

		{name: "SnapshotService.Create", permission: "disk-manager.snapshots.create"},
		{name: "SnapshotService.Delete", permission: "disk-manager.snapshots.delete"},

		{name: "OperationService.Get", permission: "disk-manager.operations.get"},
		{name: "OperationService.Cancel", permission: "disk-manager.operations.update"},

		// TODO: Maybe we should have separate permissions for placement groups?
		{name: "PlacementGroupService.Create", permission: "disk-manager.disks.create"},
		{name: "PlacementGroupService.Delete", permission: "disk-manager.disks.delete"},
		{name: "PlacementGroupService.Alter", permission: "disk-manager.disks.update"},
		{name: "PlacementGroupService.List", permission: "disk-manager.disks.list"},
		{name: "PlacementGroupService.Describe", permission: "disk-manager.disks.get"},

		{name: "PrivateService.ScheduleBlankOperation", permission: "disk-manager.operations.create"},
		{name: "PrivateService.ReleaseBaseDisk", permission: "disk-manager.disks.delete"},
		{name: "PrivateService.RebaseOverlayDisk", permission: "disk-manager.disks.update"},
		{name: "PrivateService.RetireBaseDisk", permission: "disk-manager.disks.delete"},
		{name: "PrivateService.RetireBaseDisks", permission: "disk-manager.disks.delete"},
		{name: "PrivateService.OptimizeBaseDisks", permission: "disk-manager.disks.delete"},
		{name: "PrivateService.ConfigurePool", permission: "disk-manager.disks.create"},
		{name: "PrivateService.DeletePool", permission: "disk-manager.disks.delete"},
		{name: "PrivateService.ListDisks", permission: "disk-manager.disks.list"},
		{name: "PrivateService.ListImages", permission: "disk-manager.images.list"},
		{name: "PrivateService.ListSnapshots", permission: "disk-manager.snapshots.list"},
		{name: "PrivateService.ListFilesystems", permission: "disk-manager.filesystems.list"},
		// TODO: Maybe we should have separate permissions for placement groups?
		{name: "PrivateService.ListPlacementGroups", permission: "disk-manager.disks.list"},
		// TODO: Find more suitable permission for this.
		{name: "PrivateService.GetAliveNodes", permission: "disk-manager.operations.list"},
		{name: "PrivateService.FinishExternalFilesystemCreation", permission: "disk-manager.filesystems.create"},
		{name: "PrivateService.FinishExternalFilesystemDeletion", permission: "disk-manager.filesystems.delete"},

		{name: "FilesystemService.Create", permission: "disk-manager.filesystems.create"},
		{name: "FilesystemService.Delete", permission: "disk-manager.filesystems.delete"},
		{name: "FilesystemService.Resize", permission: "disk-manager.filesystems.update"},
	}
)

////////////////////////////////////////////////////////////////////////////////

func convertError(err error) error {
	if errors.CanRetry(err) {
		return grpc_status.Errorf(grpc_codes.Unavailable, "%s", err.Error())
	}

	return err
}

func getRequestName(name string) (string, error) {
	parts := strings.Split(name, "/")
	if len(parts) < 2 {
		return "", fmt.Errorf("unknown request name %v", name)
	}

	methodName := parts[len(parts)-1]
	serviceName := parts[len(parts)-2]

	parts = strings.Split(serviceName, ".")
	if len(parts) < 1 {
		return "", fmt.Errorf("unknown request name %v", name)
	}

	serviceName = parts[len(parts)-1]

	return fmt.Sprintf("%v.%v", serviceName, methodName), nil
}

////////////////////////////////////////////////////////////////////////////////

type requestStats struct {
	count     metrics.Counter
	histogram metrics.Timer
	errors    metrics.Counter
}

func (s *requestStats) onCount() {
	s.count.Inc()
}

func (s *requestStats) recordDuration(duration time.Duration) {
	s.histogram.RecordDuration(duration)
}

func (s *requestStats) onError(err error) {
	if !errors.IsSilent(err) {
		s.errors.Inc()
	}
}

////////////////////////////////////////////////////////////////////////////////

func requestDurationBuckets() metrics.DurationBuckets {
	return metrics.NewDurationBuckets(
		10*time.Millisecond, 20*time.Millisecond, 50*time.Millisecond,
		100*time.Millisecond, 200*time.Millisecond, 500*time.Millisecond,
		1*time.Second, 2*time.Second, 5*time.Second,
	)
}

func newRequestStats(registry metrics.Registry) *requestStats {
	return &requestStats{
		count:     registry.Counter("count"),
		histogram: registry.DurationHistogram("time", requestDurationBuckets()),
		errors:    registry.Counter("errors"),
	}
}

////////////////////////////////////////////////////////////////////////////////

type requestMetrics struct {
	requestMetrics map[string]*requestStats
}

func newRequestMetrics(registry metrics.Registry) requestMetrics {
	m := make(map[string]*requestStats)

	for _, request := range requests {
		m[request.name] = newRequestStats(registry.WithTags(map[string]string{
			"request": request.name,
		}))
	}

	return requestMetrics{requestMetrics: m}
}

func (m *requestMetrics) getForRequest(requestName string) (*requestStats, error) {
	r, ok := m.requestMetrics[requestName]
	if !ok {
		return nil, fmt.Errorf("no such request name: %v", requestName)
	}

	return r, nil
}

////////////////////////////////////////////////////////////////////////////////

type interceptor struct {
	logger      logging.Logger
	metrics     requestMetrics
	authorizer  auth.Authorizer
	permissions map[string]string
}

func (i *interceptor) intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {

	ctx = logging.SetLogger(ctx, i.logger)

	requestName, err := getRequestName(info.FullMethod)
	if err != nil {
		logging.Warn(ctx, "Failed to get request name: %v", err)
		return nil, err
	}

	requestStats, err := i.metrics.getForRequest(requestName)
	if err != nil {
		logging.Warn(ctx, "Failed to get request metrics: %v", err)
		return nil, err
	}

	ctx = tracing.GetTracingContext(ctx)
	ctx, span := tracing.StartSpan(ctx, requestName)
	defer span.End()

	start := time.Now()

	resp, err := i.handleRequest(ctx, req, handler, requestName, requestStats)
	if err != nil {
		logging.Warn(ctx, "%v failed, err=%v", requestName, err)
		// Don't report errors when ctx is cancelled.
		if ctx.Err() == nil {
			requestStats.onError(err)
		}
	}

	requestStats.onCount()
	requestStats.recordDuration(time.Since(start))
	return resp, convertError(err)
}

func (i *interceptor) handleRequest(
	ctx context.Context,
	req interface{},
	handler grpc.UnaryHandler,
	requestName string,
	requestStats *requestStats,
) (interface{}, error) {

	permission, ok := i.permissions[requestName]
	if !ok {
		return nil, fmt.Errorf("permissions for %v are undefined", requestName)
	}

	accountID, err := i.authorizer.Authorize(ctx, permission)
	if err != nil {
		logging.Warn(ctx, "Authorize failed, err=%v", err)
		return nil, err
	}

	ctx = headers.SetAccountID(ctx, accountID)

	resp, err := handler(ctx, req)
	if err == nil {
		// Don't log DiskService.Assign, ImageService.Create requests because of
		// sensitive data.
		// TODO: remove this hack for DiskService.Assign request (NBS-2668).
		// TODO: should mask secrets instead.
		if requestName != "DiskService.Assign" &&
			requestName != "ImageService.Create" {

			if strings.Contains(permission, "create") ||
				strings.Contains(permission, "update") ||
				strings.Contains(permission, "delete") {

				logging.Info(
					ctx,
					"requestName=%v, req=%v, resp=%v success",
					requestName,
					req,
					resp,
				)
			} else {
				logging.Debug(
					ctx,
					"requestName=%v, req=%v, resp=%v success",
					requestName,
					req,
					resp,
				)
			}
		}
	}

	return resp, err
}

////////////////////////////////////////////////////////////////////////////////

func NewInterceptor(
	logger logging.Logger,
	metricsRegistry metrics.Registry,
	authorizer auth.Authorizer,
) grpc.UnaryServerInterceptor {

	permissions := make(map[string]string)

	for _, request := range requests {
		permissions[request.name] = request.permission
	}

	return (&interceptor{
		logger:      logger,
		metrics:     newRequestMetrics(metricsRegistry),
		authorizer:  authorizer,
		permissions: permissions,
	}).intercept
}
