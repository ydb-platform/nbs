package testcommon

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/hashicorp/go-retryablehttp"
	prometheus_client "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/config"
	snapshot_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/config"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/headers"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	pools_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/config"
	pools_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	sdk_client "github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client"
	client_config "github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client/config"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	tasks_headers "github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
	"google.golang.org/grpc"
	grpc_codes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	grpc_status "google.golang.org/grpc/status"
)

////////////////////////////////////////////////////////////////////////////////

var (
	DefaultKeyHash = []byte{
		0x6c, 0x4d, 0x74, 0x64, 0x34, 0x38, 0x50, 0x35,
		0x6a, 0x45, 0x39, 0x32, 0x6f, 0x4d, 0x5a, 0x2f,
		0x4c, 0x67, 0x62, 0x57, 0x4d, 0x30, 0x30, 0x46,
		0x6e, 0x64, 0x33, 0x63, 0x4f, 0x49, 0x42, 0x32,
		0x54, 0x37, 0x69, 0x33, 0x42, 0x6f, 0x7a, 0x6d,
		0x76, 0x6f, 0x64, 0x39, 0x42, 0x44, 0x6c, 0x4b,
		0x4c, 0x62, 0x30, 0x5a, 0x4a, 0x59, 0x4d, 0x44,
		0x57, 0x6a, 0x35, 0x53, 0x43, 0x69, 0x2f, 0x51,
	}
)

////////////////////////////////////////////////////////////////////////////////

func newDefaultClientConfig() *client_config.Config {
	endpoint := fmt.Sprintf(
		"localhost:%v",
		os.Getenv("DISK_MANAGER_RECIPE_DISK_MANAGER_PORT"),
	)
	maxRetryAttempts := uint32(1000)
	timeout := "1s"

	return &client_config.Config{
		Endpoint:            &endpoint,
		MaxRetryAttempts:    &maxRetryAttempts,
		PerRetryTimeout:     &timeout,
		BackoffTimeout:      &timeout,
		OperationPollPeriod: &timeout,
	}
}

////////////////////////////////////////////////////////////////////////////////

func ReplaceUnacceptableSymbolsFromResourceID(t *testing.T) string {
	return strings.ReplaceAll(t.Name(), "/", "_")
}

////////////////////////////////////////////////////////////////////////////////

func GetRawImageFileURL() string {
	port := os.Getenv("DISK_MANAGER_RECIPE_RAW_IMAGE_FILE_SERVER_PORT")
	return fmt.Sprintf("http://localhost:%v", port)
}

func GetRawImageSize(t *testing.T) uint64 {
	value, err := strconv.ParseUint(os.Getenv("DISK_MANAGER_RECIPE_RAW_IMAGE_SIZE"), 10, 64)
	require.NoError(t, err)
	return uint64(value)
}

func GetRawImageCrc32(t *testing.T) uint32 {
	value, err := strconv.ParseUint(os.Getenv("DISK_MANAGER_RECIPE_RAW_IMAGE_CRC32"), 10, 32)
	require.NoError(t, err)
	return uint32(value)
}

func GetNonExistentImageFileURL() string {
	port := os.Getenv("DISK_MANAGER_RECIPE_NON_EXISTENT_IMAGE_FILE_SERVER_PORT")
	return fmt.Sprintf("http://localhost:%v", port)
}

func GetQCOW2ImageFileURL() string {
	port := os.Getenv("DISK_MANAGER_RECIPE_QCOW2_IMAGE_FILE_SERVER_PORT")
	return fmt.Sprintf("http://localhost:%v", port)
}

func GetQCOW2ImageSize(t *testing.T) uint64 {
	value, err := strconv.ParseUint(os.Getenv("DISK_MANAGER_RECIPE_QCOW2_IMAGE_SIZE"), 10, 64)
	require.NoError(t, err)
	return uint64(value)
}

func GetQCOW2ImageCrc32(t *testing.T) uint32 {
	value, err := strconv.ParseUint(os.Getenv("DISK_MANAGER_RECIPE_QCOW2_IMAGE_CRC32"), 10, 32)
	require.NoError(t, err)
	return uint32(value)
}

func UseDefaultQCOW2ImageFile(t *testing.T) {
	_, err := httpGetWithRetries(GetQCOW2ImageFileURL() + "/use_default_image")
	require.NoError(t, err)
}

func UseOtherQCOW2ImageFile(t *testing.T) {
	_, err := httpGetWithRetries(GetQCOW2ImageFileURL() + "/use_other_image")
	require.NoError(t, err)
}

func GetOtherQCOW2ImageSize(t *testing.T) uint64 {
	value, err := strconv.ParseUint(os.Getenv("DISK_MANAGER_RECIPE_OTHER_QCOW2_IMAGE_SIZE"), 10, 64)
	require.NoError(t, err)
	return uint64(value)
}

func GetOtherQCOW2ImageCrc32(t *testing.T) uint32 {
	value, err := strconv.ParseUint(os.Getenv("DISK_MANAGER_RECIPE_OTHER_QCOW2_IMAGE_CRC32"), 10, 32)
	require.NoError(t, err)
	return uint32(value)
}

func GetInvalidQCOW2ImageFileURL() string {
	port := os.Getenv("DISK_MANAGER_RECIPE_INVALID_QCOW2_IMAGE_FILE_SERVER_PORT")
	return fmt.Sprintf("http://localhost:%v", port)
}

func GetQCOW2FuzzingImageFileURL() string {
	port := os.Getenv("DISK_MANAGER_RECIPE_QCOW2_FUZZING_IMAGE_FILE_SERVER_PORT")
	return fmt.Sprintf("http://localhost:%v", port)
}

func GetGeneratedVMDKImageFileURL() string {
	port := os.Getenv("DISK_MANAGER_RECIPE_GENERATED_VMDK_IMAGE_FILE_SERVER_PORT")
	return fmt.Sprintf("http://localhost:%v", port)
}

func GetGeneratedVMDKImageSize(t *testing.T) uint64 {
	value, err := strconv.ParseUint(os.Getenv("DISK_MANAGER_RECIPE_GENERATED_VMDK_IMAGE_SIZE"), 10, 64)
	require.NoError(t, err)
	return uint64(value)
}

func GetGeneratedVMDKImageCrc32(t *testing.T) uint32 {
	value, err := strconv.ParseUint(os.Getenv("DISK_MANAGER_RECIPE_GENERATED_VMDK_IMAGE_CRC32"), 10, 32)
	require.NoError(t, err)
	return uint32(value)
}

func GetBigRawImageURL() string {
	port := os.Getenv("DISK_MANAGER_RECIPE_BIG_RAW_IMAGE_FILE_SERVER_PORT")
	return fmt.Sprintf("http://localhost:%v", port)
}

func GetBigRawImageSize(t *testing.T) uint64 {
	value, err := strconv.ParseUint(os.Getenv("DISK_MANAGER_RECIPE_BIG_RAW_IMAGE_SIZE"), 10, 64)
	require.NoError(t, err)
	return uint64(value)
}

func GetBigRawImageCrc32(t *testing.T) uint32 {
	value, err := strconv.ParseUint(os.Getenv("DISK_MANAGER_RECIPE_BIG_RAW_IMAGE_CRC32"), 10, 32)
	require.NoError(t, err)
	return uint32(value)
}

func UseDefaultBigRawImageFile(t *testing.T) {
	_, err := httpGetWithRetries(GetBigRawImageURL() + "/use_default_image")
	require.NoError(t, err)
}

func UseOtherBigRawImageFile(t *testing.T) {
	_, err := httpGetWithRetries(GetBigRawImageURL() + "/use_other_image")
	require.NoError(t, err)
}

func GetOtherBigRawImageSize(t *testing.T) uint64 {
	value, err := strconv.ParseUint(os.Getenv("DISK_MANAGER_RECIPE_OTHER_BIG_RAW_IMAGE_SIZE"), 10, 64)
	require.NoError(t, err)
	return uint64(value)
}

func GetOtherBigRawImageCrc32(t *testing.T) uint32 {
	value, err := strconv.ParseUint(os.Getenv("DISK_MANAGER_RECIPE_OTHER_BIG_RAW_IMAGE_CRC32"), 10, 32)
	require.NoError(t, err)
	return uint32(value)
}

////////////////////////////////////////////////////////////////////////////////

func CancelOperation(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	operationID string,
) {

	operation, err := client.CancelOperation(ctx, &disk_manager.CancelOperationRequest{
		OperationId: operationID,
	})
	require.NoError(t, err)
	require.Equal(t, operationID, operation.Id)
	require.True(t, operation.Done)

	switch result := operation.Result.(type) {
	case *disk_manager.Operation_Error:
		status := grpc_status.FromProto(result.Error)
		require.Equal(t, grpc_codes.Canceled, status.Code())
	case *disk_manager.Operation_Response:
	default:
		require.True(t, false)
	}
}

func NewClient(ctx context.Context) (sdk_client.Client, error) {
	creds, err := credentials.NewClientTLSFromFile(
		os.Getenv("DISK_MANAGER_RECIPE_ROOT_CERTS_FILE"),
		"",
	)
	if err != nil {
		return nil, err
	}

	return sdk_client.NewClient(
		ctx,
		newDefaultClientConfig(),
		grpc.WithTransportCredentials(creds),
	)
}

func NewPrivateClient(ctx context.Context) (internal_client.PrivateClient, error) {
	creds, err := credentials.NewClientTLSFromFile(
		os.Getenv("DISK_MANAGER_RECIPE_ROOT_CERTS_FILE"),
		"",
	)
	if err != nil {
		return nil, err
	}

	return internal_client.NewPrivateClient(
		ctx,
		newDefaultClientConfig(),
		grpc.WithTransportCredentials(creds),
	)
}

func newNbsClientClientConfig() *nbs_config.ClientConfig {
	rootCertsFile := os.Getenv("DISK_MANAGER_RECIPE_ROOT_CERTS_FILE")

	durableClientTimeout := "5m"
	discoveryClientHardTimeout := "8m"
	discoveryClientSoftTimeout := "15s"

	return &nbs_config.ClientConfig{
		Zones: map[string]*nbs_config.Zone{
			"zone-a": {
				Endpoints: []string{
					fmt.Sprintf(
						"localhost:%v",
						os.Getenv("DISK_MANAGER_RECIPE_NBS_PORT"),
					),
				},
			},
			"zone-b": {
				Endpoints: []string{
					fmt.Sprintf(
						"localhost:%v",
						os.Getenv("DISK_MANAGER_RECIPE_NBS2_PORT"),
					),
				},
			},
			"zone-c": {
				Endpoints: []string{
					fmt.Sprintf(
						"localhost:%v",
						os.Getenv("DISK_MANAGER_RECIPE_NBS3_PORT"),
					),
				},
			},
			"zone-d": {
				Endpoints: []string{
					fmt.Sprintf(
						"localhost:%v",
						os.Getenv("DISK_MANAGER_RECIPE_NBS4_PORT"),
					),
				},
			},
			"zone-d-shard-1": {
				Endpoints: []string{
					fmt.Sprintf(
						"localhost:%v",
						os.Getenv("DISK_MANAGER_RECIPE_NBS5_PORT"),
					),
				},
			},
		},
		RootCertsFile:              &rootCertsFile,
		DurableClientTimeout:       &durableClientTimeout,
		DiscoveryClientHardTimeout: &discoveryClientHardTimeout,
		DiscoveryClientSoftTimeout: &discoveryClientSoftTimeout,
	}
}

func NewNbsTestingClient(
	t *testing.T,
	ctx context.Context,
	zoneID string,
) nbs.TestingClient {

	client, err := nbs.NewTestingClient(ctx, zoneID, newNbsClientClientConfig())
	require.NoError(t, err)
	return client
}

////////////////////////////////////////////////////////////////////////////////

func NewContextWithToken(token string) context.Context {
	ctx := headers.SetOutgoingAccessToken(context.Background(), token)
	return logging.SetLogger(ctx, logging.NewStderrLogger(logging.DebugLevel))
}

func NewContext() context.Context {
	return NewContextWithToken("TestToken")
}

////////////////////////////////////////////////////////////////////////////////

var lastReqNumber int

func GetRequestContext(t *testing.T, ctx context.Context) context.Context {
	lastReqNumber++

	cookie := fmt.Sprintf("%v_%v", t.Name(), lastReqNumber)
	ctx = tasks_headers.SetOutgoingIdempotencyKey(ctx, cookie)
	ctx = tasks_headers.SetOutgoingRequestID(ctx, cookie)
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

func RequireCheckpoint(
	t *testing.T,
	ctx context.Context,
	diskID string,
	checkpointID string,
) {

	nbsClient := NewNbsTestingClient(t, ctx, "zone-a")
	checkpoints, err := nbsClient.GetCheckpoints(ctx, diskID)
	require.NoError(t, err)

	require.Len(t, checkpoints, 1)
	require.EqualValues(t, checkpointID, checkpoints[0])
}

func RequireCheckpointsDoNotExist(
	t *testing.T,
	ctx context.Context,
	diskID string,
) {

	nbsClient := NewNbsTestingClient(t, ctx, "zone-a")
	checkpoints, err := nbsClient.GetCheckpoints(ctx, diskID)
	require.NoError(t, err)
	require.Empty(t, checkpoints)
}

func waitUntilCheckpointsMeetRequirements(
	t *testing.T,
	ctx context.Context,
	diskID string,
	checkRequirements func([]string) bool,
) {

	nbsClient := NewNbsTestingClient(t, ctx, "zone-a")
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()

	for range ticker.C {
		checkpoints, err := nbsClient.GetCheckpoints(ctx, diskID)
		require.NoError(t, err)

		if checkRequirements(checkpoints) {
			return
		}

		logging.Debug(
			ctx,
			"waitUntilCheckpointsMeetRequirements proceeding to next iteration",
		)
	}
}

func WaitForCheckpointDoesNotExist(
	t *testing.T,
	ctx context.Context,
	diskID string,
	checkpointID string,
) {

	waitUntilCheckpointsMeetRequirements(
		t,
		ctx,
		diskID,
		func(checkpoints []string) bool {
			return !slices.Contains(checkpoints, checkpointID)
		},
	)
}

func WaitForNoCheckpointsExist(
	t *testing.T,
	ctx context.Context,
	diskID string,
) {

	waitUntilCheckpointsMeetRequirements(
		t,
		ctx,
		diskID,
		func(checkpoints []string) bool {
			return len(checkpoints) == 0
		},
	)
}

////////////////////////////////////////////////////////////////////////////////

func CreateImage(
	t *testing.T,
	ctx context.Context,
	imageID string,
	imageSize uint64,
	folderID string,
	pooled bool,
) nbs.DiskContentInfo {

	client, err := NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID := ReplaceUnacceptableSymbolsFromResourceID(t) + "_temporary_disk_for_image_" + imageID

	reqCtx := GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: int64(imageSize),
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: "zone-a",
			DiskId: diskID,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)

	nbsClient := NewNbsTestingClient(t, ctx, "zone-a")
	diskContentInfo, err := nbsClient.FillDisk(ctx, diskID, imageSize)
	require.NoError(t, err)

	reqCtx = GetRequestContext(t, ctx)
	operation, err = client.CreateImage(reqCtx, &disk_manager.CreateImageRequest{
		Src: &disk_manager.CreateImageRequest_SrcDiskId{
			SrcDiskId: &disk_manager.DiskId{
				ZoneId: "zone-a",
				DiskId: diskID,
			},
		},
		DstImageId: imageID,
		FolderId:   folderID,
		Pooled:     pooled,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)

	response := disk_manager.CreateImageResponse{}
	err = internal_client.WaitResponse(ctx, client, operation.Id, &response)
	require.NoError(t, err)
	require.Equal(t, int64(imageSize), response.Size)
	require.Equal(t, int64(diskContentInfo.StorageSize), response.StorageSize)

	return diskContentInfo
}

func DeleteDisk(
	t *testing.T,
	ctx context.Context,
	client sdk_client.Client,
	diskID string,
) {

	reqCtx := GetRequestContext(t, ctx)
	operation, err := client.DeleteDisk(reqCtx, &disk_manager.DeleteDiskRequest{
		DiskId: &disk_manager.DiskId{
			DiskId: diskID,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)

	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)
}

////////////////////////////////////////////////////////////////////////////////

func newYDB(ctx context.Context) (*persistence.YDBClient, error) {
	endpoint := fmt.Sprintf(
		"localhost:%v",
		os.Getenv("DISK_MANAGER_RECIPE_YDB_PORT"),
	)

	// Should be in sync with settings from PersistenceConfig in test recipe.
	database := "/Root"
	rootPath := "disk_manager/recipe"

	return persistence.NewYDBClient(
		ctx,
		&persistence_config.PersistenceConfig{
			Endpoint: &endpoint,
			Database: &database,
			RootPath: &rootPath,
		},
		metrics.NewEmptyRegistry(),
	)
}

func newResourceStorage(ctx context.Context) (resources.Storage, error) {
	db, err := newYDB(ctx)
	if err != nil {
		return nil, err
	}

	endedMigrationExpirationTimeout, err := time.ParseDuration("30m")
	if err != nil {
		return nil, err
	}

	resourcesStorage, err := resources.NewStorage(
		"disks",
		"images",
		"snapshot",
		"filesystems",
		"placement_groups",
		db,
		endedMigrationExpirationTimeout,
	)

	return resourcesStorage, err
}

func newPoolStorage(ctx context.Context) (pools_storage.Storage, error) {
	db, err := newYDB(ctx)
	if err != nil {
		return nil, err
	}

	// Should be in sync with settings from PoolsConfig in test recipe.
	cloudID := "cloud"
	folderID := "pools"

	return pools_storage.NewStorage(&pools_config.PoolsConfig{
		CloudId:  &cloudID,
		FolderId: &folderID,
	}, db, metrics.NewEmptyRegistry())
}

func newSnapshotStorage(ctx context.Context) (snapshot_storage.Storage, error) {
	// Should be in sync with settings from SnapshotConfig in test recipe.
	endpoint := fmt.Sprintf(
		"localhost:%v",
		os.Getenv("DISK_MANAGER_RECIPE_YDB_PORT"),
	)
	database := "/Root"
	config := &snapshot_config.SnapshotConfig{
		PersistenceConfig: &persistence_config.PersistenceConfig{
			Endpoint: &endpoint,
			Database: &database,
		},
	}

	db, err := persistence.NewYDBClient(
		ctx,
		config.GetPersistenceConfig(),
		metrics.NewEmptyRegistry(),
	)
	if err != nil {
		return nil, err
	}

	return snapshot_storage.NewStorage(
		config,
		metrics.NewEmptyRegistry(),
		db,
		nil, // do not need s3 here
	)
}

////////////////////////////////////////////////////////////////////////////////

func CheckBaseSnapshot(
	t *testing.T,
	ctx context.Context,
	snapshotID string,
	expectedBaseSnapshotID string,
) {

	storage, err := newSnapshotStorage(ctx)
	require.NoError(t, err)

	snapshotMeta, err := storage.GetSnapshotMeta(ctx, snapshotID)
	require.NoError(t, err)
	require.EqualValues(t, expectedBaseSnapshotID, snapshotMeta.BaseSnapshotID)
}

func CheckBaseDiskSlotReleased(
	t *testing.T,
	ctx context.Context,
	overlayDiskID string,
) {

	poolStorage, err := newPoolStorage(ctx)
	require.NoError(t, err)

	err = poolStorage.CheckBaseDiskSlotReleased(ctx, overlayDiskID)
	require.NoError(t, err)
}

func CheckConsistency(t *testing.T, ctx context.Context) {
	nbsClient := NewNbsTestingClient(t, ctx, "zone-a")

	for {
		ok := true

		diskIDs, err := nbsClient.List(ctx)
		require.NoError(t, err)

		for _, diskID := range diskIDs {
			// TODO: should remove dependency on disk id here.
			if strings.HasPrefix(diskID, "proxy") {
				ok = false

				logging.Info(
					ctx,
					"waiting for proxy overlay disk %v deletion",
					diskID,
				)
				continue
			}

			if strings.HasPrefix(diskID, "base") {
				continue
			}

			require.True(
				t,
				strings.Contains(diskID, "Test"),
				"disk with unexpected id is found: %v",
				diskID,
			)
		}

		if ok {
			break
		}

		time.Sleep(time.Second)
	}

	poolStorage, err := newPoolStorage(ctx)
	require.NoError(t, err)

	err = poolStorage.CheckConsistency(ctx)
	require.NoError(t, err)

	// TODO: validate internal YDB tables (tasks, pools etc.) consistency.
}

////////////////////////////////////////////////////////////////////////////////

func GetIncremental(
	ctx context.Context,
	disk *types.Disk,
) (string, string, error) {

	storage, err := newSnapshotStorage(ctx)
	if err != nil {
		return "", "", err
	}

	return storage.GetIncremental(ctx, disk)
}

func GetDiskMeta(
	ctx context.Context,
	diskID string,
) (*resources.DiskMeta, error) {

	storage, err := newResourceStorage(ctx)
	if err != nil {
		return nil, err
	}

	return storage.GetDiskMeta(ctx, diskID)
}

func GetEncryptionKeyHash(encryptionDesc *types.EncryptionDesc) ([]byte, error) {
	switch key := encryptionDesc.Key.(type) {
	case *types.EncryptionDesc_KeyHash:
		return key.KeyHash, nil
	case nil:
		return nil, nil
	default:
		return nil, errors.NewNonRetriableErrorf("unknown key %s", key)
	}
}

////////////////////////////////////////////////////////////////////////////////

func CheckErrorDetails(
	t *testing.T,
	err error,
	code int,
	message string,
	internal bool,
) {

	status, ok := grpc_status.FromError(err)
	require.True(t, ok)

	statusDetails := status.Details()
	require.Equal(t, 1, len(statusDetails))

	errorDetails, ok := statusDetails[0].(*disk_manager.ErrorDetails)
	require.True(t, ok)

	require.Equal(t, int64(code), errorDetails.Code)
	require.Equal(t, internal, errorDetails.Internal)
	if len(message) != 0 {
		require.Equal(t, message, errorDetails.Message)
	}
}

////////////////////////////////////////////////////////////////////////////////

func httpGetWithRetries(url string) (*http.Response, error) {
	retryableClient := retryablehttp.NewClient()
	retryableClient.HTTPClient.Timeout = 500 * time.Second
	retryableClient.RetryWaitMin = time.Second
	retryableClient.RetryWaitMax = 5 * time.Second
	retryableClient.RetryMax = 100
	return retryableClient.Get(url)
}

func GetCounter(t *testing.T, name string, labels map[string]string) float64 {
	resp, err := httpGetWithRetries(
		fmt.Sprintf(
			"http://localhost:%s/metrics/",
			os.Getenv("DISK_MANAGER_RECIPE_DISK_MANAGER_MON_PORT"),
		),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	parser := expfmt.TextParser{}
	metricFamilies, err := parser.TextToMetricFamilies(resp.Body)
	require.NoError(t, err)

	retrievedMetrics, ok := metricFamilies[name]
	require.True(t, ok)
	for _, metricValue := range retrievedMetrics.GetMetric() {
		if metricMatchesLabel(labels, metricValue) {
			return metricValue.GetCounter().GetValue()
		}
	}

	require.Failf(t, "No counter with name %s", name)
	return 0
}

func metricMatchesLabel(
	labels map[string]string,
	metric *prometheus_client.Metric,
) bool {

	metricLabels := make(map[string]string)
	for _, label := range metric.GetLabel() {
		metricLabels[label.GetName()] = label.GetValue()
	}

	for name, value := range labels {
		foundValue, ok := metricLabels[name]
		if !ok {
			return false
		}

		if foundValue != value {
			return false
		}
	}

	return true
}
