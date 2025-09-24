package tests

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/auth"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client/codes"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

const (
	zoneID                            = "zone-a"
	otherZoneID                       = "zone-b"
	defaultSessionRediscoverPeriodMin = "10s"
	defaultSessionRediscoverPeriodMax = "20s"
)

////////////////////////////////////////////////////////////////////////////////

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

////////////////////////////////////////////////////////////////////////////////

func getEndpoint() string {
	return fmt.Sprintf(
		"localhost:%v",
		os.Getenv("DISK_MANAGER_RECIPE_NBS_PORT"),
	)
}

func getOtherZoneEndpoint() string {
	return fmt.Sprintf(
		"localhost:%v",
		os.Getenv("DISK_MANAGER_RECIPE_NBS2_PORT"),
	)
}

func newClientConfig(
	sessionRediscoverPeriodMin string,
	sessionRediscoverPeriodMax string,
) *config.ClientConfig {

	rootCertsFile := os.Getenv("DISK_MANAGER_RECIPE_ROOT_CERTS_FILE")

	return &config.ClientConfig{
		Zones: map[string]*config.Zone{
			zoneID: {
				Endpoints: []string{getEndpoint(), getEndpoint()},
			},
			otherZoneID: {
				Endpoints: []string{getOtherZoneEndpoint(), getOtherZoneEndpoint()},
			},
		},
		RootCertsFile:              &rootCertsFile,
		SessionRediscoverPeriodMin: &sessionRediscoverPeriodMin,
		SessionRediscoverPeriodMax: &sessionRediscoverPeriodMax,
	}
}

func newFactory(
	t *testing.T,
	ctx context.Context,
	creds auth.Credentials,
	sessionRediscoverPeriodMin string,
	sessionRediscoverPeriodMax string,
) nbs.Factory {

	clientConfig := newClientConfig(
		sessionRediscoverPeriodMin,
		sessionRediscoverPeriodMax,
	)

	factory, err := nbs.NewFactoryWithCreds(
		ctx,
		clientConfig,
		creds,
		metrics.NewEmptyRegistry(),
		metrics.NewEmptyRegistry(),
	)
	require.NoError(t, err)

	return factory
}

func newClientFull(
	t *testing.T,
	ctx context.Context,
	zone string,
	creds auth.Credentials,
	sessionRediscoverPeriodMin string,
	sessionRediscoverPeriodMax string,
) nbs.Client {

	factory := newFactory(
		t,
		ctx,
		creds,
		sessionRediscoverPeriodMin,
		sessionRediscoverPeriodMax,
	)

	client, err := factory.GetClient(ctx, zone)
	require.NoError(t, err)

	return client
}

func newClient(t *testing.T, ctx context.Context) nbs.Client {
	return newClientFull(
		t,
		ctx,
		zoneID,
		nil,
		defaultSessionRediscoverPeriodMin,
		defaultSessionRediscoverPeriodMax,
	)
}

func newTestingClient(t *testing.T, ctx context.Context) nbs.TestingClient {
	client, err := nbs.NewTestingClient(
		ctx,
		zoneID,
		newClientConfig(
			defaultSessionRediscoverPeriodMin,
			defaultSessionRediscoverPeriodMax,
		),
	)
	require.NoError(t, err)
	return client
}

func newOtherZoneClient(t *testing.T, ctx context.Context) nbs.Client {
	return newClientFull(t, ctx, otherZoneID, nil, "10s", "20s")
}

func newMultiZoneClient(t *testing.T, ctx context.Context) nbs.MultiZoneClient {
	factory := newFactory(t, ctx, nil, "10s", "20s")
	client, err := factory.GetMultiZoneClient(zoneID, otherZoneID)
	require.NoError(t, err)
	return client
}

////////////////////////////////////////////////////////////////////////////////

type mockTokenProvider struct {
	mock.Mock
}

func (m *mockTokenProvider) Token(ctx context.Context) (string, error) {
	args := m.Called(ctx)
	return args.String(0), args.Error(1)
}

////////////////////////////////////////////////////////////////////////////////

func writeBlocks(
	t *testing.T,
	ctx context.Context,
	client nbs.Client,
	diskID string,
	startIndex uint64,
	blockCount uint32,
) {

	session, err := client.MountRW(
		ctx,
		diskID,
		0,   // fillGeneration
		0,   // fillSeqNumber
		nil, // encryption
	)
	require.NoError(t, err)
	defer session.Close(ctx)

	writeBlocksToSession(t, ctx, session, startIndex, blockCount)
}

func writeBlocksToSession(
	t *testing.T,
	ctx context.Context,
	session *nbs.Session,
	startIndex uint64,
	blockCount uint32,
) {

	bytes := make([]byte, blockCount*session.BlockSize())
	rand.Read(bytes)
	err := session.Write(ctx, startIndex, bytes)
	require.NoError(t, err)
}

////////////////////////////////////////////////////////////////////////////////

func checkErrorDetails(
	t *testing.T,
	err error,
	expectedCode codes.Code,
	expectedMessage string,
	expectedInternal bool,
) {

	var detailedErr *errors.DetailedError
	if errors.As(err, &detailedErr) {
		details := detailedErr.Details
		require.Equal(t, expectedCode, codes.Code(details.Code))
		if len(expectedMessage) != 0 {
			require.Contains(t, details.Message, expectedMessage)
		}
		require.Equal(t, expectedInternal, details.Internal)
	} else {
		require.Fail(t, "Not a detailed error: %v", err.Error())
	}
}

func isInternalError(err error) bool {
	var detailedErr *errors.DetailedError
	if errors.As(err, &detailedErr) {
		return detailedErr.Details.Internal
	}

	return true
}

////////////////////////////////////////////////////////////////////////////////

func createStandardSSDDisk(
	t *testing.T,
	ctx context.Context,
	client nbs.Client,
	diskID string,
) {

	err := client.Create(ctx, nbs.CreateDiskParams{
		ID:          diskID,
		BlocksCount: 10,
		BlockSize:   4096,
		Kind:        types.DiskKind_DISK_KIND_SSD,
	})
	require.NoError(t, err)
}

func createStandardSSDNonreplDisk(
	t *testing.T,
	ctx context.Context,
	client nbs.Client,
	diskID string,
) {

	err := client.Create(ctx, nbs.CreateDiskParams{
		ID:          diskID,
		BlocksCount: 262144,
		BlockSize:   4096,
		Kind:        types.DiskKind_DISK_KIND_SSD_NONREPLICATED,
	})
	require.NoError(t, err)
}

func deleteSyncWithReties(
	t *testing.T,
	ctx context.Context,
	client nbs.Client,
	diskID string,
) {

	attemptsCount := 10
	var err error

	for attempt := 1; attempt <= attemptsCount; attempt++ {
		err = client.DeleteSync(ctx, diskID)
		if err == nil {
			return
		}

		logging.Warn(
			ctx,
			"DeleteSync request failed on attempt %v: %v",
			attempt,
			err.Error(),
		)

		time.Sleep(time.Second * 10)
	}

	require.Fail(
		t,
		"DeleteSync failed after %v attempts, last error is %v",
		attemptsCount,
		err.Error(),
	)
}

////////////////////////////////////////////////////////////////////////////////

func TestCreateDisk(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)

	diskID := t.Name()
	createStandardSSDDisk(t, ctx, client, diskID)
	// Creating the same disk twice is not an error
	createStandardSSDDisk(t, ctx, client, diskID)
}

func TestDeleteDisk(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)

	diskID := t.Name()
	createStandardSSDDisk(t, ctx, client, diskID)

	err := client.Delete(ctx, diskID)
	require.NoError(t, err)

	// Deleting the same disk twice is not an error.
	err = client.Delete(ctx, diskID)
	require.NoError(t, err)

	// Deleting non-existent disk is also not an error.
	err = client.Delete(ctx, diskID+"_does_not_exist")
	require.NoError(t, err)
}

func TestDeleteDiskSync(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)

	diskID := t.Name()
	createStandardSSDDisk(t, ctx, client, diskID)

	err := client.DeleteSync(ctx, diskID)
	require.NoError(t, err)

	// Deleting the same disk twice is not an error.
	err = client.DeleteSync(ctx, diskID)
	require.NoError(t, err)

	// Deleting non-existent disk is also not an error.
	err = client.DeleteSync(ctx, diskID+"_does_not_exist")
	require.NoError(t, err)
}

func TestCreateDeleteCheckpoint(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)

	diskID := t.Name()
	createStandardSSDDisk(t, ctx, client, diskID)

	err := client.CreateCheckpoint(
		ctx,
		nbs.CheckpointParams{
			DiskID:       diskID,
			CheckpointID: "checkpointID",
		},
	)
	require.NoError(t, err)

	err = client.DeleteCheckpoint(ctx, diskID, "checkpointID")
	require.NoError(t, err)
}

func TestDeleteCheckpointOnUnexistingDisk(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)

	diskID := t.Name()

	err := client.DeleteCheckpoint(ctx, diskID, "checkpointID")
	require.NoError(t, err)
}

func TestDeleteCheckpointData(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)

	diskID := t.Name()
	createStandardSSDDisk(t, ctx, client, diskID)

	err := client.CreateCheckpoint(
		ctx,
		nbs.CheckpointParams{
			DiskID:       diskID,
			CheckpointID: "checkpointID",
		},
	)
	require.NoError(t, err)

	err = client.DeleteCheckpointData(ctx, diskID, "checkpointID")
	require.NoError(t, err)

	// TODO: NBS-4665: check that CreateCheckpoint request returns error if checkpoint data was deleted.
	// TODO: NBS-4665: check that CreateCheckpointWithoutData request returns error if checkpoint data was deleted.
}

func TestResizeDisk(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)

	diskID := t.Name()
	createStandardSSDDisk(t, ctx, client, diskID)

	err := client.Resize(
		ctx,
		func() error { return nil },
		diskID,
		65536,
	)
	require.NoError(t, err)
}

func TestResizeDiskConcurrently(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)

	diskID := t.Name()
	createStandardSSDDisk(t, ctx, client, diskID)

	errs := make(chan error)
	workers := 3

	for i := 0; i < workers; i++ {
		go func() {
			// TODO: Should not create new client, instead reuse the old one.
			client := newClient(t, ctx)
			errs <- client.Resize(
				ctx,
				func() error { return nil },
				diskID,
				65536,
			)
		}()
	}

	for i := 0; i < workers; i++ {
		err := <-errs
		if err != nil {
			assert.True(t, errors.Is(err, errors.NewEmptyRetriableError()))
		}
	}
}

func TestResizeDiskFailureBecauseSizeIsNotDivisibleByBlockSize(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)

	diskID := t.Name()
	createStandardSSDDisk(t, ctx, client, diskID)

	err := client.Resize(
		ctx,
		func() error { return nil },
		diskID,
		65537,
	)
	require.Error(t, err)
	require.True(t, isInternalError(err))
}

func TestResizeDiskFailureBecauseSizeDecreaseIsForbidden(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)

	diskID := t.Name()
	createStandardSSDDisk(t, ctx, client, diskID)

	err := client.Resize(
		ctx,
		func() error { return nil },
		diskID,
		20480,
	)
	require.Error(t, err)
	require.True(t, isInternalError(err))
}

func TestResizeDiskFailureWhileChekpointing(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)

	diskID := t.Name()
	createStandardSSDDisk(t, ctx, client, diskID)

	err := client.Resize(
		ctx,
		func() error { return assert.AnError },
		diskID,
		65536,
	)
	require.Equal(t, assert.AnError, err)
}

func TestAlterDisk(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)

	diskID := t.Name()

	err := client.Create(ctx, nbs.CreateDiskParams{
		ID:          diskID,
		BlocksCount: 10,
		BlockSize:   4096,
		Kind:        types.DiskKind_DISK_KIND_SSD,
		CloudID:     "cloud",
		FolderID:    "folder",
	})
	require.NoError(t, err)

	err = client.Alter(
		ctx,
		func() error { return nil },
		diskID,
		"newCloud",
		"newFolder",
	)
	require.NoError(t, err)
}

func TestAlterDiskConcurrently(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)

	diskID := t.Name()

	err := client.Create(ctx, nbs.CreateDiskParams{
		ID:          diskID,
		BlocksCount: 10,
		BlockSize:   4096,
		Kind:        types.DiskKind_DISK_KIND_SSD,
		CloudID:     "cloud",
		FolderID:    "folder",
	})
	require.NoError(t, err)

	errs := make(chan error)
	workers := 3

	for i := 0; i < workers; i++ {
		go func() {
			// TODO: Should not create new client, instead reuse the old one.
			client := newClient(t, ctx)
			errs <- client.Alter(
				ctx,
				func() error { return nil },
				diskID,
				"newCloud",
				"newFolder",
			)
		}()
	}

	for i := 0; i < workers; i++ {
		err := <-errs
		if err != nil {
			assert.True(t, errors.Is(err, errors.NewEmptyRetriableError()))
		}
	}
}

func TestAlterDiskFailureWhileCheckpointing(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)

	diskID := t.Name()

	err := client.Create(ctx, nbs.CreateDiskParams{
		ID:          diskID,
		BlocksCount: 10,
		BlockSize:   4096,
		Kind:        types.DiskKind_DISK_KIND_SSD,
		CloudID:     "cloud",
		FolderID:    "folder",
	})
	require.NoError(t, err)

	err = client.Alter(
		ctx,
		func() error { return assert.AnError },
		diskID,
		"newCloud",
		"newFolder",
	)
	require.Equal(t, assert.AnError, err)
}

func TestRebaseDisk(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)

	diskID := t.Name()

	err := client.Create(ctx, nbs.CreateDiskParams{
		ID:          "base",
		BlocksCount: 10,
		BlockSize:   4096,
		Kind:        types.DiskKind_DISK_KIND_SSD,
	})
	require.NoError(t, err)

	err = client.Create(ctx, nbs.CreateDiskParams{
		ID:                   diskID,
		BaseDiskID:           "base",
		BaseDiskCheckpointID: "checkpoint",
		BlocksCount:          10,
		BlockSize:            4096,
		Kind:                 types.DiskKind_DISK_KIND_SSD,
		CloudID:              "cloud",
		FolderID:             "folder",
	})
	require.NoError(t, err)

	err = client.Create(ctx, nbs.CreateDiskParams{
		ID:          "newBase",
		BlocksCount: 10,
		BlockSize:   4096,
		Kind:        types.DiskKind_DISK_KIND_SSD,
	})
	require.NoError(t, err)

	err = client.Rebase(
		ctx,
		func() error { return nil },
		diskID,
		"base",
		"newBase",
	)
	require.NoError(t, err)

	// Check idempotency.
	err = client.Rebase(
		ctx,
		func() error { return nil },
		diskID,
		"base",
		"newBase",
	)
	require.NoError(t, err)

	err = client.Rebase(
		ctx,
		func() error { return nil },
		diskID,
		"base",
		"otherBase",
	)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	require.ErrorContains(t, err, "unexpected")
	require.True(t, isInternalError(err))
}

func TestAssignDisk(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)

	diskID := t.Name()
	createStandardSSDDisk(t, ctx, client, diskID)

	err := client.Assign(ctx, nbs.AssignDiskParams{
		ID:         diskID,
		InstanceID: "InstanceID",
		Token:      "Token",
		Host:       "Host",
	})
	require.NoError(t, err)
}

func TestUnassignDisk(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)

	diskID := t.Name()
	createStandardSSDDisk(t, ctx, client, diskID)

	err := client.Unassign(ctx, diskID)
	require.NoError(t, err)
}

func TestUnassignDeletedDisk(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)

	diskID := t.Name()
	createStandardSSDDisk(t, ctx, client, diskID)

	err := client.Delete(ctx, diskID)
	require.NoError(t, err)

	err = client.Unassign(ctx, diskID)
	require.NoError(t, err)
}

func TestTokenErrorsShouldBeRetriable(t *testing.T) {
	ctx := newContext()
	mockTokenProvider := &mockTokenProvider{}
	client := newClientFull(t, ctx, zoneID, mockTokenProvider, "10s", "20s")

	mockTokenProvider.On("Token", mock.Anything).Return("", assert.AnError).Times(10)
	mockTokenProvider.On("Token", mock.Anything).Return("", nil)

	err := client.Delete(ctx, "disk")
	require.NoError(t, err)
}

func TestGetCheckpointSize(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)

	diskID := t.Name()
	blockCount := uint64(1 << 30)
	blockSize := uint32(4096)

	err := client.Create(ctx, nbs.CreateDiskParams{
		ID:          diskID,
		BlocksCount: blockCount,
		BlockSize:   blockSize,
		Kind:        types.DiskKind_DISK_KIND_SSD,
	})
	require.NoError(t, err)

	session, err := client.MountRW(
		ctx,
		diskID,
		0,   // fillGeneration
		0,   // fillSeqNumber
		nil, // encryption
	)
	require.NoError(t, err)
	defer session.Close(ctx)

	maxUsedBlockIndex := uint64(0)
	rand.Seed(time.Now().UnixNano())

	// Set some blocks at the beginning.
	for i := uint64(0); i < 1024; i++ {
		bytes := make([]byte, 4096)
		rand.Read(bytes)

		if rand.Intn(2) == 0 {
			err = session.Write(ctx, i, bytes)
			require.NoError(t, err)

			maxUsedBlockIndex = i
		}
	}

	// Set some blocks at the tail.
	for i := blockCount - 1024; i < blockCount; i++ {
		bytes := make([]byte, 4096)
		rand.Read(bytes)

		if rand.Intn(2) == 0 {
			err = session.Write(ctx, i, bytes)
			require.NoError(t, err)

			maxUsedBlockIndex = i
		}
	}

	err = client.CreateCheckpoint(
		ctx,
		nbs.CheckpointParams{
			DiskID:       diskID,
			CheckpointID: "checkpoint",
		},
	)
	require.NoError(t, err)

	var checkpointSize uint64
	err = client.GetCheckpointSize(
		ctx,
		func(blockIndex uint64, result uint64) error {
			checkpointSize = result
			return nil
		},
		diskID,
		"checkpoint",
		0, // milestoneBlockIndex
		0, // milestoneCheckpointSize
	)
	require.NoError(t, err)
	require.Equal(t, maxUsedBlockIndex*uint64(blockSize), checkpointSize)
}

func TestIsNotFoundError(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)

	_, err := client.Describe(ctx, "unexisting")
	require.Error(t, err)
	require.True(t, nbs.IsNotFoundError(err))
	require.True(t, isInternalError(err))

	// Should work even if error is wrapped.
	err = errors.NewNonRetriableError(err)
	require.True(t, nbs.IsNotFoundError(err))
	require.True(t, isInternalError(err))
}

func TestMountRW(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)

	diskID := t.Name()
	createStandardSSDDisk(t, ctx, client, diskID)

	session, err := client.MountRW(
		ctx,
		diskID,
		0,   // fillGeneration
		0,   // fillSeqNumber
		nil, // encryption
	)
	require.NoError(t, err)
	require.NotNil(t, session)
	defer session.Close(ctx)

	expectedData := make([]byte, 4096)
	rand.Read(expectedData)

	err = session.Write(ctx, 0, expectedData)
	require.NoError(t, err)

	data := make([]byte, 4096)
	zero := false
	err = session.Read(ctx, 0, 1, "", data, &zero)
	require.NoError(t, err)
	require.Equal(t, expectedData, data)
}

func TestMountRWDoesNotConflictWithBackgroundRediscover(t *testing.T) {
	ctx := newContext()
	sessionRediscoverPeriodMaxSeconds := 1

	client := newClientFull(
		t,
		ctx,
		zoneID,
		nil,
		"500ms",
		fmt.Sprintf("%vs", sessionRediscoverPeriodMaxSeconds),
	)

	diskID := t.Name()
	createStandardSSDDisk(t, ctx, client, diskID)

	session, err := client.MountRW(
		ctx,
		diskID,
		0,   // fillGeneration
		0,   // fillSeqNumber
		nil, // encryption
	)
	require.NoError(t, err)
	require.NotNil(t, session)

	block := make([]byte, 4096)
	err = session.Write(ctx, 0, block)
	require.NoError(t, err)

	session.Close(ctx)
	// Give background rediscover some time (for better testing).
	time.Sleep(time.Duration(sessionRediscoverPeriodMaxSeconds) * time.Second)

	err = session.Write(ctx, 0, block)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyRetriableError()))
	require.True(t, isInternalError(err))

	session, err = client.MountRW(
		ctx,
		diskID,
		0,   // fillGeneration
		0,   // fillSeqNumber
		nil, // encryption
	)
	require.NoError(t, err)
	require.NotNil(t, session)
}

func TestFreeze(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)

	diskID := t.Name()
	blockCount := uint64(1 << 30)
	blockSize := uint32(4096)

	err := client.Create(ctx, nbs.CreateDiskParams{
		ID:          diskID,
		BlocksCount: blockCount,
		BlockSize:   blockSize,
		Kind:        types.DiskKind_DISK_KIND_SSD,
	})
	require.NoError(t, err)

	session, err := client.MountRW(
		ctx,
		diskID,
		0,   // fillGeneration
		0,   // fillSeqNumber
		nil, // encryption
	)
	require.NoError(t, err)
	defer session.Close(ctx)

	bytes := make([]byte, 4096)
	rand.Read(bytes)
	err = session.Write(ctx, 0, bytes)
	require.NoError(t, err)

	err = client.Freeze(
		ctx,
		func() error { return nil },
		diskID,
	)
	require.NoError(t, err)

	err = session.Write(ctx, 0, bytes)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyRetriableError()))
	require.True(t, isInternalError(err))

	err = session.Zero(ctx, 0, 1)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyRetriableError()))
	require.True(t, isInternalError(err))

	data := make([]byte, 4096)
	zero := false
	err = session.Read(ctx, 0, 1, "", data, &zero)
	require.NoError(t, err)
	require.Equal(t, bytes, data)

	err = client.Unfreeze(
		ctx,
		func() error { return nil },
		diskID,
	)
	require.NoError(t, err)

	err = session.Write(ctx, 0, bytes)
	require.NoError(t, err)
}

func TestScanDisk(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)

	diskID := t.Name()

	err := client.Create(ctx, nbs.CreateDiskParams{
		ID:          diskID,
		BlocksCount: 4096,
		BlockSize:   4096,
		Kind:        types.DiskKind_DISK_KIND_SSD,
	})
	require.NoError(t, err)

	_, err = client.GetScanDiskStatus(ctx, diskID)
	require.Error(t, err)
	require.True(t, isInternalError(err))

	batchSize := uint32(10)

	err = client.ScanDisk(ctx, diskID, batchSize)
	require.NoError(t, err)

	scanDiskStatus, err := client.GetScanDiskStatus(ctx, diskID)
	require.NoError(t, err)
	require.Empty(t, scanDiskStatus.BrokenBlobs)
}

func TestGetChangedBytes(t *testing.T) {
	for _, checkpointType := range []nbs.CheckpointType{
		nbs.CheckpointTypeNormal,
		nbs.CheckpointTypeWithoutData,
	} {
		ctx := newContext()
		client := newClient(t, ctx)

		diskID := t.Name() + strconv.Itoa(int(checkpointType))

		blockSize := uint32(4096)

		err := client.Create(ctx, nbs.CreateDiskParams{
			ID:          diskID,
			BlocksCount: 4096,
			BlockSize:   blockSize,
			Kind:        types.DiskKind_DISK_KIND_SSD,
		})
		require.NoError(t, err)

		writeBlocks(
			t,
			ctx,
			client,
			diskID,
			0, // startIndex
			1, // blockCount
		)

		checkpointID := "checkpoint"

		err = client.CreateCheckpoint(ctx, nbs.CheckpointParams{
			DiskID:         diskID,
			CheckpointID:   checkpointID,
			CheckpointType: checkpointType,
		})
		require.NoError(t, err)

		writeBlocks(
			t,
			ctx,
			client,
			diskID,
			1, // startIndex
			2, // blockCount
		)

		changedBytes, err := client.GetChangedBytes(
			ctx,
			diskID,
			"",
			checkpointID,
			false, // ignoreBaseDisk
		)
		require.NoError(t, err)
		require.Equal(t, uint64(blockSize*1), changedBytes)

		changedBytes, err = client.GetChangedBytes(
			ctx,
			diskID,
			checkpointID,
			checkpointID,
			false, // ignoreBaseDisk
		)
		require.NoError(t, err)
		require.Equal(t, uint64(0), changedBytes)

		changedBytes, err = client.GetChangedBytes(
			ctx,
			diskID,
			checkpointID,
			"",
			false, // ignoreBaseDisk
		)
		require.NoError(t, err)
		require.Equal(t, uint64(blockSize*2), changedBytes)

		changedBytes, err = client.GetChangedBytes(
			ctx,
			diskID,
			"",
			"",
			false, // ignoreBaseDisk
		)
		require.NoError(t, err)
		require.Equal(t, uint64(blockSize*3), changedBytes)
	}
}

func TestCloneDiskFromOneZoneToAnother(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)
	otherZoneClient := newOtherZoneClient(t, ctx)
	multiZoneClient := newMultiZoneClient(t, ctx)

	diskID := t.Name()
	uniqueNumber := uint64(1751352)

	err := multiZoneClient.Clone(
		ctx,
		diskID,
		"", // dstPlacementGroupID
		0,  // dstPlacementPartitionIndex
		1,  // fillGeneration
		"", // baseDiskID
	)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	require.ErrorContains(t, err, "Path not found")
	require.True(t, isInternalError(err))

	err = client.Create(ctx, nbs.CreateDiskParams{
		ID:          diskID,
		BlocksCount: uniqueNumber,
		BlockSize:   4096,
		Kind:        types.DiskKind_DISK_KIND_SSD,
	})
	require.NoError(t, err)

	err = multiZoneClient.Clone(
		ctx,
		diskID,
		"", // dstPlacementGroupID
		0,  // dstPlacementPartitionIndex
		1,  // fillGeneration
		"", // baseDiskID
	)
	require.NoError(t, err)

	params, err := client.Describe(ctx, diskID)
	require.NoError(t, err)
	require.Equal(t, uniqueNumber, params.BlocksCount)

	// Check idempotency.
	err = multiZoneClient.Clone(
		ctx,
		diskID,
		"", // dstPlacementGroupID
		0,  // dstPlacementPartitionIndex
		1,  // fillGeneration
		"", // baseDiskID
	)
	require.NoError(t, err)

	err = multiZoneClient.Clone(
		ctx,
		diskID,
		"", // dstPlacementGroupID
		0,  // dstPlacementPartitionIndex
		2,  // fillGeneration
		"", // baseDiskID
	)
	require.Error(t, err)
	require.True(t, errors.CanRetry(err))
	require.True(t, isInternalError(err))

	// Next attempt should succeed.
	err = multiZoneClient.Clone(
		ctx,
		diskID,
		"", // dstPlacementGroupID
		0,  // dstPlacementPartitionIndex
		2,  // fillGeneration
		"", // baseDiskID
	)
	require.NoError(t, err)

	err = multiZoneClient.Clone(
		ctx,
		diskID,
		"", // dstPlacementGroupID
		0,  // dstPlacementPartitionIndex
		1,  // fillGeneration
		"", // baseDiskID
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "config mismatch")
	require.True(t, isInternalError(err))

	err = otherZoneClient.FinishFillDisk(
		ctx,
		func() error { return nil },
		diskID,
		2, /* fillGeneration */
	)
	require.NoError(t, err)

	// Recreate disk-source with different BlocksCount.
	err = client.Delete(ctx, diskID)
	require.NoError(t, err)
	err = client.Create(ctx, nbs.CreateDiskParams{
		ID:          diskID,
		BlocksCount: uniqueNumber + 1,
		BlockSize:   4096,
		Kind:        types.DiskKind_DISK_KIND_SSD,
	})
	require.NoError(t, err)

	err = multiZoneClient.Clone(
		ctx,
		diskID,
		"", // dstPlacementGroupID
		0,  // dstPlacementPartitionIndex
		3,  // fillGeneration
		"", // baseDiskID
	)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	require.ErrorContains(t, err, "filling is finished")
	require.True(t, isInternalError(err))

	// Old disk-target should survive.
	params, err = otherZoneClient.Describe(ctx, diskID)
	require.NoError(t, err)
	require.Equal(t, uniqueNumber, params.BlocksCount)

	// Delete disk-source, cloning should fail with fatal error.
	err = client.Delete(ctx, diskID)
	require.NoError(t, err)
	err = multiZoneClient.Clone(
		ctx,
		diskID,
		"", // dstPlacementGroupID
		0,  // dstPlacementPartitionIndex
		2,  // fillGeneration
		"", // baseDiskID
	)
	require.Error(t, err)
	require.True(t, !errors.CanRetry(err))
	require.True(t, isInternalError(err))
}

func TestCloneDiskFromOneZoneToAnotherConcurrently(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)
	otherZoneClient := newOtherZoneClient(t, ctx)
	multiZoneClient := newMultiZoneClient(t, ctx)

	diskID := t.Name()

	err := client.Create(ctx, nbs.CreateDiskParams{
		ID:          diskID,
		BlocksCount: 4096,
		BlockSize:   4096,
		Kind:        types.DiskKind_DISK_KIND_SSD,
	})
	require.NoError(t, err)

	err = multiZoneClient.Clone(
		ctx,
		diskID,
		"", // dstPlacementGroupID
		0,  // dstPlacementPartitionIndex
		1,  // fillGeneration
		"", // baseDiskID
	)
	require.NoError(t, err)

	errs := make(chan error)

	go func() {
		// Need to add some variance for better testing.
		common.WaitForRandomDuration(1*time.Millisecond, 10*time.Millisecond)

		errs <- multiZoneClient.Clone(
			ctx,
			diskID,
			"", // dstPlacementGroupID
			0,  // dstPlacementPartitionIndex
			2,  // fillGeneration
			"", // baseDiskID
		)
	}()

	go func() {
		// Need to add some variance for better testing.
		common.WaitForRandomDuration(1*time.Millisecond, 10*time.Millisecond)

		errs <- otherZoneClient.DeleteWithFillGeneration(
			ctx,
			diskID,
			1, // fillGeneration
		)
	}()

	for i := 0; i < 2; i++ {
		err := <-errs
		if err != nil {
			require.True(t, errors.Is(err, errors.NewEmptyRetriableError()))
		}
	}
}

func TestFinishFillDisk(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)
	multiZoneClient := newMultiZoneClient(t, ctx)
	otherZoneClient := newOtherZoneClient(t, ctx)

	diskID := t.Name()

	err := client.Create(ctx, nbs.CreateDiskParams{
		ID:          diskID,
		BlocksCount: 4096,
		BlockSize:   4096,
		Kind:        types.DiskKind_DISK_KIND_SSD,
	})
	require.NoError(t, err)

	err = multiZoneClient.Clone(
		ctx,
		diskID,
		"", // dstPlacementGroupID
		0,  // dstPlacementPartitionIndex
		1,  // fillGeneration
		"", // baseDiskID
	)
	require.NoError(t, err)

	err = otherZoneClient.FinishFillDisk(
		ctx,
		func() error { return nil },
		diskID,
		2, /* fillGeneration */
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "Wrong FillGeneration")
	require.True(t, isInternalError(err))

	err = otherZoneClient.FinishFillDisk(
		ctx,
		func() error { return nil },
		diskID,
		1, /* fillGeneration */
	)
	require.NoError(t, err)

	params, err := otherZoneClient.Describe(ctx, diskID)
	require.NoError(t, err)
	require.True(t, params.IsFillFinished)
}

func TestGetChangedBlocksForLightCheckpoints(t *testing.T) {
	ctx := newContext()
	client := newClient(t, ctx)

	diskID := t.Name()
	createStandardSSDNonreplDisk(t, ctx, client, diskID)

	session, err := client.MountRW(
		ctx,
		diskID,
		0,   // fillGeneration
		0,   // fillSeqNumber
		nil, // encryption
	)
	require.NoError(t, err)
	defer session.Close(ctx)

	writeBlocksToSession(
		t,
		ctx,
		session,
		0, // startIndex
		1, // blockCount
	)

	err = client.CreateCheckpoint(ctx, nbs.CheckpointParams{
		DiskID:         diskID,
		CheckpointID:   "checkpoint_1",
		CheckpointType: nbs.CheckpointTypeLight,
	})
	require.NoError(t, err)

	var blockMask []byte

	blockMask, err = client.GetChangedBlocks(
		ctx,
		diskID,
		0,
		8,
		"",
		"checkpoint_1",
		false, // ignoreBaseDisk
	)
	require.NoError(t, err)
	require.Equal(t, 1, len(blockMask))
	require.Equal(t, uint8(0b11111111), blockMask[0])

	writeBlocksToSession(
		t,
		ctx,
		session,
		1, // startIndex
		1, // blockCount
	)

	err = client.CreateCheckpoint(ctx, nbs.CheckpointParams{
		DiskID:         diskID,
		CheckpointID:   "checkpoint_2",
		CheckpointType: nbs.CheckpointTypeLight,
	})
	require.NoError(t, err)

	blockMask, err = client.GetChangedBlocks(
		ctx,
		diskID,
		0,
		8,
		"checkpoint_1",
		"checkpoint_2",
		false, // ignoreBaseDisk
	)
	require.NoError(t, err)
	require.True(t, blockMask[0] == uint8(0b00000010))

	writeBlocksToSession(
		t,
		ctx,
		session,
		2, // startIndex
		1, // blockCount
	)

	// Checkpoint creation should be idempotent.
	err = client.CreateCheckpoint(ctx, nbs.CheckpointParams{
		DiskID:         diskID,
		CheckpointID:   "checkpoint_1",
		CheckpointType: nbs.CheckpointTypeLight,
	})
	require.NoError(t, err)

	err = client.CreateCheckpoint(ctx, nbs.CheckpointParams{
		DiskID:         diskID,
		CheckpointID:   "checkpoint_3",
		CheckpointType: nbs.CheckpointTypeLight,
	})
	require.NoError(t, err)

	blockMask, err = client.GetChangedBlocks(
		ctx,
		diskID,
		0,
		8,
		"checkpoint_2",
		"checkpoint_3",
		false, // ignoreBaseDisk
	)
	require.NoError(t, err)
	require.True(t, blockMask[0] == uint8(0b00000100))

	// Checkpoint creation should be idempotent.
	err = client.CreateCheckpoint(ctx, nbs.CheckpointParams{
		DiskID:         diskID,
		CheckpointID:   "checkpoint_3",
		CheckpointType: nbs.CheckpointTypeLight,
	})
	require.NoError(t, err)

	blockMask, err = client.GetChangedBlocks(
		ctx,
		diskID,
		0,
		8,
		"checkpoint_2",
		"checkpoint_3",
		false, // ignoreBaseDisk
	)
	require.NoError(t, err)
	require.True(t, blockMask[0] == uint8(0b00000100))

	// Should pessimize diff for old light checkpoints.
	blockMask, err = client.GetChangedBlocks(
		ctx,
		diskID,
		0,
		8,
		"checkpoint_1",
		"checkpoint_3",
		false, // ignoreBaseDisk
	)
	require.NoError(t, err)
	require.Equal(t, uint8(0b11111111), blockMask[0])

	err = client.DeleteCheckpoint(ctx, diskID, "checkpoint_1")
	require.NoError(t, err)
	err = client.DeleteCheckpoint(ctx, diskID, "checkpoint_2")
	require.NoError(t, err)
	err = client.DeleteCheckpoint(ctx, diskID, "checkpoint_3")
	require.NoError(t, err)

	err = client.Delete(ctx, diskID)
	require.NoError(t, err)
}

func TestReadFromProxyOverlayDisk(t *testing.T) {
	ctx := newContext()
	client := newTestingClient(t, ctx)

	diskID := t.Name()
	diskSize := int64(1024 * 4096)

	err := client.Create(ctx, nbs.CreateDiskParams{
		ID:              diskID,
		BlocksCount:     1024,
		BlockSize:       4096,
		Kind:            types.DiskKind_DISK_KIND_SSD,
		PartitionsCount: 1,
	})
	require.NoError(t, err)

	diskContentInfo, err := client.FillDisk(
		ctx,
		diskID,
		uint64(diskSize),
	)
	require.NoError(t, err)

	err = client.CreateCheckpoint(ctx, nbs.CheckpointParams{
		DiskID:         diskID,
		CheckpointID:   "cp",
		CheckpointType: nbs.CheckpointTypeNormal,
	})
	require.NoError(t, err)

	proxyOverlayDiskID := "proxy_" + diskID
	created, err := client.CreateProxyOverlayDisk(
		ctx,
		proxyOverlayDiskID,
		diskID,
		"cp",
	)
	require.True(t, created)
	require.NoError(t, err)

	err = client.ValidateCrc32(ctx, proxyOverlayDiskID, diskContentInfo)
	require.NoError(t, err)
}

func TestReadFromProxyOverlayDiskWithMultipartitionBaseDisk(t *testing.T) {
	ctx := newContext()
	client := newTestingClient(t, ctx)

	diskID := t.Name()
	diskSize := int64(1024 * 4096)

	err := client.Create(ctx, nbs.CreateDiskParams{
		ID:              diskID,
		BlocksCount:     1024,
		BlockSize:       4096,
		Kind:            types.DiskKind_DISK_KIND_SSD,
		PartitionsCount: 2,
	})
	require.NoError(t, err)

	diskContentInfo, err := client.FillDisk(ctx, diskID, uint64(diskSize))
	require.NoError(t, err)

	err = client.CreateCheckpoint(ctx, nbs.CheckpointParams{
		DiskID:         diskID,
		CheckpointID:   "cp",
		CheckpointType: nbs.CheckpointTypeNormal,
	})
	require.NoError(t, err)

	proxyOverlayDiskID := "proxy_" + diskID
	created, err := client.CreateProxyOverlayDisk(
		ctx,
		proxyOverlayDiskID,
		diskID,
		"cp",
	)
	require.True(t, created)
	require.NoError(t, err)

	err = client.ValidateCrc32(ctx, proxyOverlayDiskID, diskContentInfo)
	require.NoError(t, err)
}

func TestBackupDiskRegistryState(t *testing.T) {
	ctx := newContext()
	client := newTestingClient(t, ctx)

	diskID := t.Name()

	err := client.Create(ctx, nbs.CreateDiskParams{
		ID:          diskID,
		BlocksCount: 2 * 262144,
		BlockSize:   4096,
		Kind:        types.DiskKind_DISK_KIND_SSD_NONREPLICATED,
	})
	require.NoError(t, err)

	backup, err := client.BackupDiskRegistryState(ctx)
	require.NoError(t, err)

	disk := backup.GetDisk(diskID)
	require.NotNil(t, disk)
	deviceUUIDs := disk.DeviceUUIDs
	require.Equal(t, 2, len(deviceUUIDs))

	agentID := backup.GetAgentIDByDeviceUUID(deviceUUIDs[0])
	require.NotEmpty(t, agentID)
	agentID = backup.GetAgentIDByDeviceUUID(deviceUUIDs[1])
	require.NotEmpty(t, agentID)

	disk = backup.GetDisk("nonExistingDiskID")
	require.Nil(t, disk)
	agentID = backup.GetAgentIDByDeviceUUID("nonExistingDeviceID")
	require.Empty(t, agentID)

	err = client.Delete(ctx, diskID)
	require.NoError(t, err)
}

func TestDiskRegistryDisableDevices(t *testing.T) {
	ctx := newContext()
	client := newTestingClient(t, ctx)

	diskID := t.Name()
	createStandardSSDNonreplDisk(t, ctx, client, diskID)

	writeBlocks(t, ctx, client, diskID, 0 /* startIndex */, 1 /* blockCount */)

	backup, err := client.BackupDiskRegistryState(ctx)
	require.NoError(t, err)

	disk := backup.GetDisk(diskID)
	require.NotNil(t, disk)
	deviceUUIDs := disk.DeviceUUIDs
	require.Equal(t, 1, len(deviceUUIDs))

	agentID := backup.GetAgentIDByDeviceUUID(deviceUUIDs[0])
	require.NotEmpty(t, agentID)

	err = client.DisableDevices(ctx, agentID, deviceUUIDs, t.Name())
	require.NoError(t, err)

	session, err := client.MountRW(
		ctx,
		diskID,
		0,   // fillGeneration
		0,   // fillSeqNumber
		nil, // encryption
	)
	require.NoError(t, err)
	require.NotNil(t, session)
	defer session.Close(ctx)

	data := make([]byte, 4096)
	rand.Read(data)

	// Device is disabled, all read and write requests should return an error.
	err = session.Write(ctx, 0, data)
	require.Error(t, err)
	require.True(t, isInternalError(err))
	zero := false
	err = session.Read(ctx, 0, 1, "", data, &zero)
	require.Error(t, err)
	require.True(t, isInternalError(err))

	err = client.ChangeDeviceStateToOnline(ctx, deviceUUIDs[0], t.Name())
	require.NoError(t, err)
	err = client.Delete(ctx, diskID)
	require.NoError(t, err)
}

func TestDiskRegistryFindDevicesOfShadowDisk(t *testing.T) {
	ctx := newContext()
	client := newTestingClient(t, ctx)

	diskID := t.Name()
	createStandardSSDNonreplDisk(t, ctx, client, diskID)

	diskRegistryStateBackup, err := client.BackupDiskRegistryState(ctx)
	require.NoError(t, err)
	// Shadow disk should not exist because checkpoint is not created yet.
	shadowDisk := diskRegistryStateBackup.GetShadowDisk(diskID)
	require.Nil(t, shadowDisk)

	checkpointID := "checkpointID"
	err = client.CreateCheckpoint(ctx, nbs.CheckpointParams{
		DiskID:         diskID,
		CheckpointID:   checkpointID,
		CheckpointType: nbs.CheckpointTypeNormal,
	})
	require.NoError(t, err)

	diskRegistryStateBackup = nil
	shadowDisk = nil

	// Waiting for the shadow disk to be created.
	for shadowDisk == nil {
		diskRegistryStateBackup, err = client.BackupDiskRegistryState(ctx)
		require.NoError(t, err)
		shadowDisk = diskRegistryStateBackup.GetShadowDisk(diskID)
	}

	deviceUUIDs := shadowDisk.DeviceUUIDs
	require.Equal(t, 1, len(deviceUUIDs))

	err = client.DeleteCheckpoint(ctx, diskID, checkpointID)
	require.NoError(t, err)
	err = client.Delete(ctx, diskID)
	require.NoError(t, err)
}

func TestEnsureCheckpointReady(t *testing.T) {
	ctx := newContext()
	client := newTestingClient(t, ctx)

	diskID := t.Name()
	createStandardSSDNonreplDisk(t, ctx, client, diskID)

	checkpointID := "checkpoint_1"

	err := client.CreateCheckpoint(
		ctx,
		nbs.CheckpointParams{
			DiskID:       diskID,
			CheckpointID: checkpointID,
		},
	)
	require.NoError(t, err)

	for {
		// Waiting until checkpoint status turns to READY.
		err = client.EnsureCheckpointReady(ctx, diskID, checkpointID)
		if err == nil {
			break
		}
		require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	}

	err = client.DeleteCheckpointData(ctx, diskID, checkpointID)
	require.NoError(t, err)

	// Checkpoint without data should have status ERROR.
	err = client.EnsureCheckpointReady(ctx, diskID, checkpointID)
	require.True(t, errors.Is(err, errors.NewEmptyRetriableError()))

	err = client.DeleteCheckpoint(ctx, diskID, checkpointID)
	require.NoError(t, err)

	checkpointID = "checkpoint_2"

	err = client.CreateCheckpoint(
		ctx,
		nbs.CheckpointParams{
			DiskID:       diskID,
			CheckpointID: checkpointID,
		},
	)
	require.NoError(t, err)

	var diskRegistryStateBackup *nbs.DiskRegistryStateBackup
	var shadowDisk *nbs.DiskRegistryBasedDisk

	// Waiting for the shadow disk to be created.
	for shadowDisk == nil {
		diskRegistryStateBackup, err = client.BackupDiskRegistryState(ctx)
		require.NoError(t, err)
		shadowDisk = diskRegistryStateBackup.GetShadowDisk(diskID)
	}

	deviceUUIDs := shadowDisk.DeviceUUIDs
	require.Equal(t, 1, len(deviceUUIDs))
	shadowDiskDeviceUUID := deviceUUIDs[0]

	agentID := diskRegistryStateBackup.GetAgentIDByDeviceUUID(shadowDiskDeviceUUID)
	require.NotEmpty(t, agentID)

	err = client.DisableDevices(ctx, agentID, deviceUUIDs, t.Name())
	require.NoError(t, err)

	for {
		// Waiting until checkpoint status turns to ERROR.
		err = client.EnsureCheckpointReady(ctx, diskID, checkpointID)
		if errors.Is(err, errors.NewEmptyRetriableError()) {
			break
		}
		require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	}

	err = client.ChangeDeviceStateToOnline(ctx, shadowDiskDeviceUUID, t.Name())
	require.NoError(t, err)
	err = client.DeleteCheckpoint(ctx, diskID, checkpointID)
	require.NoError(t, err)
	err = client.Delete(ctx, diskID)
	require.NoError(t, err)
}

func TestAlterPlacementGroupMembership(t *testing.T) {
	ctx := newContext()
	client := newTestingClient(t, ctx)

	groupID := t.Name() + "_group"

	err := client.CreatePlacementGroup(
		ctx,
		groupID,
		types.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD,
		0, // placementPartitionCount
	)
	require.NoError(t, err)

	groupIDs, err := client.ListPlacementGroups(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(groupIDs))
	require.Equal(t, groupID, groupIDs[0])

	diskID0 := t.Name() + "0"
	diskID1 := t.Name() + "1"
	createStandardSSDNonreplDisk(t, ctx, client, diskID0)
	createStandardSSDNonreplDisk(t, ctx, client, diskID1)
	defer deleteSyncWithReties(t, ctx, client, diskID0)
	defer deleteSyncWithReties(t, ctx, client, diskID1)

	err = client.AlterPlacementGroupMembership(
		ctx,
		func() error { return nil }, // saveState
		groupID,
		0,                 // placementPartitionIndex
		[]string{diskID0}, // disksToAdd
		[]string{},        // disksToRemove
	)
	require.NoError(t, err)

	group, err := client.DescribePlacementGroup(ctx, groupID)
	require.NoError(t, err)
	require.Equal(t, 1, len(group.DiskIDs))
	require.Equal(t, diskID0, group.DiskIDs[0])

	err = client.AlterPlacementGroupMembership(
		ctx,
		func() error { return nil }, // saveState
		groupID,
		0,                 // placementPartitionIndex
		[]string{diskID1}, // disksToAdd
		[]string{diskID0}, // disksToRemove
	)
	require.NoError(t, err)

	group, err = client.DescribePlacementGroup(ctx, groupID)
	require.NoError(t, err)
	require.Equal(t, 1, len(group.DiskIDs))
	require.Equal(t, diskID1, group.DiskIDs[0])

	err = client.AlterPlacementGroupMembership(
		ctx,
		func() error { return nil }, // saveState
		groupID,
		0,                 // placementPartitionIndex
		[]string{},        // disksToAdd
		[]string{diskID1}, // disksToRemove
	)
	require.NoError(t, err)

	group, err = client.DescribePlacementGroup(ctx, groupID)
	require.NoError(t, err)
	require.Empty(t, group.DiskIDs)

	err = client.DeletePlacementGroup(ctx, groupID)
	require.NoError(t, err)

	groupIDs, err = client.ListPlacementGroups(ctx)
	require.NoError(t, err)
	require.Empty(t, groupIDs)
}

func TestAlterPlacementGroupMembershipFailureBecauseGroupDoesNotExist(t *testing.T) {
	ctx := newContext()
	client := newTestingClient(t, ctx)

	diskID := t.Name()
	createStandardSSDNonreplDisk(t, ctx, client, diskID)
	defer deleteSyncWithReties(t, ctx, client, diskID)

	err := client.AlterPlacementGroupMembership(
		ctx,
		func() error { return nil }, // saveState
		"non_existing_group",
		0,                // placementPartitionIndex
		[]string{diskID}, // disksToAdd
		[]string{},       // disksToRemove
	)
	require.Error(t, err)
	require.True(t, isInternalError(err))
}

func TestAlterPlacementGroupMembershipFailureBecauseDiskIsInAnotherGroup(t *testing.T) {
	ctx := newContext()
	client := newTestingClient(t, ctx)

	groupID0 := t.Name() + "_group0"
	groupID1 := t.Name() + "_group1"

	for _, groupID := range []string{groupID0, groupID1} {
		err := client.CreatePlacementGroup(
			ctx,
			groupID,
			types.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD,
			0, // placementPartitionCount
		)
		require.NoError(t, err)
	}

	diskID := t.Name()
	createStandardSSDNonreplDisk(t, ctx, client, diskID)
	defer deleteSyncWithReties(t, ctx, client, diskID)

	err := client.AlterPlacementGroupMembership(
		ctx,
		func() error { return nil }, // saveState
		groupID0,
		0,                // placementPartitionIndex
		[]string{diskID}, // disksToAdd
		[]string{},       // disksToRemove
	)
	require.NoError(t, err)

	err = client.AlterPlacementGroupMembership(
		ctx,
		func() error { return nil }, // saveState
		groupID1,
		0,                // placementPartitionIndex
		[]string{diskID}, // disksToAdd
		[]string{},       // disksToRemove
	)
	require.Error(t, err)
	checkErrorDetails(t, err, codes.PreconditionFailed, "", false)

	for _, groupID := range []string{groupID0, groupID1} {
		err = client.DeletePlacementGroup(ctx, groupID)
		require.NoError(t, err)
	}
}

func TestAlterPlacementGroupMembershipFailureBecauseOfTooManyDisksInGroup(t *testing.T) {
	ctx := newContext()
	client := newTestingClient(t, ctx)

	groupID := t.Name() + "_group"
	err := client.CreatePlacementGroup(
		ctx,
		groupID,
		types.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD,
		0, // placementPartitionCount
	)
	require.NoError(t, err)

	var diskIDs []string
	diskCount := 3
	for i := 0; i < diskCount; i++ {
		diskIDs = append(diskIDs, t.Name()+strconv.Itoa(i))
		createStandardSSDNonreplDisk(t, ctx, client, diskIDs[i])
		defer deleteSyncWithReties(t, ctx, client, diskIDs[i])
	}

	err = client.AlterPlacementGroupMembership(
		ctx,
		func() error { return nil }, // saveState
		groupID,
		0,          // placementPartitionIndex
		diskIDs,    // disksToAdd
		[]string{}, // disksToRemove
	)
	require.Error(t, err)
	checkErrorDetails(t, err, codes.ResourceExhausted, "", false)

	err = client.DeletePlacementGroup(ctx, groupID)
	require.NoError(t, err)
}

// TODO: enable this test after syncing ydb stable-24-3.
func TestGetClusterCapacity(t *testing.T) {
	/*
		ctx := newContext()
		client := newTestingClient(t, ctx)

		capacity, err := client.GetClusterCapacity(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, capacity)
	*/
}

func TestQueryAvailableStorage(t *testing.T) {
	ctx := newContext()
	client := newTestingClient(t, ctx)

	// Searching for an agent without any local disks.
	storageInfos, err := client.QueryAvailableStorage(
		ctx,
		[]string{"localhost"},
	)
	require.NoError(t, err)
	require.Equal(t, 1, len(storageInfos))
	require.Equal(t, "localhost", storageInfos[0].AgentID)
	require.Equal(t, uint32(0), storageInfos[0].ChunkCount)
	require.Equal(t, uint64(0), storageInfos[0].ChunkSize)

	storageInfos, err = client.QueryAvailableStorage(
		ctx,
		[]string{"unknown-agent"},
	)
	require.NoError(t, err)
	require.Empty(t, storageInfos)
}
