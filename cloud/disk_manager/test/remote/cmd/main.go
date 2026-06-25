package main

import (
	"context"
	"fmt"
	"hash/crc32"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/gofrs/uuid"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_client_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/config"
	client_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/client/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring"
	monitoring_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/config"
	monitoring_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client"
	prometheus_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/pkg/monitoring/metrics"
	test_config "github.com/ydb-platform/nbs/cloud/disk_manager/test/remote/cmd/config"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

const (
	testSuiteName = "DiskManagerRemoteTests"
	cloudID       = "DiskManagerRemoteTests"

	folderID                = "DiskManagerDataplaneRemoteTests"
	s3TestsFolderID         = "DiskManagerS3Tests"
	dataplaneForURLFolderID = "DiskManagerDataplaneFromUrlRemoteTests"

	// For better testing, disk size should be greater than SSD allocation unit
	// (32 GB).
	defaultDiskSize       = 33 << 30
	defaultDiskDataSize   = 4 << 30
	nonreplicatedDiskSize = 99857989632
	mirror2DiskSize       = 99857989632
	mirror3DiskSize       = 99857989632
	defaultBlockSize      = 4096
	largeBlockSize        = 128 * 1024

	defaultResourceExpirationTimeout = 48 * time.Hour
	periodicRunSleep                 = 10 * time.Minute
)

func parseResourceExpirationTimeout(
	testConfig *test_config.TestConfig,
) (time.Duration, error) {

	value := testConfig.GetResourceExpirationTimeout()
	if len(value) == 0 {
		return defaultResourceExpirationTimeout, nil
	}

	timeout, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf(
			"failed to parse ResourceExpirationTimeout %q: %w",
			value,
			err,
		)
	}

	if timeout < 0 {
		return 0, fmt.Errorf(
			"ResourceExpirationTimeout should be non-negative, got %q",
			value,
		)
	}

	return timeout, nil
}

func generateID() string {
	return fmt.Sprintf("%v_%v", testSuiteName, uuid.Must(uuid.NewV4()).String())
}

////////////////////////////////////////////////////////////////////////////////

type testRun struct {
	dmClient                  client.Client
	privateClient             internal_client.PrivateClient
	nbsClient                 nbs.TestingClient
	testConfig                *test_config.TestConfig
	resourceExpirationTimeout time.Duration
	metrics                   *remoteTestMetrics
	currentLaunchID           string
	lastReqNumber             int
}

func newTestRunFromConfig(
	ctx context.Context,
	testConfigFileName string,
	clientConfigFileName string,
	nbsConfigFileName string,
) (*testRun, error) {

	testConfig := &test_config.TestConfig{}
	clientConfig := &client_config.ClientConfig{}
	nbsConfig := &nbs_client_config.ClientConfig{}

	if len(testConfigFileName) != 0 {
		log.Printf("Reading test config file %v", testConfigFileName)

		configBytes, err := os.ReadFile(testConfigFileName)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to read test config file %v: %w",
				testConfigFileName,
				err,
			)
		}

		log.Printf("Parsing test config file as protobuf")

		err = proto.UnmarshalText(string(configBytes), testConfig)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to parse test config file %v as protobuf: %w",
				testConfigFileName,
				err,
			)
		}
	}

	log.Printf("Reading DM client config file %v", clientConfigFileName)

	configBytes, err := os.ReadFile(clientConfigFileName)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to read Disk Manager config file %v: %w",
			clientConfigFileName,
			err,
		)
	}

	log.Printf("Parsing DM client config file as protobuf")

	err = proto.UnmarshalText(string(configBytes), clientConfig)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to parse Disk Manager config file %v as protobuf: %w",
			clientConfigFileName,
			err,
		)
	}

	log.Printf("Reading NBS client config file %v", nbsConfigFileName)

	configBytes, err = os.ReadFile(nbsConfigFileName)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to read NBS config file %v: %w",
			nbsConfigFileName,
			err,
		)
	}

	log.Printf("Parsing NBS config file as protobuf")

	err = proto.UnmarshalText(string(configBytes), nbsConfig)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to parse NBS config file %v as protobuf: %w",
			nbsConfigFileName,
			err,
		)
	}

	resourceExpirationTimeout, err := parseResourceExpirationTimeout(testConfig)
	if err != nil {
		return nil, err
	}

	log.Printf("Creating DM client with config=%v", clientConfig)

	dmClient, err := internal_client.NewClient(ctx, clientConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	privateClient, err := internal_client.NewPrivateClientForCLI(ctx, clientConfig)
	if err != nil {
		_ = dmClient.Close()
		return nil, fmt.Errorf("failed to create private client: %w", err)
	}

	nbsClient, err := nbs.NewTestingClient(ctx, testConfig.GetZoneID(), nbsConfig)
	if err != nil {
		_ = privateClient.Close()
		_ = dmClient.Close()
		return nil, fmt.Errorf("failed to create NBS testing client: %w", err)
	}

	metrics := newRemoteTestMetricsFromConfig(ctx, testConfig)

	return &testRun{
		dmClient:                  dmClient,
		privateClient:             privateClient,
		nbsClient:                 nbsClient,
		testConfig:                testConfig,
		resourceExpirationTimeout: resourceExpirationTimeout,
		metrics:                   metrics,
	}, nil
}

func (r *testRun) Close() error {
	var err error

	if r.privateClient != nil {
		err = r.privateClient.Close()
	}

	if r.dmClient != nil {
		closeErr := r.dmClient.Close()
		if err == nil {
			err = closeErr
		}
	}

	return err
}

func (r *testRun) resetLaunchID() {
	r.currentLaunchID = generateID()
	r.lastReqNumber = 0
}

func (r *testRun) getRequestContext(ctx context.Context) context.Context {
	if len(r.currentLaunchID) == 0 {
		r.resetLaunchID()
	}

	r.lastReqNumber++

	cookie := fmt.Sprintf("%v_%v", r.currentLaunchID, r.lastReqNumber)
	ctx = headers.SetOutgoingIdempotencyKey(ctx, cookie)
	ctx = headers.SetOutgoingRequestID(ctx, cookie)
	return ctx
}

func (r *testRun) resourceExpiredBefore() *timestamppb.Timestamp {
	return timestamppb.New(time.Now().Add(-r.resourceExpirationTimeout))
}

func (r *testRun) waitOperation(
	ctx context.Context,
	operation *disk_manager.Operation,
) error {

	err := internal_client.WaitOperation(ctx, r.dmClient, operation.Id)
	if err != nil {
		return fmt.Errorf("operation %v failed: %w", operation, err)
	}

	log.Printf("Successfully done operation %v", operation)
	return nil
}

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.InfoLevel),
	)
}

////////////////////////////////////////////////////////////////////////////////

func (r *testRun) createEmptyDisk(
	ctx context.Context,
	zoneID string,
	diskID string,
	diskSize uint64,
	blockSize uint64,
	folderID string,
) error {

	req := &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size:      int64(diskSize),
		BlockSize: int64(blockSize),
		Kind:      disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: zoneID,
			DiskId: diskID,
		},
		CloudId:  cloudID,
		FolderId: folderID,
	}

	log.Printf("Sending request=%v", req)

	operation, err := r.dmClient.CreateDisk(r.getRequestContext(ctx), req)
	if err != nil {
		log.Printf("Create empty disk %+v failed: %v", diskID, err)
		return fmt.Errorf("create empty disk %+v failed: %w", diskID, err)
	}
	log.Printf(
		"Successfully scheduled create disk %+v operation %v",
		diskID,
		operation,
	)

	return r.waitOperation(ctx, operation)
}

func (r *testRun) fillNbsDisk(
	ctx context.Context,
	diskID string,
	diskSize uint64,
	diskDataSize uint64,
) (uint32, error) {

	session, err := r.nbsClient.MountRW(
		ctx,
		diskID,
		0,   // fillGeneration
		0,   // fillSeqNumber
		nil, // encryption
	)
	if err != nil {
		return 0, err
	}
	defer session.Close(ctx)

	chunkSize := uint32(4 * 1024 * 1024)
	blockSize := session.BlockSize()
	blocksInChunk := chunkSize / blockSize
	acc := crc32.NewIEEE()
	rand.Seed(time.Now().UnixNano())

	for i := uint64(0); i < diskSize; i += uint64(chunkSize) {
		blockIndex := i / uint64(blockSize)
		bytes := make([]byte, chunkSize)
		dice := rand.Intn(int(diskSize/diskDataSize) + 2)

		var err error
		switch dice {
		case 0:
			rand.Read(bytes)
			err = session.Write(ctx, blockIndex, bytes)
		case 1:
			err = session.Zero(ctx, blockIndex, blocksInChunk)
		}
		if err != nil {
			return 0, err
		}

		_, err = acc.Write(bytes)
		if err != nil {
			return 0, err
		}
	}

	return acc.Sum32(), nil
}

func (r *testRun) sendCreateDiskFromImageRequest(
	ctx context.Context,
	zoneID string,
	imageID string,
	diskID string,
	diskSize uint64,
	blockSize uint64,
	folderID string,
) (*disk_manager.Operation, error) {

	req := &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcImageId{
			SrcImageId: imageID,
		},
		Size:      int64(diskSize),
		BlockSize: int64(blockSize),
		Kind:      disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: zoneID,
			DiskId: diskID,
		},
		CloudId:  cloudID,
		FolderId: folderID,
	}

	log.Printf("Sending request=%v", req)

	operation, err := r.dmClient.CreateDisk(r.getRequestContext(ctx), req)
	if err != nil {
		log.Printf(
			"Create disk %+v request from image %v failed: %v",
			diskID,
			imageID,
			err,
		)
		return nil, fmt.Errorf("create disk %+v request failed: %w", diskID, err)
	}
	log.Printf(
		"Successfully scheduled create disk %+v operation %v",
		diskID,
		operation,
	)

	return operation, nil
}

func (r *testRun) createDiskFromImage(
	ctx context.Context,
	zoneID string,
	imageID string,
	diskID string,
	diskSize uint64,
	blockSize uint64,
	folderID string,
) error {

	operation, err := r.sendCreateDiskFromImageRequest(
		ctx,
		zoneID,
		imageID,
		diskID,
		diskSize,
		blockSize,
		folderID,
	)
	if err != nil {
		return err
	}

	return r.waitOperation(ctx, operation)
}

func (r *testRun) createDiskFromSnapshot(
	ctx context.Context,
	zoneID string,
	snapshotID string,
	diskID string,
	diskKind disk_manager.DiskKind,
	diskSize uint64,
	blockSize uint64,
	folderID string,
) error {

	req := &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcSnapshotId{
			SrcSnapshotId: snapshotID,
		},
		Size:      int64(diskSize),
		BlockSize: int64(blockSize),
		Kind:      diskKind,
		DiskId: &disk_manager.DiskId{
			ZoneId: zoneID,
			DiskId: diskID,
		},
		CloudId:  cloudID,
		FolderId: folderID,
	}

	log.Printf("Sending request=%v", req)

	operation, err := r.dmClient.CreateDisk(r.getRequestContext(ctx), req)
	if err != nil {
		log.Printf(
			"Create disk %+v from snapshot %v failed: %v",
			diskID,
			snapshotID,
			err,
		)
		return fmt.Errorf("create disk %+v failed: %w", diskID, err)
	}
	log.Printf(
		"Successfully scheduled create disk %+v operation %v",
		diskID,
		operation,
	)

	return r.waitOperation(ctx, operation)
}

func (r *testRun) sendDeleteDiskRequest(
	ctx context.Context,
	zoneID string,
	diskID string,
) (*disk_manager.Operation, error) {

	req := &disk_manager.DeleteDiskRequest{
		DiskId: &disk_manager.DiskId{
			ZoneId: zoneID,
			DiskId: diskID,
		},
	}

	log.Printf("Sending request=%v", req)

	operation, err := r.dmClient.DeleteDisk(r.getRequestContext(ctx), req)
	if err != nil {
		log.Printf(
			"Delete disk %+v failed: %v",
			diskID,
			err,
		)
		return nil, fmt.Errorf("delete disk %+v failed: %w", diskID, err)
	}

	log.Printf(
		"Successfully scheduled delete disk %+v operation %v",
		diskID,
		operation,
	)
	return operation, nil
}

func (r *testRun) sendConfigurePoolRequest(
	ctx context.Context,
	zoneID string,
	imageID string,
	useImageSize bool,
) (*disk_manager.Operation, error) {

	req := &api.ConfigurePoolRequest{
		ImageId:      imageID,
		ZoneId:       zoneID,
		Capacity:     10,
		UseImageSize: useImageSize,
	}

	log.Printf("Sending request=%v", req)

	operation, err := r.privateClient.ConfigurePool(r.getRequestContext(ctx), req)
	if err != nil {
		log.Printf(
			"Configure pool request for image %v failed: %v",
			imageID,
			err,
		)
		return nil, fmt.Errorf("configure pool request failed: %w", err)
	}
	log.Printf(
		"Successfully scheduled configure pool for image %v operation %v",
		imageID,
		operation,
	)

	return operation, nil
}

func (r *testRun) configurePool(
	ctx context.Context,
	zoneID string,
	imageID string,
	useImageSize bool,
) error {

	operation, err := r.sendConfigurePoolRequest(
		ctx,
		zoneID,
		imageID,
		useImageSize,
	)
	if err != nil {
		return err
	}

	return r.waitOperation(ctx, operation)
}

////////////////////////////////////////////////////////////////////////////////

func (r *testRun) createImage(
	ctx context.Context,
	req *disk_manager.CreateImageRequest,
) error {

	log.Printf("Sending request=%v", req)

	operation, err := r.dmClient.CreateImage(r.getRequestContext(ctx), req)
	if err != nil {
		log.Printf("Create image %v failed: %v", req.DstImageId, err)
		return fmt.Errorf("create image %v failed: %w", req.DstImageId, err)
	}
	log.Printf(
		"Successfully scheduled create image %v operation %v",
		req.DstImageId,
		operation,
	)

	return r.waitOperation(ctx, operation)
}

func (r *testRun) createImageFromURL(
	ctx context.Context,
	imageID string,
	folderID string,
	url string,
) error {

	req := &disk_manager.CreateImageRequest{
		Src: &disk_manager.CreateImageRequest_SrcUrl{
			SrcUrl: &disk_manager.ImageUrl{
				Url: url,
			},
		},
		DstImageId: imageID,
		FolderId:   folderID,
		Pooled:     true,
	}

	return r.createImage(ctx, req)
}

func (r *testRun) createImageFromImage(
	ctx context.Context,
	srcImageID string,
	folderID string,
	dstImageID string,
) error {

	req := &disk_manager.CreateImageRequest{
		Src: &disk_manager.CreateImageRequest_SrcImageId{
			SrcImageId: srcImageID,
		},
		DstImageId: dstImageID,
		FolderId:   folderID,
		Pooled:     true,
	}

	return r.createImage(ctx, req)
}

func (r *testRun) createImageFromDisk(
	ctx context.Context,
	zoneID string,
	diskID string,
	folderID string,
	imageID string,
) error {

	req := &disk_manager.CreateImageRequest{
		Src: &disk_manager.CreateImageRequest_SrcDiskId{
			SrcDiskId: &disk_manager.DiskId{
				ZoneId: zoneID,
				DiskId: diskID,
			},
		},
		DstImageId: imageID,
		FolderId:   folderID,
		Pooled:     true,
	}

	return r.createImage(ctx, req)
}

func (r *testRun) sendDeleteImageRequest(
	ctx context.Context,
	imageID string,
) (*disk_manager.Operation, error) {

	req := &disk_manager.DeleteImageRequest{
		ImageId: imageID,
	}

	log.Printf("Sending request=%v", req)

	operation, err := r.dmClient.DeleteImage(r.getRequestContext(ctx), req)
	if err != nil {
		log.Printf("Delete image %v failed: %v", imageID, err)
		return nil, fmt.Errorf("delete image %v failed: %w", imageID, err)
	}

	log.Printf(
		"Successfully scheduled delete image %v operation %v",
		imageID,
		operation,
	)
	return operation, nil
}

func (r *testRun) createSnapshot(
	ctx context.Context,
	zoneID string,
	diskID string,
	snapshotID string,
	folderID string,
) error {

	req := &disk_manager.CreateSnapshotRequest{
		Src: &disk_manager.DiskId{
			ZoneId: zoneID,
			DiskId: diskID,
		},
		SnapshotId: snapshotID,
		FolderId:   folderID,
	}

	log.Printf("Sending request=%v", req)

	operation, err := r.dmClient.CreateSnapshot(r.getRequestContext(ctx), req)
	if err != nil {
		log.Printf(
			"Create snapshot %v of disk %+v failed: %v",
			snapshotID,
			diskID,
			err,
		)
		return fmt.Errorf(
			"create snapshot %v of disk %+v failed: %w",
			snapshotID,
			diskID,
			err,
		)
	}
	log.Printf(
		"Successfully scheduled create snapshot %v operation %v",
		snapshotID,
		operation,
	)

	return r.waitOperation(ctx, operation)
}

func (r *testRun) sendDeleteSnapshotRequest(
	ctx context.Context,
	snapshotID string,
) (*disk_manager.Operation, error) {

	req := &disk_manager.DeleteSnapshotRequest{
		SnapshotId: snapshotID,
	}

	log.Printf("Sending request=%v", req)

	operation, err := r.dmClient.DeleteSnapshot(r.getRequestContext(ctx), req)
	if err != nil {
		log.Printf("Delete snapshot %v failed: %v", snapshotID, err)
		return nil, fmt.Errorf("delete snapshot %v failed: %w", snapshotID, err)
	}

	log.Printf(
		"Successfully scheduled delete snapshot %v operation %v",
		snapshotID,
		operation,
	)
	return operation, nil
}

////////////////////////////////////////////////////////////////////////////////

type resources struct {
	ZoneID    string
	Disks     []string
	Images    []string
	Snapshots []string
}

////////////////////////////////////////////////////////////////////////////////

func (r *testRun) cleanupResources(
	ctx context.Context,
	rs resources,
) error {

	operations := make([]*disk_manager.Operation, 0)
	for _, diskID := range rs.Disks {
		operation, err := r.sendDeleteDiskRequest(ctx, rs.ZoneID, diskID)
		if err != nil {
			return err
		}

		operations = append(operations, operation)
	}

	for _, imageID := range rs.Images {
		operation, err := r.sendDeleteImageRequest(ctx, imageID)
		if err != nil {
			return err
		}

		operations = append(operations, operation)
	}

	for _, snapshotID := range rs.Snapshots {
		operation, err := r.sendDeleteSnapshotRequest(ctx, snapshotID)
		if err != nil {
			return err
		}

		operations = append(operations, operation)
	}

	for _, operation := range operations {
		err := r.waitOperation(ctx, operation)
		if err != nil {
			return err
		}
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func (r *testRun) listDisks(
	ctx context.Context,
	folderID string,
) ([]string, error) {

	resp, err := r.privateClient.ListDisks(ctx, &api.ListDisksRequest{
		FolderId:       folderID,
		CreatingBefore: r.resourceExpiredBefore(),
	})
	if err != nil {
		return nil, err
	}

	return resp.DiskIds, nil
}

func (r *testRun) listImages(
	ctx context.Context,
	folderID string,
) ([]string, error) {

	resp, err := r.privateClient.ListImages(ctx, &api.ListImagesRequest{
		FolderId:       folderID,
		CreatingBefore: r.resourceExpiredBefore(),
	})
	if err != nil {
		return nil, err
	}

	return resp.ImageIds, nil
}

func (r *testRun) listSnapshots(
	ctx context.Context,
	folderID string,
) ([]string, error) {

	resp, err := r.privateClient.ListSnapshots(ctx, &api.ListSnapshotsRequest{
		FolderId:       folderID,
		CreatingBefore: r.resourceExpiredBefore(),
	})
	if err != nil {
		return nil, err
	}

	return resp.SnapshotIds, nil
}

func (r *testRun) cleanupResourcesFromPreviousRuns(
	ctx context.Context,
) error {

	disks, err := r.listDisks(ctx, folderID)
	if err != nil {
		return err
	}

	images, err := r.listImages(ctx, folderID)
	if err != nil {
		return err
	}

	snapshots, err := r.listSnapshots(ctx, folderID)
	if err != nil {
		return err
	}

	rs := resources{
		ZoneID:    r.testConfig.GetZoneID(),
		Disks:     disks,
		Images:    images,
		Snapshots: snapshots,
	}
	return r.cleanupResources(ctx, rs)
}

////////////////////////////////////////////////////////////////////////////////

func testCreateEmptyDisk(
	ctx context.Context,
	r *testRun,
) (resources, error) {

	diskID := generateID()
	rs := resources{
		Disks: []string{diskID},
	}

	err := r.createEmptyDisk(
		ctx,
		r.testConfig.GetZoneID(),
		diskID,
		1<<40,
		defaultBlockSize,
		folderID,
	)
	if err != nil {
		return resources{}, err
	}

	return rs, nil
}

func testCreateImageFromURL(
	ctx context.Context,
	r *testRun,
) (resources, error) {

	imageID := generateID()
	rs := resources{
		Images: []string{imageID},
	}

	err := r.createImageFromURL(
		ctx,
		imageID,
		dataplaneForURLFolderID,
		r.testConfig.GetImageURL(),
	)
	if err != nil {
		return resources{}, err
	}

	return rs, nil
}

func testCreateDiskFromImageImpl(
	ctx context.Context,
	r *testRun,
	shouldConfigurePool bool,
	useImageSize bool,
	folderID string,
) (resources, error) {

	diskID := generateID()
	imageID := generateID()
	rs := resources{
		Disks:  []string{diskID},
		Images: []string{imageID},
	}

	err := r.createImageFromURL(
		ctx,
		imageID,
		folderID,
		r.testConfig.GetImageURL(),
	)
	if err != nil {
		return resources{}, err
	}

	if shouldConfigurePool {
		err = r.configurePool(
			ctx,
			r.testConfig.GetZoneID(),
			imageID,
			useImageSize,
		)
		if err != nil {
			return resources{}, err
		}
	}

	err = r.createDiskFromImage(
		ctx,
		r.testConfig.GetZoneID(),
		imageID,
		diskID,
		defaultDiskSize,
		defaultBlockSize,
		folderID,
	)
	if err != nil {
		return resources{}, err
	}

	err = r.nbsClient.ValidateCrc32(
		ctx,
		diskID,
		nbs.DiskContentInfo{
			ContentSize: r.testConfig.GetImageSize(),
			Crc32:       r.testConfig.GetImageCrc32(),
		},
	)
	if err != nil {
		return resources{}, err
	}

	return rs, nil
}

func testCreateDiskFromImage(
	ctx context.Context,
	r *testRun,
) (resources, error) {

	return testCreateDiskFromImageImpl(
		ctx,
		r,
		false, // shouldConfigureDiskPool
		false, // useImageSize
		dataplaneForURLFolderID,
	)
}

// Test for NBS-2005.
func testCreateDiskFromImageUsingImageSize(
	ctx context.Context,
	r *testRun,
) (resources, error) {

	return testCreateDiskFromImageImpl(
		ctx,
		r,
		true, // shouldConfigureDiskPool
		true, // useImageSize
		folderID,
	)
}

func testCreateDisksFromImage(
	ctx context.Context,
	r *testRun,
) (resources, error) {

	imageID := generateID()
	rs := resources{
		Images: []string{imageID},
	}
	for i := 0; i < 200; i++ {
		rs.Disks = append(rs.Disks, generateID())
	}

	err := r.createImageFromURL(
		ctx,
		imageID,
		folderID,
		r.testConfig.GetImageURL(),
	)
	if err != nil {
		return resources{}, err
	}

	operations := make([]*disk_manager.Operation, 0)
	for _, diskID := range rs.Disks {
		operation, err := r.sendCreateDiskFromImageRequest(
			ctx,
			r.testConfig.GetZoneID(),
			imageID,
			diskID,
			defaultDiskSize,
			defaultBlockSize,
			folderID,
		)
		if err != nil {
			return resources{}, err
		}

		operations = append(operations, operation)
	}

	for _, operation := range operations {
		err := r.waitOperation(ctx, operation)
		if err != nil {
			return resources{}, err
		}
	}

	return rs, nil
}

func testRetireBaseDisks(
	ctx context.Context,
	r *testRun,
) (resources, error) {

	imageID := generateID()
	rs := resources{
		Images: []string{imageID},
	}

	diskCount := 800
	diskCountToValidate := 20

	for i := 0; i < diskCount; i++ {
		rs.Disks = append(rs.Disks, generateID())
	}

	err := r.createImageFromURL(
		ctx,
		imageID,
		folderID,
		r.testConfig.GetImageURL(),
	)
	if err != nil {
		return resources{}, err
	}

	useImageSize := false
	err = r.configurePool(
		ctx,
		r.testConfig.GetZoneID(),
		imageID,
		useImageSize,
	)
	if err != nil {
		return resources{}, err
	}

	operations := make([]*disk_manager.Operation, 0)
	for _, diskID := range rs.Disks {
		operation, err := r.sendCreateDiskFromImageRequest(
			ctx,
			r.testConfig.GetZoneID(),
			imageID,
			diskID,
			defaultDiskSize,
			defaultBlockSize,
			folderID,
		)
		if err != nil {
			return resources{}, err
		}

		operations = append(operations, operation)
	}

	// Should wait for first disk creation in order to ensure that pool is
	// created.
	err = r.waitOperation(ctx, operations[0])
	if err != nil {
		return resources{}, err
	}

	errs := make(chan error)

	for i := 0; i < diskCountToValidate; i++ {
		operation := operations[i]
		diskID := rs.Disks[i]

		go func() {
			err := r.waitOperation(ctx, operation)
			if err != nil {
				errs <- err
				return
			}

			err = r.nbsClient.ValidateCrc32(
				ctx,
				diskID,
				nbs.DiskContentInfo{
					ContentSize: r.testConfig.GetImageSize(),
					Crc32:       r.testConfig.GetImageCrc32(),
				},
			)
			errs <- err
		}()
	}

	reqCtx := r.getRequestContext(ctx)
	operation, err := r.privateClient.RetireBaseDisks(reqCtx, &api.RetireBaseDisksRequest{
		ImageId: imageID,
		ZoneId:  r.testConfig.GetZoneID(),
	})
	if err != nil {
		return resources{}, err
	}

	operations = append(operations, operation)

	for i := 0; i < diskCountToValidate; i++ {
		err := <-errs
		if err != nil {
			return resources{}, err
		}
	}

	for _, operation := range operations {
		err := r.waitOperation(ctx, operation)
		if err != nil {
			return resources{}, err
		}
	}

	return rs, nil
}

func testCreateDiskFromSnapshotImpl(
	ctx context.Context,
	r *testRun,
	diskKind disk_manager.DiskKind,
	diskSize uint64,
	blockSize uint64,
	folderID string,
) (resources, error) {

	zoneID := r.testConfig.GetZoneID()
	diskID1 := generateID()
	diskID2 := generateID()
	snapshotID := generateID()
	rs := resources{
		Disks:     []string{diskID1, diskID2},
		Snapshots: []string{snapshotID},
	}

	err := r.createEmptyDisk(
		ctx,
		zoneID,
		diskID1,
		diskSize,
		blockSize,
		folderID,
	)
	if err != nil {
		return resources{}, err
	}

	expectedCrc32, err := r.fillNbsDisk(
		ctx,
		diskID1,
		diskSize,
		defaultDiskDataSize,
	)
	if err != nil {
		return resources{}, err
	}

	err = r.createSnapshot(
		ctx,
		zoneID,
		diskID1,
		snapshotID,
		folderID,
	)
	if err != nil {
		return resources{}, err
	}

	err = r.createDiskFromSnapshot(
		ctx,
		zoneID,
		snapshotID,
		diskID2,
		diskKind,
		diskSize,
		blockSize,
		folderID,
	)
	if err != nil {
		return resources{}, err
	}

	err = r.nbsClient.ValidateCrc32(
		ctx,
		diskID2,
		nbs.DiskContentInfo{
			ContentSize: diskSize,
			Crc32:       expectedCrc32,
		},
	)
	if err != nil {
		return resources{}, err
	}

	return rs, nil
}

func testCreateDiskFromSnapshotInS3(
	ctx context.Context,
	r *testRun,
) (resources, error) {

	return testCreateDiskFromSnapshotImpl(
		ctx,
		r,
		disk_manager.DiskKind_DISK_KIND_SSD,
		defaultDiskSize,
		defaultBlockSize,
		s3TestsFolderID,
	)
}

func testCreateDiskFromSnapshot(
	ctx context.Context,
	r *testRun,
) (resources, error) {

	return testCreateDiskFromSnapshotImpl(
		ctx,
		r,
		disk_manager.DiskKind_DISK_KIND_SSD,
		defaultDiskSize,
		defaultBlockSize,
		folderID,
	)
}

func testCreateLargeDiskFromSnapshot(
	ctx context.Context,
	r *testRun,
) (resources, error) {

	return testCreateDiskFromSnapshotImpl(
		ctx,
		r,
		disk_manager.DiskKind_DISK_KIND_SSD,
		defaultDiskSize,
		largeBlockSize,
		folderID,
	)
}

func testCreateSsdNonreplicatedDiskFromSnapshot(
	ctx context.Context,
	r *testRun,
) (resources, error) {

	return testCreateDiskFromSnapshotImpl(
		ctx,
		r,
		disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		nonreplicatedDiskSize,
		defaultBlockSize,
		folderID,
	)
}

func testCreateHddNonreplicatedDiskFromSnapshot(
	ctx context.Context,
	r *testRun,
) (resources, error) {

	return testCreateDiskFromSnapshotImpl(
		ctx,
		r,
		disk_manager.DiskKind_DISK_KIND_HDD_NONREPLICATED,
		nonreplicatedDiskSize,
		defaultBlockSize,
		folderID,
	)
}

func testCreateImageFromImageImpl(
	ctx context.Context,
	r *testRun,
	folderID string,
) (resources, error) {

	diskSize := uint64(4 * 1024 * 1024 * 1024)

	diskID1 := generateID()
	diskID2 := generateID()
	imageID1 := generateID()
	imageID2 := generateID()
	rs := resources{
		Disks:  []string{diskID1, diskID2},
		Images: []string{imageID1, imageID2},
	}

	err := r.createEmptyDisk(
		ctx,
		r.testConfig.GetZoneID(),
		diskID1,
		diskSize,
		defaultBlockSize,
		folderID,
	)
	if err != nil {
		return resources{}, err
	}

	expectedCrc32, err := r.fillNbsDisk(ctx, diskID1, diskSize, diskSize/2)
	if err != nil {
		return resources{}, err
	}

	err = r.createImageFromDisk(
		ctx,
		r.testConfig.GetZoneID(),
		diskID1,
		folderID,
		imageID1,
	)
	if err != nil {
		return resources{}, err
	}

	err = r.createImageFromImage(ctx, imageID1, folderID, imageID2)
	if err != nil {
		return resources{}, err
	}

	err = r.createDiskFromImage(
		ctx,
		r.testConfig.GetZoneID(),
		imageID2,
		diskID2,
		diskSize,
		defaultBlockSize,
		folderID,
	)
	if err != nil {
		return resources{}, err
	}

	err = r.nbsClient.ValidateCrc32(
		ctx,
		diskID2,
		nbs.DiskContentInfo{
			ContentSize: diskSize,
			Crc32:       expectedCrc32,
		},
	)
	if err != nil {
		return resources{}, err
	}

	return rs, nil
}

func testCreateImageFromImage(
	ctx context.Context,
	r *testRun,
) (resources, error) {

	return testCreateImageFromImageImpl(
		ctx,
		r,
		folderID,
	)
}

////////////////////////////////////////////////////////////////////////////////

type testCase struct {
	Name string
	Run  func(
		context.Context,
		*testRun,
	) (resources, error)
}

type remoteTestMetrics struct {
	errors      monitoring_metrics.Counter
	fails       monitoring_metrics.CounterVec
	success     monitoring_metrics.Counter
	successTest monitoring_metrics.CounterVec
}

func newRemoteTestMetrics(
	registry monitoring_metrics.Registry,
) *remoteTestMetrics {

	registry.Gauge("remoteTestRunning").Set(1)

	return &remoteTestMetrics{
		errors: registry.Counter("error"),
		fails: registry.CounterVec("fails", []string{
			"testName",
		}),
		success: registry.Counter("success"),
		successTest: registry.CounterVec("successTest", []string{
			"testName",
		}),
	}
}

func (m *remoteTestMetrics) reportError() {
	m.errors.Inc()
}

func (m *remoteTestMetrics) reportFailure(testName string) {
	m.fails.With(map[string]string{
		"testName": testName,
	}).Inc()
}

func (m *remoteTestMetrics) reportSuccess() {
	m.success.Inc()
}

func (m *remoteTestMetrics) reportSuccessTest(testName string) {
	m.successTest.With(map[string]string{
		"testName": testName,
	}).Inc()
}

func (r *testRun) reportError() {
	r.metrics.reportError()
}

func (r *testRun) reportFailure(testName string) {
	r.metrics.reportFailure(testName)
}

func (r *testRun) reportSuccess() {
	r.metrics.reportSuccess()
}

func (r *testRun) reportSuccessTest(testName string) {
	r.metrics.reportSuccessTest(testName)
}

func newTestDenyList(testsDenyList []string) map[string]struct{} {
	denyList := make(map[string]struct{}, len(testsDenyList))
	for _, testName := range testsDenyList {
		denyList[testName] = struct{}{}
	}

	return denyList
}

func validateTestDenyList(
	tests []testCase,
	testsDenyList map[string]struct{},
) error {

	testNames := make(map[string]struct{}, len(tests))
	for _, test := range tests {
		testNames[test.Name] = struct{}{}
	}

	for testName := range testsDenyList {
		if _, ok := testNames[testName]; !ok {
			return fmt.Errorf(
				"denied test %q is not present in tests",
				testName,
			)
		}
	}

	if len(testsDenyList) == len(testNames) {
		return fmt.Errorf("tests deny list should be a strict subset of tests")
	}

	return nil
}

func (r *testRun) runTests(ctx context.Context) error {

	r.resetLaunchID()

	log.Printf("Using launch id %v", r.currentLaunchID)
	log.Printf("Using resource expiration timeout %v", r.resourceExpirationTimeout)

	tests := []testCase{
		{
			Name: "testCreateEmptyDisk",
			Run:  testCreateEmptyDisk,
		},
		{
			Name: "testCreateImageFromURL",
			Run:  testCreateImageFromURL,
		},
		{
			Name: "testCreateDiskFromImage",
			Run:  testCreateDiskFromImage,
		},
		{
			Name: "testCreateDiskFromImageUsingImageSize",
			Run:  testCreateDiskFromImageUsingImageSize,
		},
		{
			Name: "testCreateDisksFromImage",
			Run:  testCreateDisksFromImage,
		},
		{
			Name: "testRetireBaseDisks",
			Run:  testRetireBaseDisks,
		},
		{
			Name: "testCreateDiskFromSnapshotInS3",
			Run:  testCreateDiskFromSnapshotInS3,
		},
		{
			Name: "testCreateDiskFromSnapshot",
			Run:  testCreateDiskFromSnapshot,
		},
		{
			Name: "testCreateLargeDiskFromSnapshot",
			Run:  testCreateLargeDiskFromSnapshot,
		},
		{
			Name: "testCreateSsdNonreplicatedDiskFromSnapshot",
			Run:  testCreateSsdNonreplicatedDiskFromSnapshot,
		},
		{
			Name: "testCreateHddNonreplicatedDiskFromSnapshot",
			Run:  testCreateHddNonreplicatedDiskFromSnapshot,
		},
		// TODO: testCreateMirror2DiskFromSnapshot
		// TODO: testCreateMirror3DiskFromSnapshot
		{
			Name: "testCreateImageFromImage",
			Run:  testCreateImageFromImage,
		},
	}
	testsDenyList := newTestDenyList(r.testConfig.GetTestsDenyList())
	err := validateTestDenyList(tests, testsDenyList)
	if err != nil {
		r.reportError()
		return err
	}

	err = r.cleanupResourcesFromPreviousRuns(ctx)
	if err != nil {
		r.reportError()
		return fmt.Errorf(
			"failed to cleanup resources from previous runs: %w",
			err,
		)
	}

	log.Printf("Starting tests")

	for _, test := range tests {
		if _, ok := testsDenyList[test.Name]; ok {
			log.Printf("Skipping denied test %v", test.Name)
			continue
		}

		log.Printf("Starting test %v", test.Name)

		se, err := test.Run(ctx, r)
		if err != nil {
			r.reportFailure(test.Name)
			return fmt.Errorf("test %v run failed: %w", test.Name, err)
		}

		log.Printf("Test %v success", test.Name)

		err = r.cleanupResources(ctx, se)
		if err != nil {
			r.reportError()
			return fmt.Errorf(
				"test %v failed to cleanup resources from current run: %w",
				test.Name,
				err,
			)
		}

		r.reportSuccessTest(test.Name)
	}

	log.Printf("All tests successfully finished")
	r.reportSuccess()
	return nil
}

func newRemoteTestMetricsFromConfig(
	ctx context.Context,
	testConfig *test_config.TestConfig,
) *remoteTestMetrics {

	if !testConfig.GetMetricsReportingEnabled() {
		return newRemoteTestMetrics(monitoring_metrics.NewEmptyRegistry())
	}

	log.Printf("Starting Prometheus metrics reporting")

	monPort := testConfig.GetMonitoringPort()
	mon := monitoring.NewMonitoring(
		&monitoring_config.MonitoringConfig{
			Port: &monPort,
		},
		prometheus_metrics.NewPrometheusRegistry,
	)
	mon.Start(ctx)

	return newRemoteTestMetrics(mon.NewRegistry("remote_test"))
}

func (r *testRun) runTestsPeriodically(ctx context.Context) error {
	for {
		err := r.runTests(ctx)
		if err != nil {
			log.Printf("Remote tests run failed: %v", err)
		} else {
			log.Printf("Remote tests run succeeded")
		}

		log.Printf("Sleeping for %v before next run", periodicRunSleep)
		time.Sleep(periodicRunSleep)
	}
}

////////////////////////////////////////////////////////////////////////////////

func main() {
	var testConfigFileName string
	var clientConfigFileName string
	var nbsConfigFileName string

	rootCmd := &cobra.Command{
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := newContext()

			r, err := newTestRunFromConfig(
				ctx,
				testConfigFileName,
				clientConfigFileName,
				nbsConfigFileName,
			)
			if err != nil {
				return err
			}
			defer func() {
				err := r.Close()
				if err != nil {
					log.Printf("Failed to close test run: %v", err)
				}
			}()

			if r.testConfig.GetPeriodicRunsEnabled() {
				return r.runTestsPeriodically(ctx)
			}

			return r.runTests(ctx)
		},
	}

	rootCmd.PersistentFlags().StringVar(
		&testConfigFileName,
		"test-config",
		"",
		"Path to the test config file",
	)

	rootCmd.PersistentFlags().StringVar(
		&clientConfigFileName,
		"disk-manager-client-config",
		"",
		"Path to the Disk Manager client config file",
	)

	rootCmd.PersistentFlags().StringVar(
		&nbsConfigFileName,
		"nbs-client-config",
		"",
		"Path to the NBS client config file",
	)

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Failed to run: %v", err)
	}
}
