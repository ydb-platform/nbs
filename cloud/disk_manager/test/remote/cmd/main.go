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
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client"
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

	resourceExpirationTimeout = 48 * time.Hour
)

var curLaunchID string
var lastReqNumber int

////////////////////////////////////////////////////////////////////////////////

func resourceExpiredBefore() *timestamppb.Timestamp {
	return timestamppb.New(time.Now().Add(-resourceExpirationTimeout))
}

func generateID() string {
	return fmt.Sprintf("%v_%v", testSuiteName, uuid.Must(uuid.NewV4()).String())
}

func getRequestContext(ctx context.Context) context.Context {
	if len(curLaunchID) == 0 {
		curLaunchID = generateID()
	}

	lastReqNumber++

	cookie := fmt.Sprintf("%v_%v", curLaunchID, lastReqNumber)
	ctx = headers.SetOutgoingIdempotencyKey(ctx, cookie)
	ctx = headers.SetOutgoingRequestID(ctx, cookie)
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

func parseConfigs(
	testConfigFileName string,
	testConfig *test_config.TestConfig,
	clientConfigFileName string,
	clientConfig *client_config.ClientConfig,
	nbsConfigFileName string,
	nbsConfig *nbs_client_config.ClientConfig,
) error {

	if len(testConfigFileName) != 0 {
		log.Printf("Reading test config file %v", testConfigFileName)

		configBytes, err := os.ReadFile(testConfigFileName)
		if err != nil {
			return fmt.Errorf(
				"failed to read test config file %v: %w",
				testConfigFileName,
				err,
			)
		}

		log.Printf("Parsing test config file as protobuf")

		err = proto.UnmarshalText(string(configBytes), testConfig)
		if err != nil {
			return fmt.Errorf(
				"failed to parse test config file %v as protobuf: %w",
				testConfigFileName,
				err,
			)
		}
	}

	log.Printf("Reading DM client config file %v", clientConfigFileName)

	configBytes, err := os.ReadFile(clientConfigFileName)
	if err != nil {
		return fmt.Errorf(
			"failed to read Disk Manager config file %v: %w",
			clientConfigFileName,
			err,
		)
	}

	log.Printf("Parsing DM client config file as protobuf")

	err = proto.UnmarshalText(string(configBytes), clientConfig)
	if err != nil {
		return fmt.Errorf(
			"failed to parse Disk Manager config file %v as protobuf: %w",
			clientConfigFileName,
			err,
		)
	}

	log.Printf("Reading NBS client config file %v", nbsConfigFileName)

	configBytes, err = os.ReadFile(nbsConfigFileName)
	if err != nil {
		return fmt.Errorf(
			"failed to read NBS config file %v: %w",
			nbsConfigFileName,
			err,
		)
	}

	log.Printf("Parsing NBS client config file as protobuf")

	err = proto.UnmarshalText(string(configBytes), nbsConfig)
	if err != nil {
		return fmt.Errorf(
			"failed to parse NBS config file %v as protobuf: %w",
			nbsConfigFileName,
			err,
		)
	}

	return nil
}

func waitOperation(
	ctx context.Context,
	client client.Client,
	operation *disk_manager.Operation,
) error {

	err := internal_client.WaitOperation(ctx, client, operation.Id)
	if err != nil {
		return fmt.Errorf("operation %v failed: %w", operation, err)
	}

	log.Printf("Successfully done operation %v", operation)
	return nil
}

func newContext(config *client_config.ClientConfig) context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.InfoLevel),
	)
}

func newNbsTestingClient(
	ctx context.Context,
	config *nbs_client_config.ClientConfig,
	zoneID string,
) (nbs.TestingClient, error) {

	return nbs.NewTestingClient(ctx, zoneID, config)
}

////////////////////////////////////////////////////////////////////////////////

func createEmptyDisk(
	ctx context.Context,
	client client.Client,
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

	operation, err := client.CreateDisk(getRequestContext(ctx), req)
	if err != nil {
		log.Printf("Create empty disk %+v failed: %v", diskID, err)
		return fmt.Errorf("create empty disk %+v failed: %w", diskID, err)
	}
	log.Printf(
		"Successfully scheduled create disk %+v operation %v",
		diskID,
		operation,
	)

	return waitOperation(ctx, client, operation)
}

func fillNbsDisk(
	ctx context.Context,
	nbsClient nbs.Client,
	diskID string,
	diskSize uint64,
	diskDataSize uint64,
) (uint32, error) {

	session, err := nbsClient.MountRW(
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

func sendCreateDiskFromImageRequest(
	ctx context.Context,
	client client.Client,
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

	operation, err := client.CreateDisk(getRequestContext(ctx), req)
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

func createDiskFromImage(
	ctx context.Context,
	client client.Client,
	zoneID string,
	imageID string,
	diskID string,
	diskSize uint64,
	blockSize uint64,
	folderID string,
) error {

	operation, err := sendCreateDiskFromImageRequest(
		ctx,
		client,
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

	return waitOperation(ctx, client, operation)
}

func createDiskFromSnapshot(
	ctx context.Context,
	client client.Client,
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

	operation, err := client.CreateDisk(getRequestContext(ctx), req)
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

	return waitOperation(ctx, client, operation)
}

func sendDeleteDiskRequest(
	ctx context.Context,
	client client.Client,
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

	operation, err := client.DeleteDisk(getRequestContext(ctx), req)
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

func sendConfigurePoolRequest(
	ctx context.Context,
	client client.Client,
	privateClient internal_client.PrivateClient,
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

	operation, err := privateClient.ConfigurePool(getRequestContext(ctx), req)
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

func configurePool(
	ctx context.Context,
	client client.Client,
	privateClient internal_client.PrivateClient,
	zoneID string,
	imageID string,
	useImageSize bool,
) error {

	operation, err := sendConfigurePoolRequest(
		ctx,
		client,
		privateClient,
		zoneID,
		imageID,
		useImageSize,
	)
	if err != nil {
		return err
	}

	return waitOperation(ctx, client, operation)
}

////////////////////////////////////////////////////////////////////////////////

func createImage(
	ctx context.Context,
	client client.Client,
	req *disk_manager.CreateImageRequest,
) error {

	log.Printf("Sending request=%v", req)

	operation, err := client.CreateImage(getRequestContext(ctx), req)
	if err != nil {
		log.Printf("Create image %v failed: %v", req.DstImageId, err)
		return fmt.Errorf("create image %v failed: %w", req.DstImageId, err)
	}
	log.Printf(
		"Successfully scheduled create image %v operation %v",
		req.DstImageId,
		operation,
	)

	return waitOperation(ctx, client, operation)
}

func createImageFromURL(
	ctx context.Context,
	client client.Client,
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

	return createImage(ctx, client, req)
}

func createImageFromImage(
	ctx context.Context,
	client client.Client,
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

	return createImage(ctx, client, req)
}

func createImageFromDisk(
	ctx context.Context,
	client client.Client,
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

	return createImage(ctx, client, req)
}

func sendDeleteImageRequest(
	ctx context.Context,
	client client.Client,
	imageID string,
) (*disk_manager.Operation, error) {

	req := &disk_manager.DeleteImageRequest{
		ImageId: imageID,
	}

	log.Printf("Sending request=%v", req)

	operation, err := client.DeleteImage(getRequestContext(ctx), req)
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

func createSnapshot(
	ctx context.Context,
	client client.Client,
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

	operation, err := client.CreateSnapshot(getRequestContext(ctx), req)
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

	return waitOperation(ctx, client, operation)
}

func sendDeleteSnapshotRequest(
	ctx context.Context,
	client client.Client,
	snapshotID string,
) (*disk_manager.Operation, error) {

	req := &disk_manager.DeleteSnapshotRequest{
		SnapshotId: snapshotID,
	}

	log.Printf("Sending request=%v", req)

	operation, err := client.DeleteSnapshot(getRequestContext(ctx), req)
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

func cleanupResources(
	ctx context.Context,
	client client.Client,
	rs resources,
) error {

	operations := make([]*disk_manager.Operation, 0)
	for _, diskID := range rs.Disks {
		operation, err := sendDeleteDiskRequest(ctx, client, rs.ZoneID, diskID)
		if err != nil {
			return err
		}

		operations = append(operations, operation)
	}

	for _, imageID := range rs.Images {
		operation, err := sendDeleteImageRequest(ctx, client, imageID)
		if err != nil {
			return err
		}

		operations = append(operations, operation)
	}

	for _, snapshotID := range rs.Snapshots {
		operation, err := sendDeleteSnapshotRequest(ctx, client, snapshotID)
		if err != nil {
			return err
		}

		operations = append(operations, operation)
	}

	for _, operation := range operations {
		err := waitOperation(ctx, client, operation)
		if err != nil {
			return err
		}
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func listDisks(
	ctx context.Context,
	privateClient internal_client.PrivateClient,
	folderID string,
) ([]string, error) {

	resp, err := privateClient.ListDisks(ctx, &api.ListDisksRequest{
		FolderId:       folderID,
		CreatingBefore: resourceExpiredBefore(),
	})
	if err != nil {
		return nil, err
	}

	return resp.DiskIds, nil
}

func listImages(
	ctx context.Context,
	privateClient internal_client.PrivateClient,
	folderID string,
) ([]string, error) {

	resp, err := privateClient.ListImages(ctx, &api.ListImagesRequest{
		FolderId:       folderID,
		CreatingBefore: resourceExpiredBefore(),
	})
	if err != nil {
		return nil, err
	}

	return resp.ImageIds, nil
}

func listSnapshots(
	ctx context.Context,
	privateClient internal_client.PrivateClient,
	folderID string,
) ([]string, error) {

	resp, err := privateClient.ListSnapshots(ctx, &api.ListSnapshotsRequest{
		FolderId:       folderID,
		CreatingBefore: resourceExpiredBefore(),
	})
	if err != nil {
		return nil, err
	}

	return resp.SnapshotIds, nil
}

func cleanupResourcesFromPreviousRuns(
	ctx context.Context,
	client client.Client,
	privateClient internal_client.PrivateClient,
	zoneID string,
) error {

	disks, err := listDisks(ctx, privateClient, folderID)
	if err != nil {
		return err
	}

	images, err := listImages(ctx, privateClient, folderID)
	if err != nil {
		return err
	}

	snapshots, err := listSnapshots(ctx, privateClient, folderID)
	if err != nil {
		return err
	}

	rs := resources{
		ZoneID:    zoneID,
		Disks:     disks,
		Images:    images,
		Snapshots: snapshots,
	}
	return cleanupResources(ctx, client, rs)
}

////////////////////////////////////////////////////////////////////////////////

func testCreateEmptyDisk(
	ctx context.Context,
	client client.Client,
	privateClient internal_client.PrivateClient,
	testConfig *test_config.TestConfig,
	nbsConfig *nbs_client_config.ClientConfig,
) (resources, error) {

	diskID := generateID()
	rs := resources{
		Disks: []string{diskID},
	}

	err := createEmptyDisk(
		ctx,
		client,
		testConfig.GetZoneID(),
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
	client client.Client,
	privateClient internal_client.PrivateClient,
	testConfig *test_config.TestConfig,
	nbsConfig *nbs_client_config.ClientConfig,
) (resources, error) {

	imageID := generateID()
	rs := resources{
		Images: []string{imageID},
	}

	err := createImageFromURL(
		ctx,
		client,
		imageID,
		dataplaneForURLFolderID,
		testConfig.GetImageURL(),
	)
	if err != nil {
		return resources{}, err
	}

	return rs, nil
}

func testCreateDiskFromImageImpl(
	ctx context.Context,
	client client.Client,
	privateClient internal_client.PrivateClient,
	testConfig *test_config.TestConfig,
	nbsConfig *nbs_client_config.ClientConfig,
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

	err := createImageFromURL(
		ctx,
		client,
		imageID,
		folderID,
		testConfig.GetImageURL(),
	)
	if err != nil {
		return resources{}, err
	}

	if shouldConfigurePool {
		err = configurePool(
			ctx,
			client,
			privateClient,
			testConfig.GetZoneID(),
			imageID,
			useImageSize,
		)
		if err != nil {
			return resources{}, err
		}
	}

	err = createDiskFromImage(
		ctx,
		client,
		testConfig.GetZoneID(),
		imageID,
		diskID,
		defaultDiskSize,
		defaultBlockSize,
		folderID,
	)
	if err != nil {
		return resources{}, err
	}

	nbsClient, err := newNbsTestingClient(ctx, nbsConfig, testConfig.GetZoneID())
	if err != nil {
		return resources{}, err
	}

	err = nbsClient.ValidateCrc32(
		ctx,
		diskID,
		nbs.DiskContentInfo{
			ContentSize: testConfig.GetImageSize(),
			Crc32:       testConfig.GetImageCrc32(),
		},
	)
	if err != nil {
		return resources{}, err
	}

	return rs, nil
}

func testCreateDiskFromImage(
	ctx context.Context,
	client client.Client,
	privateClient internal_client.PrivateClient,
	testConfig *test_config.TestConfig,
	nbsConfig *nbs_client_config.ClientConfig,
) (resources, error) {

	return testCreateDiskFromImageImpl(
		ctx,
		client,
		privateClient,
		testConfig,
		nbsConfig,
		false, // shouldConfigureDiskPool
		false, // useImageSize
		dataplaneForURLFolderID,
	)
}

// Test for NBS-2005.
func testCreateDiskFromImageUsingImageSize(
	ctx context.Context,
	client client.Client,
	privateClient internal_client.PrivateClient,
	testConfig *test_config.TestConfig,
	nbsConfig *nbs_client_config.ClientConfig,
) (resources, error) {

	return testCreateDiskFromImageImpl(
		ctx,
		client,
		privateClient,
		testConfig,
		nbsConfig,
		true, // shouldConfigureDiskPool
		true, // useImageSize
		folderID,
	)
}

func testCreateDisksFromImage(
	ctx context.Context,
	client client.Client,
	privateClient internal_client.PrivateClient,
	testConfig *test_config.TestConfig,
	nbsConfig *nbs_client_config.ClientConfig,
) (resources, error) {

	imageID := generateID()
	rs := resources{
		Images: []string{imageID},
	}
	for i := 0; i < 200; i++ {
		rs.Disks = append(rs.Disks, generateID())
	}

	err := createImageFromURL(
		ctx,
		client,
		imageID,
		folderID,
		testConfig.GetImageURL(),
	)
	if err != nil {
		return resources{}, err
	}

	operations := make([]*disk_manager.Operation, 0)
	for _, diskID := range rs.Disks {
		operation, err := sendCreateDiskFromImageRequest(
			ctx,
			client,
			testConfig.GetZoneID(),
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
		err := waitOperation(ctx, client, operation)
		if err != nil {
			return resources{}, err
		}
	}

	return rs, nil
}

func testRetireBaseDisks(
	ctx context.Context,
	client client.Client,
	privateClient internal_client.PrivateClient,
	testConfig *test_config.TestConfig,
	nbsConfig *nbs_client_config.ClientConfig,
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

	err := createImageFromURL(
		ctx,
		client,
		imageID,
		folderID,
		testConfig.GetImageURL(),
	)
	if err != nil {
		return resources{}, err
	}

	useImageSize := false
	err = configurePool(
		ctx,
		client,
		privateClient,
		testConfig.GetZoneID(),
		imageID,
		useImageSize,
	)
	if err != nil {
		return resources{}, err
	}

	operations := make([]*disk_manager.Operation, 0)
	for _, diskID := range rs.Disks {
		operation, err := sendCreateDiskFromImageRequest(
			ctx,
			client,
			testConfig.GetZoneID(),
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
	err = waitOperation(ctx, client, operations[0])
	if err != nil {
		return resources{}, err
	}

	errs := make(chan error)

	for i := 0; i < diskCountToValidate; i++ {
		operation := operations[i]
		diskID := rs.Disks[i]

		nbsClient, err := newNbsTestingClient(ctx, nbsConfig, testConfig.GetZoneID())
		if err != nil {
			return resources{}, err
		}

		go func() {
			err := waitOperation(ctx, client, operation)
			if err != nil {
				errs <- err
				return
			}

			err = nbsClient.ValidateCrc32(
				ctx,
				diskID,
				nbs.DiskContentInfo{
					ContentSize: testConfig.GetImageSize(),
					Crc32:       testConfig.GetImageCrc32(),
				},
			)
			errs <- err
		}()
	}

	reqCtx := getRequestContext(ctx)
	operation, err := privateClient.RetireBaseDisks(reqCtx, &api.RetireBaseDisksRequest{
		ImageId: imageID,
		ZoneId:  testConfig.GetZoneID(),
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
		err := waitOperation(ctx, client, operation)
		if err != nil {
			return resources{}, err
		}
	}

	return rs, nil
}

func testCreateDiskFromSnapshotImpl(
	ctx context.Context,
	client client.Client,
	privateClient internal_client.PrivateClient,
	nbsConfig *nbs_client_config.ClientConfig,
	zoneID string,
	diskKind disk_manager.DiskKind,
	diskSize uint64,
	blockSize uint64,
	folderID string,
) (resources, error) {

	diskID1 := generateID()
	diskID2 := generateID()
	snapshotID := generateID()
	rs := resources{
		Disks:     []string{diskID1, diskID2},
		Snapshots: []string{snapshotID},
	}

	nbsClient, err := newNbsTestingClient(ctx, nbsConfig, zoneID)
	if err != nil {
		return resources{}, err
	}

	err = createEmptyDisk(
		ctx,
		client,
		zoneID,
		diskID1,
		diskSize,
		blockSize,
		folderID,
	)
	if err != nil {
		return resources{}, err
	}

	expectedCrc32, err := fillNbsDisk(
		ctx,
		nbsClient,
		diskID1,
		diskSize,
		defaultDiskDataSize,
	)
	if err != nil {
		return resources{}, err
	}

	err = createSnapshot(
		ctx,
		client,
		zoneID,
		diskID1,
		snapshotID,
		folderID,
	)
	if err != nil {
		return resources{}, err
	}

	err = createDiskFromSnapshot(
		ctx,
		client,
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

	err = nbsClient.ValidateCrc32(
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
	client client.Client,
	privateClient internal_client.PrivateClient,
	testConfig *test_config.TestConfig,
	nbsConfig *nbs_client_config.ClientConfig,
) (resources, error) {

	return testCreateDiskFromSnapshotImpl(
		ctx,
		client,
		privateClient,
		nbsConfig,
		testConfig.GetZoneID(),
		disk_manager.DiskKind_DISK_KIND_SSD,
		defaultDiskSize,
		defaultBlockSize,
		s3TestsFolderID,
	)
}

func testCreateDiskFromSnapshot(
	ctx context.Context,
	client client.Client,
	privateClient internal_client.PrivateClient,
	testConfig *test_config.TestConfig,
	nbsConfig *nbs_client_config.ClientConfig,
) (resources, error) {

	return testCreateDiskFromSnapshotImpl(
		ctx,
		client,
		privateClient,
		nbsConfig,
		testConfig.GetZoneID(),
		disk_manager.DiskKind_DISK_KIND_SSD,
		defaultDiskSize,
		defaultBlockSize,
		folderID,
	)
}

func testCreateLargeDiskFromSnapshot(
	ctx context.Context,
	client client.Client,
	privateClient internal_client.PrivateClient,
	testConfig *test_config.TestConfig,
	nbsConfig *nbs_client_config.ClientConfig,
) (resources, error) {

	return testCreateDiskFromSnapshotImpl(
		ctx,
		client,
		privateClient,
		nbsConfig,
		testConfig.GetZoneID(),
		disk_manager.DiskKind_DISK_KIND_SSD,
		defaultDiskSize,
		largeBlockSize,
		folderID,
	)
}

func testCreateSsdNonreplicatedDiskFromSnapshot(
	ctx context.Context,
	client client.Client,
	privateClient internal_client.PrivateClient,
	testConfig *test_config.TestConfig,
	nbsConfig *nbs_client_config.ClientConfig,
) (resources, error) {

	return testCreateDiskFromSnapshotImpl(
		ctx,
		client,
		privateClient,
		nbsConfig,
		testConfig.GetZoneID(),
		disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		nonreplicatedDiskSize,
		defaultBlockSize,
		folderID,
	)
}

func testCreateHddNonreplicatedDiskFromSnapshot(
	ctx context.Context,
	client client.Client,
	privateClient internal_client.PrivateClient,
	testConfig *test_config.TestConfig,
	nbsConfig *nbs_client_config.ClientConfig,
) (resources, error) {

	return testCreateDiskFromSnapshotImpl(
		ctx,
		client,
		privateClient,
		nbsConfig,
		testConfig.GetZoneID(),
		disk_manager.DiskKind_DISK_KIND_HDD_NONREPLICATED,
		nonreplicatedDiskSize,
		defaultBlockSize,
		folderID,
	)
}

func testCreateImageFromImageImpl(
	ctx context.Context,
	client client.Client,
	privateClient internal_client.PrivateClient,
	testConfig *test_config.TestConfig,
	nbsConfig *nbs_client_config.ClientConfig,
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

	nbsClient, err := newNbsTestingClient(ctx, nbsConfig, testConfig.GetZoneID())
	if err != nil {
		return resources{}, err
	}

	err = createEmptyDisk(
		ctx,
		client,
		testConfig.GetZoneID(),
		diskID1,
		diskSize,
		defaultBlockSize,
		folderID,
	)
	if err != nil {
		return resources{}, err
	}

	expectedCrc32, err := fillNbsDisk(ctx, nbsClient, diskID1, diskSize, diskSize/2)
	if err != nil {
		return resources{}, err
	}

	err = createImageFromDisk(
		ctx,
		client,
		testConfig.GetZoneID(),
		diskID1,
		folderID,
		imageID1,
	)
	if err != nil {
		return resources{}, err
	}

	err = createImageFromImage(ctx, client, imageID1, folderID, imageID2)
	if err != nil {
		return resources{}, err
	}

	err = createDiskFromImage(
		ctx,
		client,
		testConfig.GetZoneID(),
		imageID2,
		diskID2,
		diskSize,
		defaultBlockSize,
		folderID,
	)
	if err != nil {
		return resources{}, err
	}

	err = nbsClient.ValidateCrc32(
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
	client client.Client,
	privateClient internal_client.PrivateClient,
	testConfig *test_config.TestConfig,
	nbsConfig *nbs_client_config.ClientConfig,
) (resources, error) {

	return testCreateImageFromImageImpl(
		ctx,
		client,
		privateClient,
		testConfig,
		nbsConfig,
		folderID,
	)
}

////////////////////////////////////////////////////////////////////////////////

type testCase struct {
	Name string
	Run  func(
		context.Context,
		client.Client,
		internal_client.PrivateClient,
		*test_config.TestConfig,
		*nbs_client_config.ClientConfig,
	) (resources, error)
}

func runTests(
	testConfig *test_config.TestConfig,
	clientConfig *client_config.ClientConfig,
	nbsConfig *nbs_client_config.ClientConfig,
) error {

	curLaunchID = generateID()

	ctx := newContext(clientConfig)

	log.Printf("Creating DM client with config=%v", clientConfig)

	client, err := internal_client.NewClient(ctx, clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	privateClient, err := internal_client.NewPrivateClientForCLI(ctx, clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create private client: %w", err)
	}
	defer privateClient.Close()

	err = cleanupResourcesFromPreviousRuns(
		ctx,
		client,
		privateClient,
		testConfig.GetZoneID(),
	)
	if err != nil {
		return fmt.Errorf(
			"failed to cleanup resources from previous runs: %w",
			err,
		)
	}

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

	log.Printf("Starting tests")

	for _, test := range tests {
		log.Printf("Starting test %v", test.Name)

		se, err := test.Run(ctx, client, privateClient, testConfig, nbsConfig)
		if err != nil {
			return fmt.Errorf("test %v run failed: %w", test.Name, err)
		}

		log.Printf("Test %v success", test.Name)

		err = cleanupResources(ctx, client, se)
		if err != nil {
			return fmt.Errorf(
				"test %v failed to cleanup resources from current run: %w",
				test.Name,
				err,
			)
		}
	}

	log.Printf("All tests successfully finished")
	return nil
}

////////////////////////////////////////////////////////////////////////////////

func main() {
	var testConfigFileName string
	testConfig := &test_config.TestConfig{}

	var clientConfigFileName string
	clientConfig := &client_config.ClientConfig{}

	var nbsConfigFileName string
	nbsConfig := &nbs_client_config.ClientConfig{}

	rootCmd := &cobra.Command{
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return parseConfigs(
				testConfigFileName,
				testConfig,
				clientConfigFileName,
				clientConfig,
				nbsConfigFileName,
				nbsConfig,
			)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runTests(testConfig, clientConfig, nbsConfig)
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
