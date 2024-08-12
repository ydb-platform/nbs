package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

func newContext(verbose bool) context.Context {
	loggingLevel := logging.InfoLevel
	if verbose {
		loggingLevel = logging.DebugLevel
	}

	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(loggingLevel),
	)
}

////////////////////////////////////////////////////////////////////////////////

func createUniqueName(
	baseName string,
	suffix string,
) string {

	name := baseName
	if len(suffix) != 0 {
		name = fmt.Sprintf("%v-%v", name, suffix)
	}

	return fmt.Sprintf("%v-%v", name, time.Now().Unix())
}

////////////////////////////////////////////////////////////////////////////////

func createSnapshotFromDisk(
	ctx context.Context,
	ycp *ycpWrapper,
	diskID string,
	suffix string,
) (string, error) {

	name := createUniqueName("acc-snapshot", suffix)

	logging.Info(
		ctx,
		"Creating snapshot with name %v from disk %v",
		name,
		diskID,
	)

	result, err := ycp.exec(
		ctx,
		"compute snapshot create",
		map[string]interface{}{
			"name":    name,
			"disk_id": diskID,
		},
	)
	if err != nil {
		return "", err
	}

	snapshotID := result["id"].(string)
	logging.Info(ctx, "Created snapshot %v from disk %v", snapshotID, diskID)

	return snapshotID, nil
}

////////////////////////////////////////////////////////////////////////////////

type snapshotInfo struct {
	diskSize string
}

func getSnapshotInfo(
	ctx context.Context,
	ycp *ycpWrapper,
	snapshotID string,
) (snapshotInfo, error) {

	result, err := ycp.exec(ctx, "compute snapshot get", map[string]interface{}{
		"snapshot_id": snapshotID,
	})
	if err != nil {
		return snapshotInfo{}, err
	}

	var diskSize string
	if val, ok := result["disk_size"]; ok {
		diskSize = val.(string)
	} else {
		return snapshotInfo{}, fmt.Errorf(
			"missing disk_size field in result %+v",
			result,
		)
	}

	return snapshotInfo{diskSize: diskSize}, nil
}

////////////////////////////////////////////////////////////////////////////////

func createImageFromSnapshot(
	ctx context.Context,
	ycp *ycpWrapper,
	snapshotID string,
	pooled bool,
	suffix string,
) (string, error) {

	name := createUniqueName("acc-image", suffix)

	logging.Info(
		ctx,
		"Creating image with name %v from snapshot %v, pooled=%v",
		name,
		snapshotID,
		pooled,
	)

	result, err := ycp.exec(ctx, "compute image create", map[string]interface{}{
		"name":        name,
		"snapshot_id": snapshotID,
		"pooled":      pooled,
	})
	if err != nil {
		return "", err
	}

	imageID := result["id"].(string)
	logging.Info(ctx, "Created image %v from snapshot %v", imageID, snapshotID)

	return imageID, nil
}

////////////////////////////////////////////////////////////////////////////////

func createImageFromImage(
	ctx context.Context,
	ycp *ycpWrapper,
	imageID string,
	suffix string,
) (string, error) {

	name := createUniqueName("acc-image", suffix)

	logging.Info(
		ctx,
		"Creating image with name %v from image %v",
		name,
		imageID,
	)

	result, err := ycp.exec(ctx, "compute image create", map[string]interface{}{
		"name":     name,
		"image_id": imageID,
	})
	if err != nil {
		return "", err
	}

	newImageID := result["id"].(string)
	logging.Info(ctx, "Created image %v from image %v", newImageID, imageID)

	return newImageID, nil
}

////////////////////////////////////////////////////////////////////////////////

func createImageFromDisk(
	ctx context.Context,
	ycp *ycpWrapper,
	diskID string,
	suffix string,
) (string, error) {

	name := createUniqueName("acc-image", suffix)

	logging.Info(ctx, "Creating image with name %v from disk %v", name, diskID)

	result, err := ycp.exec(ctx, "compute image create", map[string]interface{}{
		"name":    name,
		"disk_id": diskID,
	})
	if err != nil {
		return "", err
	}

	imageID := result["id"].(string)
	logging.Info(ctx, "Created image %v from disk %v", imageID, diskID)

	return imageID, nil
}

////////////////////////////////////////////////////////////////////////////////

func createImageFromURL(
	ctx context.Context,
	ycp *ycpWrapper,
	suffix string,
	url string,
) (string, error) {

	name := createUniqueName("acc-image", suffix)

	logging.Info(ctx, "Creating image with name %v from url %v", name, url)

	result, err := ycp.exec(ctx, "compute image create", map[string]interface{}{
		"name": name,
		"uri":  url,
	})
	if err != nil {
		return "", err
	}

	imageID := result["id"].(string)
	logging.Info(ctx, "Created image %v from url %v", imageID, url)

	return imageID, nil
}

////////////////////////////////////////////////////////////////////////////////

func createDiskFromSnapshot(
	ctx context.Context,
	ycp *ycpWrapper,
	snapshotID string,
	zoneID string,
	size string,
	suffix string,
) (string, error) {

	name := createUniqueName("acc-disk", suffix)

	logging.Info(
		ctx,
		"Creating disk with name %v from snapshot %v, zoneID=%v, size=%v",
		name,
		snapshotID,
		zoneID,
		size,
	)

	result, err := ycp.exec(ctx, "compute disk create", map[string]interface{}{
		"name":        name,
		"type_id":     "network-ssd",
		"snapshot_id": snapshotID,
		"zone_id":     zoneID,
		"size":        size,
	})
	if err != nil {
		return "", err
	}

	diskID := result["id"].(string)
	logging.Info(ctx, "Created disk %v from snapshot %v", diskID, snapshotID)

	return diskID, nil
}

////////////////////////////////////////////////////////////////////////////////

func createDiskFromImage(
	ctx context.Context,
	ycp *ycpWrapper,
	imageID string,
	zoneID string,
	size string,
	suffix string,
) (string, error) {

	name := createUniqueName("acc-disk", suffix)

	logging.Info(
		ctx,
		"Creating disk with name %v from image %v, zoneID=%v, size=%v",
		name,
		imageID,
		zoneID,
		size,
	)

	result, err := ycp.exec(ctx, "compute disk create", map[string]interface{}{
		"name":     name,
		"type_id":  "network-ssd",
		"image_id": imageID,
		"zone_id":  zoneID,
		"size":     size,
	})
	if err != nil {
		return "", err
	}

	diskID := result["id"].(string)
	logging.Info(ctx, "Created disk %v from image %v", diskID, imageID)

	return diskID, nil
}

////////////////////////////////////////////////////////////////////////////////

func deleteSnapshot(
	ctx context.Context,
	ycp *ycpWrapper,
	snapshotID string,
) error {

	logging.Info(ctx, "Deleting snapshot %v", snapshotID)

	_, err := ycp.exec(ctx, "compute snapshot delete", map[string]interface{}{
		"snapshot_id": snapshotID,
	})
	return err
}

////////////////////////////////////////////////////////////////////////////////

func deleteImage(
	ctx context.Context,
	ycp *ycpWrapper,
	imageID string,
) error {

	logging.Info(ctx, "Deleting image %v", imageID)

	_, err := ycp.exec(ctx, "compute image delete", map[string]interface{}{
		"image_id": imageID,
	})
	return err
}

////////////////////////////////////////////////////////////////////////////////

func deleteDisk(
	ctx context.Context,
	ycp *ycpWrapper,
	diskID string,
) error {

	logging.Info(ctx, "Deleting disk %v", diskID)

	_, err := ycp.exec(ctx, "compute disk delete", map[string]interface{}{
		"disk_id": diskID,
	})
	return err
}

////////////////////////////////////////////////////////////////////////////////

func deleteOrSaveEntity(
	ctx context.Context,
	ycp *ycpWrapper,
	errs chan<- error,
	savedIdsFilePath string,
	ids []string,
	deleteEntity func(context.Context, *ycpWrapper, string) error,
) error {

	if len(savedIdsFilePath) == 0 {
		for _, id := range ids {
			go func(id string) {
				errs <- deleteEntity(ctx, ycp, id)
			}(id)
		}
	} else {
		f, err := os.OpenFile(savedIdsFilePath, os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			return err
		}
		defer f.Close()

		for _, id := range ids {
			if _, err = f.WriteString(id + "\n"); err != nil {
				return err
			}
		}
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func mainTest(
	ctx context.Context,
	ycp *ycpWrapper,
	srcDiskID string,
	zoneID string,
	suffix string,
	outputDiskIDs string,
	outputSnapshotIDs string,
	skipImages bool,
) error {

	var snapshotIDs []string
	var imageIDs []string
	var diskIDs []string

	// Create snapshot from disk
	snapshotID, err := createSnapshotFromDisk(
		ctx,
		ycp,
		srcDiskID,
		suffix,
	)
	if err != nil {
		return err
	}
	snapshotIDs = append(snapshotIDs, snapshotID)

	snapshotInfo, err := getSnapshotInfo(ctx, ycp, snapshotID)
	if err != nil {
		return err
	}

	// Create incremental snapshot from disk
	snapshotID2, err := createSnapshotFromDisk(
		ctx,
		ycp,
		srcDiskID,
		suffix,
	)
	if err != nil {
		return err
	}
	snapshotIDs = append(snapshotIDs, snapshotID2)

	// Create pooled image from snapshot
	imageID1 := ""
	if !skipImages {
		imageID1, err = createImageFromSnapshot(
			ctx,
			ycp,
			snapshotID,
			true,
			suffix,
		)
		if err != nil {
			return err
		}
		imageIDs = append(imageIDs, imageID1)
	}

	// Create disk from snapshot
	diskID1, err := createDiskFromSnapshot(
		ctx,
		ycp,
		snapshotID,
		zoneID,
		snapshotInfo.diskSize,
		suffix,
	)
	if err != nil {
		return err
	}
	diskIDs = append(diskIDs, diskID1)

	if !skipImages {
		// Create disk from image
		diskID2, err := createDiskFromImage(
			ctx,
			ycp,
			imageID1,
			zoneID,
			snapshotInfo.diskSize,
			suffix,
		)
		if err != nil {
			return err
		}
		diskIDs = append(diskIDs, diskID2)

		// Create image from disk
		imageID2, err := createImageFromDisk(ctx, ycp, diskID2, suffix)
		if err != nil {
			return err
		}
		imageIDs = append(imageIDs, imageID2)

		// Create image from image
		imageID3, err := createImageFromImage(ctx, ycp, imageID2, suffix)
		if err != nil {
			return err
		}
		imageIDs = append(imageIDs, imageID3)
	}

	// Delete all resources
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make(chan error)

	err = deleteOrSaveEntity(ctx, ycp, errs, outputDiskIDs, diskIDs, deleteDisk)
	if err != nil {
		return err
	}

	err = deleteOrSaveEntity(
		ctx,
		ycp,
		errs,
		outputSnapshotIDs,
		snapshotIDs,
		deleteSnapshot,
	)
	if err != nil {
		return err
	}

	err = deleteOrSaveEntity(ctx, ycp, errs, "", imageIDs, deleteImage)
	if err != nil {
		return err
	}

	ycpEntityCount := len(imageIDs)
	if len(outputDiskIDs) == 0 {
		ycpEntityCount += len(diskIDs)
	}
	if len(outputSnapshotIDs) == 0 {
		ycpEntityCount += len(snapshotIDs)
	}
	for i := 0; i < ycpEntityCount; i++ {
		err := <-errs
		if err != nil {
			return err
		}
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func createImageFromURLTest(
	ctx context.Context,
	ycp *ycpWrapper,
	suffix string,
	url string,
) error {

	imageID, err := createImageFromURL(ctx, ycp, suffix, url)
	if err != nil {
		return err
	}

	return deleteImage(ctx, ycp, imageID)
}

////////////////////////////////////////////////////////////////////////////////

func cancelTest(
	ctx context.Context,
	ycp *ycpWrapper,
	srcDiskID string,
) error {

	// Start snapshot creation operation
	result, err := ycp.execAsync(
		ctx,
		"compute snapshot create",
		map[string]interface{}{
			"disk_id": srcDiskID,
		},
	)
	if err != nil {
		return err
	}

	operationID := result["id"].(string)
	snapshotID := result["metadata"].(map[string]interface{})["snapshot_id"].(string)

	logging.Info(
		ctx,
		"Started snapshot creation operation: operationID %v, snapshotID %v",
		operationID,
		snapshotID,
	)

	// Check snapshot status
	result, err = ycp.exec(ctx, "compute snapshot get", map[string]interface{}{
		"snapshot_id": snapshotID,
	})
	if err != nil {
		return err
	}

	status := result["status"].(string)
	if status != "CREATING" {
		logging.Warn(
			ctx,
			"invalid snapshot status: expected CREATING, actual %v",
			status,
		)
	}

	// Delete snapshot before creation finished
	err = deleteSnapshot(ctx, ycp, snapshotID)
	if err != nil {
		return err
	}

	// Check that snapshot actually deleted
	_, err = getSnapshotInfo(ctx, ycp, snapshotID)
	if err == nil {
		return fmt.Errorf("snapshot %v not deleted", snapshotID)
	}

	// Check snapshot creation operation
	result, err = ycp.exec(ctx, "compute operation get", map[string]interface{}{
		"operation_id": operationID,
	})
	if err != nil {
		return err
	}

	done := result["done"].(bool)
	if !done {
		return fmt.Errorf("operation %v not done", operationID)
	}

	operationError, ok := result["error"].(map[string]interface{})
	if ok {
		message := operationError["message"].(string)
		logging.Info(ctx, "Error message: %v", message)
		if message != "Operation was cancelled" {
			return fmt.Errorf("operation %v not cancelled", operationID)
		}
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func runTest(
	ctx context.Context,
	srcDiskIDs []string,
	testFunc func(ctx context.Context, srcDiskID string) error,
) error {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make(chan error)

	for _, srcDiskID := range srcDiskIDs {
		srcDiskID := srcDiskID
		go func() {
			errs <- testFunc(ctx, srcDiskID)
		}()
	}

	for range srcDiskIDs {
		err := <-errs
		if err != nil {
			return err
		}
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func run(
	profile string,
	folderID string,
	srcDiskIDs []string,
	zoneID string,
	suffix string,
	outputDiskIDs string,
	outputSnapshotIDs string,
	urlForCreateImageFromURLTest string,
	ycpConfigPath string,
	verbose bool,
	skipImages bool,
) error {

	ctx := newContext(verbose)
	ycp := newYcpWrapper(profile, folderID, ycpConfigPath)

	logging.Info(
		ctx,
		"Test started: profile %v, folderID %v, zoneID %v",
		profile,
		folderID,
		zoneID,
	)

	logging.Info(ctx, "Run main test")
	err := runTest(
		ctx,
		srcDiskIDs,
		func(ctx context.Context, srcDiskID string) error {
			return mainTest(
				ctx,
				ycp,
				srcDiskID,
				zoneID,
				suffix,
				outputDiskIDs,
				outputSnapshotIDs,
				skipImages,
			)
		},
	)
	if err != nil {
		return err
	}

	if len(urlForCreateImageFromURLTest) != 0 {
		logging.Info(ctx, "Run createImageFromURL test")
		err := createImageFromURLTest(ctx, ycp, suffix, urlForCreateImageFromURLTest)
		if err != nil {
			return err
		}
	}

	logging.Info(ctx, "Run cancel test")
	err = runTest(
		ctx,
		srcDiskIDs,
		func(ctx context.Context, srcDiskID string) error {
			return cancelTest(ctx, ycp, srcDiskID)
		},
	)
	if err != nil {
		return err
	}

	logging.Info(ctx, "Test finished")
	return nil
}

////////////////////////////////////////////////////////////////////////////////

func main() {
	var profile string
	var folderID string
	var srcDiskIDs []string
	var zoneID string
	var suffix string
	var outputDiskIDs string
	var outputSnapshotIDs string
	var urlForCreateImageFromURLTest string
	var ycpConfigPath string
	var verbose bool
	var skipImages bool

	rootCmd := &cobra.Command{
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(
				profile,
				folderID,
				srcDiskIDs,
				zoneID,
				suffix,
				outputDiskIDs,
				outputSnapshotIDs,
				urlForCreateImageFromURLTest,
				ycpConfigPath,
				verbose,
				skipImages,
			)
		},
	}

	rootCmd.Flags().StringVar(&profile, "profile", "", "ycp profile")
	if err := rootCmd.MarkFlagRequired("profile"); err != nil {
		log.Fatalf("Error setting flag 'profile' as required: %v", err)
	}

	rootCmd.Flags().StringVar(&folderID, "folder-id", "", "folder id")
	if err := rootCmd.MarkFlagRequired("folder-id"); err != nil {
		log.Fatalf("Error setting flag 'folder-id' as required: %v", err)
	}

	rootCmd.Flags().StringSliceVar(
		&srcDiskIDs,
		"src-disk-ids",
		nil,
		"comma-separated source disk ids",
	)
	if err := rootCmd.MarkFlagRequired("src-disk-ids"); err != nil {
		log.Fatalf("Error setting flag 'src-disk-ids' as required: %v", err)
	}

	rootCmd.Flags().StringVar(&zoneID, "zone-id", "ru-central1-a", "zone id")
	rootCmd.Flags().StringVar(
		&suffix,
		"suffix",
		"",
		"add unique suffix to entity name (ex. acc-disk-<suffix>-<timestamp>)")
	rootCmd.Flags().StringVar(
		&outputDiskIDs,
		"output-disk-ids",
		"",
		"path to file where all created disks ids would be stored (this parameter turns off ycp disk collecting, so you MUST manually delete all of them after test)",
	)
	rootCmd.Flags().StringVar(
		&outputSnapshotIDs,
		"output-snapshot-ids",
		"",
		"path to file where all created snapshot ids would be stored (this parameter turns off ycp snapshot collecting, so you MUST manually delete all of them after test)",
	)
	rootCmd.Flags().StringVar(
		&urlForCreateImageFromURLTest,
		"url-for-create-image-from-url-test",
		"",
		"url from which test image will be created",
	)
	rootCmd.Flags().StringVar(
		&ycpConfigPath,
		"ycp-config-path",
		"",
		"path to ycp config",
	)
	rootCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "verbose logging")
	rootCmd.Flags().BoolVar(
		&skipImages,
		"skip-images",
		false,
		"skip creation of images and creation of disks from images",
	)

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Failed to execute: %v", err)
	}
}
