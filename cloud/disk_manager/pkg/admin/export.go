package admin

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	client_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/client/config"
	sdk_client "github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

// Export of a snapshot or image through the public API: the export runs on
// the export nodes of Disk Manager, this command only schedules it and waits.
type exportToS3 struct {
	clientConfig      *client_config.ClientConfig
	id                string
	cloudID           string
	folderID          string
	s3Endpoint        string
	s3Region          string
	s3Bucket          string
	s3Prefix          string
	s3CredentialsFile string
	s3PartSize        string
}

func (c *exportToS3) destination() (*disk_manager.ExportDestination, error) {
	credentials, err := persistence.NewS3CredentialsFromFile(
		c.s3CredentialsFile,
	)
	if err != nil {
		return nil, fmt.Errorf("loading destination S3 credentials: %w", err)
	}

	var partSize uint64
	if len(c.s3PartSize) != 0 {
		partSize, err = parseS3PartSize(c.s3PartSize)
		if err != nil {
			return nil, err
		}
	}

	return &disk_manager.ExportDestination{
		Endpoint:        c.s3Endpoint,
		Region:          c.s3Region,
		Bucket:          c.s3Bucket,
		Prefix:          c.s3Prefix,
		AccessKeyId:     credentials.ID,
		SecretAccessKey: credentials.Secret,
		PartSize:        partSize,
	}, nil
}

func (c *exportToS3) run(
	export func(
		client sdk_client.Client,
		dst *disk_manager.ExportDestination,
	) (*disk_manager.Operation, error),
) error {

	dst, err := c.destination()
	if err != nil {
		return err
	}

	ctx := newContext(c.clientConfig)

	client, err := internal_client.NewClient(ctx, c.clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	resp, err := export(client, dst)
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %v\n", resp.Id)

	response := &disk_manager.ExportResponse{}
	err = internal_client.WaitResponse(ctx, client, resp.Id, response)
	if err != nil {
		return err
	}

	fmt.Printf(
		"Exported to s3://%v/%v: %v parts, %v bytes, manifest %v\n",
		response.Bucket,
		response.Prefix,
		response.PartCount,
		response.Size,
		response.ManifestKey,
	)
	return nil
}

func (c *exportToS3) addFlags(cmd *cobra.Command, idUsage string) {
	cmd.Flags().StringVar(&c.id, "id", "", idUsage)
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	cmd.Flags().StringVar(
		&c.cloudID,
		"cloud-id",
		"",
		"cloud ID of the resource owner; required, checked against the "+
			"export deny list",
	)
	if err := cmd.MarkFlagRequired("cloud-id"); err != nil {
		log.Fatalf("Error setting flag cloud-id as required: %v", err)
	}

	cmd.Flags().StringVar(
		&c.folderID,
		"folder-id",
		"",
		"folder ID of the resource owner; required",
	)
	if err := cmd.MarkFlagRequired("folder-id"); err != nil {
		log.Fatalf("Error setting flag folder-id as required: %v", err)
	}

	cmd.Flags().StringVar(
		&c.s3Endpoint,
		"s3-endpoint",
		"",
		"Destination S3-compatible HTTP(S) endpoint; required",
	)
	if err := cmd.MarkFlagRequired("s3-endpoint"); err != nil {
		log.Fatalf("Error setting flag s3-endpoint as required: %v", err)
	}

	cmd.Flags().StringVar(
		&c.s3Region,
		"s3-region",
		"",
		"Destination signing region; empty for storages without regions",
	)
	cmd.Flags().StringVar(
		&c.s3Bucket,
		"s3-bucket",
		"",
		"Destination bucket; must exist",
	)
	if err := cmd.MarkFlagRequired("s3-bucket"); err != nil {
		log.Fatalf("Error setting flag s3-bucket as required: %v", err)
	}

	cmd.Flags().StringVar(
		&c.s3Prefix,
		"s3-prefix",
		"",
		"Optional key prefix of the exported objects",
	)
	cmd.Flags().StringVar(
		&c.s3CredentialsFile,
		"s3-credentials-file",
		"",
		"JSON file with id and secret of the destination; required",
	)
	if err := cmd.MarkFlagRequired("s3-credentials-file"); err != nil {
		log.Fatalf(
			"Error setting flag s3-credentials-file as required: %v",
			err,
		)
	}

	cmd.Flags().StringVar(
		&c.s3PartSize,
		"s3-part-size",
		"",
		"Part size, a multiple of 4MiB up to 1GiB; the server default if empty",
	)
}

////////////////////////////////////////////////////////////////////////////////

func newExportSnapshotToS3Cmd(
	clientConfig *client_config.ClientConfig,
) *cobra.Command {

	c := &exportToS3{clientConfig: clientConfig}

	cmd := &cobra.Command{
		Use: "export-to-s3",
		Short: "Export the follower copy of a snapshot to an S3 bucket " +
			"through the Disk Manager API",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run(
				func(
					client sdk_client.Client,
					dst *disk_manager.ExportDestination,
				) (*disk_manager.Operation, error) {

					return client.ExportSnapshot(
						getRequestContext(newContext(c.clientConfig)),
						&disk_manager.ExportSnapshotRequest{
							SnapshotId: c.id,
							CloudId:    c.cloudID,
							FolderId:   c.folderID,
							Dst:        dst,
						},
					)
				},
			)
		},
	}

	c.addFlags(cmd, "ID of snapshot to export; required")
	return cmd
}

func newExportImageToS3Cmd(
	clientConfig *client_config.ClientConfig,
) *cobra.Command {

	c := &exportToS3{clientConfig: clientConfig}

	cmd := &cobra.Command{
		Use: "export-to-s3",
		Short: "Export the follower copy of an image to an S3 bucket " +
			"through the Disk Manager API",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run(
				func(
					client sdk_client.Client,
					dst *disk_manager.ExportDestination,
				) (*disk_manager.Operation, error) {

					return client.ExportImage(
						getRequestContext(newContext(c.clientConfig)),
						&disk_manager.ExportImageRequest{
							ImageId:  c.id,
							CloudId:  c.cloudID,
							FolderId: c.folderID,
							Dst:      dst,
						},
					)
				},
			)
		},
	}

	c.addFlags(cmd, "ID of image to export; required")
	return cmd
}
