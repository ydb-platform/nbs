package admin

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	client_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/client/config"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

// Temporary API export command for testing the persistent DM task pipeline.
type exportSnapshotTask struct {
	clientConfig       *client_config.ClientConfig
	snapshotID         string
	cloudID            string
	folderID           string
	computeTaskID      string
	idempotencyKey     string
	destination        disk_manager.ExportDestination
	decryptionInfoFile string
	wait               bool
}

func (c *exportSnapshotTask) request() (
	*disk_manager.ExportSnapshotRequest,
	error,
) {

	if c.snapshotID == "" || c.folderID == "" || c.computeTaskID == "" ||
		c.destination.Bucket == "" {

		return nil, fmt.Errorf(
			"--id, --folder-id, --compute-task-id and " +
				"--s3-bucket must be nonempty",
		)
	}

	endpoint, err := url.Parse(c.destination.Endpoint)
	if err != nil || endpoint.Hostname() == "" ||
		(endpoint.Scheme != "http" && endpoint.Scheme != "https") ||
		endpoint.User != nil || endpoint.RawQuery != "" ||
		endpoint.Fragment != "" {

		return nil, fmt.Errorf(
			"--s3-endpoint must be an HTTP(S) URL without " +
				"credentials, query or fragment",
		)
	}

	if strings.ContainsAny(c.destination.Bucket, "/\\\r\n") {
		return nil, fmt.Errorf("invalid --s3-bucket")
	}

	request := &disk_manager.ExportSnapshotRequest{
		SnapshotId: c.snapshotID,
		CloudId:    c.cloudID,
		FolderId:   c.folderID,
		Dst:        &c.destination,
	}
	if c.decryptionInfoFile == "" {
		return request, nil
	}

	data, err := os.ReadFile(c.decryptionInfoFile)
	if err != nil {
		return nil, fmt.Errorf("reading decryption info: %w", err)
	}

	info := &disk_manager.DecryptionInfo{}
	if err := protojson.Unmarshal(data, info); err != nil {
		// Parser errors can contain the supplied decryption metadata.
		return nil, fmt.Errorf("invalid decryption info JSON")
	}

	if info.KekId == "" || len(info.EncryptedDek) == 0 {
		return nil, fmt.Errorf(
			"decryption info requires kek_id and encrypted_dek",
		)
	}

	request.DecryptionInfo = info
	return request, nil
}

func (c *exportSnapshotTask) run(
	request *disk_manager.ExportSnapshotRequest,
	output io.Writer,
) error {

	ctx, stop := signal.NotifyContext(
		newContext(c.clientConfig),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	client, err := internal_client.NewClient(ctx, c.clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	idempotencyKey := c.idempotencyKey
	if idempotencyKey == "" {
		idempotencyKey = "disk-manager-admin-export_" + c.computeTaskID
	}

	requestContext := headers.Replace(
		getRequestContext(ctx),
		map[string]string{
			"x-operation-id":  c.computeTaskID,
			"idempotency-key": idempotencyKey,
		},
	)
	operation, err := client.ExportSnapshot(requestContext, request)
	if err != nil {
		return err
	}

	if _, err := fmt.Fprintf(output, "Operation: %v\n", operation.Id); err != nil {
		return err
	}

	if !c.wait {
		return nil
	}

	response := &disk_manager.ExportResponse{}
	err = internal_client.WaitResponse(ctx, client, operation.Id, response)
	if err != nil {
		return err
	}

	return json.NewEncoder(output).Encode(response)
}

func newExportSnapshotTaskCmd(
	clientConfig *client_config.ClientConfig,
) *cobra.Command {

	c := &exportSnapshotTask{clientConfig: clientConfig}
	cmd := &cobra.Command{
		Use:   "export-task [snapshot-id]",
		Short: "Start a persistent snapshot export through the DM API (testing)",
		Long: "Temporary command for testing SnapshotService.Export. " +
			"The DM task obtains S3 credentials from Compute and checkpoints " +
			"completed parts. Requires a client config; transfer parameters " +
			"and source selection come from the server's ExportConfig.",
		Args: cobra.MaximumNArgs(1),
		Annotations: map[string]string{
			skipServerConfigParsingAnnotation: "true",
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			request, err := c.request()
			if err != nil {
				return err
			}

			// RPC and transfer failures are not command-line usage errors.
			cmd.SilenceUsage = true
			return c.run(request, cmd.OutOrStdout())
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&c.snapshotID, "id", "", "Snapshot ID; required")
	flags.StringVar(&c.folderID, "folder-id", "", "Snapshot folder ID; required")
	flags.StringVar(&c.cloudID, "cloud-id", "", "Snapshot cloud ID; optional")
	flags.StringVar(
		&c.computeTaskID,
		"compute-task-id",
		"",
		"Compute export task ID sent in x-operation-id; required",
	)
	flags.StringVar(
		&c.idempotencyKey,
		"idempotency-key",
		"",
		"Request key; defaults to disk-manager-admin-export_<compute-task-id>",
	)
	flags.StringVar(
		&c.destination.Endpoint,
		"s3-endpoint",
		"",
		"Destination S3 HTTP(S) endpoint; required",
	)
	flags.StringVar(
		&c.destination.Bucket,
		"s3-bucket",
		"",
		"Destination S3 bucket; required",
	)
	flags.StringVar(&c.destination.Prefix, "s3-prefix", "", "Object key prefix")
	flags.StringVar(&c.destination.Region, "s3-region", "", "S3 signing region")
	flags.StringVar(
		&c.decryptionInfoFile,
		"decryption-info-file",
		"",
		"DecryptionInfo JSON; encrypted_dek must be Base64; optional",
	)
	flags.BoolVar(
		&c.wait,
		"wait",
		false,
		"Wait for completion and print bucket/manifest_key as JSON",
	)
	for _, name := range []string{
		"id", "folder-id", "compute-task-id", "s3-endpoint", "s3-bucket",
	} {
		if err := cmd.MarkFlagRequired(name); err != nil {
			log.Fatalf("Error setting flag %v as required: %v", name, err)
		}
	}

	return cmd
}
