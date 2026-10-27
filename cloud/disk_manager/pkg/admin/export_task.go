package admin

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	client_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/client/config"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

// API export options for testing the persistent DM task pipeline.
type exportTask struct {
	clientConfig       *client_config.ClientConfig
	kind               exportResourceKind
	resourceID         string
	cloudID            string
	folderID           string
	computeTaskID      string
	idempotencyKey     string
	destination        disk_manager.ExportDestination
	decryptionInfoFile string
	verbose            bool
	wait               bool
}

func (c *exportTask) request() (proto.Message, error) {

	if c.resourceID == "" || c.folderID == "" || c.computeTaskID == "" ||
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

	info, err := readExportDecryptionInfo(c.decryptionInfoFile)
	if err != nil {
		return nil, err
	}

	if c.kind == exportImageKind {
		return &disk_manager.ExportImageRequest{
			ImageId:        c.resourceID,
			CloudId:        c.cloudID,
			FolderId:       c.folderID,
			Dst:            &c.destination,
			DecryptionInfo: info,
		}, nil
	}

	return &disk_manager.ExportSnapshotRequest{
		SnapshotId:     c.resourceID,
		CloudId:        c.cloudID,
		FolderId:       c.folderID,
		Dst:            &c.destination,
		DecryptionInfo: info,
	}, nil
}

func readExportDecryptionInfo(path string) (*disk_manager.DecryptionInfo, error) {
	if path == "" {
		return nil, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading decryption info: %w", err)
	}

	var info struct {
		KekID        string `json:"kek_id"`
		KekVersion   string `json:"kek_version"`
		EncryptedDEK string `json:"encrypted_dek"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&info); err != nil {
		// Parser errors can contain the supplied decryption metadata.
		return nil, fmt.Errorf("invalid decryption info JSON")
	}

	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("invalid decryption info JSON")
	}

	if info.KekID == "" || info.EncryptedDEK == "" {
		return nil, fmt.Errorf(
			"decryption info requires kek_id and encrypted_dek",
		)
	}

	if _, err := base64.StdEncoding.DecodeString(info.EncryptedDEK); err != nil {
		return nil, fmt.Errorf("encrypted_dek must be valid Base64")
	}

	return &disk_manager.DecryptionInfo{
		KekId:        info.KekID,
		KekVersion:   info.KekVersion,
		EncryptedDek: []byte(info.EncryptedDEK),
	}, nil
}

func (c *exportTask) run(
	request proto.Message,
	output io.Writer,
) error {

	ctx, stop := signal.NotifyContext(
		newContext(c.clientConfig),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()
	if c.verbose {
		ctx = logging.SetLogger(ctx, newExportLogger(logging.DebugLevel))
	}

	client, err := internal_client.NewClient(ctx, c.clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	idempotencyKey := c.idempotencyKey
	if idempotencyKey == "" {
		prefix := "disk-manager-admin-export_"
		if c.kind == exportImageKind {
			prefix = "disk-manager-admin-image-export_"
		}
		idempotencyKey = prefix + c.computeTaskID
	}

	requestContext := headers.Replace(
		getRequestContext(ctx),
		map[string]string{
			"x-task-id":       c.computeTaskID,
			"idempotency-key": idempotencyKey,
		},
	)
	var operation *disk_manager.Operation
	switch req := request.(type) {
	case *disk_manager.ExportSnapshotRequest:
		operation, err = client.ExportSnapshot(requestContext, req)
	case *disk_manager.ExportImageRequest:
		operation, err = client.ExportImage(requestContext, req)
	default:
		return fmt.Errorf("unsupported export request type")
	}
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

func addExportTaskFlags(cmd *cobra.Command, c *exportTask) {
	flags := cmd.Flags()
	flags.StringVar(
		&c.folderID, "folder-id", "", "Resource folder ID; required with --async",
	)
	flags.StringVar(&c.cloudID, "cloud-id", "", "Resource cloud ID; --async only")
	flags.StringVar(
		&c.computeTaskID,
		"compute-task-id",
		"",
		"Compute export task ID sent in x-task-id; required with --async",
	)
	flags.StringVar(
		&c.idempotencyKey,
		"idempotency-key",
		"",
		"--async request key; defaults to a resource-specific prefix "+
			"and Compute task ID",
	)
	flags.StringVar(
		&c.decryptionInfoFile,
		"decryption-info-file",
		"",
		"--async DecryptionInfo JSON; encrypted_dek must be Base64; optional",
	)
	flags.BoolVar(
		&c.wait,
		"wait",
		false,
		"With --async, wait for completion and print bucket/manifest_key "+
			"as JSON",
	)
}

func validateExportModeOptions(cmd *cobra.Command, async bool) error {
	incompatible := []string{
		"folder-id", "cloud-id", "compute-task-id", "idempotency-key",
		"decryption-info-file", "wait",
	}
	if async {
		incompatible = []string{
			"source", "src-disk-id", "read-workers", "partition", "partition-count",
			"s3-progress-file", "s3-credentials-file", "s3-compression", "s3-part-size",
			"s3-request-timeout", "s3-upload-workers", "s3-max-retries",
		}
	}
	for _, name := range incompatible {
		if cmd.Flags().Changed(name) {
			if async {
				return fmt.Errorf(
					"--%s cannot be used with --async; source and "+
						"transfer settings come from the server's ExportConfig "+
						"and S3 credentials from Compute",
					name,
				)
			}
			return fmt.Errorf("--%s requires --async", name)
		}
	}
	return nil
}
