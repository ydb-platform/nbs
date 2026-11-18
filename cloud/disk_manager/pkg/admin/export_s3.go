package admin

import (
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/exporter"
)

func parseS3PartSize(value string) (uint64, error) {
	var multiplier uint64 = 1
	for _, unit := range []struct {
		suffix     string
		multiplier uint64
	}{
		{"GiB", 1024 * 1024 * 1024},
		{"MiB", 1024 * 1024},
		{"KiB", 1024},
		{"B", 1},
	} {
		if strings.HasSuffix(value, unit.suffix) {
			value = strings.TrimSuffix(value, unit.suffix)
			multiplier = unit.multiplier
			break
		}
	}

	size, err := strconv.ParseUint(value, 10, 64)
	if err != nil || size > math.MaxUint64/multiplier {
		return 0, fmt.Errorf(
			"invalid --s3-part-size; use an integer byte count or " +
				"KiB, MiB, GiB suffix",
		)
	}

	size *= multiplier
	if err := exporter.ValidateS3PartSize(size); err != nil {
		return 0, err
	}

	return size, nil
}

func (c *exportResource) validateExportOptions(cmd *cobra.Command) error {
	if c.resourceID == "" {
		return fmt.Errorf("%v ID is required", c.kind)
	}

	if err := c.validateSourceOptions(cmd); err != nil {
		return err
	}

	if err := exporter.ValidateReadWorkers(c.readWorkerCount); err != nil {
		return err
	}

	if err := exporter.ValidatePartition(
		c.partition,
		c.partitionCount,
	); err != nil {
		return err
	}

	c.s3Mode = false
	cmd.Flags().Visit(
		func(flag *pflag.Flag) {
			if strings.HasPrefix(flag.Name, "s3-") {
				c.s3Mode = true
			}
		},
	)
	if !c.s3Mode {
		return nil
	}

	if c.s3Endpoint == "" || c.s3Bucket == "" || c.s3CredentialsFile == "" {
		return fmt.Errorf(
			"S3 export requires --s3-endpoint, --s3-bucket and " +
				"--s3-credentials-file",
		)
	}

	if cmd.Flags().Changed("partition") ||
		cmd.Flags().Changed("partition-count") {

		return fmt.Errorf(
			"S3 export cannot be combined with explicit " +
				"--partition or --partition-count",
		)
	}

	endpoint, err := url.Parse(c.s3Endpoint)
	if err != nil || endpoint.Hostname() == "" ||
		(endpoint.Scheme != "http" && endpoint.Scheme != "https") ||
		endpoint.User != nil || endpoint.RawQuery != "" ||
		endpoint.Fragment != "" {

		return fmt.Errorf(
			"--s3-endpoint must be an HTTP(S) URL without " +
				"credentials, query or fragment",
		)
	}

	if strings.ContainsAny(c.s3Bucket, "/\\\r\n") {
		return fmt.Errorf("invalid --s3-bucket")
	}

	if c.s3RequestTimeout <= 0 {
		return fmt.Errorf("--s3-request-timeout must be positive")
	}

	if c.s3MaxRetries < -1 {
		return fmt.Errorf("--s3-max-retries must be -1 or nonnegative")
	}

	if err := exporter.ValidateS3UploadWorkers(c.s3UploadWorkers); err != nil {
		return err
	}

	if err := exporter.ValidateS3Compression(c.s3Compression); err != nil {
		return err
	}

	c.parsedS3PartSize, err = parseS3PartSize(c.s3PartSize)
	return err
}
