package admin

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/exporter"
)

func TestSnapshotExportOptionValidation(t *testing.T) {
	required := []string{
		"--s3-endpoint=http://localhost:1234",
		"--s3-bucket=destination",
		"--s3-credentials-file=credentials.json",
	}

	for _, tc := range []struct {
		name  string
		args  []string
		valid bool
		s3    bool
	}{
		{"stdout", nil, true, false},
		{
			"stdout partition",
			[]string{"--partition=2", "--partition-count=3"},
			true,
			false,
		},

		{"s3 defaults", required, true, true},
		{
			"backup s3",
			append(
				append([]string{}, required...),
				"--source=backup",
				"--src-disk-id=disk",
			),
			true,
			true,
		},

		{
			"backup s3 partition",
			append(
				append([]string{}, required...),
				"--source=backup",
				"--src-disk-id=disk",
				"--partition=1",
			),
			false,
			false,
		},

		{
			"s3 region token file",
			append(
				append([]string{}, required...),
				"--s3-region=custom",
				"--s3-part-size=4MiB",
				"--s3-max-retries=0",
			),
			true,
			true,
		},

		{"endpoint only", required[:1], false, false},
		{"optional only", []string{"--s3-prefix=parent"}, false, false},
		{"explicit empty optional", []string{"--s3-region="}, false, false},
		{
			"explicit default part size",
			[]string{"--s3-part-size=256MiB"},
			false,
			false,
		},

		{"workers zero", []string{"--read-workers=0"}, false, false},
		{"workers negative", []string{"--read-workers=-1"}, false, false},
		{"partition invalid", []string{"--partition=0"}, false, false},
		{
			"explicit partition default",
			append(append([]string{}, required...), "--partition=1"),
			false,
			false,
		},

		{
			"explicit partition count default",
			append(append([]string{}, required...), "--partition-count=1"),
			false,
			false,
		},

		{
			"timeout zero",
			append(append([]string{}, required...), "--s3-request-timeout=0s"),
			false,
			false,
		},

		{
			"timeout negative",
			append(append([]string{}, required...), "--s3-request-timeout=-1s"),
			false,
			false,
		},

		{
			"retries negative",
			append(append([]string{}, required...), "--s3-max-retries=-1"),
			false,
			false,
		},

		{
			"invalid part",
			append(append([]string{}, required...), "--s3-part-size=1MiB"),
			false,
			false,
		},

		{
			"oversized part",
			append(append([]string{}, required...), "--s3-part-size=2GiB"),
			false,
			false,
		},

		{
			"invalid endpoint",
			[]string{
				"--s3-endpoint=localhost",
				"--s3-bucket=destination",
				"--s3-credentials-file=c",
			},

			false,
			false,
		},

		{
			"endpoint credentials",
			[]string{
				"--s3-endpoint=https://id:secret@example.com",
				"--s3-bucket=destination",
				"--s3-credentials-file=c",
			},

			false,
			false,
		},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				cmd := newExportSnapshotCmd(&config.ServerConfig{})
				require.NoError(
					t,
					cmd.ParseFlags(
						append([]string{"--id=snapshot"}, tc.args...),
					),
				)
				c := &exportSnapshot{
					partition:        1,
					partitionCount:   1,
					readWorkerCount:  exporter.DefaultStreamReadWorkerCount,
					s3PartSize:       "256MiB",
					s3RequestTimeout: 5 * time.Minute,
					s3MaxRetries:     3,
				}

				c.source, _ = cmd.Flags().GetString("source")
				c.srcDiskID, _ = cmd.Flags().GetString("src-disk-id")
				c.partition, _ = cmd.Flags().GetUint32("partition")
				c.partitionCount, _ = cmd.Flags().GetUint32("partition-count")
				c.readWorkerCount, _ = cmd.Flags().GetInt("read-workers")
				c.s3Endpoint, _ = cmd.Flags().GetString("s3-endpoint")
				c.s3Bucket, _ = cmd.Flags().GetString("s3-bucket")
				c.s3CredentialsFile, _ = cmd.Flags().GetString(
					"s3-credentials-file",
				)
				c.s3PartSize, _ = cmd.Flags().GetString("s3-part-size")
				c.s3RequestTimeout, _ = cmd.Flags().GetDuration(
					"s3-request-timeout",
				)
				c.s3MaxRetries, _ = cmd.Flags().GetInt("s3-max-retries")
				err := c.validateExportOptions(cmd)
				if tc.valid {
					require.NoError(t, err)
					require.Equal(t, tc.s3, c.s3Mode)
					if tc.s3 {
						require.Positive(t, c.parsedS3PartSize)
					}
				} else {
					require.Error(t, err)
				}
			},
		)
	}
}

func TestSnapshotExportPartSizeParsing(t *testing.T) {
	for _, value := range []string{
		"4MiB",
		"4194304",
		"4194304B",
		"4096KiB",
		"256MiB",
		"1GiB",
	} {
		_, err := parseS3PartSize(value)
		require.NoError(t, err, value)
	}

	for _, value := range []string{
		"",
		"0",
		"-4MiB",
		"4MB",
		"4.0MiB",
		"1MiB",
		"2GiB",
		"18446744073709551615GiB",
		"18446744073709551616",
	} {
		_, err := parseS3PartSize(value)
		require.Error(t, err, value)
	}

	require.Error(t, exporter.ValidateReadWorkers(math.MaxInt))
}
