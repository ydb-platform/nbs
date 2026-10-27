package admin

import (
	"bytes"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/exporter"
)

func TestExportUsageOnlyForInvalidOptions(t *testing.T) {
	for _, kind := range []exportResourceKind{
		exportSnapshotKind,
		exportImageKind,
	} {
		for _, tc := range []struct {
			name      string
			extraArgs []string
			usage     bool
			err       string
		}{
			{
				name:  "runtime failure",
				err:   "loading destination S3 credentials",
				usage: false,
			},
			{
				name:      "invalid options",
				extraArgs: []string{"--s3-upload-workers=0"},
				err:       "S3 upload worker count",
				usage:     true,
			},
		} {
			t.Run(
				string(kind)+"/"+tc.name,
				func(t *testing.T) {
					cmd := newExportCmd(nil, &config.ServerConfig{}, kind, "", "")
					var output bytes.Buffer
					cmd.SetOut(&output)
					cmd.SetErr(&output)
					args := []string{
						"--id=snapshot",
						"--s3-endpoint=http://127.0.0.1:1234",
						"--s3-bucket=destination",
						"--s3-credentials-file=" +
							filepath.Join(t.TempDir(), "missing.json"),
					}
					cmd.SetArgs(append(args, tc.extraArgs...))
					require.ErrorContains(t, cmd.Execute(), tc.err)
					if tc.usage {
						require.Contains(t, output.String(), "Usage:")
					} else {
						require.NotContains(t, output.String(), "Usage:")
					}
				},
			)
		}
	}
}

func TestExportOptionValidation(t *testing.T) {
	for _, kind := range []exportResourceKind{
		exportSnapshotKind,
		exportImageKind,
	} {
		t.Run(
			string(kind),
			func(t *testing.T) {
				testExportOptionValidation(t, kind)
			},
		)
	}
}

func testExportOptionValidation(
	t *testing.T,
	kind exportResourceKind,
) {

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
		{"checkpoint requires S3", []string{"--s3-progress-file=progress.json"}, false, false},
		{"checkpoint", append(append([]string{}, required...), "--s3-progress-file=progress.json"), true, true},
		{"compression selects S3", []string{"--s3-compression=lz4"}, false, false},
		{"none selects S3", []string{"--s3-compression=none"}, false, false},
		{
			"explicit none",
			append(append([]string{}, required...), "--s3-compression=none"),
			true, true,
		},
		{
			"invalid compression",
			append(append([]string{}, required...), "--s3-compression=gzip"),
			false, false,
		},
		{
			"empty compression",
			append(append([]string{}, required...), "--s3-compression="),
			false, false,
		},
		{
			"upload workers zero",
			append(append([]string{}, required...), "--s3-upload-workers=0"),
			false,
			false,
		},
		{
			"upload workers too many",
			append(append([]string{}, required...), "--s3-upload-workers=65"),
			false,
			false,
		},
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
			append(
				append([]string{}, required...),
				"--s3-request-timeout=0s",
			),
			false,
			false,
		},

		{
			"timeout negative",
			append(
				append([]string{}, required...),
				"--s3-request-timeout=-1s",
			),
			false,
			false,
		},

		{
			"unlimited retries",
			append(append([]string{}, required...), "--s3-max-retries=-1"),
			true,
			true,
		},

		{
			"invalid negative retries",
			append(append([]string{}, required...), "--s3-max-retries=-2"),
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
				"--s3-endpoint=https://id:secret@127.0.0.1",
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
				args := []string{"--id=resource"}
				for _, arg := range tc.args {
					if kind == exportImageKind && arg == "--src-disk-id=disk" {
						continue
					}

					args = append(args, arg)
				}

				cmd := newExportCmd(
					nil,
					&config.ServerConfig{},
					kind,
					"",
					"",
				)
				require.NoError(
					t,
					cmd.ParseFlags(args),
				)
				c := &exportResource{
					kind:             kind,
					resourceID:       "resource",
					partition:        1,
					partitionCount:   1,
					readWorkerCount:  exporter.DefaultStreamReadWorkerCount,
					s3PartSize:       "256MiB",
					s3RequestTimeout: 5 * time.Minute,
					s3MaxRetries:     -1,
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
				c.s3Compression, _ = cmd.Flags().GetString("s3-compression")
				c.s3RequestTimeout, _ = cmd.Flags().GetDuration(
					"s3-request-timeout",
				)
				c.s3MaxRetries, _ = cmd.Flags().GetInt("s3-max-retries")
				c.s3UploadWorkers, _ = cmd.Flags().GetInt("s3-upload-workers")
				err := c.validateExportOptions(cmd)
				if tc.valid {
					require.NoError(t, err)
					require.Equal(t, tc.s3, c.s3Mode)
					if tc.name == "stdout" || tc.name == "s3 defaults" {
						require.Equal(t, "lz4", c.s3Compression)
						require.Equal(t, -1, c.s3MaxRetries)
					}

					if tc.name == "explicit none" {
						require.Equal(t, "none", c.s3Compression)
					}

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
