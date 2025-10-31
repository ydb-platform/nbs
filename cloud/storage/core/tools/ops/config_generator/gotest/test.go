package gotest

import (
	"bytes"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/library/go/test/canon"
	"github.com/ydb-platform/nbs/library/go/test/yatest"
)

type testcase struct {
	name        string
	servicePath string
	specFile    string
}

func TestGenerator(t *testing.T) {
	testCases := []testcase{
		{
			name:        "test",
			servicePath: "cloud/storage/core/tools/ops/config_generator/gotest/test_service",
		},
		{
			name:        "test_with_service_name",
			servicePath: "cloud/storage/core/tools/ops/config_generator/gotest/test_service",
			specFile:    "spec_service_name.yaml",
		},
	}

	binary, err := yatest.BinaryPath(
		"cloud/storage/core/tools/ops/config_generator/config_generator")
	require.NoError(t, err)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			testServicePath := yatest.SourcePath(
				tc.servicePath)

			tmpDir, err := ioutil.TempDir(os.TempDir(), "canon_")
			require.NoError(t, err)
			defer func() { _ = os.RemoveAll(tmpDir) }()

			args := []string{
				"--arcadia-root-path", tmpDir,
				"--service-path", testServicePath,
			}
			if tc.specFile != "" {
				args = append(args, "--spec-file", tc.specFile)
			}

			cmd := exec.Command(binary, args...)
			cmd.Stderr = os.Stderr
			_, err = cmd.Output()
			if err != nil {
				println(err)
			}
			require.NoError(t, err)

			buf := bytes.Buffer{}
			err = filepath.Walk(tmpDir, func(path string, info os.FileInfo, err error) error {
				require.NoError(t, err)
				if !info.IsDir() {
					b, err := os.ReadFile(path)
					require.NoError(t, err)
					buf.WriteString(strings.ReplaceAll(path, tmpDir, "") + ":\n")
					buf.Write(b)
					buf.WriteString("\n")
				}
				return nil
			})
			require.NoError(t, err)

			canonFile := filepath.Join(tmpDir, tc.name+".golden")
			err = ioutil.WriteFile(canonFile, buf.Bytes(), 0644)
			require.NoError(t, err)

			canon.SaveFile(t, canonFile, canon.WithLocal(true))
		})
	}
}
