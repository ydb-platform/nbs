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

func TestGenerator(t *testing.T) {
	binary, err := yatest.BinaryPath(
		"cloud/storage/core/tools/ops/config_generator/config_generator")
	require.NoError(t, err)

	testServicePath := yatest.SourcePath(
		"cloud/storage/core/tools/ops/config_generator/gotest/test_service")

	tmpDir, err := ioutil.TempDir(os.TempDir(), "canon_")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	cmd := exec.Command(binary, "--arcadia-root-path", tmpDir, "--service-path", testServicePath)
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

	canonFile := filepath.Join(tmpDir, "test.golden")
	err = ioutil.WriteFile(canonFile, buf.Bytes(), 0644)
	require.NoError(t, err)

	canon.SaveFile(t, canonFile, canon.WithLocal(true))
}
