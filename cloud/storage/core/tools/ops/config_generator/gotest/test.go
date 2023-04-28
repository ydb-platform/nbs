package gotest

import (
	"bytes"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"a.yandex-team.ru/library/go/test/canon"
	"a.yandex-team.ru/library/go/test/yatest"

	"github.com/stretchr/testify/require"
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
			b, err := ioutil.ReadFile(path)
			require.NoError(t, err)
			buf.WriteString(strings.ReplaceAll(path, tmpDir, "") + ":\n")
			buf.Write(b)
		}
		return nil
	})
	require.NoError(t, err)

	canonFile := filepath.Join(tmpDir, "test.golden")
	err = ioutil.WriteFile(canonFile, buf.Bytes(), 0644)
	require.NoError(t, err)

	canon.SaveFile(t, canonFile, canon.WithLocal(true))
}
