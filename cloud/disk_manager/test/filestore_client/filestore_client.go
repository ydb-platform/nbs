package filestore_client

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/library/go/test/yatest"
)

////////////////////////////////////////////////////////////////////////////////

type FilestoreClient struct {
	binaryPath string
	port       string
	t          *testing.T
}

func NewFilestoreClient(t *testing.T) *FilestoreClient {
	binaryPath, err := yatest.BinaryPath(
		"cloud/filestore/apps/client/filestore-client",
	)
	require.NoError(t, err)

	port := os.Getenv("DISK_MANAGER_RECIPE_NFS_PORT")
	require.NotEmpty(t, port, "DISK_MANAGER_RECIPE_NFS_PORT env var is not set")

	return &FilestoreClient{
		binaryPath: binaryPath,
		port:       port,
		t:          t,
	}
}

type FilestoreEntry struct {
	DirPath string
	Name    string
}

func (c *FilestoreClient) FindAll(
	ctx context.Context,
	filesystemID string,
) []FilestoreEntry {
	cmd := exec.Command(
		c.binaryPath,
		"find",
		"--server-port", c.port,
		"--filesystem", filesystemID,
		"--depth", "100",
	)

	out, err := cmd.CombinedOutput()
	require.NoError(c.t, err, "filestore-client find failed: %s", string(out))

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	var entries []FilestoreEntry
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Format: <directory full path>\t<name>\t<node id>
		parts := strings.Split(line, "\t")
		if len(parts) < 2 {
			continue
		}

		entries = append(entries, FilestoreEntry{
			DirPath: parts[0],
			Name:    parts[1],
		})

		logging.Debug(
			ctx,
			"filestore-client find entry: filesystem_id=%s dir_path=%s name=%s",
			filesystemID,
			parts[0],
			parts[1],
		)
	}

	slices.SortFunc(entries, func(a, b FilestoreEntry) int {
		if cmp := strings.Compare(a.DirPath, b.DirPath); cmp != 0 {
			return cmp
		}

		return strings.Compare(a.Name, b.Name)
	})

	return entries
}

func (c *FilestoreClient) FindAllPaths(
	ctx context.Context,
	filesystemID string,
) []string {
	entries := c.FindAll(ctx, filesystemID)
	paths := make([]string, len(entries))
	for i, e := range entries {
		require.NotEmpty(c.t, e.DirPath)
		require.NotEmpty(c.t, e.Name)

		paths[i] = fmt.Sprintf("%s%s", e.DirPath, e.Name)
		require.NotEmpty(c.t, paths[i])
	}

	slices.Sort(paths)
	return paths
}
