package listers

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	nfs_client "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

func validateRootPath(rootPath string) error {
	info, err := os.Stat(rootPath)
	if err != nil {
		return errors.NewNonRetriableErrorf(
			"failed to open local filesystem at %s: %w",
			rootPath,
			err,
		)
	}

	if !info.IsDir() {
		return errors.NewNonRetriableErrorf(
			"path %s is not a directory",
			rootPath,
		)
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func cookieToLastFileNumber(cookie string) (int, error) {
	if cookie == "" {
		return 0, nil
	}

	n, err := strconv.Atoi(cookie)
	if err != nil {
		return 0, errors.NewNonRetriableErrorf(
			"invalid cookie %q: %w",
			cookie,
			err,
		)
	}

	return n, nil
}

////////////////////////////////////////////////////////////////////////////////

type localFSLister struct {
	rootPath          string
	maxEntriesPerPage int
	dirsByInode       map[uint64]string
}

type LocalFSOpener struct {
	rootPath          string
	maxEntriesPerPage int
}

func NewLocalFSOpener(rootPath string, maxEntriesPerPage int) *LocalFSOpener {
	return &LocalFSOpener{
		rootPath:          rootPath,
		maxEntriesPerPage: maxEntriesPerPage,
	}
}

func (o *LocalFSOpener) OpenFilesystem(
	ctx context.Context,
	filesystemID string,
	checkpointID string,
) (FilesystemLister, error) {
	rootPath := filepath.Join(o.rootPath, filesystemID)
	if checkpointID != "" {
		rootPath = filepath.Join(rootPath, checkpointID)
	}

	err := validateRootPath(rootPath)
	if err != nil {
		return nil, err
	}

	return &localFSLister{
		rootPath:          rootPath,
		maxEntriesPerPage: o.maxEntriesPerPage,
		dirsByInode: map[uint64]string{
			nfs.RootNodeID: rootPath,
		},
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

func (l *localFSLister) ListNodes(
	ctx context.Context,
	nodeID uint64,
	cookie string,
) ([]nfs.Node, string, error) {
	dirPath, err := l.nodeIDToPath(nodeID)
	if err != nil {
		return nil, "", err
	}

	skip, err := cookieToLastFileNumber(cookie)
	if err != nil {
		return nil, "", err
	}

	dir, err := os.Open(dirPath)
	if err != nil {
		return nil, "", errors.NewRetriableErrorf(
			"failed to open directory %s: %w",
			dirPath,
			err,
		)
	}
	defer dir.Close()

	return l.readPage(dir, dirPath, nodeID, skip)
}

func (l *localFSLister) readPage(
	dir fs.ReadDirFile,
	dirPath string,
	parentID uint64,
	skip int,
) ([]nfs.Node, string, error) {
	// Skip already processed entries.
	if skip > 0 {
		_, err := dir.ReadDir(skip)
		if err != nil {
			return nil, "", errors.NewRetriableErrorf(
				"failed to skip entries in directory %s: %w",
				dirPath,
				err,
			)
		}
	}

	entries, err := dir.ReadDir(l.maxEntriesPerPage + 1)
	if err != nil && err != io.EOF {
		return nil, "", errors.NewRetriableErrorf(
			"failed to read directory %s: %w",
			dirPath,
			err,
		)
	}

	hasMore := len(entries) > l.maxEntriesPerPage
	if hasMore {
		entries = entries[:l.maxEntriesPerPage]
	}

	nodes := make([]nfs.Node, 0, len(entries))
	for _, entry := range entries {
		node, err := l.entryToNode(dirPath, parentID, entry)
		if err != nil {
			return nil, "", err
		}
		nodes = append(nodes, node)
	}

	nextCookie := ""
	if hasMore {
		nextCookie = strconv.Itoa(skip + len(entries))
	}

	return nodes, nextCookie, nil
}

////////////////////////////////////////////////////////////////////////////////

func (l *localFSLister) nodeIDToPath(nodeID uint64) (string, error) {
	dirPath, ok := l.dirsByInode[nodeID]
	if !ok {
		return "", errors.NewNonRetriableErrorf(
			"unknown node ID %d",
			nodeID,
		)
	}

	return dirPath, nil
}

func (l *localFSLister) entryToNode(
	parentPath string,
	parentID uint64,
	entry os.DirEntry,
) (nfs.Node, error) {
	fullPath := filepath.Join(parentPath, entry.Name())

	info, err := entry.Info()
	if err != nil {
		return nfs.Node{}, errors.NewRetriableErrorf(
			"failed to get file info for %s: %w",
			fullPath,
			err,
		)
	}

	node := nfs.Node{
		ParentID: parentID,
		NodeID:   nfs.RootNodeID,
		Name:     entry.Name(),
		Size:     uint64(info.Size()),
		Mode:     uint32(info.Mode().Perm()),
		Type:     getNodeType(info),
	}

	// Get timestamps and ownership from syscall stat.
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		node.NodeID = stat.Ino
		node.Atime = uint64(stat.Atim.Sec)
		node.Mtime = uint64(stat.Mtim.Sec)
		node.Ctime = uint64(stat.Ctim.Sec)
		node.UID = uint64(stat.Uid)
		node.GID = uint64(stat.Gid)

		// Register discovered directories so we can resolve their
		// inode back to a path when the traversal schedules them.
		if info.IsDir() {
			l.dirsByInode[stat.Ino] = fullPath
		}
	}

	// Handle symlinks.
	if info.Mode()&os.ModeSymlink != 0 {
		target, err := os.Readlink(fullPath)
		if err == nil {
			node.LinkTarget = target
		}
	}

	return node, nil
}

////////////////////////////////////////////////////////////////////////////////

func getNodeType(info os.FileInfo) nfs_client.NodeType {
	mode := info.Mode()

	switch {
	case mode.IsDir():
		return nfs_client.NODE_KIND_DIR
	case mode&os.ModeSymlink != 0:
		return nfs_client.NODE_KIND_SYMLINK
	case mode&os.ModeSocket != 0:
		return nfs_client.NODE_KIND_SOCK
	case mode.IsRegular():
		return nfs_client.NODE_KIND_FILE
	default:
		return nfs_client.NODE_KIND_FILE
	}
}

////////////////////////////////////////////////////////////////////////////////

func (l *localFSLister) Close(ctx context.Context) error {
	// No resources to clean up for local filesystem.
	return nil
}
