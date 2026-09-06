#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/folder/path.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

// Manages the local per-session state files of the FUSE driver components
// (HandleOpsQueue, WriteBackCache and DirectoryHandleStorage): their
// creation, advisory locking and cleanup.
//
// A single instance is shared by all filesystem loops, so it holds state files
// of any number of filesystems and sessions at the same time.
struct IPersistentStateManager
{
    struct TAcquireStateFileResult
    {
        NProto::TError Error;
        // Path to the locked state file. Valid iff there is no error.
        TFsPath FilePath;
    };

    virtual ~IPersistentStateManager() = default;

    // HandleOpsQueue

    // Returns true iff the component is configured and the state file of the
    // given session is present on disk.
    virtual bool HasHandleOpsQueueState(
        const TString& fileSystemId,
        const TString& sessionId) const = 0;
    // If the corresponding state file exists, acquires the advisory lock and
    // returns the file, otherwise creates the file first.
    virtual TAcquireStateFileResult AcquireHandleOpsQueueStateFile(
        const TString& fileSystemId,
        const TString& sessionId) = 0;
    virtual NProto::TError DeleteHandleOpsQueueStateFile(
        const TString& fileSystemId,
        const TString& sessionId) = 0;

    // WriteBackCache

    virtual bool HasWriteBackCacheState(
        const TString& fileSystemId,
        const TString& sessionId) const = 0;
    // If the corresponding state file exists, acquires the advisory lock and
    // returns the file, otherwise creates the file first.
    virtual TAcquireStateFileResult AcquireWriteBackCacheStateFile(
        const TString& fileSystemId,
        const TString& sessionId) = 0;
    virtual NProto::TError DeleteWriteBackCacheStateFile(
        const TString& fileSystemId,
        const TString& sessionId) = 0;

    // DirectoryHandleStorage

    // If the corresponding state file exists, acquires the advisory lock and
    // returns the file, otherwise creates the file first.
    virtual TAcquireStateFileResult AcquireDirectoryHandleStorageStateFile(
        const TString& fileSystemId,
        const TString& sessionId) = 0;
    virtual NProto::TError DeleteDirectoryHandleStorageStateFile(
        const TString& fileSystemId,
        const TString& sessionId) = 0;

    // All components

    // Releases the locks of all the state files of the session held by this
    // manager, keeping the files on disk so that a future session can
    // restore them.
    virtual void ReleaseStateFiles(
        const TString& fileSystemId,
        const TString& sessionId) = 0;
};

////////////////////////////////////////////////////////////////////////////////

// Creates the manager which keeps the state files on disk under the given
// base paths, following the layout <basePath>/<fileSystemId>/<sessionId>/
// <fileName>. A component whose base path is empty is not configured:
// acquiring its state file fails with an error and Has*State() returns
// false for it. Different components may be configured with the same base
// path and thus share a session directory.
IPersistentStateManagerPtr CreatePersistentStateManager(
    TString handleOpsQueueBasePath,
    TString writeBackCacheBasePath,
    TString directoryHandlesStorageBasePath);

// Creates a manager which manages no state files at all: Has*State() returns
// false, Acquire*StateFile() fails and Delete*StateFile() as well as
// ReleaseStateFiles() are no-ops. Suitable for the cases where no state files
// are used at all.
IPersistentStateManagerPtr CreatePersistentStateManagerStub();

}   // namespace NCloud::NFileStore::NFuse
