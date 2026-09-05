#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/folder/path.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/system/file_lock.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

// Encapsulates the local per-session state files of the FUSE driver
// components (HandleOpsQueue, WriteBackCache and DirectoryHandleStorage):
// the on-disk layout (<basePath>/<fileSystemId>/<sessionId>/<fileName>),
// file creation, advisory locking and cleanup.
//
// A single instance is shared by all filesystem loops, so it holds state files
// of any number of filesystems and sessions at the same time.
class TPersistentStateManager
{
public:
    struct TAcquireStateFileResult
    {
        NProto::TError Error;
        // Path to the locked state file. Valid iff there is no error.
        TFsPath FilePath;
    };

    TPersistentStateManager(
        TString handleOpsQueueBasePath,
        TString writeBackCacheBasePath,
        TString directoryHandlesStorageBasePath);

    // HandleOpsQueue

    // Returns true iff the component is configured (base path is set) and
    // the state file of the given session is present on disk.
    bool HasHandleOpsQueueState(
        const TString& fileSystemId,
        const TString& sessionId) const;
    // If the corresponding state file exists, acquires the advisory lock and
    // returns the file, otherwise creates the file first.
    TAcquireStateFileResult AcquireHandleOpsQueueStateFile(
        const TString& fileSystemId,
        const TString& sessionId);
    NProto::TError DeleteHandleOpsQueueStateFile(
        const TString& fileSystemId,
        const TString& sessionId);

    // WriteBackCache

    bool HasWriteBackCacheState(
        const TString& fileSystemId,
        const TString& sessionId) const;
    // If the corresponding state file exists, acquires the advisory lock and
    // returns the file, otherwise creates the file first.
    TAcquireStateFileResult AcquireWriteBackCacheStateFile(
        const TString& fileSystemId,
        const TString& sessionId);
    NProto::TError DeleteWriteBackCacheStateFile(
        const TString& fileSystemId,
        const TString& sessionId);

    // DirectoryHandleStorage

    // If the corresponding state file exists, acquires the advisory lock and
    // returns the file, otherwise creates the file first.
    TAcquireStateFileResult AcquireDirectoryHandleStorageStateFile(
        const TString& fileSystemId,
        const TString& sessionId);
    NProto::TError DeleteDirectoryHandleStorageStateFile(
        const TString& fileSystemId,
        const TString& sessionId);

private:
    struct TComponentConfig
    {
        const TString BasePath;
        // Points to a static string.
        const TStringBuf FileName;

        TComponentConfig(TString basePath, TStringBuf fileName)
            : BasePath(std::move(basePath))
            , FileName(fileName)
        {}
    };

    // Locked state files held in one session directory, keyed by file name.
    using TSessionDirLocks = THashMap<TString, THolder<TFileLock>>;

    TFsPath GetSessionDir(
        const TComponentConfig& component,
        const TString& fileSystemId,
        const TString& sessionId) const;

    bool HasState(
        const TComponentConfig& component,
        const TString& fileSystemId,
        const TString& sessionId) const;

    TAcquireStateFileResult AcquireStateFile(
        const TComponentConfig& component,
        const TString& fileSystemId,
        const TString& sessionId);

    NProto::TError DeleteStateFile(
        const TComponentConfig& component,
        const TString& fileSystemId,
        const TString& sessionId);

private:
    // Guards SessionDirs and the filesystem operations on the state files.
    mutable TMutex Mutex;

    // Session directories that hold at least one locked state file, keyed by
    // path. A directory may be shared by several components, it is removed
    // once it holds no more state files.
    THashMap<TString, TSessionDirLocks> SessionDirs;

    const TComponentConfig HandleOpsQueue;
    const TComponentConfig WriteBackCache;
    const TComponentConfig DirectoryHandleStorage;
};

////////////////////////////////////////////////////////////////////////////////

// Creates a persistent state with no components configured: Has*State()
// returns false, Acquire*StateFile() fails and Delete*StateFile() is a no-op.
// Suitable for the cases where no state files are used at all.
TPersistentStateManagerPtr CreatePersistentStateManagerStub();

}   // namespace NCloud::NFileStore::NFuse
