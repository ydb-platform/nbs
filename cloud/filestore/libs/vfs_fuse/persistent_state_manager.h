#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/folder/path.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

// Holds the advisory lock on an acquired state file. The guard is owned by
// the loop that has acquired the file, so the lock lives as long as the loop
// does: destroying the guard releases the lock keeping the file on disk, so
// that a future session can restore the state, e.g. when the loop is
// suspended or dropped because its start has failed. DeleteStateFile() is
// for the case when the state is not needed anymore, i.e. when the session
// is destroyed.
class TAcquireStateFileGuard
{
public:
    // Implementation detail of the persistent state manager.
    struct TImpl;

private:
    THolder<TImpl> Impl;

public:
    TAcquireStateFileGuard();
    explicit TAcquireStateFileGuard(THolder<TImpl> impl);
    TAcquireStateFileGuard(TAcquireStateFileGuard&& other) noexcept;
    TAcquireStateFileGuard& operator=(TAcquireStateFileGuard&& other) noexcept;
    ~TAcquireStateFileGuard();

    // Whether a state file is held.
    explicit operator bool() const;

    // Path to the held state file. Aborts if the guard holds none.
    const TFsPath& GetFilePath() const;

    // Releases the lock and removes the state file, and the session directory
    // too once it is empty. Leaves the guard holding nothing; no-op if it
    // holds nothing already.
    NProto::TError DeleteStateFile();
};

////////////////////////////////////////////////////////////////////////////////

// Manages the local per-session state files of the FUSE driver components
// (HandleOpsQueue, WriteBackCache and DirectoryHandleStorage): their
// creation and advisory locking. The acquired files are handed out as guards
// which take care of the locks and of the cleanup.
//
// A single instance is shared by all filesystem loops, so it serves any
// number of filesystems and sessions at the same time.
struct IPersistentStateManager
{
    virtual ~IPersistentStateManager() = default;

    // HandleOpsQueue

    // Returns true iff the component is configured and the state file of the
    // given session is present on disk.
    virtual bool HasHandleOpsQueueState(
        const TString& fileSystemId,
        const TString& sessionId) const = 0;
    // If the corresponding state file exists, acquires the advisory lock and
    // returns the file, otherwise creates the file first.
    virtual TResultOrError<TAcquireStateFileGuard>
    AcquireHandleOpsQueueStateFile(
        const TString& fileSystemId,
        const TString& sessionId) = 0;

    // WriteBackCache

    virtual bool HasWriteBackCacheState(
        const TString& fileSystemId,
        const TString& sessionId) const = 0;
    // If the corresponding state file exists, acquires the advisory lock and
    // returns the file, otherwise creates the file first.
    virtual TResultOrError<TAcquireStateFileGuard>
    AcquireWriteBackCacheStateFile(
        const TString& fileSystemId,
        const TString& sessionId) = 0;

    // DirectoryHandleStorage

    virtual bool HasDirectoryHandleStorageState(
        const TString& fileSystemId,
        const TString& sessionId) const = 0;
    // If the corresponding state file exists, acquires the advisory lock and
    // returns the file, otherwise creates the file first.
    virtual TResultOrError<TAcquireStateFileGuard>
    AcquireDirectoryHandleStorageStateFile(
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
// false and Acquire*StateFile() fails. Suitable for the cases where no state
// files are used at all.
IPersistentStateManagerPtr CreatePersistentStateManagerStub();

}   // namespace NCloud::NFileStore::NFuse
