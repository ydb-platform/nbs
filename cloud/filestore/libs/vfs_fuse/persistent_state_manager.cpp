#include "persistent_state_manager.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/system/error.h>
#include <util/system/file_lock.h>
#include <util/system/fs.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>

namespace NCloud::NFileStore::NFuse {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf HandleOpsQueueFileName = "handle_ops_queue";
constexpr TStringBuf WriteBackCacheFileName = "write_back_cache";
constexpr TStringBuf DirectoryHandleStorageFileName = "directory_handles_storage";

////////////////////////////////////////////////////////////////////////////////

// Keeps the state files on disk under the configured base paths, following
// the layout <basePath>/<fileSystemId>/<sessionId>/<fileName>, and holds the
// advisory locks on the acquired ones.
class TPersistentStateManager final
    : public IPersistentStateManager
{
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

    // Guards SessionDirs and the filesystem operations on the state files.
    mutable TMutex Mutex;

    // Session directories that hold at least one locked state file, keyed by
    // path. A directory may be shared by several components, so only the
    // requested files are ever removed from it, and the directory itself is
    // removed once it is empty. A directory found not empty after the last
    // state file held in it has been deleted contains state nobody tracks
    // (e.g. of a component which is not configured anymore), which is
    // reported as a critical event.
    THashMap<TString, TSessionDirLocks> SessionDirs;

    const TComponentConfig HandleOpsQueue;
    const TComponentConfig WriteBackCache;
    const TComponentConfig DirectoryHandleStorage;

public:
    TPersistentStateManager(
        TString handleOpsQueueBasePath,
        TString writeBackCacheBasePath,
        TString directoryHandlesStorageBasePath);

    // HandleOpsQueue

    bool HasHandleOpsQueueState(
        const TString& fileSystemId,
        const TString& sessionId) const override;
    TAcquireStateFileResult AcquireHandleOpsQueueStateFile(
        const TString& fileSystemId,
        const TString& sessionId) override;
    NProto::TError DeleteHandleOpsQueueStateFile(
        const TString& fileSystemId,
        const TString& sessionId) override;

    // WriteBackCache

    bool HasWriteBackCacheState(
        const TString& fileSystemId,
        const TString& sessionId) const override;
    TAcquireStateFileResult AcquireWriteBackCacheStateFile(
        const TString& fileSystemId,
        const TString& sessionId) override;
    NProto::TError DeleteWriteBackCacheStateFile(
        const TString& fileSystemId,
        const TString& sessionId) override;

    // DirectoryHandleStorage

    TAcquireStateFileResult AcquireDirectoryHandleStorageStateFile(
        const TString& fileSystemId,
        const TString& sessionId) override;
    NProto::TError DeleteDirectoryHandleStorageStateFile(
        const TString& fileSystemId,
        const TString& sessionId) override;

    // All components

    void ReleaseStateFiles(
        const TString& fileSystemId,
        const TString& sessionId) override;

private:
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

    void ReleaseStateFile(
        const TComponentConfig& component,
        const TString& fileSystemId,
        const TString& sessionId);
};

////////////////////////////////////////////////////////////////////////////////

TPersistentStateManager::TPersistentStateManager(
        TString handleOpsQueueBasePath,
        TString writeBackCacheBasePath,
        TString directoryHandlesStorageBasePath)
    : HandleOpsQueue(
          std::move(handleOpsQueueBasePath),
          HandleOpsQueueFileName)
    , WriteBackCache(
          std::move(writeBackCacheBasePath),
          WriteBackCacheFileName)
    , DirectoryHandleStorage(
          std::move(directoryHandlesStorageBasePath),
          DirectoryHandleStorageFileName)
{}

////////////////////////////////////////////////////////////////////////////////
// Generic implementation

TFsPath TPersistentStateManager::GetSessionDir(
    const TComponentConfig& component,
    const TString& fileSystemId,
    const TString& sessionId) const
{
    Y_DEBUG_ABORT_UNLESS(component.BasePath);
    return TFsPath(component.BasePath) / fileSystemId / sessionId;
}

bool TPersistentStateManager::HasState(
    const TComponentConfig& component,
    const TString& fileSystemId,
    const TString& sessionId) const
{
    if (!component.BasePath) {
        return false;
    }

    const auto filePath =
        GetSessionDir(component, fileSystemId, sessionId) / component.FileName;

    TGuard guard(Mutex);
    return filePath.Exists();
}

TPersistentStateManager::TAcquireStateFileResult
TPersistentStateManager::AcquireStateFile(
    const TComponentConfig& component,
    const TString& fileSystemId,
    const TString& sessionId)
{
    if (!component.BasePath) {
        return {
            .Error = MakeError(
                E_INVALID_STATE,
                TStringBuilder() << "Base path for " << component.FileName
                                 << " is not set"),
            .FilePath = {}};
    }

    const auto dir = GetSessionDir(component, fileSystemId, sessionId);
    const TString fileName(component.FileName);
    auto filePath = dir / fileName;

    TGuard guard(Mutex);

    const auto* locks = SessionDirs.FindPtr(dir.GetPath());
    if (locks && locks->contains(fileName)) {
        return {
            .Error = MakeError(
                E_INVALID_STATE,
                TStringBuilder() << "State file " << filePath
                                 << " is already acquired"),
            .FilePath = {}};
    }

    if (!NFs::MakeDirectoryRecursive(dir)) {
        return {
            .Error = MakeError(
                E_FAIL,
                TStringBuilder() << "Failed to create directories, path: "
                                 << dir),
            .FilePath = {}};
    }

    // Touch(), the TFileLock constructor (which opens the file) and
    // TryAcquire() all report failures by throwing.
    THolder<TFileLock> lock;
    try {
        filePath.Touch();

        lock = MakeHolder<TFileLock>(filePath);
        if (!lock->TryAcquire()) {
            return {
                .Error = MakeError(
                    E_INVALID_STATE,
                    TStringBuilder() << "State file " << filePath
                                     << " is locked by another owner"),
                .FilePath = {}};
        }
    } catch (const yexception& e) {
        return {
            .Error = MakeError(
                E_FAIL,
                TStringBuilder() << "Failed to lock file, path: " << filePath
                                 << ", reason: " << e.what()),
            .FilePath = {}};
    }

    SessionDirs[dir.GetPath()].emplace(fileName, std::move(lock));
    return {.Error = {}, .FilePath = std::move(filePath)};
}

NProto::TError TPersistentStateManager::DeleteStateFile(
    const TComponentConfig& component,
    const TString& fileSystemId,
    const TString& sessionId)
{
    if (!component.BasePath) {
        return {};
    }

    const auto dir = GetSessionDir(component, fileSystemId, sessionId);
    const TString fileName(component.FileName);
    const auto filePath = dir / fileName;

    TGuard guard(Mutex);

    // Release the lock if the state file is held. Stop referencing the
    // session directory when this manager holds no more state files in it,
    // no matter whether the removal below succeeds.
    NProto::TError releaseError;
    bool dirHoldsNoLocks = true;
    if (auto dirIt = SessionDirs.find(dir.GetPath());
        dirIt != SessionDirs.end())
    {
        auto& locks = dirIt->second;
        if (auto lockIt = locks.find(fileName); lockIt != locks.end()) {
            auto lock = std::move(lockIt->second);
            locks.erase(lockIt);

            // Release() reports failures by throwing. The lock is dropped
            // either way once |lock| goes out of scope, since closing the
            // file releases it.
            try {
                lock->Release();
            } catch (const yexception& e) {
                releaseError = MakeError(
                    E_FAIL,
                    TStringBuilder() << "Failed to unlock file " << filePath
                                     << ", reason: " << e.what());
            }
        }

        dirHoldsNoLocks = locks.empty();
        if (dirHoldsNoLocks) {
            SessionDirs.erase(dirIt);
        }
    }

    // The state file may be present without being held, e.g. when it was
    // left behind by a previous session and the component is now disabled.
    // Only this very file is removed: the directory may hold state files of
    // other components, whether held by this manager or not.
    if (filePath.Exists() && !NFs::Remove(filePath)) {
        return MakeError(
            E_FAIL,
            TStringBuilder() << "Failed to remove file " << filePath
                             << ", reason: " << LastSystemErrorText());
    }

    // If other state files are still held in the directory it is obviously
    // not empty, so there is nothing to try. Otherwise remove it if empty: a
    // directory found not empty at this point contains state nobody tracks.
    if (dirHoldsNoLocks && dir.Exists() && !NFs::Remove(dir)) {
        const int err = LastSystemError();
        if (err == ENOTEMPTY || err == EEXIST) {
            ReportPersistentStateSessionDirNotEmpty(
                TStringBuilder() << "Session dir " << dir
                                 << " is not empty after the state file "
                                 << fileName << " has been deleted");
        } else {
            return MakeError(
                E_FAIL,
                TStringBuilder() << "Failed to remove dir " << dir
                                 << ", reason: " << LastSystemErrorText(err));
        }
    }

    return releaseError;
}

void TPersistentStateManager::ReleaseStateFile(
    const TComponentConfig& component,
    const TString& fileSystemId,
    const TString& sessionId)
{
    if (!component.BasePath) {
        return;
    }

    const auto dir = GetSessionDir(component, fileSystemId, sessionId);
    const TString fileName(component.FileName);

    TGuard guard(Mutex);

    auto dirIt = SessionDirs.find(dir.GetPath());
    if (dirIt == SessionDirs.end()) {
        return;
    }

    // Destroying the lock closes the file, which releases the lock without
    // any chance of failure, unlike an explicit Release().
    auto& locks = dirIt->second;
    locks.erase(fileName);

    // The session directory is kept together with the state files, only the
    // reference to it is dropped once nothing is held in it anymore.
    if (locks.empty()) {
        SessionDirs.erase(dirIt);
    }
}

////////////////////////////////////////////////////////////////////////////////
// HandleOpsQueue

bool TPersistentStateManager::HasHandleOpsQueueState(
    const TString& fileSystemId,
    const TString& sessionId) const
{
    return HasState(HandleOpsQueue, fileSystemId, sessionId);
}

TPersistentStateManager::TAcquireStateFileResult
TPersistentStateManager::AcquireHandleOpsQueueStateFile(
    const TString& fileSystemId,
    const TString& sessionId)
{
    return AcquireStateFile(HandleOpsQueue, fileSystemId, sessionId);
}

NProto::TError TPersistentStateManager::DeleteHandleOpsQueueStateFile(
    const TString& fileSystemId,
    const TString& sessionId)
{
    return DeleteStateFile(HandleOpsQueue, fileSystemId, sessionId);
}

////////////////////////////////////////////////////////////////////////////////
// WriteBackCache

bool TPersistentStateManager::HasWriteBackCacheState(
    const TString& fileSystemId,
    const TString& sessionId) const
{
    return HasState(WriteBackCache, fileSystemId, sessionId);
}

TPersistentStateManager::TAcquireStateFileResult
TPersistentStateManager::AcquireWriteBackCacheStateFile(
    const TString& fileSystemId,
    const TString& sessionId)
{
    return AcquireStateFile(WriteBackCache, fileSystemId, sessionId);
}

NProto::TError TPersistentStateManager::DeleteWriteBackCacheStateFile(
    const TString& fileSystemId,
    const TString& sessionId)
{
    return DeleteStateFile(WriteBackCache, fileSystemId, sessionId);
}

////////////////////////////////////////////////////////////////////////////////
// DirectoryHandleStorage

TPersistentStateManager::TAcquireStateFileResult
TPersistentStateManager::AcquireDirectoryHandleStorageStateFile(
    const TString& fileSystemId,
    const TString& sessionId)
{
    return AcquireStateFile(DirectoryHandleStorage, fileSystemId, sessionId);
}

NProto::TError TPersistentStateManager::DeleteDirectoryHandleStorageStateFile(
    const TString& fileSystemId,
    const TString& sessionId)
{
    return DeleteStateFile(DirectoryHandleStorage, fileSystemId, sessionId);
}

////////////////////////////////////////////////////////////////////////////////
// All components

void TPersistentStateManager::ReleaseStateFiles(
    const TString& fileSystemId,
    const TString& sessionId)
{
    ReleaseStateFile(HandleOpsQueue, fileSystemId, sessionId);
    ReleaseStateFile(WriteBackCache, fileSystemId, sessionId);
    ReleaseStateFile(DirectoryHandleStorage, fileSystemId, sessionId);
}

////////////////////////////////////////////////////////////////////////////////

class TPersistentStateManagerStub final
    : public IPersistentStateManager
{
public:
    // HandleOpsQueue

    bool HasHandleOpsQueueState(
        const TString& fileSystemId,
        const TString& sessionId) const override
    {
        Y_UNUSED(fileSystemId, sessionId);
        return false;
    }

    TAcquireStateFileResult AcquireHandleOpsQueueStateFile(
        const TString& fileSystemId,
        const TString& sessionId) override
    {
        Y_UNUSED(fileSystemId, sessionId);
        return NotImplemented(HandleOpsQueueFileName);
    }

    NProto::TError DeleteHandleOpsQueueStateFile(
        const TString& fileSystemId,
        const TString& sessionId) override
    {
        Y_UNUSED(fileSystemId, sessionId);
        return {};
    }

    // WriteBackCache

    bool HasWriteBackCacheState(
        const TString& fileSystemId,
        const TString& sessionId) const override
    {
        Y_UNUSED(fileSystemId, sessionId);
        return false;
    }

    TAcquireStateFileResult AcquireWriteBackCacheStateFile(
        const TString& fileSystemId,
        const TString& sessionId) override
    {
        Y_UNUSED(fileSystemId, sessionId);
        return NotImplemented(WriteBackCacheFileName);
    }

    NProto::TError DeleteWriteBackCacheStateFile(
        const TString& fileSystemId,
        const TString& sessionId) override
    {
        Y_UNUSED(fileSystemId, sessionId);
        return {};
    }

    // DirectoryHandleStorage

    TAcquireStateFileResult AcquireDirectoryHandleStorageStateFile(
        const TString& fileSystemId,
        const TString& sessionId) override
    {
        Y_UNUSED(fileSystemId, sessionId);
        return NotImplemented(DirectoryHandleStorageFileName);
    }

    NProto::TError DeleteDirectoryHandleStorageStateFile(
        const TString& fileSystemId,
        const TString& sessionId) override
    {
        Y_UNUSED(fileSystemId, sessionId);
        return {};
    }

    // All components

    void ReleaseStateFiles(
        const TString& fileSystemId,
        const TString& sessionId) override
    {
        Y_UNUSED(fileSystemId, sessionId);
    }

private:
    static TAcquireStateFileResult NotImplemented(TStringBuf fileName)
    {
        return {
            .Error = MakeError(
                E_NOT_IMPLEMENTED,
                TStringBuilder() << "State file " << fileName
                                 << " is not supported by the stub"),
            .FilePath = {}};
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IPersistentStateManagerPtr CreatePersistentStateManager(
    TString handleOpsQueueBasePath,
    TString writeBackCacheBasePath,
    TString directoryHandlesStorageBasePath)
{
    return std::make_shared<TPersistentStateManager>(
        std::move(handleOpsQueueBasePath),
        std::move(writeBackCacheBasePath),
        std::move(directoryHandlesStorageBasePath));
}

IPersistentStateManagerPtr CreatePersistentStateManagerStub()
{
    return std::make_shared<TPersistentStateManagerStub>();
}

}   // namespace NCloud::NFileStore::NFuse
