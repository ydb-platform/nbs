#include "persistent_state_manager.h"

#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/system/error.h>
#include <util/system/fs.h>

namespace NCloud::NFileStore::NFuse {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf HandleOpsQueueFileName = "handle_ops_queue";
constexpr TStringBuf WriteBackCacheFileName = "write_back_cache";
constexpr TStringBuf DirectoryHandleStorageFileName = "directory_handles_storage";

}   // namespace

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
    // session directory when it holds no more state files, no matter whether
    // the removal below succeeds.
    NProto::TError releaseError;
    bool dirHoldsStateFiles = false;
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

        dirHoldsStateFiles = !locks.empty();
        if (!dirHoldsStateFiles) {
            SessionDirs.erase(dirIt);
        }
    }

    // The state file may be present without being held, e.g. when it was
    // left behind by a previous session and the component is now disabled.
    if (filePath.Exists() && !NFs::Remove(filePath)) {
        return MakeError(
            E_FAIL,
            TStringBuilder() << "Failed to remove file " << filePath
                             << ", reason: " << LastSystemErrorText());
    }

    if (!dirHoldsStateFiles && dir.Exists()) {
        try {
            NFs::RemoveRecursive(dir);
        } catch (const yexception& e) {
            return MakeError(
                E_FAIL,
                TStringBuilder() << "Failed to remove dir " << dir
                                 << ", reason: " << e.what());
        }
    }

    return releaseError;
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

TPersistentStateManagerPtr CreatePersistentStateManagerStub()
{
    return std::make_shared<TPersistentStateManager>(
        TString{},   // handleOpsQueueBasePath
        TString{},   // writeBackCacheBasePath
        TString{});  // directoryHandlesStorageBasePath
}

}   // namespace NCloud::NFileStore::NFuse
