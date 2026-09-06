#include "persistent_state_manager.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/strbuf.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/system/error.h>
#include <util/system/file_lock.h>
#include <util/system/fs.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>
#include <util/system/yassert.h>

namespace NCloud::NFileStore::NFuse {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf HandleOpsQueueFileName = "handle_ops_queue";
constexpr TStringBuf WriteBackCacheFileName = "write_back_cache";
constexpr TStringBuf DirectoryHandleStorageFileName = "directory_handles_storage";

////////////////////////////////////////////////////////////////////////////////

// Keeps track of the state files held by the guards, per session directory.
// Shared by the manager and all the guards it has handed out, since the
// guards are owned by the loops and may outlive the manager.
struct TStateFileRegistry
{
    // Guards the registry and the filesystem operations on the state files.
    TMutex Mutex;

    // Session directories that hold at least one acquired state file, keyed
    // by path, with the names of the files held. A directory may be shared
    // by several components.
    THashMap<TString, THashSet<TString>> HeldStateFiles;
};

using TStateFileRegistryPtr = std::shared_ptr<TStateFileRegistry>;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TAcquireStateFileGuard::TImpl
{
    TStateFileRegistryPtr Registry;

    TFsPath Dir;
    TString FileName;
    TFsPath FilePath;

    THolder<TFileLock> Lock;

    // Returns whether this is the last state file held in the directory.
    // Should be guarded by TStateFileRegistry::Mutex.
    bool UnregisterLocked()
    {
        auto dirIt = Registry->HeldStateFiles.find(Dir.GetPath());
        if (dirIt == Registry->HeldStateFiles.end()) {
            return true;
        }

        auto& fileNames = dirIt->second;
        fileNames.erase(FileName);
        if (!fileNames.empty()) {
            return false;
        }

        Registry->HeldStateFiles.erase(dirIt);
        return true;
    }
};

TAcquireStateFileGuard::TAcquireStateFileGuard() = default;

TAcquireStateFileGuard::TAcquireStateFileGuard(THolder<TImpl> impl)
    : Impl(std::move(impl))
{}

TAcquireStateFileGuard::TAcquireStateFileGuard(
    TAcquireStateFileGuard&& other) noexcept = default;

TAcquireStateFileGuard& TAcquireStateFileGuard::operator=(
    TAcquireStateFileGuard&& other) noexcept = default;

TAcquireStateFileGuard::~TAcquireStateFileGuard()
{
    if (!Impl) {
        return;
    }

    TGuard guard(Impl->Registry->Mutex);
    Impl->UnregisterLocked();

    // Destroying the lock closes the file, which releases the lock without
    // any chance of failure, unlike an explicit Release(). It has to happen
    // while the registry is still locked: otherwise an acquisition racing
    // with us finds the file unregistered but still locked. The file itself
    // is kept together with its session directory.
    Impl->Lock.Reset();
}

TAcquireStateFileGuard::operator bool() const
{
    return !!Impl;
}

const TFsPath& TAcquireStateFileGuard::GetFilePath() const
{
    Y_ABORT_UNLESS(Impl, "The guard holds no state file");
    return Impl->FilePath;
}

NProto::TError TAcquireStateFileGuard::DeleteStateFile()
{
    if (!Impl) {
        return {};
    }

    // Leave nothing behind whatever happens below, so that a repeated call
    // is a no-op and the destructor has nothing to do.
    auto impl = std::move(Impl);

    TGuard guard(impl->Registry->Mutex);

    const bool lastStateFile = impl->UnregisterLocked();

    // Release() reports failures by throwing. The lock is dropped either way
    // once |impl| goes out of scope, since closing the file releases it.
    NProto::TError releaseError;
    try {
        impl->Lock->Release();
    } catch (const yexception& e) {
        releaseError = MakeError(
            E_FAIL,
            TStringBuilder() << "Failed to unlock file " << impl->FilePath
                             << ", reason: " << e.what());
    }
    impl->Lock.Reset();

    // Only this very file is removed: the directory may hold state files of
    // other components, whether held by other guards or not.
    if (impl->FilePath.Exists() && !NFs::Remove(impl->FilePath)) {
        return MakeError(
            E_FAIL,
            TStringBuilder() << "Failed to remove file " << impl->FilePath
                             << ", reason: " << LastSystemErrorText());
    }

    // If other state files are still held in the directory it is obviously
    // not empty, so there is nothing to try. Otherwise remove it if empty: a
    // directory found not empty at this point contains state nobody tracks
    // (e.g. of a component which is not configured anymore).
    if (lastStateFile && !NFs::Remove(impl->Dir)) {
        const int err = LastSystemError();
        if (err == ENOENT) {
            // Already gone, e.g. removed together with the file by hand.
        } else if (err == ENOTEMPTY || err == EEXIST) {
            ReportPersistentStateSessionDirNotEmpty(
                TStringBuilder() << "Session dir " << impl->Dir
                                 << " is not empty after the state file "
                                 << impl->FileName << " has been deleted");
        } else {
            return MakeError(
                E_FAIL,
                TStringBuilder() << "Failed to remove dir " << impl->Dir
                                 << ", reason: " << LastSystemErrorText(err));
        }
    }

    return releaseError;
}

namespace {

////////////////////////////////////////////////////////////////////////////////

// Keeps the state files on disk under the configured base paths, following
// the layout <basePath>/<fileSystemId>/<sessionId>/<fileName>.
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

    const TStateFileRegistryPtr Registry =
        std::make_shared<TStateFileRegistry>();

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
    TResultOrError<TAcquireStateFileGuard> AcquireHandleOpsQueueStateFile(
        const TString& fileSystemId,
        const TString& sessionId) override;

    // WriteBackCache

    bool HasWriteBackCacheState(
        const TString& fileSystemId,
        const TString& sessionId) const override;
    TResultOrError<TAcquireStateFileGuard> AcquireWriteBackCacheStateFile(
        const TString& fileSystemId,
        const TString& sessionId) override;

    // DirectoryHandleStorage

    bool HasDirectoryHandleStorageState(
        const TString& fileSystemId,
        const TString& sessionId) const override;
    TResultOrError<TAcquireStateFileGuard>
    AcquireDirectoryHandleStorageStateFile(
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

    TResultOrError<TAcquireStateFileGuard> AcquireStateFile(
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

    TGuard guard(Registry->Mutex);
    return filePath.Exists();
}

TResultOrError<TAcquireStateFileGuard>
TPersistentStateManager::AcquireStateFile(
    const TComponentConfig& component,
    const TString& fileSystemId,
    const TString& sessionId)
{
    if (!component.BasePath) {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder() << "Base path for " << component.FileName
                             << " is not set");
    }

    auto dir = GetSessionDir(component, fileSystemId, sessionId);
    TString fileName(component.FileName);
    auto filePath = dir / fileName;

    TGuard guard(Registry->Mutex);

    const auto* fileNames = Registry->HeldStateFiles.FindPtr(dir.GetPath());
    if (fileNames && fileNames->contains(fileName)) {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder() << "State file " << filePath
                             << " is already acquired");
    }

    if (!NFs::MakeDirectoryRecursive(dir)) {
        return MakeError(
            E_FAIL,
            TStringBuilder() << "Failed to create directories, path: " << dir);
    }

    // Touch(), the TFileLock constructor (which opens the file) and
    // TryAcquire() all report failures by throwing.
    THolder<TFileLock> lock;
    try {
        filePath.Touch();

        lock = MakeHolder<TFileLock>(filePath);
        if (!lock->TryAcquire()) {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder() << "State file " << filePath
                                 << " is locked by another owner");
        }
    } catch (const yexception& e) {
        return MakeError(
            E_FAIL,
            TStringBuilder() << "Failed to lock file, path: " << filePath
                             << ", reason: " << e.what());
    }

    Registry->HeldStateFiles[dir.GetPath()].insert(fileName);

    return TAcquireStateFileGuard(MakeHolder<TAcquireStateFileGuard::TImpl>(
        TAcquireStateFileGuard::TImpl{
            .Registry = Registry,
            .Dir = std::move(dir),
            .FileName = std::move(fileName),
            .FilePath = std::move(filePath),
            .Lock = std::move(lock)}));
}

////////////////////////////////////////////////////////////////////////////////
// HandleOpsQueue

bool TPersistentStateManager::HasHandleOpsQueueState(
    const TString& fileSystemId,
    const TString& sessionId) const
{
    return HasState(HandleOpsQueue, fileSystemId, sessionId);
}

TResultOrError<TAcquireStateFileGuard>
TPersistentStateManager::AcquireHandleOpsQueueStateFile(
    const TString& fileSystemId,
    const TString& sessionId)
{
    return AcquireStateFile(HandleOpsQueue, fileSystemId, sessionId);
}

////////////////////////////////////////////////////////////////////////////////
// WriteBackCache

bool TPersistentStateManager::HasWriteBackCacheState(
    const TString& fileSystemId,
    const TString& sessionId) const
{
    return HasState(WriteBackCache, fileSystemId, sessionId);
}

TResultOrError<TAcquireStateFileGuard>
TPersistentStateManager::AcquireWriteBackCacheStateFile(
    const TString& fileSystemId,
    const TString& sessionId)
{
    return AcquireStateFile(WriteBackCache, fileSystemId, sessionId);
}

////////////////////////////////////////////////////////////////////////////////
// DirectoryHandleStorage

bool TPersistentStateManager::HasDirectoryHandleStorageState(
    const TString& fileSystemId,
    const TString& sessionId) const
{
    return HasState(DirectoryHandleStorage, fileSystemId, sessionId);
}

TResultOrError<TAcquireStateFileGuard>
TPersistentStateManager::AcquireDirectoryHandleStorageStateFile(
    const TString& fileSystemId,
    const TString& sessionId)
{
    return AcquireStateFile(DirectoryHandleStorage, fileSystemId, sessionId);
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

    TResultOrError<TAcquireStateFileGuard> AcquireHandleOpsQueueStateFile(
        const TString& fileSystemId,
        const TString& sessionId) override
    {
        Y_UNUSED(fileSystemId, sessionId);
        return NotImplemented(HandleOpsQueueFileName);
    }

    // WriteBackCache

    bool HasWriteBackCacheState(
        const TString& fileSystemId,
        const TString& sessionId) const override
    {
        Y_UNUSED(fileSystemId, sessionId);
        return false;
    }

    TResultOrError<TAcquireStateFileGuard> AcquireWriteBackCacheStateFile(
        const TString& fileSystemId,
        const TString& sessionId) override
    {
        Y_UNUSED(fileSystemId, sessionId);
        return NotImplemented(WriteBackCacheFileName);
    }

    // DirectoryHandleStorage

    bool HasDirectoryHandleStorageState(
        const TString& fileSystemId,
        const TString& sessionId) const override
    {
        Y_UNUSED(fileSystemId, sessionId);
        return false;
    }

    TResultOrError<TAcquireStateFileGuard>
    AcquireDirectoryHandleStorageStateFile(
        const TString& fileSystemId,
        const TString& sessionId) override
    {
        Y_UNUSED(fileSystemId, sessionId);
        return NotImplemented(DirectoryHandleStorageFileName);
    }

private:
    static NProto::TError NotImplemented(TStringBuf fileName)
    {
        return MakeError(
            E_NOT_IMPLEMENTED,
            TStringBuilder() << "State file " << fileName
                             << " is not supported by the stub");
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
