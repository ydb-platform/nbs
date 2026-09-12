#include "persistent_state_manager.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/system/error.h>
#include <util/system/file.h>
#include <util/system/file_lock.h>
#include <util/system/fs.h>
#include <util/system/fstat.h>
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
    TAcquireStateFileGuard&& other) noexcept
{
    if (this != &other) {
        Reset();
        Impl = std::move(other.Impl);
    }
    return *this;
}

TAcquireStateFileGuard::~TAcquireStateFileGuard()
{
    Reset();
}

void TAcquireStateFileGuard::Reset() noexcept
{
    if (!Impl) {
        return;
    }

    auto impl = std::move(Impl);

    TGuard guard(impl->Registry->Mutex);
    impl->UnregisterLocked();

    // Destroying the lock closes the file, which releases the lock without
    // any chance of failure, unlike an explicit Release(). It has to happen
    // while the registry is still locked: otherwise an acquisition racing
    // with us finds the file unregistered but still locked. The file itself
    // is kept together with its session directory.
    impl->Lock.Reset();
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
        // Size a new state file is created with, 0 means empty.
        const ui64 StateFileSize;
        // Limit of the total size of the state files, 0 means no limit.
        const ui64 TotalSizeLimit;

        TComponentConfig(
                TString basePath,
                TStringBuf fileName,
                ui64 stateFileSize,
                ui64 totalSizeLimit)
            : BasePath(std::move(basePath))
            , FileName(fileName)
            , StateFileSize(stateFileSize)
            , TotalSizeLimit(totalSizeLimit)
        {}
    };

    const TStateFileRegistryPtr Registry =
        std::make_shared<TStateFileRegistry>();

    const TComponentConfig HandleOpsQueue;
    const TComponentConfig WriteBackCache;
    const TComponentConfig DirectoryHandleStorage;

public:
    explicit TPersistentStateManager(TPersistentStateManagerConfig config);

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

    // Sums up the sizes of the state files of the component found under its
    // base path, of all the filesystems and sessions.
    TResultOrError<ui64> CalculateTotalSize(
        const TComponentConfig& component) const;

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
        TPersistentStateManagerConfig config)
    : HandleOpsQueue(
          std::move(config.HandleOpsQueueBasePath),
          HandleOpsQueueFileName,
          config.HandleOpsQueueStateFileSize,
          config.HandleOpsQueueTotalSizeLimit)
    , WriteBackCache(
          std::move(config.WriteBackCacheBasePath),
          WriteBackCacheFileName,
          config.WriteBackCacheStateFileSize,
          config.WriteBackCacheTotalSizeLimit)
    , DirectoryHandleStorage(
          std::move(config.DirectoryHandlesStorageBasePath),
          DirectoryHandleStorageFileName,
          0,   // stateFileSize: sized by the component itself
          0)   // totalSizeLimit: not limited
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

TResultOrError<ui64> TPersistentStateManager::CalculateTotalSize(
    const TComponentConfig& component) const
{
    const TFsPath basePath(component.BasePath);
    if (!basePath.Exists()) {
        return 0;
    }

    // Listing reports failures by throwing; a size of a file missing in a
    // session directory is 0.
    ui64 totalSize = 0;
    try {
        TVector<TFsPath> fileSystemDirs;
        basePath.List(fileSystemDirs);
        for (const auto& fileSystemDir: fileSystemDirs) {
            if (!fileSystemDir.IsDirectory()) {
                continue;
            }

            TVector<TFsPath> sessionDirs;
            fileSystemDir.List(sessionDirs);
            for (const auto& sessionDir: sessionDirs) {
                if (!sessionDir.IsDirectory()) {
                    continue;
                }

                const auto filePath = sessionDir / component.FileName;
                totalSize += TFileStat(filePath.GetPath()).Size;
            }
        }
    } catch (const yexception& e) {
        return MakeError(
            E_FAIL,
            TStringBuilder() << "Failed to calculate the total size of "
                             << component.FileName << " state files under "
                             << basePath << ", reason: " << e.what());
    }

    return totalSize;
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

    // An existing state file is acquired regardless of the limit, so that the
    // state of a previous session is always restored. A new one is created
    // only if it fits into the limit, otherwise the component is not to be
    // used by the session at all, which is what an empty guard means.
    const bool isNew = !filePath.Exists();
    if (isNew && component.TotalSizeLimit) {
        auto totalSize = CalculateTotalSize(component);
        if (HasError(totalSize)) {
            return totalSize.GetError();
        }

        if (totalSize.GetResult() + component.StateFileSize >
            component.TotalSizeLimit)
        {
            return TAcquireStateFileGuard();
        }
    }

    if (!NFs::MakeDirectoryRecursive(dir)) {
        return MakeError(
            E_FAIL,
            TStringBuilder() << "Failed to create session dir: " << dir
                             << ", reason: " << LastSystemErrorText());
    }

    // Touch(), the TFileLock constructor (which opens the file), TryAcquire()
    // and Resize() all report failures by throwing.
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

        // Only a file created just now is sized: the size of an existing one
        // is part of the state it carries.
        if (isNew && component.StateFileSize) {
            TFile file(
                filePath,
                EOpenModeFlag::OpenExisting | EOpenModeFlag::RdWr);
            file.Resize(component.StateFileSize);
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
    TPersistentStateManagerConfig config)
{
    return std::make_shared<TPersistentStateManager>(std::move(config));
}

IPersistentStateManagerPtr CreatePersistentStateManagerStub()
{
    return std::make_shared<TPersistentStateManagerStub>();
}

}   // namespace NCloud::NFileStore::NFuse
