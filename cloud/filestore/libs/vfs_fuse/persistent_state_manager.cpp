#include "persistent_state_manager.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>

#include <util/generic/hash.h>
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

#include <functional>
#include <optional>

namespace NCloud::NFileStore::NFuse {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf HandleOpsQueueFileName = "handle_ops_queue";
constexpr TStringBuf WriteBackCacheFileName = "write_back_cache";
constexpr TStringBuf DirectoryHandleStorageFileName = "directory_handles_storage";

////////////////////////////////////////////////////////////////////////////////

struct TComponentConfig
{
    const TString BasePath;
    // State file name. Points to a static string.
    const TStringBuf FileName;
    // The size a new state file is created with. 0 means empty, in which
    // case the file is sized by the component itself.
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

////////////////////////////////////////////////////////////////////////////////

// Keeps track of the state files: which ones are on disk, how big they are
// and which ones are registered, i.e. acquired by a guard. Created from a
// listing of the state
// files on disk, and kept up to date by its operations from then on, which is
// valid as long as the manager is the only one to create and delete the
// files. Not thread-safe: the manager guards it with its mutex.
class TStateFileRegistry
{
public:
    struct TStateFile
    {
        // The size the file is known to have on disk: the actual one for a
        // listed file, the one it was created with otherwise (a component
        // may adjust it slightly, which is only picked up by a listing).
        ui64 Size = 0;
        // Whether the file is registered, i.e. acquired by a guard.
        bool Registered = false;
    };

    // The state files, by session directory and by file name (i.e. by
    // component). A directory may be shared by several components.
    using TStateFiles = THashMap<TString, THashMap<TString, TStateFile>>;

private:
    TStateFiles StateFiles;

public:
    explicit TStateFileRegistry(TStateFiles stateFiles)
        : StateFiles(std::move(stateFiles))
    {}

    bool IsRegistered(const TString& dir, const TString& fileName) const
    {
        const auto* dirFiles = StateFiles.FindPtr(dir);
        const auto* file = dirFiles ? dirFiles->FindPtr(fileName) : nullptr;
        return file && file->Registered;
    }

    // Registers the state file, adding it with the given size if it is not
    // known yet (the size of a known one is left as is).
    void Register(const TString& dir, const TString& fileName, ui64 size)
    {
        const TStateFile unknownFile{.Size = size, .Registered = false};
        auto it = StateFiles[dir].insert({fileName, unknownFile}).first;
        it->second.Registered = true;
    }

    // Unregisters the state file, forgetting it altogether if it has been
    // deleted from disk. Returns whether no state file is known to be in the
    // directory anymore.
    bool Unregister(
        const TString& dir,
        const TString& fileName,
        bool fileDeleted)
    {
        auto* dirFiles = StateFiles.FindPtr(dir);
        if (!dirFiles) {
            return true;
        }

        if (fileDeleted) {
            dirFiles->erase(fileName);
        } else if (auto* file = dirFiles->FindPtr(fileName)) {
            file->Registered = false;
        }

        if (!dirFiles->empty()) {
            return false;
        }

        StateFiles.erase(dir);
        return true;
    }

    // The total size of the state files of the component.
    ui64 GetTotalSize(const TString& fileName) const
    {
        ui64 totalSize = 0;
        for (const auto& [dir, dirFiles]: StateFiles) {
            if (const auto* file = dirFiles.FindPtr(fileName)) {
                totalSize += file->Size;
            }
        }
        return totalSize;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TAcquireStateFileGuard::TImpl
{
    TFsPath Dir;
    // State file name.
    TString FileName;
    TFsPath FilePath;

    THolder<TFileLock> Lock;

    // Hands the state file back to the manager, which owns the bookkeeping
    // and the synchronization: with |deleteFile| the file is removed from
    // disk, otherwise it is only released and kept for a future session.
    // Keeps the manager alive for as long as the guard lives.
    std::function<NProto::TError(TImpl& impl, bool deleteFile)> Release;
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
    impl->Release(*impl, false /* deleteFile */);
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
    return impl->Release(*impl, true /* deleteFile */);
}

namespace {

////////////////////////////////////////////////////////////////////////////////

// Keeps the state files under the configured base paths, following the layout
// <basePath>/<fileSystemId>/<sessionId>/<fileName>.
class TPersistentStateManager final
    : public IPersistentStateManager
    , public std::enable_shared_from_this<TPersistentStateManager>
{
private:
    // Guards the registry and the filesystem operations on the state files.
    TMutex Mutex;

    // Created from a listing of the state files on disk, before the first
    // operation on them, see EnsureRegistryInitializedLocked().
    std::optional<TStateFileRegistry> Registry;

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

    // Lists the state files of the component found under its base path, of
    // all the filesystems and sessions.
    NProto::TError ListStateFiles(
        const TComponentConfig& component,
        TStateFileRegistry::TStateFiles& stateFiles) const;

    // Creates the registry from a listing of the state files of all the
    // configured components, unless that has been done already. Must be
    // called with Mutex locked.
    NProto::TError EnsureRegistryInitializedLocked();

    // What a guard calls when it is done with its state file, see
    // TAcquireStateFileGuard::TImpl::Release.
    NProto::TError ReleaseStateFile(
        TAcquireStateFileGuard::TImpl& impl,
        bool deleteFile);

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

NProto::TError TPersistentStateManager::ListStateFiles(
    const TComponentConfig& component,
    TStateFileRegistry::TStateFiles& stateFiles) const
{
    // Listing an absent base path yields nothing, which is fine: the files
    // created from now on are tracked just the same.
    const TFsPath basePath(component.BasePath);
    if (!basePath.Exists()) {
        return {};
    }

    const TString fileName(component.FileName);

    // Listing reports failures by throwing.
    //
    // Layout is <basePath>/<fileSystemId>/<sessionId>/<stateFileName>
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

                const auto filePath = sessionDir / fileName;
                if (!filePath.Exists()) {
                    continue;
                }

                const ui64 size = TFileStat(filePath.GetPath()).Size;
                stateFiles[sessionDir.GetPath()][fileName].Size = size;
            }
        }
    } catch (const yexception& e) {
        return MakeError(
            E_FAIL,
            TStringBuilder() << "Failed to list " << fileName
                             << " state files under " << basePath
                             << ", reason: " << e.what());
    }

    return {};
}

NProto::TError TPersistentStateManager::EnsureRegistryInitializedLocked()
{
    if (Registry) {
        return {};
    }

    TStateFileRegistry::TStateFiles stateFiles;
    for (const auto* component:
         {&HandleOpsQueue, &WriteBackCache, &DirectoryHandleStorage})
    {
        if (!component->BasePath) {
            continue;
        }

        if (auto error = ListStateFiles(*component, stateFiles);
            HasError(error))
        {
            return error;
        }
    }

    Registry.emplace(std::move(stateFiles));
    return {};
}

NProto::TError TPersistentStateManager::ReleaseStateFile(
    TAcquireStateFileGuard::TImpl& impl,
    bool deleteFile)
{
    TGuard guard(Mutex);

    if (!deleteFile) {
        Registry->Unregister(
            impl.Dir.GetPath(),
            impl.FileName,
            false /* fileDeleted */);

        // Destroying the lock closes the file, which releases the lock
        // without any chance of failure, unlike an explicit Release(). It
        // has to happen while the mutex is still held: otherwise an
        // acquisition racing with us finds the file unregistered but still
        // locked. The file itself is kept together with its session
        // directory.
        impl.Lock.Reset();
        return {};
    }

    // Release() reports failures by throwing. The lock is dropped either way
    // once |impl| goes away, since closing the file releases it.
    NProto::TError releaseError;
    try {
        impl.Lock->Release();
    } catch (const yexception& e) {
        releaseError = MakeError(
            E_FAIL,
            TStringBuilder() << "Failed to unlock file " << impl.FilePath
                             << ", reason: " << e.what());
    }
    impl.Lock.Reset();

    // Only this very file is removed: the directory may hold state files of
    // other components, whether registered or not.
    NProto::TError removeError;
    const bool fileDeleted =
        !impl.FilePath.Exists() || NFs::Remove(impl.FilePath);
    if (!fileDeleted) {
        removeError = MakeError(
            E_FAIL,
            TStringBuilder() << "Failed to remove file " << impl.FilePath
                             << ", reason: " << LastSystemErrorText());
    }

    // Whatever happened to the file, it is not registered anymore
    const bool noStateFilesLeftInDir =
        Registry->Unregister(impl.Dir.GetPath(), impl.FileName, fileDeleted);

    if (HasError(removeError)) {
        return removeError;
    }

    // If other state files are known to be in the directory it is not
    // empty, so there is nothing to try. Otherwise remove it if empty: a
    // directory found not empty at this point contains state nobody tracks
    // (e.g. of a component which is not configured anymore).
    if (noStateFilesLeftInDir && !NFs::Remove(impl.Dir)) {
        const int err = LastSystemError();
        if (err == ENOENT) {
            // Already gone, e.g. removed together with the file by hand.
        } else if (err == ENOTEMPTY || err == EEXIST) {
            ReportPersistentStateSessionDirNotEmpty(
                TStringBuilder() << "Session dir " << impl.Dir
                                 << " is not empty after the state file "
                                 << impl.FileName << " has been deleted");
        } else {
            return MakeError(
                E_FAIL,
                TStringBuilder() << "Failed to remove dir " << impl.Dir
                                 << ", reason: " << LastSystemErrorText(err));
        }
    }

    return releaseError;
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

    TGuard guard(Mutex);

    if (auto error = EnsureRegistryInitializedLocked(); HasError(error)) {
        return error;
    }

    if (Registry->IsRegistered(dir.GetPath(), fileName)) {
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
    if (isNew && component.TotalSizeLimit &&
        Registry->GetTotalSize(fileName) + component.StateFileSize >
            component.TotalSizeLimit)
    {
        // State file is not created: the total file size limit has been
        // reached.
        return TAcquireStateFileGuard();
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

    Registry->Register(dir.GetPath(), fileName, component.StateFileSize);

    return TAcquireStateFileGuard(MakeHolder<TAcquireStateFileGuard::TImpl>(
        TAcquireStateFileGuard::TImpl{
            .Dir = std::move(dir),
            .FileName = std::move(fileName),
            .FilePath = std::move(filePath),
            .Lock = std::move(lock),
            .Release = [manager = shared_from_this()](
                           TAcquireStateFileGuard::TImpl& impl,
                           bool deleteFile)
            {
                return manager->ReleaseStateFile(impl, deleteFile);
            }}));
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
