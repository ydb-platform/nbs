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

// Keeps track of the state files: which ones are present, how big they are
// and which ones are acquired by a guard. Filled from a listing of the state
// files, and kept up to date by its operations from then on, which is valid as
// long as the manager is the only one to create and delete the files.
//
// Not thread-safe: the manager guards it with its mutex.
class TStateFileRegistry
{
public:
    struct TStateFile
    {
        // The size the file is known: the actual one for a listed file, the one
        // it was created with otherwise (a component may adjust it slightly,
        // which is only picked up by a listing).
        ui64 Size = 0;
        // Whether the file is acquired by a guard.
        bool Acquired = false;
    };

    // The state files, by session directory and by file name (i.e. by
    // component). A directory may be shared by several components.
    using TStateFiles = THashMap<TString, THashMap<TString, TStateFile>>;

private:
    TStateFiles StateFiles;

public:
    bool IsRegistered(const TString& dir, const TString& fileName) const
    {
        const auto* dirFiles = StateFiles.FindPtr(dir);
        return dirFiles && dirFiles->contains(fileName);
    }

    bool IsAcquired(const TString& dir, const TString& fileName) const
    {
        const auto* dirFiles = StateFiles.FindPtr(dir);
        const auto* file = dirFiles ? dirFiles->FindPtr(fileName) : nullptr;
        return file && file->Acquired;
    }

    // Registers the state file, adding it with the given size if it is not
    // known yet (the size of a known one is left as is).
    // A file found (listed) is registered as not acquired, it might be later
    // acquired by a guard as such.
    void Register(
        const TString& dir,
        const TString& fileName,
        ui64 size,
        bool fileAcquired)
    {
        const TStateFile unknownFile{.Size = size, .Acquired = false};
        auto it = StateFiles[dir].insert({fileName, unknownFile}).first;
        it->second.Acquired = fileAcquired;
    }

    // Unregisters the state file, forgetting it altogether if it has been
    // deleted. Returns whether no state file is known to be in the directory
    // anymore.
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
            file->Acquired = false;
        }

        if (!dirFiles->empty()) {
            return false;
        }

        // Erase the directory, since it no longer contains any files.
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
    // and the synchronization: with |deleteFile| the file is removed, otherwise
    // it is only released and kept for a future session.
    //
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

    // Filled from a listing of the state files before the first operation on
    // them, see EnsureRegistryInitializedLocked().
    TStateFileRegistry Registry;
    bool RegistryInitialized = false;

    const TComponentConfig HandleOpsQueue;
    const TComponentConfig WriteBackCache;
    const TComponentConfig DirectoryHandleStorage;

public:
    explicit TPersistentStateManager(TPersistentStateManagerConfig config);

    // HandleOpsQueue

    TResultOrError<bool> HasHandleOpsQueueState(
        const TString& fileSystemId,
        const TString& sessionId) override;
    TResultOrError<TAcquireStateFileGuard> AcquireHandleOpsQueueStateFile(
        const TString& fileSystemId,
        const TString& sessionId) override;

    // WriteBackCache

    TResultOrError<bool> HasWriteBackCacheState(
        const TString& fileSystemId,
        const TString& sessionId) override;
    TResultOrError<TAcquireStateFileGuard> AcquireWriteBackCacheStateFile(
        const TString& fileSystemId,
        const TString& sessionId) override;

    // DirectoryHandleStorage

    TResultOrError<bool> HasDirectoryHandleStorageState(
        const TString& fileSystemId,
        const TString& sessionId) override;
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
    // all the filesystems and sessions, into the registry. Must be called
    // with Mutex locked.
    NProto::TError ListStateFilesLocked(const TComponentConfig& component);

    // Fills the registry from a listing of the state files of all the
    // configured components, unless that has been done already. Must be
    // called with Mutex locked.
    NProto::TError EnsureRegistryInitializedLocked();

    // What a guard calls when it is done with its state file, see
    // TAcquireStateFileGuard::TImpl::Release.
    NProto::TError ReleaseStateFile(
        TAcquireStateFileGuard::TImpl& impl,
        bool deleteFile);

    TResultOrError<bool> HasState(
        const TComponentConfig& component,
        const TString& fileSystemId,
        const TString& sessionId);

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

////////////////////////////////////////////////////////////////////////////////

// Whether the path is a directory to descend into. We should report weird
// (non-directory) entries, but must not fail every session start.
bool IsDirectory(const TFsPath& path)
{
    const TFileStat stat(path);
    if (stat.IsNull()) {
        const int err = LastSystemError();
        if (err != ENOENT) {
            ReportPersistentStateUnstatableEntry(
                TStringBuilder() << "Failed to stat " << path
                                 << ", reason: " << LastSystemErrorText(err));
        }
        return false;
    }

    return stat.IsDir();
}

// Lists the children of a directory found under a base path. Such a
// directory being unlistable is reported but, just like an unstatable entry
// (see IsDirectory()), must not fail every session start: only what is
// under it goes unnoticed. A directory which has just disappeared is not
// worth reporting though.
bool ListDirectory(const TFsPath& dir, TVector<TFsPath>& children)
{
    const auto report = [&](const yexception& e)
    {
        ReportPersistentStateUnlistableDir(
            TStringBuilder() << "Failed to list " << dir
                             << ", reason: " << e.what());
    };

    try {
        dir.List(children);
        return true;
    } catch (const TSystemError& e) {
        if (e.Status() != ENOENT) {
            report(e);
        }
        return false;
    } catch (const yexception& e) {
        report(e);
        return false;
    }
}

NProto::TError TPersistentStateManager::ListStateFilesLocked(
    const TComponentConfig& component)
{
    const TFsPath basePath(component.BasePath);
    const TString fileName(component.FileName);

    const auto makeError = [&](const yexception& e)
    {
        return MakeError(
            E_FAIL,
            TStringBuilder() << "Failed to list " << fileName
                             << " state files under " << basePath
                             << ", reason: " << e.what());
    };

    // Listing reports failures by throwing, with the errno of the underlying
    // failure. The base path is not checked for existence beforehand on
    // purpose: such a check reads any failure, e.g. an I/O error, as
    // "absent", which would make the listing come out empty and every state
    // file on disk go unnoticed.
    TVector<TFsPath> fileSystemDirs;
    try {
        basePath.List(fileSystemDirs);
    } catch (const TSystemError& e) {
        if (e.Status() == ENOENT) {
            return {};
        }

        return makeError(e);
    } catch (const yexception& e) {
        return makeError(e);
    }

    // Layout is <basePath>/<fileSystemId>/<sessionId>/<stateFileName>.
    //
    // Unlike the base path, a failure at any of the entries under it is not
    // a failure of the listing: the entry is reported and skipped, see
    // IsDirectory() and ListDirectory().
    for (const auto& fileSystemDir: fileSystemDirs) {
        TVector<TFsPath> sessionDirs;
        if (!IsDirectory(fileSystemDir) ||
            !ListDirectory(fileSystemDir, sessionDirs))
        {
            continue;
        }

        for (const auto& sessionDir: sessionDirs) {
            TVector<TFsPath> files;
            if (!IsDirectory(sessionDir) ||
                !ListDirectory(sessionDir, files))
            {
                continue;
            }

            for (const auto& file: files) {
                if (file.GetName() != fileName) {
                    continue;
                }

                // The same rule for the state file itself: if its size
                // cannot be established, the file is not registered at all
                // rather than counted as empty.
                const TFileStat stat(file);
                if (stat.IsNull()) {
                    const int err = LastSystemError();
                    if (err != ENOENT) {
                        ReportPersistentStateUnstatableEntry(
                            TStringBuilder()
                            << "Failed to stat " << file
                            << ", reason: " << LastSystemErrorText(err));
                    }
                    continue;
                }

                Registry.Register(
                    sessionDir.GetPath(),
                    fileName,
                    stat.Size,
                    false /* fileAcquired */);
            }
        }
    }

    return {};
}

NProto::TError TPersistentStateManager::EnsureRegistryInitializedLocked()
{
    if (RegistryInitialized) {
        return {};
    }

    for (const auto* component:
         {&HandleOpsQueue, &WriteBackCache, &DirectoryHandleStorage})
    {
        if (!component->BasePath) {
            continue;
        }

        if (auto error = ListStateFilesLocked(*component); HasError(error)) {
            // Start over next time rather than build on a partial listing.
            Registry = {};
            return error;
        }
    }

    RegistryInitialized = true;
    return {};
}

NProto::TError TPersistentStateManager::ReleaseStateFile(
    TAcquireStateFileGuard::TImpl& impl,
    bool deleteFile)
{
    TGuard guard(Mutex);

    if (!deleteFile) {
        Registry.Unregister(
            impl.Dir.GetPath(),
            impl.FileName,
            false /* fileDeleted */);

        // Destroying the lock closes the file, which releases the lock
        // without any chance of failure, unlike an explicit Release(). It
        // has to happen while the mutex is still held: otherwise an
        // acquisition racing with us finds the file not acquired but still
        // locked.
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
    // other components, whether acquired or not.
    NProto::TError removeError;
    bool fileDeleted = true;
    if (!NFs::Remove(impl.FilePath)) {
        // A file already gone, e.g. removed by hand, is as good as deleted.
        const int err = LastSystemError();
        if (err != ENOENT) {
            fileDeleted = false;
            removeError = MakeError(
                E_FAIL,
                TStringBuilder() << "Failed to remove file " << impl.FilePath
                                 << ", reason: " << LastSystemErrorText(err));
        }
    }

    // Whatever happened to the file, it is not acquired anymore.
    const bool noStateFilesLeftInDir =
        Registry.Unregister(impl.Dir.GetPath(), impl.FileName, fileDeleted);

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

TResultOrError<bool> TPersistentStateManager::HasState(
    const TComponentConfig& component,
    const TString& fileSystemId,
    const TString& sessionId)
{
    if (!component.BasePath) {
        return false;
    }

    const auto dir = GetSessionDir(component, fileSystemId, sessionId);
    const TString fileName(component.FileName);

    TGuard guard(Mutex);

    if (auto error = EnsureRegistryInitializedLocked(); HasError(error)) {
        return error;
    }

    return Registry.IsRegistered(dir.GetPath(), fileName);
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

    if (Registry.IsAcquired(dir.GetPath(), fileName)) {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder() << "State file " << filePath
                             << " is already acquired");
    }

    // An existing state file is acquired regardless of the limit, so that the
    // state of a previous session is always restored. A new one is created
    // only if it fits into the limit, otherwise the component is not to be
    // used by the session at all, which is what an empty guard means.
    const bool isNew = !Registry.IsRegistered(dir.GetPath(), fileName);
    if (isNew && component.TotalSizeLimit &&
        Registry.GetTotalSize(fileName) + component.StateFileSize >
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

    // A new state file comes into existence with the configured size right
    // away. A file which exists although the registry does not know it is not
    // ours to size (it may well belong to another owner): it is left as it is
    // and only tried for the lock below.
    if (isNew) {
        try {
            TFile file(
                filePath,
                EOpenModeFlag::CreateNew | EOpenModeFlag::RdWr);
            if (component.StateFileSize) {
                file.Resize(component.StateFileSize);
            }
        } catch (const TSystemError& e) {
            if (e.Status() != EEXIST) {
                return MakeError(
                    E_FAIL,
                    TStringBuilder() << "Failed to create file, path: "
                                     << filePath << ", reason: " << e.what());
            }
        } catch (const yexception& e) {
            return MakeError(
                E_FAIL,
                TStringBuilder() << "Failed to create file, path: " << filePath
                                 << ", reason: " << e.what());
        }
    }

    // The file exists from this point on whatever happens below, so the
    // registry has to know it even if the lock cannot be taken.
    Registry.Register(
        dir.GetPath(),
        fileName,
        component.StateFileSize,
        false /* fileAcquired */);

    THolder<TFileLock> lock;
    try {
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

    auto impl = MakeHolder<TAcquireStateFileGuard::TImpl>(
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
            }});

    // Marking the file acquired is the last step and cannot fail (the
    // registry entry exists already), so a file never ends up marked
    // acquired without a guard actually holding it.
    Registry.Register(
        impl->Dir.GetPath(),
        impl->FileName,
        component.StateFileSize,
        true /* fileAcquired */);

    return TAcquireStateFileGuard(std::move(impl));
}

////////////////////////////////////////////////////////////////////////////////
// HandleOpsQueue

TResultOrError<bool> TPersistentStateManager::HasHandleOpsQueueState(
    const TString& fileSystemId,
    const TString& sessionId)
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

TResultOrError<bool> TPersistentStateManager::HasWriteBackCacheState(
    const TString& fileSystemId,
    const TString& sessionId)
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

TResultOrError<bool> TPersistentStateManager::HasDirectoryHandleStorageState(
    const TString& fileSystemId,
    const TString& sessionId)
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

    TResultOrError<bool> HasHandleOpsQueueState(
        const TString& fileSystemId,
        const TString& sessionId) override
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

    TResultOrError<bool> HasWriteBackCacheState(
        const TString& fileSystemId,
        const TString& sessionId) override
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

    TResultOrError<bool> HasDirectoryHandleStorageState(
        const TString& fileSystemId,
        const TString& sessionId) override
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
