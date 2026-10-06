#include "state_file_locator.h"

#include "file_lock.h"

#include <util/folder/path.h>
#include <util/generic/algorithm.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/string/printf.h>
#include <util/system/error.h>
#include <util/system/fstat.h>

#include <cerrno>

namespace NCloud::NFileStore::NWriteBackCacheStateTool {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TStorageFileNames
{
    static constexpr TStringBuf WriteBackCache = "write_back_cache";
    static constexpr TStringBuf DirectoryHandles = "directory_handles_storage";
    static constexpr TStringBuf HandleOpsQueue = "handle_ops_queue";
};

NProto::EStateFileType GetFileType(const TString& fileName)
{
    if (fileName == TStorageFileNames::WriteBackCache) {
        return NProto::EStateFileType::WriteBackCache;
    }
    if (fileName == TStorageFileNames::DirectoryHandles) {
        return NProto::EStateFileType::DirectoryHandles;
    }
    if (fileName == TStorageFileNames::HandleOpsQueue) {
        return NProto::EStateFileType::HandleOpsQueue;
    }
    return NProto::EStateFileType::Unknown;
}

NProto::TStateFileInfo GetStateFileInfo(const TFsPath& path)
{
    // Probe with an exclusive lock so IsLocked also reflects concurrent
    // read-only tool invocations, which hold shared locks.
    TFile file(
        path.GetPath(),
        EOpenModeFlag::OpenExisting | EOpenModeFlag::RdOnly);
    const bool isLocked = !TryLock(file, EFileLockType::Exclusive);

    NProto::TStateFileInfo res;
    res.SetFilePath(path.GetPath());
    res.SetFileSystemId(path.Parent().Parent().GetName());
    res.SetSessionId(path.Parent().GetName());
    res.SetFileType(GetFileType(path.GetName()));
    res.SetFileSize(static_cast<ui64>(file.GetLength()));
    res.SetIsLocked(isLocked);

    return res;
}

/**
 * State files are stored under the following directory structure:
 *  - <state_dir>/
 *    - <fs_id>/
 *      - <session_id>/
 *        - <state_file>
 *
 * Known files are write_back_cache, directory_handles_storage and
 * handle_ops_queue. Other regular files are listed with type Unknown.
 */

class TStateFileLocator: public IStateFileLocator
{
private:
    TString StateDir;

public:
    explicit TStateFileLocator(const TString& stateDir)
        : StateDir(stateDir)
    {}

    TResultOrError<NProto::TStateFileList> ListStateFiles() override
    {
        try {
            return ListStateFilesImpl();
        } catch (...) {
            return MakeError(
                E_IO,
                Sprintf(
                    "Failed to list state files in '%s': %s",
                    StateDir.c_str(),
                    CurrentExceptionMessage().c_str()));
        }
    }

    TResultOrError<TFile> LocateAndOpenStateFile(
        const TString& fsId,
        const TString& sessionId,
        TMaybe<NProto::EStateFileType> fileType,
        bool readOnly) override
    {
        if (fsId.empty()) {
            return MakeError(E_ARGUMENT, "Filesystem ID must not be empty");
        }

        auto stateFileListOrError = ListStateFiles();
        if (HasError(stateFileListOrError)) {
            return stateFileListOrError.GetError();
        }

        const auto& stateFileList = stateFileListOrError.GetResult();

        TVector<TString> candidates;
        for (const auto& stateFile: stateFileList.GetFiles()) {
            if (stateFile.GetFileSystemId() != fsId) {
                continue;
            }
            if (!sessionId.empty() && stateFile.GetSessionId() != sessionId) {
                continue;
            }
            if (fileType && stateFile.GetFileType() != *fileType) {
                continue;
            }
            candidates.push_back(stateFile.GetFilePath());
        }

        if (candidates.empty()) {
            return MakeError(
                E_NOT_FOUND,
                Sprintf(
                    "No state file found for fsId='%s', sessionId='%s'",
                    fsId.c_str(),
                    sessionId.c_str()));
        }

        if (candidates.size() > 1) {
            return MakeError(
                E_INVALID_STATE,
                Sprintf(
                    "Multiple state files found for fsId='%s', sessionId='%s'",
                    fsId.c_str(),
                    sessionId.c_str()));
        }

        const auto& path = candidates.front();
        try {
            const auto accessMode =
                readOnly ? EOpenModeFlag::RdOnly : EOpenModeFlag::RdWr;
            return TFile(path, EOpenModeFlag::OpenExisting | accessMode);
        } catch (...) {
            return MakeError(
                E_IO,
                Sprintf(
                    "Failed to open state file '%s': %s",
                    path.c_str(),
                    CurrentExceptionMessage().c_str()));
        }
    }

private:
    TResultOrError<NProto::TStateFileList> ListStateFilesImpl()
    {
        const TFsPath stateDir(StateDir);

        const TFileStat stateDirStat(stateDir);
        if (stateDirStat.IsNull()) {
            const int error = LastSystemError();
            if (error != ENOENT && error != ENOTDIR) {
                return MakeError(
                    E_IO,
                    Sprintf(
                        "Failed to inspect state directory '%s': %s",
                        stateDir.GetPath().c_str(),
                        LastSystemErrorText(error)));
            }

            return MakeError(
                E_NOT_FOUND,
                Sprintf(
                    "State directory '%s' does not exist",
                    stateDir.GetPath().c_str()));
        }

        if (!stateDirStat.IsDir()) {
            return MakeError(
                E_INVALID_STATE,
                Sprintf(
                    "State directory '%s' is not a directory",
                    stateDir.GetPath().c_str()));
        }

        TVector<NProto::TStateFileInfo> stateFiles;

        TVector<TFsPath> fsDirs;
        stateDir.List(fsDirs);

        for (const auto& fsDir: fsDirs) {
            const TFileStat fsDirStat(fsDir);
            if (fsDirStat.IsNull()) {
                const int error = LastSystemError();
                return MakeError(
                    E_IO,
                    Sprintf(
                        "Failed to inspect path '%s': %s",
                        fsDir.GetPath().c_str(),
                        LastSystemErrorText(error)));
            }

            if (!fsDirStat.IsDir()) {
                return MakeError(
                    E_INVALID_STATE,
                    Sprintf(
                        "State directory has invalid structure. Directory '%s' "
                        "must contain only sub-directories but a non-directory "
                        "'%s' found",
                        stateDir.GetPath().c_str(),
                        fsDir.GetPath().c_str()));
            }

            TVector<TFsPath> sessionDirs;
            fsDir.List(sessionDirs);
            for (const auto& sessionDir: sessionDirs) {
                const TFileStat sessionDirStat(sessionDir);
                if (sessionDirStat.IsNull()) {
                    const int error = LastSystemError();
                    return MakeError(
                        E_IO,
                        Sprintf(
                            "Failed to inspect path '%s': %s",
                            sessionDir.GetPath().c_str(),
                            LastSystemErrorText(error)));
                }

                if (!sessionDirStat.IsDir()) {
                    return MakeError(
                        E_INVALID_STATE,
                        Sprintf(
                            "State directory has invalid structure. Directory "
                            "'%s' must contain only sub-directories but a "
                            "non-directory '%s' found",
                            fsDir.GetPath().c_str(),
                            sessionDir.GetPath().c_str()));
                }

                TVector<TFsPath> stateFilePaths;
                sessionDir.List(stateFilePaths);
                for (const auto& stateFilePath: stateFilePaths) {
                    const TFileStat stateFileStat(stateFilePath);
                    if (stateFileStat.IsNull()) {
                        const int error = LastSystemError();
                        return MakeError(
                            E_IO,
                            Sprintf(
                                "Failed to inspect path '%s': %s",
                                stateFilePath.GetPath().c_str(),
                                LastSystemErrorText(error)));
                    }

                    if (!stateFileStat.IsFile()) {
                        return MakeError(
                            E_INVALID_STATE,
                            Sprintf(
                                "State directory has invalid structure. "
                                "Directory '%s' must contain only files but a "
                                "non-file '%s' found",
                                sessionDir.GetPath().c_str(),
                                stateFilePath.GetPath().c_str()));
                    }

                    stateFiles.push_back(GetStateFileInfo(stateFilePath));
                }
            }
        }

        SortBy(
            stateFiles,
            [](const auto& stateFile) { return stateFile.GetFilePath(); });

        NProto::TStateFileList res;
        res.SetStateDirectory(stateDir.GetPath());

        for (const auto& stateFile: stateFiles) {
            *res.AddFiles() = stateFile;
        }

        return res;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<IStateFileLocator> CreateStateFileLocator(
    const TString& stateDir)
{
    return std::make_shared<TStateFileLocator>(stateDir);
}

}   // namespace NCloud::NFileStore::NWriteBackCacheStateTool
