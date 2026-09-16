#include "state_file_locator.h"

#include <util/folder/path.h>
#include <util/generic/algorithm.h>
#include <util/string/printf.h>
#include <util/system/file.h>
#include <util/system/file_lock.h>

namespace NCloud::NFileStore::NWriteBackCacheStateTool {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf WriteBackCacheFileName = "write_back_cache";
constexpr TStringBuf DirectoryHandlesFileName = "directory_handles_storage";

NProto::EStateFileType GetFileType(const TString& fileName)
{
    if (fileName == WriteBackCacheFileName) {
        return NProto::EStateFileType::WriteBackCache;
    }
    if (fileName == DirectoryHandlesFileName) {
        return NProto::EStateFileType::DirectoryHandles;
    }
    return NProto::EStateFileType::Unknown;
}

NProto::TStateFileInfo GetStateFileInfo(const TFsPath& path)
{
    TFileLock fileLock(path.GetPath());

    NProto::TStateFileInfo res;
    res.SetFilePath(path.GetPath());
    res.SetFileSystemId(path.Parent().Parent().GetName());
    res.SetSessionId(path.Parent().GetName());
    res.SetFileType(GetFileType(path.GetName()));
    res.SetFileSize(static_cast<ui64>(fileLock.GetLength()));
    res.SetIsLocked(!fileLock.TryAcquire());

    return res;
}

/**
 * State files are stored in the directory under the following structure:
 *  - <state_dir>/
 *    - <fs_id>/
 *      - <session_id>/
 *        - <state_file>
 *
 * WriteBackCache creates files named "write_back_cache"
 * DirectoryHandles creates files named "directory_handles_storage"
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
        const TFsPath stateDir(StateDir);

        if (!stateDir.Exists()) {
            return MakeError(
                E_NOT_FOUND,
                Sprintf(
                    "State directory '%s' does not exist",
                    stateDir.GetPath().c_str()));
        }

        if (!stateDir.IsDirectory()) {
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
            if (!fsDir.IsDirectory()) {
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
                if (!sessionDir.IsDirectory()) {
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
                    if (!stateFilePath.IsFile()) {
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

    TResultOrError<TString> LocateStateFile(
        const TString& fsId,
        const TString& sessionId,
        NProto::EStateFileType fileType) override
    {
        Y_ENSURE(!fsId.empty(), "File system ID must not be empty");

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
            if (fileType != NProto::EStateFileType::Unknown &&
                stateFile.GetFileType() != fileType)
            {
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

        return candidates.front();
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
