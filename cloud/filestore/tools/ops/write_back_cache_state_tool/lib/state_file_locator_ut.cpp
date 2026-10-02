#include "existing_file_lock.h"
#include "state_file_locator.h"

#include <cloud/storage/core/libs/file_backed_containers/file_ring_buffer_accessor.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/system/file_lock.h>

namespace NCloud::NFileStore::NWriteBackCacheStateTool {

namespace {

////////////////////////////////////////////////////////////////////////////////

TFsPath CreateStateFile(
    const TFsPath& stateDir,
    TStringBuf fileSystemId,
    TStringBuf sessionId,
    TStringBuf fileName = "write_back_cache",
    TStringBuf contents = "state")
{
    const auto path =
        stateDir / TString(fileSystemId) / TString(sessionId) / TString(fileName);
    path.Parent().MkDirs();
    TFileOutput(path).Write(contents);
    return path;
}

NProto::TStateFileInfo GetOnlyFile(
    const TResultOrError<NProto::TStateFileList>& result)
{
    UNIT_ASSERT_C(!HasError(result), FormatError(result.GetError()));
    UNIT_ASSERT_VALUES_EQUAL(1, result.GetResult().GetFiles().size());
    return result.GetResult().GetFiles(0);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStateFileLocatorTest)
{
    Y_UNIT_TEST(ShouldRejectMissingOrNonDirectoryStatePath)
    {
        TTempDir tempDir;

        auto missingLocator =
            CreateStateFileLocator((tempDir.Path() / "missing").GetPath());
        auto missing = missingLocator->ListStateFiles();
        UNIT_ASSERT(HasError(missing));
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, missing.GetError().GetCode());

        const auto regularFile = tempDir.Path() / "regular-file";
        TFileOutput(regularFile).Write("not a directory");
        auto fileLocator = CreateStateFileLocator(regularFile.GetPath());
        auto notDirectory = fileLocator->ListStateFiles();
        UNIT_ASSERT(HasError(notDirectory));
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            notDirectory.GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldListAndClassifyStateFiles)
    {
        TTempDir tempDir;
        const auto stateFile = CreateStateFile(
            tempDir.Path(),
            "fs-1",
            "session-1",
            "write_back_cache",
            "12345");

        auto locator = CreateStateFileLocator(tempDir.Name());
        const auto info = GetOnlyFile(locator->ListStateFiles());

        UNIT_ASSERT_VALUES_EQUAL(stateFile.GetPath(), info.GetFilePath());
        UNIT_ASSERT_VALUES_EQUAL("fs-1", info.GetFileSystemId());
        UNIT_ASSERT_VALUES_EQUAL("session-1", info.GetSessionId());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::EStateFileType::WriteBackCache),
            static_cast<int>(info.GetFileType()));
        UNIT_ASSERT_VALUES_EQUAL(5, info.GetFileSize());
        UNIT_ASSERT(!info.GetIsLocked());
    }

    Y_UNIT_TEST(ShouldReportSharedAndExclusiveLocks)
    {
        TTempDir tempDir;
        const auto stateFile =
            CreateStateFile(tempDir.Path(), "fs-1", "session-1");
        auto locator = CreateStateFileLocator(tempDir.Name());

        {
            TFileLock lock(stateFile.GetPath(), EFileLockType::Shared);
            UNIT_ASSERT(lock.TryAcquire());
            UNIT_ASSERT(GetOnlyFile(locator->ListStateFiles()).GetIsLocked());
        }

        {
            TFileLock lock(stateFile.GetPath(), EFileLockType::Exclusive);
            UNIT_ASSERT(lock.TryAcquire());
            UNIT_ASSERT(GetOnlyFile(locator->ListStateFiles()).GetIsLocked());
        }

        UNIT_ASSERT(!GetOnlyFile(locator->ListStateFiles()).GetIsLocked());
    }

    Y_UNIT_TEST(ShouldLocateBySessionAndRejectAmbiguousOrMissingState)
    {
        TTempDir tempDir;
        const auto first =
            CreateStateFile(tempDir.Path(), "fs-1", "session-1");
        CreateStateFile(tempDir.Path(), "fs-1", "session-2");
        CreateStateFile(
            tempDir.Path(),
            "fs-1",
            "session-1",
            "directory_handles_storage");

        auto locator = CreateStateFileLocator(tempDir.Name());

        auto found = locator->LocateStateFile(
            "fs-1",
            "session-1",
            NProto::EStateFileType::WriteBackCache);
        UNIT_ASSERT(!HasError(found));
        UNIT_ASSERT_VALUES_EQUAL(first.GetPath(), found.GetResult());

        auto ambiguous = locator->LocateStateFile(
            "fs-1",
            "",
            NProto::EStateFileType::WriteBackCache);
        UNIT_ASSERT(HasError(ambiguous));
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            ambiguous.GetError().GetCode());

        auto missing = locator->LocateStateFile(
            "missing-fs",
            "",
            NProto::EStateFileType::WriteBackCache);
        UNIT_ASSERT(HasError(missing));
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, missing.GetError().GetCode());

        auto invalid = locator->LocateStateFile(
            "",
            "",
            NProto::EStateFileType::WriteBackCache);
        UNIT_ASSERT(HasError(invalid));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, invalid.GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldNotCreateMissingFileWhileLocking)
    {
        TTempDir tempDir;
        const auto missing = tempDir.Path() / "missing";

        UNIT_ASSERT_EXCEPTION(
            TExistingFileLock(
                missing.GetPath(),
                EFileLockType::Shared),
            TSystemError);
        UNIT_ASSERT(!missing.Exists());
    }

    Y_UNIT_TEST(ShouldMapLockedFileWhenPathIsReplaced)
    {
        TTempDir tempDir;
        const auto stateFile = tempDir.Path() / "state";
        const auto movedStateFile = tempDir.Path() / "locked-state";
        TFileOutput(stateFile).Write("locked");

        TExistingFileLock lock(
            stateFile.GetPath(),
            EFileLockType::Exclusive,
            EOpenModeFlag::RdWr);
        UNIT_ASSERT(lock.TryAcquire());

        stateFile.RenameTo(movedStateFile);
        TFileOutput(stateFile).Write("replacement");

        TFileMapFileRingBufferAccessor accessor(
            lock.GetFile(),
            EFileRingBufferAccessorValidationMode::Debug,
            TMemoryMapCommon::EOpenModeFlag::oRdWr);
        auto error = accessor.Map();
        UNIT_ASSERT_C(!HasError(error), FormatError(error));

        auto rawData = accessor.GetRawData();
        UNIT_ASSERT_VALUES_EQUAL(
            "locked",
            TString(rawData.data(), rawData.size()));
        rawData[0] = 'L';
        error = accessor.Flush();
        UNIT_ASSERT_C(!HasError(error), FormatError(error));

        UNIT_ASSERT_VALUES_EQUAL(
            "Locked",
            TFileInput(movedStateFile).ReadAll());
        UNIT_ASSERT_VALUES_EQUAL(
            "replacement",
            TFileInput(stateFile).ReadAll());
    }
}

}   // namespace NCloud::NFileStore::NWriteBackCacheStateTool
