#include "state_file_locator.h"

#include "file_lock.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/scope.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/file.h>
#include <util/system/sysstat.h>

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
    const auto path = stateDir / TString(fileSystemId) / TString(sessionId) /
                      TString(fileName);
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

void AssertInvalidStructure(
    const TFsPath& stateDir,
    const TFsPath& offendingPath)
{
    auto locator = CreateStateFileLocator(stateDir.GetPath());
    const auto result = locator->ListStateFiles();

    UNIT_ASSERT_C(HasError(result), "Expected invalid state directory");
    UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, result.GetError().GetCode());
    UNIT_ASSERT_C(
        result.GetError().GetMessage().Contains(offendingPath.GetPath()),
        result.GetError().GetMessage());
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

    Y_UNIT_TEST(ShouldReturnErrorForUnreadableStateDirectory)
    {
        TTempDir tempDir;
        const auto stateDir = tempDir.Path() / "state";
        stateDir.MkDirs();

        UNIT_ASSERT_VALUES_EQUAL(0, Chmod(stateDir.c_str(), 0));
        Y_DEFER
        {
            Chmod(stateDir.c_str(), MODE0777);
        };

        auto locator = CreateStateFileLocator(stateDir.GetPath());
        const auto result = locator->ListStateFiles();
        UNIT_ASSERT(HasError(result));
        UNIT_ASSERT_VALUES_EQUAL(E_IO, result.GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldListAndClassifyStateFiles)
    {
        TTempDir tempDir;

        const auto writeBackCache = CreateStateFile(
            tempDir.Path(),
            "fs-2",
            "session-1",
            "write_back_cache",
            "12345");

        const auto handleOpsQueue = CreateStateFile(
            tempDir.Path(),
            "fs-1",
            "session-2",
            "handle_ops_queue",
            "1234");

        const auto unknown = CreateStateFile(
            tempDir.Path(),
            "fs-1",
            "session-1",
            "other_state",
            "123");

        const auto directoryHandles = CreateStateFile(
            tempDir.Path(),
            "fs-1",
            "session-1",
            "directory_handles_storage",
            "12");

        auto locator = CreateStateFileLocator(tempDir.Name());
        const auto result = locator->ListStateFiles();
        UNIT_ASSERT_C(!HasError(result), FormatError(result.GetError()));

        const auto& list = result.GetResult();
        UNIT_ASSERT_VALUES_EQUAL(tempDir.Name(), list.GetStateDirectory());
        UNIT_ASSERT_VALUES_EQUAL(4, list.GetFiles().size());

        struct TExpectedFile
        {
            TFsPath Path;
            TStringBuf FileSystemId;
            TStringBuf SessionId;
            NProto::EStateFileType Type;
            ui64 Size;
        };

        const TVector<TExpectedFile> expected = {
            {directoryHandles,
             "fs-1",
             "session-1",
             NProto::EStateFileType::DirectoryHandles,
             2},
            {unknown, "fs-1", "session-1", NProto::EStateFileType::Unknown, 3},
            {handleOpsQueue,
             "fs-1",
             "session-2",
             NProto::EStateFileType::HandleOpsQueue,
             4},
            {writeBackCache,
             "fs-2",
             "session-1",
             NProto::EStateFileType::WriteBackCache,
             5},
        };

        for (size_t i = 0; i < expected.size(); ++i) {
            const auto& actual = list.GetFiles(i);
            UNIT_ASSERT_VALUES_EQUAL(
                expected[i].Path.GetPath(),
                actual.GetFilePath());

            UNIT_ASSERT_VALUES_EQUAL(
                expected[i].FileSystemId,
                actual.GetFileSystemId());

            UNIT_ASSERT_VALUES_EQUAL(
                expected[i].SessionId,
                actual.GetSessionId());

            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<int>(expected[i].Type),
                static_cast<int>(actual.GetFileType()));

            UNIT_ASSERT_VALUES_EQUAL(expected[i].Size, actual.GetFileSize());
            UNIT_ASSERT(!actual.GetIsLocked());
        }
    }

    Y_UNIT_TEST(ShouldRejectInvalidStateDirectoryStructure)
    {
        TTempDir fileSystemLevel;
        const auto fileSystemEntry = fileSystemLevel.Path() / "not-a-directory";
        TFileOutput(fileSystemEntry).Write("state");
        AssertInvalidStructure(fileSystemLevel.Path(), fileSystemEntry);

        TTempDir sessionLevel;
        const auto sessionEntry =
            sessionLevel.Path() / "fs-1" / "not-a-directory";
        sessionEntry.Parent().MkDirs();
        TFileOutput(sessionEntry).Write("state");
        AssertInvalidStructure(sessionLevel.Path(), sessionEntry);

        TTempDir stateFileLevel;
        const auto stateFileEntry =
            stateFileLevel.Path() / "fs-1" / "session-1" / "not-a-file";
        stateFileEntry.MkDirs();
        AssertInvalidStructure(stateFileLevel.Path(), stateFileEntry);
    }

    Y_UNIT_TEST(ShouldReportSharedAndExclusiveLocks)
    {
        TTempDir tempDir;
        const auto stateFile =
            CreateStateFile(tempDir.Path(), "fs-1", "session-1");
        auto locator = CreateStateFileLocator(tempDir.Name());

        for (const bool exclusive: {false, true}) {
            TFile owner(stateFile.GetPath(), OpenExisting | RdOnly);
            const auto acquired = TryLock(owner, exclusive);
            UNIT_ASSERT_C(!HasError(acquired), FormatError(acquired.GetError()));
            UNIT_ASSERT(acquired.GetResult());

            TFile contender(stateFile.GetPath(), OpenExisting | RdOnly);
            const auto busy = TryLock(contender, true /* exclusive */);
            UNIT_ASSERT_C(!HasError(busy), FormatError(busy.GetError()));
            UNIT_ASSERT(!busy.GetResult());
            UNIT_ASSERT(GetOnlyFile(locator->ListStateFiles()).GetIsLocked());
        }

        UNIT_ASSERT(!GetOnlyFile(locator->ListStateFiles()).GetIsLocked());
    }

    Y_UNIT_TEST(ShouldReturnErrorForInvalidLockHandle)
    {
        TFile file;
        const auto result = TryLock(file, true /* exclusive */);
        UNIT_ASSERT(HasError(result));
        UNIT_ASSERT_VALUES_EQUAL(E_IO, result.GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldIgnoreUnrelatedPathsWhenLocatingStateFile)
    {
        for (const auto& unrelatedPath: {
                 "fs-2/session-1/not-a-file",
                 "fs-1/session-2/not-a-file",
                 "fs-1/session-1/directory_handles_storage"})
        {
            TTempDir tempDir;
            const auto stateFile =
                CreateStateFile(tempDir.Path(), "fs-1", "session-1");

            const auto invalidPath = tempDir.Path() / unrelatedPath;
            invalidPath.MkDirs();
            AssertInvalidStructure(tempDir.Path(), invalidPath);

            auto locator = CreateStateFileLocator(tempDir.Name());
            const auto found = locator->LocateAndOpenStateFile(
                "fs-1",
                "session-1",
                NProto::EStateFileType::WriteBackCache,
                true);

            UNIT_ASSERT_C(!HasError(found), FormatError(found.GetError()));
            UNIT_ASSERT_VALUES_EQUAL(
                stateFile.GetPath(),
                found.GetResult().GetName());
            UNIT_ASSERT_VALUES_EQUAL(
                "state",
                TFileInput(found.GetResult()).ReadAll());
        }
    }

    Y_UNIT_TEST(ShouldLocateBySessionAndRejectAmbiguousOrMissingState)
    {
        TTempDir tempDir;

        const auto first = CreateStateFile(tempDir.Path(), "fs-1", "session-1");
        CreateStateFile(tempDir.Path(), "fs-1", "session-2");

        CreateStateFile(
            tempDir.Path(),
            "fs-1",
            "session-1",
            "directory_handles_storage");

        const auto unknown =
            CreateStateFile(tempDir.Path(), "fs-1", "session-1", "other_state");

        const auto onlySession =
            CreateStateFile(tempDir.Path(), "fs-2", "session-3");

        auto locator = CreateStateFileLocator(tempDir.Name());

        auto found = locator->LocateAndOpenStateFile(
            "fs-1",
            "session-1",
            NProto::EStateFileType::WriteBackCache,
            true);

        UNIT_ASSERT(!HasError(found));
        UNIT_ASSERT_VALUES_EQUAL(first.GetPath(), found.GetResult().GetName());

        const char replacement = 'S';
        UNIT_ASSERT_EXCEPTION(
            found.GetResult().Pwrite(&replacement, 1, 0),
            TFileError);
        {
            TFile file = found.GetResult();
            const auto acquired = TryLock(file, false /* exclusive */);
            UNIT_ASSERT_C(!HasError(acquired), FormatError(acquired.GetError()));
            UNIT_ASSERT(acquired.GetResult());
        }

        auto foundUnknown = locator->LocateAndOpenStateFile(
            "fs-1",
            "session-1",
            NProto::EStateFileType::Unknown,
            true);

        UNIT_ASSERT(!HasError(foundUnknown));
        UNIT_ASSERT_VALUES_EQUAL(
            unknown.GetPath(),
            foundUnknown.GetResult().GetName());

        auto foundWithoutSession = locator->LocateAndOpenStateFile(
            "fs-2",
            "",
            NProto::EStateFileType::WriteBackCache,
            true);

        UNIT_ASSERT(!HasError(foundWithoutSession));
        UNIT_ASSERT_VALUES_EQUAL(
            onlySession.GetPath(),
            foundWithoutSession.GetResult().GetName());

        auto foundWithoutType =
            locator->LocateAndOpenStateFile("fs-2", "", Nothing(), true);

        UNIT_ASSERT(!HasError(foundWithoutType));
        UNIT_ASSERT_VALUES_EQUAL(
            onlySession.GetPath(),
            foundWithoutType.GetResult().GetName());

        auto ambiguousWithoutType = locator->LocateAndOpenStateFile(
            "fs-1",
            "session-1",
            Nothing(),
            true);

        UNIT_ASSERT(HasError(ambiguousWithoutType));
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            ambiguousWithoutType.GetError().GetCode());

        auto ambiguous = locator->LocateAndOpenStateFile(
            "fs-1",
            "",
            NProto::EStateFileType::WriteBackCache,
            true);

        UNIT_ASSERT(HasError(ambiguous));
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            ambiguous.GetError().GetCode());

        auto missing = locator->LocateAndOpenStateFile(
            "missing-fs",
            "",
            NProto::EStateFileType::WriteBackCache,
            true);

        UNIT_ASSERT(HasError(missing));
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, missing.GetError().GetCode());

        auto invalid = locator->LocateAndOpenStateFile(
            "",
            "",
            NProto::EStateFileType::WriteBackCache,
            true);

        UNIT_ASSERT(HasError(invalid));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, invalid.GetError().GetCode());

        auto writable = locator->LocateAndOpenStateFile(
            "fs-2",
            "session-3",
            NProto::EStateFileType::WriteBackCache,
            false);

        UNIT_ASSERT(!HasError(writable));
        writable.GetResult().Pwrite(&replacement, 1, 0);
        UNIT_ASSERT_VALUES_EQUAL("State", TFileInput(onlySession).ReadAll());
    }
}

}   // namespace NCloud::NFileStore::NWriteBackCacheStateTool
