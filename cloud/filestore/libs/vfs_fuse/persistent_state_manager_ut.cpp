#include "persistent_state_manager.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/system/file_lock.h>
#include <util/system/fs.h>

namespace NCloud::NFileStore::NFuse {

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString FileSystemId = "fs";
const TString SessionId = "session";

// In production all the components are configured with the same base path,
// so their state files of one session live in one directory. The fixture
// mirrors that: every manager it creates shares StatePath between the
// components.
struct TFixture: public NUnitTest::TBaseFixture
{
    TTempDir TempDir;
    TString StatePath = TempDir.Path() / "state";

    TPersistentStateManager CreateManager()
    {
        return TPersistentStateManager(StatePath, StatePath, StatePath);
    }

    TFsPath SessionDir(
        const TString& fileSystemId,
        const TString& sessionId) const
    {
        return TFsPath(StatePath) / fileSystemId / sessionId;
    }
};

bool IsLocked(const TFsPath& path)
{
    TFileLock lock(path);
    if (lock.TryAcquire()) {
        lock.Release();
        return false;
    }
    return true;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPersistentStateManagerTest)
{
    Y_UNIT_TEST_F(ShouldAcquireCreateAndLockStateFile, TFixture)
    {
        auto manager = CreateManager();

        UNIT_ASSERT(!manager.HasHandleOpsQueueState(FileSystemId, SessionId));

        auto result =
            manager.AcquireHandleOpsQueueStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(result.Error), result.Error.GetMessage());

        const auto expected =
            SessionDir(FileSystemId, SessionId) / "handle_ops_queue";
        UNIT_ASSERT_VALUES_EQUAL(
            expected.GetPath(),
            result.FilePath.GetPath());
        UNIT_ASSERT(result.FilePath.Exists());
        UNIT_ASSERT(manager.HasHandleOpsQueueState(FileSystemId, SessionId));
        UNIT_ASSERT(IsLocked(result.FilePath));
    }

    Y_UNIT_TEST_F(ShouldFailToAcquireSameStateFileTwice, TFixture)
    {
        auto manager = CreateManager();

        auto first =
            manager.AcquireWriteBackCacheStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(first.Error), first.Error.GetMessage());

        auto second =
            manager.AcquireWriteBackCacheStateFile(FileSystemId, SessionId);
        UNIT_ASSERT(HasError(second.Error));
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, second.Error.GetCode());

        // Other components are not affected by the failed attempt.
        auto other =
            manager.AcquireHandleOpsQueueStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(other.Error), other.Error.GetMessage());
    }

    Y_UNIT_TEST_F(ShouldDeleteStateFileAndSessionDir, TFixture)
    {
        auto manager = CreateManager();

        auto result =
            manager.AcquireHandleOpsQueueStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(result.Error), result.Error.GetMessage());

        auto error =
            manager.DeleteHandleOpsQueueStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(error), error.GetMessage());

        UNIT_ASSERT(!result.FilePath.Exists());
        UNIT_ASSERT(!SessionDir(FileSystemId, SessionId).Exists());
        UNIT_ASSERT(!manager.HasHandleOpsQueueState(FileSystemId, SessionId));
    }

    Y_UNIT_TEST_F(ShouldTreatDeleteOfMissingStateFileAsNoop, TFixture)
    {
        auto manager = CreateManager();

        auto error =
            manager.DeleteHandleOpsQueueStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(error), error.GetMessage());
    }

    Y_UNIT_TEST_F(ShouldKeepStateFileOnDestruction, TFixture)
    {
        TFsPath filePath;
        {
            auto manager = CreateManager();
            auto result = manager.AcquireHandleOpsQueueStateFile(
                FileSystemId,
                SessionId);
            UNIT_ASSERT_C(!HasError(result.Error), result.Error.GetMessage());
            filePath = result.FilePath;
        }

        // The state file survives so that a future session can restore it,
        // and the lock is released so it can be re-acquired.
        UNIT_ASSERT(filePath.Exists());
        UNIT_ASSERT(!IsLocked(filePath));

        auto manager = CreateManager();
        UNIT_ASSERT(manager.HasHandleOpsQueueState(FileSystemId, SessionId));

        auto result =
            manager.AcquireHandleOpsQueueStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(result.Error), result.Error.GetMessage());
    }

    Y_UNIT_TEST_F(ShouldKeepSessionDirUntilLastStateFileDeleted, TFixture)
    {
        auto manager = CreateManager();

        auto hoq =
            manager.AcquireHandleOpsQueueStateFile(FileSystemId, SessionId);
        auto wbc =
            manager.AcquireWriteBackCacheStateFile(FileSystemId, SessionId);
        auto dhs = manager.AcquireDirectoryHandleStorageStateFile(
            FileSystemId,
            SessionId);
        UNIT_ASSERT_C(!HasError(hoq.Error), hoq.Error.GetMessage());
        UNIT_ASSERT_C(!HasError(wbc.Error), wbc.Error.GetMessage());
        UNIT_ASSERT_C(!HasError(dhs.Error), dhs.Error.GetMessage());

        // Deleting one state file keeps the shared directory and the other
        // state files held in it, whatever the order of deletion.
        auto error =
            manager.DeleteWriteBackCacheStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(error), error.GetMessage());
        UNIT_ASSERT(!wbc.FilePath.Exists());
        UNIT_ASSERT(hoq.FilePath.Exists());
        UNIT_ASSERT(dhs.FilePath.Exists());
        UNIT_ASSERT(SessionDir(FileSystemId, SessionId).Exists());

        error = manager.DeleteDirectoryHandleStorageStateFile(
            FileSystemId,
            SessionId);
        UNIT_ASSERT_C(!HasError(error), error.GetMessage());
        UNIT_ASSERT(!dhs.FilePath.Exists());
        UNIT_ASSERT(hoq.FilePath.Exists());
        UNIT_ASSERT(SessionDir(FileSystemId, SessionId).Exists());

        // Deleting the last one removes the directory.
        error = manager.DeleteHandleOpsQueueStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(error), error.GetMessage());
        UNIT_ASSERT(!hoq.FilePath.Exists());
        UNIT_ASSERT(!SessionDir(FileSystemId, SessionId).Exists());
    }

    Y_UNIT_TEST_F(ShouldNotDisturbHeldSiblingWhenDeletingUnheldFile, TFixture)
    {
        // Emulate a session start with the directory handle storage disabled
        // after it had been enabled: its file is left on disk unheld, while
        // the other components' files in the same directory are held.
        TFsPath orphan;
        {
            auto previous = CreateManager();
            auto result = previous.AcquireDirectoryHandleStorageStateFile(
                FileSystemId,
                SessionId);
            UNIT_ASSERT_C(!HasError(result.Error), result.Error.GetMessage());
            orphan = result.FilePath;
        }

        auto manager = CreateManager();
        auto hoq =
            manager.AcquireHandleOpsQueueStateFile(FileSystemId, SessionId);
        auto wbc =
            manager.AcquireWriteBackCacheStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(hoq.Error), hoq.Error.GetMessage());
        UNIT_ASSERT_C(!HasError(wbc.Error), wbc.Error.GetMessage());
        UNIT_ASSERT(orphan.Exists());

        // Cleaning up the unheld file must remove only that file.
        auto error = manager.DeleteDirectoryHandleStorageStateFile(
            FileSystemId,
            SessionId);
        UNIT_ASSERT_C(!HasError(error), error.GetMessage());
        UNIT_ASSERT(!orphan.Exists());
        UNIT_ASSERT(hoq.FilePath.Exists());
        UNIT_ASSERT(wbc.FilePath.Exists());
        UNIT_ASSERT(SessionDir(FileSystemId, SessionId).Exists());
        UNIT_ASSERT(IsLocked(hoq.FilePath));
        UNIT_ASSERT(IsLocked(wbc.FilePath));
    }

    Y_UNIT_TEST_F(ShouldManageSessionsIndependently, TFixture)
    {
        auto manager = CreateManager();

        auto first =
            manager.AcquireHandleOpsQueueStateFile(FileSystemId, "session-1");
        auto second =
            manager.AcquireHandleOpsQueueStateFile(FileSystemId, "session-2");
        UNIT_ASSERT_C(!HasError(first.Error), first.Error.GetMessage());
        UNIT_ASSERT_C(!HasError(second.Error), second.Error.GetMessage());
        UNIT_ASSERT_UNEQUAL(
            first.FilePath.Parent().GetPath(),
            second.FilePath.Parent().GetPath());

        UNIT_ASSERT(manager.HasHandleOpsQueueState(FileSystemId, "session-1"));
        UNIT_ASSERT(manager.HasHandleOpsQueueState(FileSystemId, "session-2"));

        auto error = manager.DeleteHandleOpsQueueStateFile(
            FileSystemId,
            "session-1");
        UNIT_ASSERT_C(!HasError(error), error.GetMessage());

        UNIT_ASSERT(!manager.HasHandleOpsQueueState(FileSystemId, "session-1"));
        UNIT_ASSERT(!SessionDir(FileSystemId, "session-1").Exists());
        UNIT_ASSERT(manager.HasHandleOpsQueueState(FileSystemId, "session-2"));
        UNIT_ASSERT(second.FilePath.Exists());
    }

    Y_UNIT_TEST_F(ShouldDeleteStateFileLeftByPreviousSession, TFixture)
    {
        // Emulate an orphan file: acquire it and let the manager go away, so
        // the file stays on disk without being held.
        TFsPath orphan;
        {
            auto manager = CreateManager();
            auto result = manager.AcquireDirectoryHandleStorageStateFile(
                FileSystemId,
                SessionId);
            UNIT_ASSERT_C(!HasError(result.Error), result.Error.GetMessage());
            orphan = result.FilePath;
        }
        UNIT_ASSERT(orphan.Exists());

        auto manager = CreateManager();
        auto error = manager.DeleteDirectoryHandleStorageStateFile(
            FileSystemId,
            SessionId);
        UNIT_ASSERT_C(!HasError(error), error.GetMessage());

        UNIT_ASSERT(!orphan.Exists());
        UNIT_ASSERT(!SessionDir(FileSystemId, SessionId).Exists());
    }

    Y_UNIT_TEST_F(ShouldReacquireStateFileAfterDelete, TFixture)
    {
        auto manager = CreateManager();

        auto first =
            manager.AcquireWriteBackCacheStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(first.Error), first.Error.GetMessage());

        auto error =
            manager.DeleteWriteBackCacheStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(error), error.GetMessage());

        auto second =
            manager.AcquireWriteBackCacheStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(second.Error), second.Error.GetMessage());
        UNIT_ASSERT(second.FilePath.Exists());
    }

    Y_UNIT_TEST_F(ShouldFailToAcquireUnconfiguredComponent, TFixture)
    {
        TPersistentStateManager manager(
            StatePath,
            {},   // writeBackCacheBasePath
            StatePath);

        UNIT_ASSERT(!manager.HasWriteBackCacheState(FileSystemId, SessionId));

        auto result =
            manager.AcquireWriteBackCacheStateFile(FileSystemId, SessionId);
        UNIT_ASSERT(HasError(result.Error));
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, result.Error.GetCode());

        auto error =
            manager.DeleteWriteBackCacheStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(error), error.GetMessage());

        // The configured components of the same manager still work.
        auto ok =
            manager.AcquireHandleOpsQueueStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(ok.Error), ok.Error.GetMessage());
    }

    Y_UNIT_TEST_F(ShouldFailToAcquireStateFileLockedByAnotherOwner, TFixture)
    {
        auto owner = CreateManager();
        auto result =
            owner.AcquireHandleOpsQueueStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(result.Error), result.Error.GetMessage());

        // A different manager (e.g. another process sharing the base path)
        // must not be able to take the same state file while it is held.
        auto other = CreateManager();
        auto contended =
            other.AcquireHandleOpsQueueStateFile(FileSystemId, SessionId);
        UNIT_ASSERT(HasError(contended.Error));
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, contended.Error.GetCode());

        // Once released it becomes acquirable again.
        auto error =
            owner.DeleteHandleOpsQueueStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(error), error.GetMessage());

        auto retried =
            other.AcquireHandleOpsQueueStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(retried.Error), retried.Error.GetMessage());
    }

    Y_UNIT_TEST_F(ShouldReportErrorInsteadOfThrowingOnAcquireFailure, TFixture)
    {
        auto manager = CreateManager();

        // Put a directory where the state file has to be created: touching
        // and opening it then fail (EISDIR), which the underlying util calls
        // report by throwing. The manager must turn that into an error.
        const auto filePath =
            SessionDir(FileSystemId, SessionId) / "handle_ops_queue";
        UNIT_ASSERT(NFs::MakeDirectoryRecursive(filePath));

        TPersistentStateManager::TAcquireStateFileResult result;
        UNIT_ASSERT_NO_EXCEPTION(
            result = manager.AcquireHandleOpsQueueStateFile(
                FileSystemId,
                SessionId));
        UNIT_ASSERT(HasError(result.Error));
        UNIT_ASSERT_VALUES_EQUAL(E_FAIL, result.Error.GetCode());

        // Nothing was registered as held, so a later delete is a clean no-op
        // for the manager (the stray directory is removed as the session
        // directory is not referenced).
        NProto::TError error;
        UNIT_ASSERT_NO_EXCEPTION(
            error = manager.DeleteHandleOpsQueueStateFile(
                FileSystemId,
                SessionId));
        UNIT_ASSERT_C(!HasError(error), error.GetMessage());
    }

    Y_UNIT_TEST(ShouldTreatStubAsUnconfigured)
    {
        auto manager = CreatePersistentStateManagerStub();

        UNIT_ASSERT(!manager->HasHandleOpsQueueState(FileSystemId, SessionId));
        UNIT_ASSERT(!manager->HasWriteBackCacheState(FileSystemId, SessionId));

        UNIT_ASSERT(HasError(
            manager->AcquireHandleOpsQueueStateFile(FileSystemId, SessionId)
                .Error));
        UNIT_ASSERT(HasError(
            manager->AcquireWriteBackCacheStateFile(FileSystemId, SessionId)
                .Error));
        UNIT_ASSERT(HasError(
            manager
                ->AcquireDirectoryHandleStorageStateFile(
                    FileSystemId,
                    SessionId)
                .Error));

        UNIT_ASSERT(!HasError(
            manager->DeleteHandleOpsQueueStateFile(FileSystemId, SessionId)));
    }
}

}   // namespace NCloud::NFileStore::NFuse
