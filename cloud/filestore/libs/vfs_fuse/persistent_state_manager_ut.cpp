#include "persistent_state_manager.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
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

    NMonitoring::TDynamicCountersPtr Counters =
        MakeIntrusive<NMonitoring::TDynamicCounters>();
    NMonitoring::TDynamicCounters::TCounterPtr SessionDirNotEmptyCounter;

    TFixture()
    {
        InitCriticalEventsCounter(Counters);
        SessionDirNotEmptyCounter = Counters->GetCounter(
            GetCriticalEventForPersistentStateSessionDirNotEmpty(),
            true);
    }

    IPersistentStateManagerPtr CreateManager()
    {
        return CreatePersistentStateManager(StatePath, StatePath, StatePath);
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

        UNIT_ASSERT(!manager->HasHandleOpsQueueState(FileSystemId, SessionId));

        auto result =
            manager->AcquireHandleOpsQueueStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(result), result.GetError().GetMessage());

        auto guard = result.ExtractResult();
        UNIT_ASSERT(guard);

        const auto expected =
            SessionDir(FileSystemId, SessionId) / "handle_ops_queue";
        UNIT_ASSERT_VALUES_EQUAL(
            expected.GetPath(),
            guard.GetFilePath().GetPath());
        UNIT_ASSERT(guard.GetFilePath().Exists());
        UNIT_ASSERT(manager->HasHandleOpsQueueState(FileSystemId, SessionId));
        UNIT_ASSERT(IsLocked(guard.GetFilePath()));
    }

    Y_UNIT_TEST_F(ShouldFailToAcquireSameStateFileTwice, TFixture)
    {
        auto manager = CreateManager();

        auto first =
            manager->AcquireWriteBackCacheStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(first), first.GetError().GetMessage());
        auto guard = first.ExtractResult();

        auto second =
            manager->AcquireWriteBackCacheStateFile(FileSystemId, SessionId);
        UNIT_ASSERT(HasError(second));
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, second.GetError().GetCode());

        // Other components are not affected by the failed attempt.
        auto other =
            manager->AcquireHandleOpsQueueStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(other), other.GetError().GetMessage());
    }

    Y_UNIT_TEST_F(ShouldDeleteStateFileAndSessionDir, TFixture)
    {
        auto manager = CreateManager();

        auto result =
            manager->AcquireHandleOpsQueueStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(result), result.GetError().GetMessage());
        auto guard = result.ExtractResult();
        const auto filePath = guard.GetFilePath();

        auto error = guard.DeleteStateFile();
        UNIT_ASSERT_C(!HasError(error), error.GetMessage());

        UNIT_ASSERT(!guard);
        UNIT_ASSERT(!filePath.Exists());
        UNIT_ASSERT(!SessionDir(FileSystemId, SessionId).Exists());
        UNIT_ASSERT(!manager->HasHandleOpsQueueState(FileSystemId, SessionId));
    }

    Y_UNIT_TEST_F(ShouldTreatRepeatedDeleteAsNoop, TFixture)
    {
        auto manager = CreateManager();

        auto result =
            manager->AcquireHandleOpsQueueStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(result), result.GetError().GetMessage());
        auto guard = result.ExtractResult();

        auto error = guard.DeleteStateFile();
        UNIT_ASSERT_C(!HasError(error), error.GetMessage());

        // Delete should be idempotent.
        error = guard.DeleteStateFile();
        UNIT_ASSERT_C(!HasError(error), error.GetMessage());

        // An empty guard has nothing to delete either.
        TAcquireStateFileGuard empty;
        UNIT_ASSERT(!empty);
        error = empty.DeleteStateFile();
        UNIT_ASSERT_C(!HasError(error), error.GetMessage());
    }

    Y_UNIT_TEST_F(ShouldKeepStateFileOnGuardDestruction, TFixture)
    {
        auto manager = CreateManager();

        TFsPath filePath;
        {
            auto result = manager->AcquireHandleOpsQueueStateFile(
                FileSystemId,
                SessionId);
            UNIT_ASSERT_C(!HasError(result), result.GetError().GetMessage());
            auto guard = result.ExtractResult();
            filePath = guard.GetFilePath();
            UNIT_ASSERT(IsLocked(filePath));
        }

        // The state file survives so that a future session can restore it,
        // and the lock is released so it can be re-acquired, even by the
        // same manager.
        UNIT_ASSERT(filePath.Exists());
        UNIT_ASSERT(!IsLocked(filePath));
        UNIT_ASSERT(manager->HasHandleOpsQueueState(FileSystemId, SessionId));

        auto result =
            manager->AcquireHandleOpsQueueStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(result), result.GetError().GetMessage());
    }

    Y_UNIT_TEST_F(ShouldKeepStateFileOnManagerDestruction, TFixture)
    {
        // A guard may outlive the manager which has handed it out.
        TFsPath filePath;
        {
            TAcquireStateFileGuard guard;
            {
                auto manager = CreateManager();
                auto result = manager->AcquireHandleOpsQueueStateFile(
                    FileSystemId,
                    SessionId);
                UNIT_ASSERT_C(
                    !HasError(result),
                    result.GetError().GetMessage());
                guard = result.ExtractResult();
            }
            filePath = guard.GetFilePath();
            UNIT_ASSERT(IsLocked(filePath));
        }

        UNIT_ASSERT(filePath.Exists());
        UNIT_ASSERT(!IsLocked(filePath));
    }

    Y_UNIT_TEST_F(ShouldReleaseHeldStateFileOnMoveAssignment, TFixture)
    {
        auto manager = CreateManager();

        auto hoqResult =
            manager->AcquireHandleOpsQueueStateFile(FileSystemId, SessionId);
        auto wbcResult =
            manager->AcquireWriteBackCacheStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(hoqResult), hoqResult.GetError().GetMessage());
        UNIT_ASSERT_C(!HasError(wbcResult), wbcResult.GetError().GetMessage());
        auto guard = hoqResult.ExtractResult();
        auto other = wbcResult.ExtractResult();
        const auto hoqPath = guard.GetFilePath();
        const auto wbcPath = other.GetFilePath();

        // Assigning onto a guard which holds a file releases that file just
        // like destroying the guard would: the lock is dropped, the file is
        // kept, and it becomes acquirable again.
        guard = std::move(other);

        UNIT_ASSERT(guard);
        UNIT_ASSERT_VALUES_EQUAL(
            wbcPath.GetPath(),
            guard.GetFilePath().GetPath());
        UNIT_ASSERT(!other);

        UNIT_ASSERT(hoqPath.Exists());
        UNIT_ASSERT(!IsLocked(hoqPath));
        UNIT_ASSERT(IsLocked(wbcPath));

        auto again =
            manager->AcquireHandleOpsQueueStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(again), again.GetError().GetMessage());
    }

    Y_UNIT_TEST_F(ShouldKeepSessionDirUntilLastStateFileDeleted, TFixture)
    {
        auto manager = CreateManager();

        auto hoqResult =
            manager->AcquireHandleOpsQueueStateFile(FileSystemId, SessionId);
        auto wbcResult =
            manager->AcquireWriteBackCacheStateFile(FileSystemId, SessionId);
        auto dhsResult = manager->AcquireDirectoryHandleStorageStateFile(
            FileSystemId,
            SessionId);
        UNIT_ASSERT_C(!HasError(hoqResult), hoqResult.GetError().GetMessage());
        UNIT_ASSERT_C(!HasError(wbcResult), wbcResult.GetError().GetMessage());
        UNIT_ASSERT_C(!HasError(dhsResult), dhsResult.GetError().GetMessage());
        auto hoq = hoqResult.ExtractResult();
        auto wbc = wbcResult.ExtractResult();
        auto dhs = dhsResult.ExtractResult();
        const auto hoqPath = hoq.GetFilePath();
        const auto wbcPath = wbc.GetFilePath();
        const auto dhsPath = dhs.GetFilePath();

        // Deleting one state file keeps the shared directory and the other
        // state files held in it, whatever the order of deletion.
        auto error = wbc.DeleteStateFile();
        UNIT_ASSERT_C(!HasError(error), error.GetMessage());
        UNIT_ASSERT(!wbcPath.Exists());
        UNIT_ASSERT(hoqPath.Exists());
        UNIT_ASSERT(dhsPath.Exists());
        UNIT_ASSERT(SessionDir(FileSystemId, SessionId).Exists());
        UNIT_ASSERT_VALUES_EQUAL(0, SessionDirNotEmptyCounter->Val());

        error = dhs.DeleteStateFile();
        UNIT_ASSERT_C(!HasError(error), error.GetMessage());
        UNIT_ASSERT(!dhsPath.Exists());
        UNIT_ASSERT(hoqPath.Exists());
        UNIT_ASSERT(SessionDir(FileSystemId, SessionId).Exists());
        UNIT_ASSERT_VALUES_EQUAL(0, SessionDirNotEmptyCounter->Val());

        // Deleting the last one removes the directory.
        error = hoq.DeleteStateFile();
        UNIT_ASSERT_C(!HasError(error), error.GetMessage());
        UNIT_ASSERT(!hoqPath.Exists());
        UNIT_ASSERT(!SessionDir(FileSystemId, SessionId).Exists());
        UNIT_ASSERT_VALUES_EQUAL(0, SessionDirNotEmptyCounter->Val());
    }

    Y_UNIT_TEST_F(ShouldManageSessionsIndependently, TFixture)
    {
        auto manager = CreateManager();

        auto firstResult =
            manager->AcquireHandleOpsQueueStateFile(FileSystemId, "session-1");
        auto secondResult =
            manager->AcquireHandleOpsQueueStateFile(FileSystemId, "session-2");
        UNIT_ASSERT_C(
            !HasError(firstResult),
            firstResult.GetError().GetMessage());
        UNIT_ASSERT_C(
            !HasError(secondResult),
            secondResult.GetError().GetMessage());
        auto first = firstResult.ExtractResult();
        auto second = secondResult.ExtractResult();
        UNIT_ASSERT_UNEQUAL(
            first.GetFilePath().Parent().GetPath(),
            second.GetFilePath().Parent().GetPath());

        UNIT_ASSERT(manager->HasHandleOpsQueueState(FileSystemId, "session-1"));
        UNIT_ASSERT(manager->HasHandleOpsQueueState(FileSystemId, "session-2"));

        auto error = first.DeleteStateFile();
        UNIT_ASSERT_C(!HasError(error), error.GetMessage());

        UNIT_ASSERT(
            !manager->HasHandleOpsQueueState(FileSystemId, "session-1"));
        UNIT_ASSERT(!SessionDir(FileSystemId, "session-1").Exists());
        UNIT_ASSERT(manager->HasHandleOpsQueueState(FileSystemId, "session-2"));
        UNIT_ASSERT(second.GetFilePath().Exists());
    }

    Y_UNIT_TEST_F(ShouldDeleteStateFileLeftByPreviousSession, TFixture)
    {
        // Emulate an orphan file: acquire it and let the guard go away, so
        // the file stays on disk without being held. Then, as the loop does
        // for a disabled component, acquire it again just to delete it.
        TFsPath orphan;
        {
            auto manager = CreateManager();
            auto result = manager->AcquireDirectoryHandleStorageStateFile(
                FileSystemId,
                SessionId);
            UNIT_ASSERT_C(!HasError(result), result.GetError().GetMessage());
            orphan = result.ExtractResult().GetFilePath();
        }
        UNIT_ASSERT(orphan.Exists());

        auto manager = CreateManager();
        UNIT_ASSERT(
            manager->HasDirectoryHandleStorageState(FileSystemId, SessionId));

        auto result = manager->AcquireDirectoryHandleStorageStateFile(
            FileSystemId,
            SessionId);
        UNIT_ASSERT_C(!HasError(result), result.GetError().GetMessage());

        auto error = result.ExtractResult().DeleteStateFile();
        UNIT_ASSERT_C(!HasError(error), error.GetMessage());

        UNIT_ASSERT(!orphan.Exists());
        UNIT_ASSERT(!SessionDir(FileSystemId, SessionId).Exists());
        UNIT_ASSERT(
            !manager->HasDirectoryHandleStorageState(FileSystemId, SessionId));
    }

    Y_UNIT_TEST_F(ShouldNotDeleteUnheldSiblingStateFiles, TFixture)
    {
        // A state file of a component which is not configured anymore (or
        // just not acquired) is left in the session directory. Deleting the
        // state file of another component must not take it away.
        TFsPath unheld;
        {
            auto previous = CreateManager();
            auto result = previous->AcquireWriteBackCacheStateFile(
                FileSystemId,
                SessionId);
            UNIT_ASSERT_C(!HasError(result), result.GetError().GetMessage());
            unheld = result.ExtractResult().GetFilePath();
        }
        UNIT_ASSERT(unheld.Exists());

        auto manager = CreateManager();
        auto result = manager->AcquireDirectoryHandleStorageStateFile(
            FileSystemId,
            SessionId);
        UNIT_ASSERT_C(!HasError(result), result.GetError().GetMessage());
        auto dhs = result.ExtractResult();
        const auto dhsPath = dhs.GetFilePath();

        auto error = dhs.DeleteStateFile();
        UNIT_ASSERT_C(!HasError(error), error.GetMessage());

        UNIT_ASSERT(!dhsPath.Exists());
        UNIT_ASSERT(unheld.Exists());
        UNIT_ASSERT(SessionDir(FileSystemId, SessionId).Exists());

        // ... but the untracked leftover is reported, since nobody is going
        // to clean it up.
        UNIT_ASSERT_VALUES_EQUAL(1, SessionDirNotEmptyCounter->Val());
    }

    Y_UNIT_TEST_F(ShouldNotDisturbHeldSiblingWhenDeletingOrphan, TFixture)
    {
        // Emulate a session start with the directory handle storage disabled
        // after it had been enabled: its file is left on disk unheld, while
        // the other components' files in the same directory are held.
        TFsPath orphan;
        {
            auto previous = CreateManager();
            auto result = previous->AcquireDirectoryHandleStorageStateFile(
                FileSystemId,
                SessionId);
            UNIT_ASSERT_C(!HasError(result), result.GetError().GetMessage());
            orphan = result.ExtractResult().GetFilePath();
        }

        auto manager = CreateManager();
        auto hoqResult =
            manager->AcquireHandleOpsQueueStateFile(FileSystemId, SessionId);
        auto wbcResult =
            manager->AcquireWriteBackCacheStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(hoqResult), hoqResult.GetError().GetMessage());
        UNIT_ASSERT_C(!HasError(wbcResult), wbcResult.GetError().GetMessage());
        auto hoq = hoqResult.ExtractResult();
        auto wbc = wbcResult.ExtractResult();
        UNIT_ASSERT(orphan.Exists());

        // Cleaning up the orphan must remove only that file.
        auto dhsResult = manager->AcquireDirectoryHandleStorageStateFile(
            FileSystemId,
            SessionId);
        UNIT_ASSERT_C(!HasError(dhsResult), dhsResult.GetError().GetMessage());
        auto error = dhsResult.ExtractResult().DeleteStateFile();
        UNIT_ASSERT_C(!HasError(error), error.GetMessage());

        UNIT_ASSERT(!orphan.Exists());
        UNIT_ASSERT(hoq.GetFilePath().Exists());
        UNIT_ASSERT(wbc.GetFilePath().Exists());
        UNIT_ASSERT(SessionDir(FileSystemId, SessionId).Exists());
        UNIT_ASSERT(IsLocked(hoq.GetFilePath()));
        UNIT_ASSERT(IsLocked(wbc.GetFilePath()));
        UNIT_ASSERT_VALUES_EQUAL(0, SessionDirNotEmptyCounter->Val());
    }

    Y_UNIT_TEST_F(ShouldReacquireStateFileAfterDelete, TFixture)
    {
        auto manager = CreateManager();

        auto first =
            manager->AcquireWriteBackCacheStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(first), first.GetError().GetMessage());

        auto error = first.ExtractResult().DeleteStateFile();
        UNIT_ASSERT_C(!HasError(error), error.GetMessage());

        auto second =
            manager->AcquireWriteBackCacheStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(second), second.GetError().GetMessage());
        UNIT_ASSERT(second.GetResult().GetFilePath().Exists());
    }

    Y_UNIT_TEST_F(ShouldFailToAcquireUnconfiguredComponent, TFixture)
    {
        auto manager = CreatePersistentStateManager(
            StatePath,
            {},   // writeBackCacheBasePath
            StatePath);

        UNIT_ASSERT(!manager->HasWriteBackCacheState(FileSystemId, SessionId));

        auto result =
            manager->AcquireWriteBackCacheStateFile(FileSystemId, SessionId);
        UNIT_ASSERT(HasError(result));
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, result.GetError().GetCode());

        // The configured components of the same manager still work.
        auto ok =
            manager->AcquireHandleOpsQueueStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(ok), ok.GetError().GetMessage());
    }

    Y_UNIT_TEST_F(ShouldFailToAcquireStateFileLockedByAnotherOwner, TFixture)
    {
        auto owner = CreateManager();
        auto result =
            owner->AcquireHandleOpsQueueStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(result), result.GetError().GetMessage());
        auto guard = result.ExtractResult();

        // A different manager (e.g. another process sharing the base path)
        // must not be able to take the same state file while it is held.
        auto other = CreateManager();
        auto contended =
            other->AcquireHandleOpsQueueStateFile(FileSystemId, SessionId);
        UNIT_ASSERT(HasError(contended));
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            contended.GetError().GetCode());

        // Once released it becomes acquirable again.
        auto error = guard.DeleteStateFile();
        UNIT_ASSERT_C(!HasError(error), error.GetMessage());

        auto retried =
            other->AcquireHandleOpsQueueStateFile(FileSystemId, SessionId);
        UNIT_ASSERT_C(!HasError(retried), retried.GetError().GetMessage());
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

        NProto::TError error;
        UNIT_ASSERT_NO_EXCEPTION(
            error = manager
                        ->AcquireHandleOpsQueueStateFile(
                            FileSystemId,
                            SessionId)
                        .GetError());
        UNIT_ASSERT(HasError(error));
        UNIT_ASSERT_VALUES_EQUAL(E_FAIL, error.GetCode());
    }

    Y_UNIT_TEST(ShouldTreatStubAsUnconfigured)
    {
        auto manager = CreatePersistentStateManagerStub();

        UNIT_ASSERT(!manager->HasHandleOpsQueueState(FileSystemId, SessionId));
        UNIT_ASSERT(!manager->HasWriteBackCacheState(FileSystemId, SessionId));
        UNIT_ASSERT(
            !manager->HasDirectoryHandleStorageState(FileSystemId, SessionId));

        UNIT_ASSERT(HasError(
            manager->AcquireHandleOpsQueueStateFile(FileSystemId, SessionId)));
        UNIT_ASSERT(HasError(
            manager->AcquireWriteBackCacheStateFile(FileSystemId, SessionId)));
        UNIT_ASSERT(HasError(manager->AcquireDirectoryHandleStorageStateFile(
            FileSystemId,
            SessionId)));
    }
}

}   // namespace NCloud::NFileStore::NFuse
