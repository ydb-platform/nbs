#include "actor_trimfreshlog.h"

#include <cloud/storage/core/libs/tablet/model/commit.h>

#include <contrib/ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TTrimFreshLogActorTest)
{
    struct TActorSystem: TTestActorRuntimeBase
    {
        void Start()
        {
            SetDispatchTimeout(TDuration::Seconds(5));
            InitNodes();
        }
    };

    Y_UNIT_TEST(ShouldCompleteWhenNoCollectGarbageRequestsAreGenerated)
    {
        TActorSystem runtime;
        runtime.Start();

        const auto requestActorId = runtime.AllocateEdgeActor();
        const auto partitionActorId = runtime.AllocateEdgeActor();

        TTabletStorageInfoPtr tabletInfo = MakeIntrusive<TTabletStorageInfo>();
        tabletInfo->TabletID = 42;

        const ui64 trimFreshLogToCommitId = MakeCommitId(3, 17);

        runtime.Register(new TTrimFreshLogActor(
            MakeIntrusive<TRequestInfo>(
                requestActorId,
                0ull,
                MakeIntrusive<TCallContext>()),
            partitionActorId,
            std::move(tabletInfo),
            trimFreshLogToCommitId,
            3,    // recordGeneration
            1,    // perGenerationCounter
            {},   // freshChannels
            {},   // volumeLabels
            TDuration::Zero()));

        auto completed = runtime.GrabEdgeEvent<
            TEvPartitionCommonPrivate::TEvTrimFreshLogCompleted>(
            partitionActorId);
        UNIT_ASSERT(!HasError(completed->Get()->GetError()));
        UNIT_ASSERT_VALUES_EQUAL(
            trimFreshLogToCommitId,
            completed->Get()->CommitId);

        auto response = runtime.GrabEdgeEvent<
            TEvPartitionCommonPrivate::TEvTrimFreshLogResponse>(requestActorId);
        UNIT_ASSERT(!HasError(response->Get()->GetError()));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
