#include "mirror_request_actor.h"

#include "ut_env.h"

#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/storage/core/libs/kikimr/helpers.h>

#include <contrib/ydb/core/mind/bscontroller/bsc.h>
#include <contrib/ydb/core/testlib/basics/runtime.h>
#include <contrib/ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <chrono>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTestMirrorPartition
    : public TActor<TTestMirrorPartition>
{
private:
    const TVector<TActorId> Partitions;
    ui64 RequestIdentityKey = 0;

public:
    explicit TTestMirrorPartition(TVector<TActorId> partitions)
        : TActor(&TThis::StateWork)
        , Partitions(std::move(partitions))
    {}

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvService::TEvWriteBlocksRequest, HandleWriteBlocks);
            IgnoreFunc(TEvNonreplPartitionPrivate::TEvWriteOrZeroCompleted);
            default:
                HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION);
                break;
        }
    }

    void HandleWriteBlocks(
        const TEvService::TEvWriteBlocksRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();

        auto requestInfo =
            CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

        NCloud::Register<TMirrorRequestActor<TEvService::TWriteBlocksMethod>>(
            ctx,
            std::move(requestInfo),
            Partitions,
            msg->Record,
            msg->Record.GetDiskId(),
            SelfId(),   // parentActorId
            ++RequestIdentityKey);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    TTestBasicRuntime Runtime;
    TActorId PartitionId = TActorId{0, "partition"};
    TVector<TActorId> FakeReplicaIds{
        TActorId{1, 1},
        TActorId{2, 2},
        TActorId{3, 3},
    };

    [[nodiscard]] bool IsFakeReplica(TActorId actorId) const
    {
        return AnyOf(FakeReplicaIds, [&](auto r) { return r == actorId; });
    }

    TPartitionClient PartitionClient()
    {
        return {Runtime, PartitionId, DefaultBlockSize};
    }

    void SetUp(NUnitTest::TTestContext& /*testContext*/) override
    {
        Runtime.AddLocalService(
            PartitionId,
            TActorSetupCmd(
                new TTestMirrorPartition(FakeReplicaIds),
                TMailboxType::Simple,
                0));

        NKikimr::SetupTabletServices(Runtime);
        Runtime.DispatchEvents({}, 10ms);
    }

    void ShouldHandleErrorFromReplica(bool emptySender)
    {
        Runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                if (event->GetTypeRewrite() !=
                        TEvService::EvWriteBlocksRequest ||
                    !IsFakeReplica(event->Recipient))
                {
                    return false;
                }

                NProto::TError error;

                if (FakeReplicaIds[1] == event->Recipient) {
                    error = MakeError(
                        E_RDMA_UNAVAILABLE,
                        "unable to allocate request");
                }

                const TActorId sender = emptySender
                    ? TActorId{}
                    : event->Recipient;

                runtime.Send(new IEventHandle(
                    event->Sender,
                    sender,
                    new TEvService::TEvWriteBlocksResponse(error),
                    0,   // flags
                    event->Cookie));

                return true;
            });

        TPartitionClient client = PartitionClient();
        client.SendWriteBlocksRequest(TBlockRange64::WithLength(0, 1024), 'X');
        auto response = client.RecvWriteBlocksResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            response->GetStatus(),
            FormatError(response->GetError()));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMirrorRequestActorTest)
{
    Y_UNIT_TEST_F(ShouldHandleErrorFromReplicaWithEmptySender, TFixture)
    {
        ShouldHandleErrorFromReplica(true);
    }

    Y_UNIT_TEST_F(ShouldHandleErrorFromReplica, TFixture)
    {
        ShouldHandleErrorFromReplica(false);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
