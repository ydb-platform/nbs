#include "migration_request_actor.h"

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
    const TActorId Leader;
    const TActorId Follower;

    ui64 RequestIdentityKey = 0;

public:
    TTestMirrorPartition(const TActorId& leader, const TActorId& follower)
        : TActor(&TThis::StateWork)
        , Leader(leader)
        , Follower(follower)
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

        NCloud::Register<TMigrationRequestActor<TEvService::TWriteBlocksMethod>>(
            ctx,
            std::move(requestInfo),
            Leader,
            Follower,
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
    TActorId FakeLeader = TActorId{1, 1};
    TActorId FakeFollower = TActorId{2, 2};

    TPartitionClient PartitionClient()
    {
        return {Runtime, PartitionId, DefaultBlockSize};
    }

    void SetupRuntime()
    {
        Runtime.AddLocalService(
            PartitionId,
            TActorSetupCmd(
                new TTestMirrorPartition(FakeLeader, FakeFollower),
                TMailboxType::Simple,
                0));

        NKikimr::SetupTabletServices(Runtime);
        Runtime.DispatchEvents({}, 10ms);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMigrationRequestActorTest)
{
    Y_UNIT_TEST_F(ShouldForwardNonFatalErrorToClient, TFixture)
    {
        SetupRuntime();

        std::optional<bool> followerGotNonRetriableError;

        Runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvNonreplPartitionPrivate::EvWriteOrZeroCompleted)
                {
                    const auto* msg = event->template Get<
                        TEvNonreplPartitionPrivate::TEvWriteOrZeroCompleted>();

                    followerGotNonRetriableError =
                        msg->FollowerGotNonRetriableError;

                    return true;
                }

                if (event->GetTypeRewrite() !=
                        TEvService::EvWriteBlocksRequest ||
                    (event->Recipient != FakeLeader &&
                     event->Recipient != FakeFollower))
                {
                    return false;
                }

                NProto::TError error;

                if (FakeFollower == event->Recipient) {
                    error = MakeError(
                        E_RDMA_UNAVAILABLE,
                        "unable to allocate request");
                }

                runtime.SendAsync(new IEventHandle(
                    event->Sender,
                    event->Recipient,
                    new TEvService::TEvWriteBlocksResponse(error),
                    0,   // flags
                    event->Cookie));

                return true;
            });

        TPartitionClient client = PartitionClient();
        client.SendWriteBlocksRequest(TBlockRange64::WithLength(0, 1024), 'X');
        auto response = client.RecvWriteBlocksResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_RDMA_UNAVAILABLE,
            response->GetStatus(),
            FormatError(response->GetError()));

            UNIT_ASSERT(followerGotNonRetriableError.has_value());
            UNIT_ASSERT(!followerGotNonRetriableError.value());
    }

    Y_UNIT_TEST_F(ShouldIgnoreFatalErrorFromFollower, TFixture)
    {
        SetupRuntime();

        std::optional<bool> followerGotNonRetriableError;

        Runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvNonreplPartitionPrivate::EvWriteOrZeroCompleted)
                {
                    const auto* msg = event->template Get<
                        TEvNonreplPartitionPrivate::TEvWriteOrZeroCompleted>();

                    followerGotNonRetriableError =
                        msg->FollowerGotNonRetriableError;

                    return true;
                }

                if (event->GetTypeRewrite() !=
                        TEvService::EvWriteBlocksRequest ||
                    (event->Recipient != FakeLeader &&
                     event->Recipient != FakeFollower))
                {
                    return false;
                }

                NProto::TError error;

                if (FakeFollower == event->Recipient) {
                    error = MakeError(E_IO, "I/O error");
                }

                runtime.SendAsync(new IEventHandle(
                    event->Sender,
                    event->Recipient,
                    new TEvService::TEvWriteBlocksResponse(error),
                    0,   // flags
                    event->Cookie));

                return true;
            });

        TPartitionClient client = PartitionClient();
        client.SendWriteBlocksRequest(TBlockRange64::WithLength(0, 1024), 'X');
        auto response = client.RecvWriteBlocksResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetStatus(),
            FormatError(response->GetError()));

        UNIT_ASSERT(followerGotNonRetriableError.has_value());
        UNIT_ASSERT(followerGotNonRetriableError.value());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
