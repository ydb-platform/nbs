#include "migration_request_actor.h"

#include "ut_env.h"

#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/storage/core/libs/kikimr/helpers.h>

#include <ydb/core/mind/bscontroller/bsc.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <chrono>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NMonitoring;
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
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::PARTITION,
                    __PRETTY_FUNCTION__);
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

    Y_UNIT_TEST_F(ShouldDistinguishLeaderAndFollowerResponses, TFixture)
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

                TActorId recipient = event->Recipient;
                NProto::TError error;

                if (FakeFollower == event->Recipient) {
                    error = MakeError(E_IO, "I/O error");
                    // override the recipient
                    recipient = TActorId{42, 42};
                }

                runtime.SendAsync(new IEventHandle(
                    event->Sender,
                    recipient,
                    new TEvService::TEvWriteBlocksResponse(error),
                    0,   // flags
                    event->Cookie));

                return true;
            });

        TPartitionClient client = PartitionClient();

        const ui64 expectedCookie = 1000500;

        client.SendRequest(
            client.GetActorId(),
            client.CreateWriteBlocksRequest(
                TBlockRange64::WithLength(0, 1024),
                'X'),
            expectedCookie);

        auto response = client.RecvResponse<TEvService::TEvWriteBlocksResponse>(
            expectedCookie);

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetStatus(),
            FormatError(response->GetError()));

        UNIT_ASSERT(followerGotNonRetriableError.has_value());
        UNIT_ASSERT(followerGotNonRetriableError.value());
    }

// UnexpectedCookie crit. event halts the program in Debug build
#ifdef NDEBUG
    Y_UNIT_TEST_F(ShouldHandleInvalidCookie, TFixture)
    {
        const ui64 correctCookie = 1000500;
        const ui64 incorrectCookie = 0x1000500;

        auto counters = MakeIntrusive<TDynamicCounters>();
        InitCriticalEventsCounter(counters);
        auto unexpectedCookie =
            counters->GetCounter("AppImpossibleEvents/UnexpectedCookie", true);

        SetupRuntime();

        Runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                if (event->GetTypeRewrite() !=
                        TEvService::EvWriteBlocksRequest ||
                    (event->Recipient != FakeLeader &&
                     event->Recipient != FakeFollower))
                {
                    return false;
                }

                runtime.SendAsync(new IEventHandle(
                    event->Sender,
                    event->Recipient,
                    new TEvService::TEvWriteBlocksResponse(),
                    0,   // flags
                    incorrectCookie));

                return true;
            });

        TPartitionClient client = PartitionClient();

        UNIT_ASSERT_VALUES_EQUAL(0, unexpectedCookie->Val());

        client.SendRequest(
            client.GetActorId(),
            client.CreateWriteBlocksRequest(
                TBlockRange64::WithLength(0, 1024),
                'X'),
            correctCookie);

        Runtime.DispatchEvents({}, 10ms);

        TAutoPtr<NActors::IEventHandle> handle;
        Runtime.GrabEdgeEventRethrow<TEvService::TEvWriteBlocksResponse>(
            handle,
            5s);
        UNIT_ASSERT(!handle);

        UNIT_ASSERT_VALUES_EQUAL(1, unexpectedCookie->Val());
    }
#endif // NDEBUG

    Y_UNIT_TEST_F(ShouldHandleLeaderAndFollowerUndelivery, TFixture)
    {
        enum class EState
        {
            GrabEvents,
            SendResponses
        };
        SetupRuntime();

        EState state = EState::GrabEvents;
        std::optional<bool> followerGotNonRetriableError;
        TAutoPtr<IEventHandle> leaderRequest;
        TAutoPtr<IEventHandle> followerRequest;
        bool leaderUndelivery = false;
        bool firstReplyWithUndelivery = false;

        auto undeliveryFilter =
            [&](auto& runtime, TAutoPtr<IEventHandle>& event)
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

            if (state == EState::GrabEvents &&
                event->GetTypeRewrite() == TEvService::EvWriteBlocksRequest)
            {
                if (event->Recipient == FakeLeader) {
                    leaderRequest = event;
                } else if (event->Recipient == FakeFollower) {
                    followerRequest = event;
                } else {
                    return false;
                }

                if (leaderRequest && followerRequest) {
                    state = EState::SendResponses;

                    auto undeliveredEvent =
                        leaderUndelivery ? leaderRequest : followerRequest;
                    auto successfulEvent =
                        leaderUndelivery ? followerRequest : leaderRequest;

                    auto undeliveredResponse = std::make_unique<IEventHandle>(
                        undeliveredEvent->Sender,
                        undeliveredEvent->Sender,
                        undeliveredEvent->ReleaseBase().Release(),
                        0,
                        undeliveredEvent->Cookie,
                        nullptr);
                    auto successfulResponse = std::make_unique<IEventHandle>(
                        successfulEvent->Sender,
                        successfulEvent->Recipient,
                        new TEvService::TEvWriteBlocksResponse(MakeError(S_OK)),
                        0,   // flags
                        successfulEvent->Cookie);

                    if (firstReplyWithUndelivery) {
                        runtime.SendAsync(undeliveredResponse.release());
                        runtime.SendAsync(successfulResponse.release());
                    } else {
                        runtime.SendAsync(successfulResponse.release());
                        runtime.SendAsync(undeliveredResponse.release());
                    }
                }

                return true;
            }

            return false;
        };
        Runtime.SetEventFilter(undeliveryFilter);

        TPartitionClient client = PartitionClient();

        auto doCheck = [&](bool a, bool b)
        {
            state = EState::GrabEvents;
            followerGotNonRetriableError.reset();
            leaderRequest.Reset();
            followerRequest.Reset();
            leaderUndelivery = a;
            firstReplyWithUndelivery = b;

            client.SendWriteBlocksRequest(
                TBlockRange64::WithLength(0, 1024),
                'X');
            auto response = client.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                FormatError(response->GetError()));

            UNIT_ASSERT(followerGotNonRetriableError.has_value());
            UNIT_ASSERT(!followerGotNonRetriableError.value());
        };

        doCheck(false, false);
        doCheck(false, true);
        doCheck(true, false);
        doCheck(true, true);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
