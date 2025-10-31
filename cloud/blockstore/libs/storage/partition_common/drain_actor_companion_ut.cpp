#include "drain_actor_companion.h"

#include <cloud/blockstore/libs/storage/api/partition.h>
#include <cloud/blockstore/libs/storage/core/config.h>

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

enum ETestEvents
{
    EvSetWriteInProgressCount =
        EventSpaceBegin(NKikimr::TEvents::ES_USERSPACE + 10),
};

struct TEvSetWriteInProgressCount
    : public NActors::TEventBase<
          TEvSetWriteInProgressCount,
          ETestEvents::EvSetWriteInProgressCount>
{
    ui32 WriteCount = 0;

    TEvSetWriteInProgressCount(ui32 writeCount)
        : WriteCount(writeCount)
    {}

    DEFINE_SIMPLE_LOCAL_EVENT(
        TEvSetWriteInProgressCount,
        "TEvSetWriteInProgressCount");
};

////////////////////////////////////////////////////////////////////////////////

class TMyTestEnv final
{
private:
    TTestBasicRuntime Runtime;
    TActorId Sender;

public:
    TMyTestEnv()
    {
        SetupLogging();
        NKikimr::SetupTabletServices(Runtime);

        Sender = Runtime.AllocateEdgeActor();
    }

    void SetupLogging()
    {
        Runtime.AppendToLogSettings(
            TBlockStoreComponents::START,
            TBlockStoreComponents::END,
            GetComponentName);

        for (ui32 i = TBlockStoreComponents::START;
             i < TBlockStoreComponents::END;
             ++i)
        {
            Runtime.SetLogPriority(i, NLog::PRI_TRACE);
        }
    }

    TActorId Register(IActorPtr actor)
    {
        auto actorId = Runtime.Register(actor.release());
        Runtime.EnableScheduleForActor(actorId);

        return actorId;
    }

    void Send(const TActorId& recipient, IEventBasePtr event)
    {
        Runtime.Send(new IEventHandle(recipient, Sender, event.release()));
    }

    void DispatchEvents()
    {
        Runtime.DispatchEvents(TDispatchOptions(), TDuration());
    }

    THolder<NPartition::TEvPartition::TEvDrainResponse> GrabDrainResponse()
    {
        return Runtime
            .GrabEdgeEvent<NPartition::TEvPartition::TEvDrainResponse>(
                TDuration());
    }

    THolder<NPartition::TEvPartition::TEvWaitForInFlightWritesResponse>
    GrabWaitForInFlightWritesResponse()
    {
        return Runtime.GrabEdgeEvent<
            NPartition::TEvPartition::TEvWaitForInFlightWritesResponse>(
            TDuration());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TActorWithDrain
    : public TActor<TActorWithDrain>
    , private IRequestsInProgress
{
    ui32 CurrentWriteRequestInProgress = 0;
    bool WaitingForInFlightWrites = false;
    TDrainActorCompanion drainCompanion{*this, "LoggingId"};

public:
    TActorWithDrain()
        : TActor(&TThis::Main)
    {}

    void Main(TAutoPtr<IEventHandle>& ev)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(NPartition::TEvPartition::TEvDrainRequest, HandleDrain);
            HFunc(
                NPartition::TEvPartition::TEvWaitForInFlightWritesRequest,
                HandleWaitForInFlightWrites);
            HFunc(TEvSetWriteInProgressCount, HandleSetWriteCount);
            default:
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::PARTITION,
                    __PRETTY_FUNCTION__);
        }
    }

private:
    bool WriteRequestInProgress() const override
    {
        return CurrentWriteRequestInProgress != 0;
    }

    void WaitForInFlightWrites() override {
        WaitingForInFlightWrites = true;
    }

    bool IsWaitingForInFlightWrites() const override {
        return WaitingForInFlightWrites && WriteRequestInProgress();
    }

    void HandleDrain(
        const NPartition::TEvPartition::TEvDrainRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        drainCompanion.HandleDrain(ev, ctx);
    }

    void HandleWaitForInFlightWrites(
        const NPartition::TEvPartition::TEvWaitForInFlightWritesRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        drainCompanion.HandleWaitForInFlightWrites(ev, ctx);
    }

    void HandleSetWriteCount(
        const TEvSetWriteInProgressCount::TPtr& ev,
        const TActorContext& ctx)
    {
        CurrentWriteRequestInProgress = ev->Get()->WriteCount;
        if (!WriteRequestInProgress()) {
            WaitingForInFlightWrites = false;
        }
        drainCompanion.ProcessDrainRequests(ctx);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDrainActorCompanionTest)
{
    Y_UNIT_TEST(Basic)
    {
        TMyTestEnv testEnv;

        auto actorId = testEnv.Register(std::make_unique<TActorWithDrain>());

        // Ask for drain
        testEnv.Send(
            actorId,
            std::make_unique<NPartition::TEvPartition::TEvDrainRequest>());
        testEnv.DispatchEvents();

        {  // Get drain response
            auto drainResponse = testEnv.GrabDrainResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, drainResponse->GetStatus());
        }
    }

    Y_UNIT_TEST(WriteInProgress)
    {
        TMyTestEnv testEnv;

        auto actorId = testEnv.Register(std::make_unique<TActorWithDrain>());

        // Set actor have one write inflight
        testEnv.Send(actorId, std::make_unique<TEvSetWriteInProgressCount>(1));

        // Ask for drain
        testEnv.Send(
            actorId,
            std::make_unique<NPartition::TEvPartition::TEvDrainRequest>());
        testEnv.DispatchEvents();

        {  // Assert drain not finished since write in progress
            auto drainResponse = testEnv.GrabDrainResponse();
            UNIT_ASSERT_VALUES_EQUAL(nullptr, drainResponse);
        }

        // Finish write
        testEnv.Send(actorId, std::make_unique<TEvSetWriteInProgressCount>(0));
        testEnv.DispatchEvents();

        {  // Get drain response
            auto drainResponse = testEnv.GrabDrainResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, drainResponse->GetStatus());
        }
    }

    Y_UNIT_TEST(MultiWriteInProgress)
    {
        TMyTestEnv testEnv;

        auto actorId = testEnv.Register(std::make_unique<TActorWithDrain>());

        // Set actor have write inflight
        testEnv.Send(actorId, std::make_unique<TEvSetWriteInProgressCount>(1));

        // Ask for drain
        testEnv.Send(
            actorId,
            std::make_unique<NPartition::TEvPartition::TEvDrainRequest>());
        testEnv.DispatchEvents();

        {  // Assert drain not finished since write in progress
            auto drainResponse = testEnv.GrabDrainResponse();
            UNIT_ASSERT_VALUES_EQUAL(nullptr, drainResponse);
        }

        // Set actor have two writes inflight
        testEnv.Send(actorId, std::make_unique<TEvSetWriteInProgressCount>(2));
        testEnv.DispatchEvents();

        {  // Assert drain not finished since write in progress
            auto drainResponse = testEnv.GrabDrainResponse();
            UNIT_ASSERT_VALUES_EQUAL(nullptr, drainResponse);
        }

        // Finish first write
        testEnv.Send(actorId, std::make_unique<TEvSetWriteInProgressCount>(1));
        testEnv.DispatchEvents();

        {  // Assert drain not finished since write in progress
            auto drainResponse = testEnv.GrabDrainResponse();
            UNIT_ASSERT_VALUES_EQUAL(nullptr, drainResponse);
        }

        // Finish second write
        testEnv.Send(actorId, std::make_unique<TEvSetWriteInProgressCount>(0));
        testEnv.DispatchEvents();

        {  // Get drain response
            auto drainResponse = testEnv.GrabDrainResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, drainResponse->GetStatus());
        }
    }

    Y_UNIT_TEST(MultiDrainRequests)
    {
        TMyTestEnv testEnv;

        auto actorId = testEnv.Register(std::make_unique<TActorWithDrain>());

        // Set actor have write inflight
        testEnv.Send(actorId, std::make_unique<TEvSetWriteInProgressCount>(1));

        // Ask for drain twice
        testEnv.Send(
            actorId,
            std::make_unique<NPartition::TEvPartition::TEvDrainRequest>());
        testEnv.Send(
            actorId,
            std::make_unique<NPartition::TEvPartition::TEvDrainRequest>());
        testEnv.DispatchEvents();

        {  // Assert drain not finished since write in progress
            auto drainResponse = testEnv.GrabDrainResponse();
            UNIT_ASSERT_VALUES_EQUAL(nullptr, drainResponse);
        }

        // Finish write
        testEnv.Send(actorId, std::make_unique<TEvSetWriteInProgressCount>(0));
        testEnv.DispatchEvents();

        {  // Get first drain response
            auto drainResponse = testEnv.GrabDrainResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, drainResponse->GetStatus());
        }

        {  // Get second drain response
            auto drainResponse = testEnv.GrabDrainResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, drainResponse->GetStatus());
        }

        {  // Assert no more drain responses
            auto drainResponse = testEnv.GrabDrainResponse();
            UNIT_ASSERT_VALUES_EQUAL(nullptr, drainResponse);
        }
    }

    Y_UNIT_TEST(MultiDrainOverflow)
    {
        TMyTestEnv testEnv;

        // Check maxDrainRequests from TDrainActorCompanion::HandleDrain
        constexpr ui32 MaxDrainRequestCount = 10;

        auto actorId = testEnv.Register(std::make_unique<TActorWithDrain>());

        // Set actor have write inflight
        testEnv.Send(actorId, std::make_unique<TEvSetWriteInProgressCount>(1));

        // Ask for drain MaxDrainRequestCount
        for (ui32 i = 0; i < MaxDrainRequestCount; ++i) {
            testEnv.Send(
                actorId,
                std::make_unique<NPartition::TEvPartition::TEvDrainRequest>());
        }

        // Ask for drain once more
        testEnv.Send(
            actorId,
            std::make_unique<NPartition::TEvPartition::TEvDrainRequest>());
        {  // Assert drain rejected
            auto drainResponse = testEnv.GrabDrainResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, drainResponse->GetStatus());
        }

        // Finish write
        testEnv.Send(actorId, std::make_unique<TEvSetWriteInProgressCount>(0));
        testEnv.DispatchEvents();

        // Expect MaxDrainRequestCount drain responses
        for (ui32 i = 0; i < MaxDrainRequestCount; ++i) {
            {  // Get first drain response
                auto drainResponse = testEnv.GrabDrainResponse();
                UNIT_ASSERT_VALUES_EQUAL(S_OK, drainResponse->GetStatus());
            }
        }

        {  // Assert no more drain responses
            auto drainResponse = testEnv.GrabDrainResponse();
            UNIT_ASSERT_VALUES_EQUAL(nullptr, drainResponse);
        }
    }

    Y_UNIT_TEST(BasicWaitForInFlightWrites)
    {
        TMyTestEnv testEnv;

        auto actorId = testEnv.Register(std::make_unique<TActorWithDrain>());

        // Waiting for in-flight writes.
        testEnv.Send(
            actorId,
            std::make_unique<
                NPartition::TEvPartition::TEvWaitForInFlightWritesRequest>());
        testEnv.DispatchEvents();

        {   // Get the response
            auto waitResponse = testEnv.GrabWaitForInFlightWritesResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, waitResponse->GetStatus());
        }
    }

    Y_UNIT_TEST(WaitForInFlightWritesWithWriteInProgress)
    {
        TMyTestEnv testEnv;

        auto actorId = testEnv.Register(std::make_unique<TActorWithDrain>());

        // Set actor have one write inflight
        testEnv.Send(actorId, std::make_unique<TEvSetWriteInProgressCount>(1));

        // Waiting for in-flight writes.
        testEnv.Send(
            actorId,
            std::make_unique<
                NPartition::TEvPartition::TEvWaitForInFlightWritesRequest>());
        testEnv.DispatchEvents();

        {   // Assert waiting is not finished since the write is in progress
            auto waitResponse = testEnv.GrabWaitForInFlightWritesResponse();
            UNIT_ASSERT_VALUES_EQUAL(nullptr, waitResponse);
        }

        // Sending another wait.
        testEnv.Send(
            actorId,
            std::make_unique<
                NPartition::TEvPartition::TEvWaitForInFlightWritesRequest>());
        testEnv.DispatchEvents();

        {   // It should be rejected because the actor is already waiting.
            auto waitResponse = testEnv.GrabWaitForInFlightWritesResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, waitResponse->GetStatus());
        }

        // Finish write
        testEnv.Send(actorId, std::make_unique<TEvSetWriteInProgressCount>(0));
        testEnv.DispatchEvents();

        {   // Get the response
            auto waitResponse = testEnv.GrabWaitForInFlightWritesResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, waitResponse->GetStatus());
        }
    }
}

}  // namespace NCloud::NBlockStore::NStorage
