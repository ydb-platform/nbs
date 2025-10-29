#include "long_running_operation_companion.h"

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TMyTestEnv final
{
public:
    TActorId Sender;

private:
    TTestBasicRuntime Runtime;

public:
    TMyTestEnv()
    {
        NKikimr::SetupTabletServices(Runtime);

        Sender = Runtime.AllocateEdgeActor();
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

    void DispatchEvents(TDuration timeout)
    {
        Runtime.DispatchEvents(TDispatchOptions(), timeout);
    }

    THolder<TEvPartitionCommonPrivate::TEvLongRunningOperation>
    GrabLongRunningEvent()
    {
        return Runtime
            .GrabEdgeEvent<TEvPartitionCommonPrivate::TEvLongRunningOperation>(
                TDuration());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TActorWithCompanion
    : public TActor<TActorWithCompanion>
    , public TLongRunningOperationCompanion
{
public:
    TActorWithCompanion(TActorId parentActor)
        : TActor(&TThis::Main)
        , TLongRunningOperationCompanion(
              parentActor,
              parentActor,
              TDuration::MilliSeconds(500),
              TLongRunningOperationCompanion::EOperation::DontCare,
              0)
    {}

    void Main(TAutoPtr<IEventHandle>& ev)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvBootstrap, HandleBootstrap);

            HFunc(
                TEvents::TEvWakeup,
                TLongRunningOperationCompanion::HandleTimeout);
        }
    }

    void HandleBootstrap(
        const TEvents::TEvBootstrap::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        TLongRunningOperationCompanion::RequestStarted(ctx);
    }
};

}   // namespace
////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLongRunningOperationCompanionTest)
{
    Y_UNIT_TEST(Basic)
    {
        TMyTestEnv testEnv;

        auto actorId = testEnv.Register(
            std::make_unique<TActorWithCompanion>(testEnv.Sender));

        testEnv.Send(actorId, std::make_unique<TEvents::TEvBootstrap>());

        testEnv.DispatchEvents(TDuration());
        {
            auto longRunningEvent = testEnv.GrabLongRunningEvent();
            UNIT_ASSERT_VALUES_EQUAL(nullptr, longRunningEvent);
        }

        testEnv.DispatchEvents(TDuration::Seconds(1));
        {
            // Get two LongRunningEvent (for TPartitionActor and TVolumeActor)
            auto longRunningEvent = testEnv.GrabLongRunningEvent();
            UNIT_ASSERT_VALUES_EQUAL(
                TLongRunningOperationCompanion::EOperation::DontCare,
                longRunningEvent.Get()->Operation);

            longRunningEvent = testEnv.GrabLongRunningEvent();
            UNIT_ASSERT_VALUES_EQUAL(
                TLongRunningOperationCompanion::EOperation::DontCare,
                longRunningEvent.Get()->Operation);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
