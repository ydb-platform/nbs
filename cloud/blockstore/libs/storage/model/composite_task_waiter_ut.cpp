#include "composite_task_waiter.h"

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TMyTestEnv final
{
private:
    TTestBasicRuntime Runtime;
    TActorId Sender;

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

    void DispatchEvents()
    {
        Runtime.DispatchEvents(TDispatchOptions(), TDuration());
    }

    THolder<TEvents::TEvPong> GrabPongResponse()
    {
        return Runtime.GrabEdgeEvent<TEvents::TEvPong>(TDuration());
    }
};

////////////////////////////////////////////////////////////////////////////////

// Very simple actor. It just responds to each Ping request with a Pong.
class TPingPongActor: public TActor<TPingPongActor>
{
public:
    TPingPongActor()
        : TActor(&TThis::Main)
    {}

    void Main(TAutoPtr<IEventHandle>& ev)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvPing, HandlePing);
        }
    }

private:
    void HandlePing(const TEvents::TEvPing::TPtr& ev, const TActorContext& ctx)
    {
        ctx.Send(
            ev->Sender,
            std::make_unique<TEvents::TEvPong>(),
            0,
            ev->Cookie);
    }
};

////////////////////////////////////////////////////////////////////////////////

// This actor launches a dozen Pings to the underlying actor for every Ping
// request and responds with a Pong when it receives all the responses from the
// underlying actor.
class TPrincipalPingPongActor: public TActor<TPrincipalPingPongActor>
{
    TActorId Dependent;
    TCompositeTaskList TaskList;

public:
    TPrincipalPingPongActor(TActorId dependent)
        : TActor(&TThis::Main)
        , Dependent(dependent)
    {}

    void Main(TAutoPtr<IEventHandle>& ev)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvPing, HandlePing);
            HFunc(TEvents::TEvPong, HandleDependentPong);
        }
    }

private:
    void HandlePing(const TEvents::TEvPing::TPtr& ev, const TActorContext& ctx)
    {
        constexpr size_t DEPENDENT_TASK_COUNT = 10;

        auto* principalTask = TaskList.StartPrincipalTask();

        TRequestInfo requestInfo{ev->Sender, 0, nullptr};
        // Prepare response. We will send it when receive all Pong responses
        // from the underlying actor.
        principalTask->ArmReply(
            requestInfo,
            std::make_unique<TEvents::TEvPong>());

        for (size_t i = 0; i < DEPENDENT_TASK_COUNT; ++i) {
            ctx.Send(
                Dependent,
                std::make_unique<TEvents::TEvPing>(),
                0,
                TaskList.StartDependentTaskAwait(
                    principalTask->GetPrincipalTaskId()));
        }
    }

    void HandleDependentPong(
        const TEvents::TEvPong::TPtr& ev,
        const TActorContext& ctx)
    {
        TaskList.FinishDependentTaskAwait(ev->Cookie, ctx);
    }
};

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCompositeTaskListTest)
{
    Y_UNIT_TEST(Basic)
    {
        TMyTestEnv testEnv;
        auto dependent = testEnv.Register(std::make_unique<TPingPongActor>());
        auto principal = testEnv.Register(
            std::make_unique<TPrincipalPingPongActor>(dependent));

        testEnv.Send(principal, std::make_unique<TEvents::TEvPing>());
        testEnv.Send(principal, std::make_unique<TEvents::TEvPing>());

        testEnv.DispatchEvents();

        UNIT_ASSERT(testEnv.GrabPongResponse());
        UNIT_ASSERT(testEnv.GrabPongResponse());
    }
}

} // namespace NCloud::NBlockStore::NStorage
