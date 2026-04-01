#include "commit_queue.h"
#include "cloud/blockstore/libs/storage/core/request_info.h"

#include <contrib/ydb/core/testlib/actors/test_runtime.h>
#include <contrib/ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

enum ETestEvents
{
    EvBegin = EventSpaceBegin(NKikimr::TEvents::ES_USERSPACE + 10),

    EvEnqueue,
    EvDequeue,
    EvValueDequeued,
};

struct TEnqueue
{
    ui64 Value = 0;
    ui64 CommitId = 0;
};

struct TValueDequeued
{
    ui64 Value = 0;
};

using TEvEnqueue = TRequestEvent<TEnqueue, EvEnqueue>;

using TEvDequeue = TRequestEvent<TEmpty, EvDequeue>;

using TEvValueDequeued = TRequestEvent<TValueDequeued, EvValueDequeued>;

////////////////////////////////////////////////////////////////////////////////

class TMyTestEnv final
{
public:
    TActorId Sender;

private:
    TTestActorRuntime Runtime;

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

    void Send(const TActorId& recipient, IEventBasePtr event, ui64 cookie = 0)
    {
        Runtime.Send(
            new IEventHandle(recipient, Sender, event.release(), 0, cookie));
    }

    void DispatchEvents(TDuration timeout)
    {
        Runtime.DispatchEvents(TDispatchOptions(), timeout);
    }

    TAutoPtr<IEventHandle> GrabValueDequeued(TDuration timeout)
    {
        TAutoPtr<IEventHandle> handle;
        Runtime.GrabEdgeEventRethrow<TEvValueDequeued>(handle, timeout);
        return handle;
    }

    TTestActorRuntimeBase& AccessRuntime()
    {
        return Runtime;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TCommitQueueActor: public TActor<TCommitQueueActor>
{
    TCommitQueue Queue;

    TCommitQueueActor()
        : TActor(&TThis::Main)
    {}

    void Main(TAutoPtr<IEventHandle>& ev)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvEnqueue, HandleEnqueue);
            HFunc(TEvDequeue, HandleDequeue);
        }
    }

    void HandleEnqueue(const TEvEnqueue::TPtr& ev, const TActorContext& ctx)
    {
        Y_UNUSED(ctx);

        auto* msg = ev->Get();
        auto requestInfo =
            CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

        auto callback = [requestInfo = std::move(requestInfo),
                         value = msg->Value](const NActors::TActorContext& ctx)
        {
            NCloud::Reply(
                ctx,
                *requestInfo,
                std::make_unique<TEvValueDequeued>(value));
        };

        Queue.Enqueue(std::move(callback), msg->CommitId);
    }

    void HandleDequeue(const TEvDequeue::TPtr& ev, const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        auto callback = Queue.Dequeue();
        callback(ctx);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCommitQueueTest)
{
    Y_UNIT_TEST(ShouldKeepTrackOfCommits)
    {
        TMyTestEnv testEnv;

        auto actorId = testEnv.Register(std::make_unique<TCommitQueueActor>());

        testEnv.Send(actorId, std::make_unique<TEvEnqueue>(1, 1), 1);
        testEnv.Send(actorId, std::make_unique<TEvEnqueue>(2, 2), 2);
        testEnv.Send(actorId, std::make_unique<TEvEnqueue>(3, 3), 3);

        testEnv.DispatchEvents(10ms);

        auto* actor = dynamic_cast<TCommitQueueActor*>(
            testEnv.AccessRuntime().FindActor(actorId));

        UNIT_ASSERT(!actor->Queue.Empty());

        auto checkQueue = [&](ui64 expectedValue)
        {
            UNIT_ASSERT_EQUAL(actor->Queue.Peek(), expectedValue);
            testEnv.Send(actorId, std::make_unique<TEvDequeue>());
            auto ev = testEnv.GrabValueDequeued(100ms);
            UNIT_ASSERT_VALUES_EQUAL(
                expectedValue,
                ev->Get<TEvValueDequeued>()->Value);
        };

        checkQueue(1);
        checkQueue(2);
        checkQueue(3);

        UNIT_ASSERT(actor->Queue.Empty());
    }

}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
