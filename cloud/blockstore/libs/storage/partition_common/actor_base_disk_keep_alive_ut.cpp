#include "actor_base_disk_keep_alive.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/model/log_title.h>

#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// EdgeActor is defined in actor_describe_base_disk_blocks_ut.cpp (same UT
// binary); reuse them.
extern NActors::TActorId EdgeActor;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration KeepAliveInterval = TDuration::Seconds(5);

TChildLogTitle MakeLogTitle()
{
    TLogTitle title(
        /*startTime = */ 0,
        TLogTitle::TPartition{
            .TabletId = 0,
            .DiskId = "OverlayDiskId",
            .PartitionIndex = 0,
            .PartitionCount = 1,
            .Generation = 1,
        });

    return title.GetChildWithTags(
        /*startTime = */ 0,
        {{"BaseDiskId", "BaseDiskId"}});
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBaseDiskKeepAliveActorTests)
{
    struct TActorSystem: NActors::TTestActorRuntimeBase
    {
        void Start()
        {
            SetScheduledEventFilter(
                [](NActors::TTestActorRuntimeBase& runtime,
                   TAutoPtr<NActors::IEventHandle>& event,
                   TDuration delay,
                   TInstant& deadline)
                {
                    Y_UNUSED(delay);
                    Y_UNUSED(deadline);
                    return !runtime.IsScheduleForActorEnabled(
                        event->GetRecipientRewrite());
                });
            InitNodes();
            AppendToLogSettings(
                TBlockStoreComponents::START,
                TBlockStoreComponents::END,
                GetComponentName);
            for (ui32 i = TBlockStoreComponents::START;
                 i < TBlockStoreComponents::END;
                 ++i)
            {
                SetLogPriority(i, NActors::NLog::PRI_INFO);
            }
        }

        void SimulateSleep(TDuration duration)
        {
            AdvanceCurrentTime(duration);
            DispatchEvents(
                NActors::TDispatchOptions(),
                TDuration::MilliSeconds(50));
        }
    };

    struct TSetupEnvironment: public TCurrentTestCase
    {
        TActorSystem ActorSystem;

        void SetUp(NUnitTest::TTestContext&) override
        {
            ActorSystem.Start();
            EdgeActor = ActorSystem.AllocateEdgeActor();
        }
    };

    Y_UNIT_TEST_F(ShouldPingOnBootstrap, TSetupEnvironment)
    {
        ActorSystem.Register(new TBaseDiskKeepAliveActor(
            "BaseDiskId",
            KeepAliveInterval,
            MakeLogTitle()));

        auto request =
            ActorSystem.GrabEdgeEvent<TEvVolumeProxy::TEvKeepAliveRequest>();

        UNIT_ASSERT(request);
        UNIT_ASSERT_VALUES_EQUAL("BaseDiskId", request->DiskId);
    }

    Y_UNIT_TEST_F(ShouldPingPeriodically, TSetupEnvironment)
    {
        const auto actorId = ActorSystem.Register(new TBaseDiskKeepAliveActor(
            "BaseDiskId",
            KeepAliveInterval,
            MakeLogTitle()));
        ActorSystem.EnableScheduleForActor(actorId);

        auto first =
            ActorSystem.GrabEdgeEvent<TEvVolumeProxy::TEvKeepAliveRequest>();
        UNIT_ASSERT(first);
        UNIT_ASSERT_VALUES_EQUAL("BaseDiskId", first->DiskId);

        ActorSystem.SimulateSleep(KeepAliveInterval + TDuration::Seconds(1));

        auto second =
            ActorSystem.GrabEdgeEvent<TEvVolumeProxy::TEvKeepAliveRequest>();
        UNIT_ASSERT(second);
        UNIT_ASSERT_VALUES_EQUAL("BaseDiskId", second->DiskId);
    }

    Y_UNIT_TEST_F(ShouldNotSendOverlappingPings, TSetupEnvironment)
    {
        const auto actorId = ActorSystem.Register(new TBaseDiskKeepAliveActor(
            "BaseDiskId",
            KeepAliveInterval,
            MakeLogTitle()));

        int pingCount = 0;
        ActorSystem.SetEventFilter(
            [&](NActors::TTestActorRuntimeBase& runtime,
                TAutoPtr<NActors::IEventHandle>& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvVolumeProxy::EvKeepAliveRequest) {
                    UNIT_ASSERT_VALUES_EQUAL(actorId, event->Sender);
                    ++pingCount;
                    return true;
                }
                return false;
            });

        UNIT_ASSERT_VALUES_EQUAL(0, pingCount);

        ActorSystem.DispatchEvents(
            {.FinalEvents = {{NActors::TEvents::TSystem::Bootstrap}}},
            TDuration::MilliSeconds(50));

        UNIT_ASSERT_VALUES_EQUAL(1, pingCount);

        // Inject an early Wakeup — the new ping must be suppressed as
        // overlapping.
        ActorSystem.Send(
            new NActors::IEventHandle(
                actorId,
                EdgeActor,
                new NActors::TEvents::TEvWakeup()),
            0,
            true);

        ActorSystem.DispatchEvents(
            {.FinalEvents = {{NActors::TEvents::TEvWakeup::EventType}}},
            TDuration::MilliSeconds(50));

        UNIT_ASSERT_VALUES_EQUAL(1, pingCount);

        // Now reply to the ping to reset the in-flight flag and send Wakeup
        // again.
        ActorSystem.Send(
            new NActors::IEventHandle(
                actorId,
                EdgeActor,
                new TEvVolumeProxy::TEvKeepAliveResponse()),
            0,
            true);
        ActorSystem.Send(
            new NActors::IEventHandle(
                actorId,
                EdgeActor,
                new NActors::TEvents::TEvWakeup()),
            0,
            true);

        ActorSystem.DispatchEvents(
            {.FinalEvents = {{NActors::TEvents::TEvWakeup::EventType}}},
            TDuration::MilliSeconds(50));

        UNIT_ASSERT_VALUES_EQUAL(2, pingCount);
    }

    Y_UNIT_TEST_F(ShouldSurvivePingFailure, TSetupEnvironment)
    {
        const auto actorId = ActorSystem.Register(new TBaseDiskKeepAliveActor(
            "BaseDiskId",
            KeepAliveInterval,
            MakeLogTitle()));
        ActorSystem.EnableScheduleForActor(actorId);

        auto first =
            ActorSystem.GrabEdgeEvent<TEvVolumeProxy::TEvKeepAliveRequest>();
        UNIT_ASSERT(first);
        UNIT_ASSERT_VALUES_EQUAL("BaseDiskId", first->DiskId);

        // Reply with an error — actor must keep scheduling.
        ActorSystem.Send(new NActors::IEventHandle(
            actorId,
            EdgeActor,
            new TEvVolumeProxy::TEvKeepAliveResponse(
                MakeError(E_REJECTED, "induced failure"))));

        ActorSystem.DispatchEvents(
            NActors::TDispatchOptions(),
            TDuration::MilliSeconds(50));

        auto second =
            ActorSystem.GrabEdgeEvent<TEvVolumeProxy::TEvKeepAliveRequest>();
        UNIT_ASSERT(second);
        UNIT_ASSERT_VALUES_EQUAL("BaseDiskId", second->DiskId);
    }

    Y_UNIT_TEST_F(ShouldResendPingWhenPreviousResponseIsLost, TSetupEnvironment)
    {
        const auto actorId = ActorSystem.Register(new TBaseDiskKeepAliveActor(
            "BaseDiskId",
            KeepAliveInterval,
            MakeLogTitle()));
        ActorSystem.EnableScheduleForActor(actorId);

        auto first =
            ActorSystem.GrabEdgeEvent<TEvVolumeProxy::TEvKeepAliveRequest>();
        UNIT_ASSERT(first);
        UNIT_ASSERT_VALUES_EQUAL("BaseDiskId", first->DiskId);

        // After a full interval passes with no response, the actor must give
        // up on the lost ping and send a fresh one instead of getting stuck.
        ActorSystem.AdvanceCurrentTime(
            KeepAliveInterval + TDuration::Seconds(1));

        auto second =
            ActorSystem.GrabEdgeEvent<TEvVolumeProxy::TEvKeepAliveRequest>();
        UNIT_ASSERT(second);
        UNIT_ASSERT_VALUES_EQUAL("BaseDiskId", second->DiskId);
    }

    Y_UNIT_TEST_F(ShouldStopOnPoisonPill, TSetupEnvironment)
    {
        const auto actorId = ActorSystem.Register(new TBaseDiskKeepAliveActor(
            "BaseDiskId",
            KeepAliveInterval,
            MakeLogTitle()));

        int pingCount = 0;
        ActorSystem.SetEventFilter(
            [&](NActors::TTestActorRuntimeBase& runtime,
                TAutoPtr<NActors::IEventHandle>& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvVolumeProxy::EvKeepAliveRequest) {
                    UNIT_ASSERT_VALUES_EQUAL(actorId, event->Sender);
                    ++pingCount;
                    return true;
                }
                return false;
            });

        ActorSystem.DispatchEvents(
            {.FinalEvents = {{NActors::TEvents::TSystem::Bootstrap}}},
            TDuration::MilliSeconds(50));

        UNIT_ASSERT_VALUES_EQUAL(1, pingCount);

        // Reply OK, then poison the actor.
        ActorSystem.Send(
            new NActors::IEventHandle(
                actorId,
                EdgeActor,
                new TEvVolumeProxy::TEvKeepAliveResponse()),
            0,
            true);
        ActorSystem.Send(
            new NActors::IEventHandle(
                actorId,
                EdgeActor,
                new NActors::TEvents::TEvPoisonPill()),
            0,
            true);

        ActorSystem.DispatchEvents(
            {.FinalEvents = {{NActors::TEvents::TEvPoisonPill::EventType}}},
            TDuration::MilliSeconds(50));

        UNIT_ASSERT_VALUES_EQUAL(1, pingCount);

        // A wakeup delivered after the poison must not produce a ping.
        ActorSystem.Send(
            new NActors::IEventHandle(
                actorId,
                EdgeActor,
                new NActors::TEvents::TEvWakeup()),
            0,
            true);

        ActorSystem.DispatchEvents(
            {.FinalEvents = {{NActors::TEvents::TEvWakeup::EventType}}},
            TDuration::MilliSeconds(50));

        UNIT_ASSERT_VALUES_EQUAL(1, pingCount);
    }

    Y_UNIT_TEST_F(ShouldCancelInflightPingOnPoison, TSetupEnvironment)
    {
        const auto actorId = ActorSystem.Register(new TBaseDiskKeepAliveActor(
            "BaseDiskId",
            KeepAliveInterval,
            MakeLogTitle()));

        int pingCount = 0;
        ActorSystem.SetEventFilter(
            [&](NActors::TTestActorRuntimeBase& runtime,
                TAutoPtr<NActors::IEventHandle>& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvVolumeProxy::EvKeepAliveRequest) {
                    UNIT_ASSERT_VALUES_EQUAL(actorId, event->Sender);
                    ++pingCount;
                    return true;
                }
                return false;
            });

        ActorSystem.DispatchEvents(
            {.FinalEvents = {{NActors::TEvents::TSystem::Bootstrap}}},
            TDuration::MilliSeconds(50));

        UNIT_ASSERT_VALUES_EQUAL(1, pingCount);

        // Poison while the response is still outstanding, then deliver a
        // (late) response and a wakeup — the actor is gone, all are dropped.
        ActorSystem.Send(
            new NActors::IEventHandle(
                actorId,
                EdgeActor,
                new NActors::TEvents::TEvPoisonPill()),
            0,
            true);
        ActorSystem.Send(
            new NActors::IEventHandle(
                actorId,
                EdgeActor,
                new TEvVolumeProxy::TEvKeepAliveResponse()),
            0,
            true);
        ActorSystem.Send(
            new NActors::IEventHandle(
                actorId,
                EdgeActor,
                new NActors::TEvents::TEvWakeup()),
            0,
            true);

        ActorSystem.DispatchEvents(
            {.FinalEvents = {{NActors::TEvents::TEvWakeup::EventType}}},
            TDuration::MilliSeconds(50));

        UNIT_ASSERT_VALUES_EQUAL(1, pingCount);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
