#include "partition_statistic_actor.h"

#include <contrib/ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartitionStatisticActorTests)
{
    struct TActorSystem: NActors::TTestActorRuntimeBase
    {
        void Start()
        {
            InitNodes();
            AppendToLogSettings(
                TBlockStoreComponents::START,
                TBlockStoreComponents::END,
                GetComponentName);
        }
    };

    struct TSetupEnvironment: public TCurrentTestCase
    {
        NActors::TActorId EdgeActor;
        TActorSystem ActorSystem;

        void SetUp(NUnitTest::TTestContext& ctx) override
        {
            Y_UNUSED(ctx);

            ActorSystem.Start();
            EdgeActor = ActorSystem.AllocateEdgeActor();
        }
    };

    Y_UNIT_TEST_F(
        ShouldPartitionStatisticActorSendResponseToVolume,
        TSetupEnvironment)
    {
        NProto::TPartitionConfig partitionConfig;
        TPartitionInfoList partInfoList;
        partInfoList.emplace_back(
            0,   // tabletId
            partitionConfig,
            0,                       // partitionIndex
            TDuration::Seconds(0),   // timeoutIncrement
            TDuration::Seconds(0)    // timeoutMax
        );

        TActorsStack actors;
        actors.Push(EdgeActor);
        partInfoList[0].SetStarted(actors);

        auto partitionStatisticActor = ActorSystem.Register(
            new TPartitionStatisticActor{EdgeActor, partInfoList});

        ActorSystem
            .GrabEdgeEvent<TEvStatsService::TEvUpdatePartCountersRequest>();

        NBlobMetrics::TBlobLoadMetrics metrics;
        NKikimrTabletBase::TMetrics tabletMetrics;

        auto response =
            std::make_unique<TEvStatsService::TEvUpdatePartCountersResponse>(
                0,         // VolumeSystemCpu
                0,         // VolumeUserCpu
                nullptr,   // DiskCounters
                metrics,
                tabletMetrics,
                EdgeActor   // PartActorId
            );

        ActorSystem.Send(new NActors::IEventHandle(
            partitionStatisticActor,
            EdgeActor,
            response.release()));

        auto partStatisticActorResponse =
            ActorSystem
                .GrabEdgeEvent<TEvStatsService::TEvUpdatedAllPartCounters>();

        // Check that TPartitionStatisticActor send statistic to volume
        UNIT_ASSERT(partStatisticActorResponse->Counters.size() == 1);
    }

    Y_UNIT_TEST_F(
        ShouldPartitionStatisticActorSendResponseToVolumeIfTimeout,
        TSetupEnvironment)
    {
        auto _ = ActorSystem.SetScheduledEventFilter(
            [](NActors::TTestActorRuntimeBase& runtime,
               TAutoPtr<NActors::IEventHandle>& event,
               auto&& delay,
               auto&& deadline)
            {
                Y_UNUSED(runtime);
                Y_UNUSED(event);
                Y_UNUSED(delay);
                Y_UNUSED(deadline);
                return false;
            });

        auto partitionStatisticActor =
            ActorSystem.Register(new TPartitionStatisticActor{EdgeActor, {}});

        Y_UNUSED(partitionStatisticActor);

        ActorSystem.AdvanceCurrentTime(UpdateCountersInterval);
        ActorSystem.DispatchEvents({}, TDuration::MilliSeconds(10));

        auto partStatisticActorResponse =
            ActorSystem
                .GrabEdgeEvent<TEvStatsService::TEvUpdatedAllPartCounters>();

        // Check that TPartitionStatisticActor send Error to volume
        UNIT_ASSERT(HasError(partStatisticActorResponse->Error));
    }

    Y_UNIT_TEST_F(
        ShouldPartitionStatisticActorSendStatisticResponseToVolumeIfTwoPartitions,
        TSetupEnvironment)
    {
        NProto::TPartitionConfig partitionConfig;
        TPartitionInfoList partInfoList;
        partInfoList.emplace_back(
            0,   // tabletId
            partitionConfig,
            0,                       // partitionIndex
            TDuration::Seconds(0),   // timeoutIncrement
            TDuration::Seconds(0)    // timeoutMax
        );

        partInfoList.emplace_back(
            1,   // tabletId
            partitionConfig,
            1,                       // partitionIndex
            TDuration::Seconds(0),   // timeoutIncrement
            TDuration::Seconds(0)    // timeoutMax
        );

        TActorsStack actors;
        auto part1ActorId = ActorSystem.AllocateEdgeActor();
        actors.Push(part1ActorId);
        partInfoList[0].SetStarted(actors);

        auto part2ActorId = ActorSystem.AllocateEdgeActor();
        actors.Clear();
        actors.Push(part2ActorId);
        partInfoList[1].SetStarted(actors);

        TVector<NActors::IEventHandle*> eventHandles;

        NBlobMetrics::TBlobLoadMetrics metrics;
        NKikimrTabletBase::TMetrics tabletMetrics;

        auto _ = ActorSystem.SetEventFilter(
            [&](NActors::TTestActorRuntimeBase& runtime,
                TAutoPtr<NActors::IEventHandle>& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() ==
                    TEvStatsService::EvUpdatePartCountersRequest)
                {
                    auto response = std::make_unique<
                        TEvStatsService::TEvUpdatePartCountersResponse>(
                        0,         // VolumeSystemCpu
                        0,         // VolumeUserCpu
                        nullptr,   // DiskCounters
                        metrics,
                        tabletMetrics,
                        event->Recipient   // PartActorId
                    );

                    eventHandles.push_back(new NActors::IEventHandle(
                        event->Sender,
                        event->Recipient,
                        response.release()));
                }
                return false;
            });

        auto partitionStatisticActor = ActorSystem.Register(
            new TPartitionStatisticActor{EdgeActor, partInfoList});

        Y_UNUSED(partitionStatisticActor);

        ActorSystem.AdvanceCurrentTime(TDuration::Seconds(1));
        ActorSystem.DispatchEvents({}, TDuration::MilliSeconds(10));

        // check that actor send two requests
        UNIT_ASSERT(eventHandles.size() == 2);

        for (auto* handle: eventHandles) {
            ActorSystem.Send(handle);
        }

        auto partStatisticActorResponse =
            ActorSystem
                .GrabEdgeEvent<TEvStatsService::TEvUpdatedAllPartCounters>();

        // Check that TPartitionStatisticActor send statistic for both
        //  partitions to volume
        UNIT_ASSERT(partStatisticActorResponse->Counters.size() == 2);
    }

    Y_UNIT_TEST_F(
        ShouldPartitionStatisticActorSendErrorAndCountersToVolumeIfTimeout,
        TSetupEnvironment)
    {
        NProto::TPartitionConfig partitionConfig;
        TPartitionInfoList partInfoList;
        partInfoList.emplace_back(
            0,   // tabletId
            partitionConfig,
            0,                       // partitionIndex
            TDuration::Seconds(0),   // timeoutIncrement
            TDuration::Seconds(0)    // timeoutMax
        );

        partInfoList.emplace_back(
            1,   // tabletId
            partitionConfig,
            1,                       // partitionIndex
            TDuration::Seconds(0),   // timeoutIncrement
            TDuration::Seconds(0)    // timeoutMax
        );

        TActorsStack actors;
        auto part1ActorId = ActorSystem.AllocateEdgeActor();
        actors.Push(part1ActorId);
        partInfoList[0].SetStarted(actors);

        auto part2ActorId = ActorSystem.AllocateEdgeActor();
        actors.Clear();
        actors.Push(part2ActorId);
        partInfoList[1].SetStarted(actors);

        TVector<NActors::IEventHandle*> eventHandles;

        NBlobMetrics::TBlobLoadMetrics metrics;
        NKikimrTabletBase::TMetrics tabletMetrics;

        auto _ = ActorSystem.SetEventFilter(
            [&](NActors::TTestActorRuntimeBase& runtime,
                TAutoPtr<NActors::IEventHandle>& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() ==
                    TEvStatsService::EvUpdatePartCountersRequest)
                {
                    auto response = std::make_unique<
                        TEvStatsService::TEvUpdatePartCountersResponse>(
                        0,         // VolumeSystemCpu
                        0,         // VolumeUserCpu
                        nullptr,   // DiskCounters
                        metrics,
                        tabletMetrics,
                        event->Recipient   // PartActorId
                    );

                    eventHandles.push_back(new NActors::IEventHandle(
                        event->Sender,
                        event->Recipient,
                        response.release()));
                }
                return false;
            });

        auto filter = ActorSystem.SetScheduledEventFilter(
            [](NActors::TTestActorRuntimeBase& runtime,
               TAutoPtr<NActors::IEventHandle>& event,
               auto&& delay,
               auto&& deadline)
            {
                Y_UNUSED(runtime);
                Y_UNUSED(event);
                Y_UNUSED(delay);
                Y_UNUSED(deadline);
                return false;
            });

        Y_UNUSED(filter);

        auto partitionStatisticActor = ActorSystem.Register(
            new TPartitionStatisticActor{EdgeActor, partInfoList});

        Y_UNUSED(partitionStatisticActor);

        ActorSystem.AdvanceCurrentTime(TDuration::Seconds(1));
        ActorSystem.DispatchEvents({}, TDuration::MilliSeconds(10));

        // check that actor send two requests
        UNIT_ASSERT(eventHandles.size() == 2);

        // reply only for one request
        ActorSystem.Send(eventHandles[0]);

        auto partStatisticActorResponse =
            ActorSystem
                .GrabEdgeEvent<TEvStatsService::TEvUpdatedAllPartCounters>();

        // Check that TPartitionStatisticActor send statistic of one partition
        // to volume
        UNIT_ASSERT(partStatisticActorResponse->Counters.size() == 1);

        // Check that TPartitionStatisticActor send Error to volume
        UNIT_ASSERT(HasError(partStatisticActorResponse->Error));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
