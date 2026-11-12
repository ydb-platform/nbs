#include <cloud/filestore/libs/storage/tablet_rdma/rdma_private.h>
#include <cloud/filestore/libs/storage/tablet_rdma/rdma_tablet_registry.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <contrib/ydb/core/base/tablet_pipe.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TEnv
{
    TTestEnv Env;
    const ui32 NodeIdx;
    const ui64 TabletId;
    const TRdmaTabletRegistryPtr Registry;

    // Pipe server -> tablet actor, as the pipe servers announce themselves.
    THashMap<TActorId, TActorId> PipeServers;
    TTestActorRuntime::TEventObserverHolder PipeObserver;

    TEnv()
        : NodeIdx(Env.AddDynamicNode())
        , TabletId(Env.BootIndexTablet(NodeIdx))
        , Registry(Env.GetRdmaTabletRegistry(GetRuntime().GetNodeId(NodeIdx)))
    {
        using TEvServerConnected = TEvTabletPipe::TEvServerConnected;
        PipeObserver = GetRuntime().AddObserver<TEvServerConnected>(
            [this](TEvServerConnected::TPtr& ev)
            {
                if (ev->Get()->TabletId == TabletId) {
                    PipeServers[ev->Get()->ServerId] =
                        ev->GetRecipientRewrite();
                }
            });
    }

    TTestActorRuntime& GetRuntime()
    {
        return Env.GetRuntime();
    }

    TActorId AttachForwarder()
    {
        const auto forwarderId = GetRuntime().AllocateEdgeActor(NodeIdx);
        Registry->AttachForwarder(forwarderId);
        return forwarderId;
    }

    TActorId WaitTabletGone(const TActorId& forwarderId)
    {
        using TEvTabletGone = TEvRdmaTabletPrivate::TEvTabletGone;
        auto ev = GetRuntime().GrabEdgeEvent<TEvTabletGone>(
            forwarderId,
            TDuration::Seconds(10));
        UNIT_ASSERT(ev);
        return ev->Get()->TabletActorId;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRdmaTabletRegistryTest)
{
    Y_UNIT_TEST(ShouldRegisterPipeOfConnectedClient)
    {
        TEnv env;
        TIndexTabletClient tablet(env.GetRuntime(), env.NodeIdx, env.TabletId);

        UNIT_ASSERT_VALUES_EQUAL(1, env.PipeServers.size());
        const auto [pipeServerId, tabletActorId] = *env.PipeServers.begin();
        UNIT_ASSERT(tabletActorId);
        UNIT_ASSERT_VALUES_EQUAL(
            tabletActorId,
            env.Registry->FindTablet(pipeServerId));
    }

    Y_UNIT_TEST(ShouldUnregisterPipeOfDisconnectedClient)
    {
        TEnv env;
        TIndexTabletClient tablet1(env.GetRuntime(), env.NodeIdx, env.TabletId);
        const auto pipeServer1 = env.PipeServers.begin()->first;

        env.PipeServers.clear();
        TIndexTabletClient tablet2(env.GetRuntime(), env.NodeIdx, env.TabletId);
        const auto [pipeServer2, tabletActorId] = *env.PipeServers.begin();

        tablet1.DisconnectPipe();

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvTabletPipe::EvServerDisconnected);
        env.GetRuntime().DispatchEvents(options);

        UNIT_ASSERT_VALUES_EQUAL(
            TActorId(),
            env.Registry->FindTablet(pipeServer1));
        UNIT_ASSERT_VALUES_EQUAL(
            tabletActorId,
            env.Registry->FindTablet(pipeServer2));
    }

    Y_UNIT_TEST(ShouldUnregisterPipesAndNotifyForwarderWhenTabletDies)
    {
        TEnv env;
        const auto forwarderId = env.AttachForwarder();

        TIndexTabletClient tablet(env.GetRuntime(), env.NodeIdx, env.TabletId);
        const auto [pipeServerId, tabletActorId] = *env.PipeServers.begin();

        env.PipeServers.clear();
        tablet.RebootTablet();

        UNIT_ASSERT_VALUES_EQUAL(
            tabletActorId,
            env.WaitTabletGone(forwarderId));
        UNIT_ASSERT_VALUES_EQUAL(
            TActorId(),
            env.Registry->FindTablet(pipeServerId));

        tablet.WaitReady();

        UNIT_ASSERT_VALUES_EQUAL(1, env.PipeServers.size());
        const auto [nextPipeServerId, nextTabletActorId] =
            *env.PipeServers.begin();
        UNIT_ASSERT_VALUES_UNEQUAL(tabletActorId, nextTabletActorId);
        UNIT_ASSERT_VALUES_EQUAL(
            nextTabletActorId,
            env.Registry->FindTablet(nextPipeServerId));
    }

    Y_UNIT_TEST(ShouldNotUnregisterPipesOfAnotherTabletActor)
    {
        TEnv env;
        const auto forwarderId = env.AttachForwarder();

        TIndexTabletClient tablet(env.GetRuntime(), env.NodeIdx, env.TabletId);
        const auto tabletActorId = env.PipeServers.begin()->second;

        // e.g. the next generation that registered before the previous one
        // unregistered
        const auto otherPipeServerId =
            env.GetRuntime().AllocateEdgeActor(env.NodeIdx);
        const auto otherTabletActorId =
            env.GetRuntime().AllocateEdgeActor(env.NodeIdx);
        env.Registry->RegisterPipe(otherPipeServerId, otherTabletActorId);

        tablet.RebootTablet();

        UNIT_ASSERT_VALUES_EQUAL(
            tabletActorId,
            env.WaitTabletGone(forwarderId));
        UNIT_ASSERT_VALUES_EQUAL(
            otherTabletActorId,
            env.Registry->FindTablet(otherPipeServerId));
    }
}

}   // namespace NCloud::NFileStore::NStorage
