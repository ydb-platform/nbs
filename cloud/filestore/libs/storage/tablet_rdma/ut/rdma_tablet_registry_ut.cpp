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

TActorId MakeActorId(ui64 localId)
{
    return TActorId(1, 0, localId, 0);
}

////////////////////////////////////////////////////////////////////////////////

struct TTabletEnv
{
    using TEvServerConnected = TEvTabletPipe::TEvServerConnected;

    TTestEnv Env;
    const ui32 NodeIdx;
    const ui64 TabletId;
    const TRdmaTabletRegistryPtr Registry;

    // Pipe server -> tablet actor, as the pipe servers announce themselves.
    THashMap<TActorId, TActorId> PipeServers;
    TTestActorRuntime::TEventObserverHolder PipeObserver;

    TTabletEnv()
        : NodeIdx(Env.AddDynamicNode())
        , TabletId(Env.BootIndexTablet(NodeIdx))
        , Registry(Env.GetRdmaTabletRegistry(GetRuntime().GetNodeId(NodeIdx)))
    {
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

    std::pair<TActorId, TActorId> GetPipeServer() const
    {
        UNIT_ASSERT_VALUES_EQUAL(1, PipeServers.size());
        return *PipeServers.begin();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRdmaTabletRegistryTest)
{
    Y_UNIT_TEST(ShouldFindTabletOfRegisteredPipe)
    {
        TRdmaTabletRegistry registry;
        registry.RegisterPipe(MakeActorId(1), MakeActorId(10));
        registry.RegisterPipe(MakeActorId(2), MakeActorId(10));
        registry.RegisterPipe(MakeActorId(3), MakeActorId(20));

        UNIT_ASSERT_VALUES_EQUAL(
            MakeActorId(10),
            registry.FindTablet(MakeActorId(1)));
        UNIT_ASSERT_VALUES_EQUAL(
            MakeActorId(10),
            registry.FindTablet(MakeActorId(2)));
        UNIT_ASSERT_VALUES_EQUAL(
            MakeActorId(20),
            registry.FindTablet(MakeActorId(3)));
        UNIT_ASSERT_VALUES_EQUAL(
            TActorId(),
            registry.FindTablet(MakeActorId(4)));
    }

    Y_UNIT_TEST(ShouldUnregisterPipe)
    {
        TRdmaTabletRegistry registry;
        registry.RegisterPipe(MakeActorId(1), MakeActorId(10));
        registry.RegisterPipe(MakeActorId(2), MakeActorId(10));

        registry.UnregisterPipe(MakeActorId(1));
        registry.UnregisterPipe(MakeActorId(3));

        UNIT_ASSERT_VALUES_EQUAL(
            TActorId(),
            registry.FindTablet(MakeActorId(1)));
        UNIT_ASSERT_VALUES_EQUAL(
            MakeActorId(10),
            registry.FindTablet(MakeActorId(2)));

        registry.UnregisterPipe(MakeActorId(2));

        UNIT_ASSERT_VALUES_EQUAL(
            TActorId(),
            registry.FindTablet(MakeActorId(2)));
    }

    Y_UNIT_TEST(ShouldMovePipeRegisteredForAnotherTablet)
    {
        TRdmaTabletRegistry registry;
        registry.RegisterPipe(MakeActorId(1), MakeActorId(10));
        registry.RegisterPipe(MakeActorId(1), MakeActorId(20));

        UNIT_ASSERT_VALUES_EQUAL(
            MakeActorId(20),
            registry.FindTablet(MakeActorId(1)));

        registry.UnregisterTablet(MakeActorId(10));
        UNIT_ASSERT_VALUES_EQUAL(
            MakeActorId(20),
            registry.FindTablet(MakeActorId(1)));

        registry.UnregisterTablet(MakeActorId(20));
        UNIT_ASSERT_VALUES_EQUAL(
            TActorId(),
            registry.FindTablet(MakeActorId(1)));
    }

    Y_UNIT_TEST(ShouldUnregisterOnlyPipesOfTablet)
    {
        TRdmaTabletRegistry registry;
        registry.RegisterPipe(MakeActorId(1), MakeActorId(10));
        registry.RegisterPipe(MakeActorId(2), MakeActorId(10));
        // e.g. the next generation of the tablet
        registry.RegisterPipe(MakeActorId(3), MakeActorId(20));

        registry.UnregisterTablet(MakeActorId(10));

        UNIT_ASSERT_VALUES_EQUAL(
            TActorId(),
            registry.FindTablet(MakeActorId(1)));
        UNIT_ASSERT_VALUES_EQUAL(
            TActorId(),
            registry.FindTablet(MakeActorId(2)));
        UNIT_ASSERT_VALUES_EQUAL(
            MakeActorId(20),
            registry.FindTablet(MakeActorId(3)));

        registry.RegisterPipe(MakeActorId(1), MakeActorId(10));
        UNIT_ASSERT_VALUES_EQUAL(
            MakeActorId(10),
            registry.FindTablet(MakeActorId(1)));
    }

    Y_UNIT_TEST(ShouldRegisterPipeUntilClientDisconnects)
    {
        TTabletEnv env;
        TIndexTabletClient tablet(env.GetRuntime(), env.NodeIdx, env.TabletId);

        const auto [pipeServerId, tabletActorId] = env.GetPipeServer();
        UNIT_ASSERT(tabletActorId);
        UNIT_ASSERT_VALUES_EQUAL(
            tabletActorId,
            env.Registry->FindTablet(pipeServerId));

        tablet.DisconnectPipe();

        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle& ev)
            {
                return ev.GetTypeRewrite() ==
                           TEvTabletPipe::EvServerDisconnected &&
                       ev.Get<TEvTabletPipe::TEvServerDisconnected>()
                               ->ServerId == pipeServerId;
            });
        env.GetRuntime().DispatchEvents(options);

        UNIT_ASSERT_VALUES_EQUAL(
            TActorId(),
            env.Registry->FindTablet(pipeServerId));
    }

    Y_UNIT_TEST(ShouldUnregisterPipesWhenTabletDies)
    {
        using TEvServerDisconnected = TEvTabletPipe::TEvServerDisconnected;

        TTabletEnv env;
        TIndexTabletClient tablet(env.GetRuntime(), env.NodeIdx, env.TabletId);

        const auto [pipeServerId, tabletActorId] = env.GetPipeServer();
        UNIT_ASSERT_VALUES_EQUAL(
            tabletActorId,
            env.Registry->FindTablet(pipeServerId));

        // only the tablet's death may unregister the pipe
        auto& runtime = env.GetRuntime();
        auto dropDisconnect = runtime.AddObserver<TEvServerDisconnected>(
            [&](TEvServerDisconnected::TPtr& ev)
            {
                if (ev->Get()->ServerId == pipeServerId) {
                    ev.Reset();
                }
            });

        env.PipeServers.clear();
        tablet.RebootTablet();
        tablet.WaitReady();

        UNIT_ASSERT_VALUES_EQUAL(
            TActorId(),
            env.Registry->FindTablet(pipeServerId));

        const auto [nextPipeServerId, nextTabletActorId] = env.GetPipeServer();
        UNIT_ASSERT_VALUES_UNEQUAL(tabletActorId, nextTabletActorId);
        UNIT_ASSERT_VALUES_EQUAL(
            nextTabletActorId,
            env.Registry->FindTablet(nextPipeServerId));
    }
}

}   // namespace NCloud::NFileStore::NStorage
