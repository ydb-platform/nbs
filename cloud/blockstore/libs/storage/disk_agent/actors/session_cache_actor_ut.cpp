#include "session_cache_actor.h"

#include <cloud/blockstore/libs/storage/disk_agent/disk_agent_private.h>

#include <cloud/storage/core/libs/common/proto_helpers.h>

#include <contrib/ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/system/fs.h>

#include <chrono>

namespace NCloud::NBlockStore::NStorage::NDiskAgent {

using namespace NActors;
using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TActorSystem
    : NActors::TTestActorRuntimeBase
{
    void Start()
    {
        SetDispatchTimeout(5s);
        InitNodes();
        AppendToLogSettings(
            TBlockStoreComponents::START,
            TBlockStoreComponents::END,
            GetComponentName);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFixture
    : public NUnitTest::TBaseFixture
{
    const TTempDir TempDir;
    const TString CachedSessionsPath = TempDir.Path() / "nbs-disk-agent-sessions.txt";

    TActorSystem ActorSystem;
    TActorId SessionCacheActor;
    TActorId EdgeActor;

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        ActorSystem.Start();

        EdgeActor = ActorSystem.AllocateEdgeActor();

        SessionCacheActor = ActorSystem.Register(
            CreateSessionCacheActor(CachedSessionsPath).release());

        ActorSystem.DispatchEvents(
            {.FinalEvents = {{TEvents::TSystem::Bootstrap}}},
            10ms);
    }

    void UpdateSessionCache(TVector<NProto::TDiskAgentDeviceSession> sessions)
    {
        ActorSystem.Send(
            SessionCacheActor,
            EdgeActor,
            std::make_unique<TEvDiskAgentPrivate::TEvUpdateSessionCacheRequest>(
                std::move(sessions)).release());

        auto response = ActorSystem.GrabEdgeEvent<
            TEvDiskAgentPrivate::TEvUpdateSessionCacheResponse>();

        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetStatus(),
            response->GetError());
    }

    auto LoadSessionCache()
    {
        NProto::TDiskAgentDeviceSessionCache proto;

        ParseProtoTextFromFileRobust(CachedSessionsPath, proto);

        return TVector<NProto::TDiskAgentDeviceSession>(
            std::make_move_iterator(proto.MutableSessions()->begin()),
            std::make_move_iterator(proto.MutableSessions()->end())
        );
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSessionCacheActorTest)
{
    Y_UNIT_TEST_F(ShouldUpdateSessionCache, TFixture)
    {
        UNIT_ASSERT(!NFs::Exists(CachedSessionsPath));

        UpdateSessionCache({});

        UNIT_ASSERT(NFs::Exists(CachedSessionsPath));
        UNIT_ASSERT(LoadSessionCache().empty());

        {
            NProto::TDiskAgentDeviceSession writer;
            writer.SetClientId("client-1");
            writer.SetDiskId("vol0");

            NProto::TDiskAgentDeviceSession reader;
            reader.SetClientId("client-2");
            reader.SetReadOnly(true);
            reader.SetDiskId("vol0");
            reader.SetLastActivityTs(42);

            UpdateSessionCache({ writer, reader });
        }

        {
            auto sessions = LoadSessionCache();

            // writer was dropped because of LastActivityTs == 0
            UNIT_ASSERT_VALUES_EQUAL(1, sessions.size());
            UNIT_ASSERT_VALUES_EQUAL("client-2", sessions[0].GetClientId());
            UNIT_ASSERT_VALUES_EQUAL(42, sessions[0].GetLastActivityTs());
        }

        {
            NProto::TDiskAgentDeviceSession writer;
            writer.SetClientId("client-1");
            writer.SetDiskId("vol0");
            writer.SetLastActivityTs(1000);

            NProto::TDiskAgentDeviceSession reader;
            reader.SetClientId("client-2");
            reader.SetReadOnly(true);
            reader.SetDiskId("vol0");
            reader.SetLastActivityTs(2000);

            UpdateSessionCache({ writer, reader });
        }

        {
            auto sessions = LoadSessionCache();

            UNIT_ASSERT_VALUES_EQUAL(2, sessions.size());
            SortBy(sessions, [] (auto& s) { return s.GetClientId(); });

            UNIT_ASSERT_VALUES_EQUAL("client-1", sessions[0].GetClientId());
            UNIT_ASSERT_VALUES_EQUAL(1000, sessions[0].GetLastActivityTs());

            UNIT_ASSERT_VALUES_EQUAL("client-2", sessions[1].GetClientId());
            UNIT_ASSERT_VALUES_EQUAL(2000, sessions[1].GetLastActivityTs());
        }

        UpdateSessionCache({});
        UNIT_ASSERT(LoadSessionCache().empty());
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NDiskAgent
