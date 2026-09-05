#include "mount_registry.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>

#include <functional>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

TMountInfo MakeMountInfo(const TString& diskId, const TString& clientId)
{
    TMountInfo info;
    info.DiskId = diskId;
    info.ClientId = clientId;
    return info;
}

TString MakePeer(ui64 sessionId)
{
    return TStringBuilder() << "10.0.0.1:" << sessionId;
}

TInstant MakeStartTs(ui64 sessionId)
{
    return TInstant::Seconds(100 + sessionId);
}

////////////////////////////////////////////////////////////////////////////////

struct TFixture
    : public NUnitTest::TBaseFixture
{
    TMountRegistry Registry{TLog{}};

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        Registry.Start();
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {
        Registry.Stop();
    }

    void AddConnection(ui64 sessionId)
    {
        Registry.AddConnection(
            sessionId,
            MakePeer(sessionId),
            MakeStartTs(sessionId));
    }

    // Updates are applied on the registry thread, so the result only shows up
    // some time after the call returns.
    TVector<TConnectionInfo> WaitForConnections(
        const std::function<bool(const TVector<TConnectionInfo>&)>& ready)
    {
        const auto deadline = TInstant::Now() + TDuration::Seconds(5);

        for (;;) {
            auto connections = Registry.GetConnections();
            if (ready(connections) || TInstant::Now() >= deadline) {
                return connections;
            }

            Sleep(TDuration::MilliSeconds(10));
        }
    }

    TVector<TConnectionInfo> WaitForMounts(size_t expectedCount)
    {
        return WaitForConnections(
            [=](const auto& connections)
            {
                return connections.size() == 1 &&
                       connections[0].Mounts.size() == expectedCount;
            });
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMountRegistryTest)
{
    Y_UNIT_TEST_F(ShouldTrackSeveralMountsOfOneSession, TFixture)
    {
        AddConnection(42);
        Registry.AddMount(42, MakeMountInfo("vol-1", "client-a"));
        Registry.AddMount(42, MakeMountInfo("vol-2", "client-a"));

        auto connections = WaitForMounts(2);
        UNIT_ASSERT_VALUES_EQUAL(1, connections.size());
        UNIT_ASSERT_VALUES_EQUAL(42, connections[0].SessionId);
        UNIT_ASSERT_VALUES_EQUAL("10.0.0.1:42", connections[0].Peer);
        UNIT_ASSERT_VALUES_EQUAL(MakeStartTs(42), connections[0].StartTs);

        const auto& mounts = connections[0].Mounts;
        UNIT_ASSERT_VALUES_EQUAL(2, mounts.size());
        UNIT_ASSERT_VALUES_EQUAL("vol-1", mounts[0].DiskId);
        UNIT_ASSERT_VALUES_EQUAL("vol-2", mounts[1].DiskId);
    }

    Y_UNIT_TEST_F(ShouldReplaceRepeatedMountOfSameVolume, TFixture)
    {
        AddConnection(42);
        Registry.AddMount(42, MakeMountInfo("vol-1", "client-a"));

        auto remount = MakeMountInfo("vol-1", "client-a");
        remount.MountSeqNumber = 7;
        Registry.AddMount(42, std::move(remount));

        auto connections = WaitForConnections(
            [](const auto& connections)
            {
                return connections.size() == 1 &&
                       connections[0].Mounts.size() == 1 &&
                       connections[0].Mounts[0].MountSeqNumber == 7;
            });

        UNIT_ASSERT_VALUES_EQUAL(1, connections.size());
        UNIT_ASSERT_VALUES_EQUAL(1, connections[0].Mounts.size());
        UNIT_ASSERT_VALUES_EQUAL(7, connections[0].Mounts[0].MountSeqNumber);
    }

    Y_UNIT_TEST_F(ShouldForgetUnmountedVolume, TFixture)
    {
        AddConnection(42);
        Registry.AddMount(42, MakeMountInfo("vol-1", "client-a"));
        Registry.AddMount(42, MakeMountInfo("vol-2", "client-a"));
        Registry.RemoveMount(42, "vol-1", "client-a");

        auto connections = WaitForMounts(1);
        UNIT_ASSERT_VALUES_EQUAL(1, connections.size());
        UNIT_ASSERT_VALUES_EQUAL(1, connections[0].Mounts.size());
        UNIT_ASSERT_VALUES_EQUAL("vol-2", connections[0].Mounts[0].DiskId);
    }

    Y_UNIT_TEST_F(ShouldKeepMountsOfDifferentSessionsApart, TFixture)
    {
        AddConnection(1);
        AddConnection(2);
        Registry.AddMount(1, MakeMountInfo("vol-1", "client-a"));
        Registry.AddMount(2, MakeMountInfo("vol-2", "client-b"));

        auto connections = WaitForConnections(
            [](const auto& connections)
            {
                return connections.size() == 2 &&
                       !connections[0].Mounts.empty() &&
                       !connections[1].Mounts.empty();
            });

        UNIT_ASSERT_VALUES_EQUAL(2, connections.size());

        // ordered by connection time, see MakeStartTs()
        UNIT_ASSERT_VALUES_EQUAL(1, connections[0].SessionId);
        UNIT_ASSERT_VALUES_EQUAL(1, connections[0].Mounts.size());
        UNIT_ASSERT_VALUES_EQUAL("vol-1", connections[0].Mounts[0].DiskId);

        UNIT_ASSERT_VALUES_EQUAL(2, connections[1].SessionId);
        UNIT_ASSERT_VALUES_EQUAL(1, connections[1].Mounts.size());
        UNIT_ASSERT_VALUES_EQUAL("vol-2", connections[1].Mounts[0].DiskId);
    }

    Y_UNIT_TEST_F(ShouldForgetMountsOfClosedConnection, TFixture)
    {
        AddConnection(42);
        Registry.AddMount(42, MakeMountInfo("vol-1", "client-a"));
        Registry.RemoveConnection(42);

        auto connections = WaitForConnections(
            [](const auto& connections) { return connections.empty(); });

        UNIT_ASSERT_VALUES_EQUAL(0, connections.size());
    }

    Y_UNIT_TEST_F(ShouldShowConnectionWithoutMounts, TFixture)
    {
        AddConnection(42);

        auto connections = WaitForMounts(0);
        UNIT_ASSERT_VALUES_EQUAL(1, connections.size());
        UNIT_ASSERT_VALUES_EQUAL(42, connections[0].SessionId);
        UNIT_ASSERT(connections[0].Mounts.empty());
    }

    Y_UNIT_TEST_F(ShouldIgnoreMountOfUnknownConnection, TFixture)
    {
        // a mount must never create a connection entry: that would resurrect
        // a connection that has already been closed
        Registry.AddMount(42, MakeMountInfo("vol-1", "client-a"));

        // the announcement below is applied after the mount above, so seeing
        // it means the mount has been processed too
        AddConnection(1);

        auto connections = WaitForConnections(
            [](const auto& connections) { return !connections.empty(); });

        UNIT_ASSERT_VALUES_EQUAL(1, connections.size());
        UNIT_ASSERT_VALUES_EQUAL(1, connections[0].SessionId);
        UNIT_ASSERT(connections[0].Mounts.empty());
    }

    Y_UNIT_TEST_F(ShouldIgnoreMountOfClosedConnection, TFixture)
    {
        AddConnection(42);
        Registry.RemoveConnection(42);
        Registry.AddMount(42, MakeMountInfo("vol-1", "client-a"));

        AddConnection(1);

        auto connections = WaitForConnections(
            [](const auto& connections) { return !connections.empty(); });

        UNIT_ASSERT_VALUES_EQUAL(1, connections.size());
        UNIT_ASSERT_VALUES_EQUAL(1, connections[0].SessionId);
    }

}

}   // namespace NCloud::NBlockStore::NStorage
