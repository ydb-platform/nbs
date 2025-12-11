#include "balancing.h"

#include <cloud/blockstore/public/api/protos/discovery.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NDiscovery {

namespace {

////////////////////////////////////////////////////////////////////////////////

TInstanceInfo::TStat Stat(int timeSecs, int byteCount)
{
    TInstanceInfo::TStat stat;
    stat.Ts = TInstant::Seconds(timeSecs);
    stat.Bytes = byteCount;
    return stat;
}

TInstanceInfo Instance(
    TString host,
    ui16 port,
    TInstanceInfo::EStatus status,
    TInstanceInfo::TStat prevStat,
    TInstanceInfo::TStat lastStat,
    double balancingScore)
{
    TInstanceInfo ii;
    ii.Host = std::move(host);
    ii.Port = port;
    ii.Status = status;
    ii.PrevStat = std::move(prevStat);
    ii.LastStat = std::move(lastStat);
    ii.BalancingScore = balancingScore;
    return ii;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(BalancingTest)
{
    Y_UNIT_TEST(ShouldCalculateBalancingScoreAndReorder)
    {
        TInstanceList instances;

        instances.Instances = {
            Instance(
                "host1",
                1,
                TInstanceInfo::EStatus::Reachable,
                Stat(1, 100),
                Stat(2, 300),
                12345),
            Instance(
                "host2",
                2,
                TInstanceInfo::EStatus::Reachable,
                Stat(1, 100),
                Stat(2, 200),
                12345),
            Instance(
                "unreachablehost",
                3,
                TInstanceInfo::EStatus::Unreachable,
                Stat(1, 100),
                Stat(2, 120),
                100500),
            Instance(
                "newhost",
                4,
                TInstanceInfo::EStatus::Reachable,
                {},
                Stat(1, 200),
                0),
            Instance(
                "newesthost",
                5,
                TInstanceInfo::EStatus::Unreachable,
                {},
                {},
                0),
            Instance(
                "idlehost",
                6,
                TInstanceInfo::EStatus::Reachable,
                Stat(1, 100),
                Stat(11, 100),
                0),
        };

        auto balancingPolicy = CreateBalancingPolicy();
        balancingPolicy->Reorder(instances);

        UNIT_ASSERT_VALUES_EQUAL(6, instances.Instances.size());

        UNIT_ASSERT_VALUES_EQUAL("idlehost", instances.Instances[0].Host);
        UNIT_ASSERT_DOUBLES_EQUAL(
            10,
            instances.Instances[0].BalancingScore,
            1e-5);

        UNIT_ASSERT_VALUES_EQUAL("newhost", instances.Instances[1].Host);
        UNIT_ASSERT_DOUBLES_EQUAL(
            1,
            instances.Instances[1].BalancingScore,
            1e-5);

        UNIT_ASSERT_VALUES_EQUAL("host2", instances.Instances[2].Host);
        UNIT_ASSERT_DOUBLES_EQUAL(
            0.01,
            instances.Instances[2].BalancingScore,
            1e-5);

        UNIT_ASSERT_VALUES_EQUAL("host1", instances.Instances[3].Host);
        UNIT_ASSERT_DOUBLES_EQUAL(
            0.005,
            instances.Instances[3].BalancingScore,
            1e-5);

        UNIT_ASSERT_VALUES_EQUAL(
            "unreachablehost",
            instances.Instances[4].Host);
        UNIT_ASSERT_DOUBLES_EQUAL(
            0,
            instances.Instances[4].BalancingScore,
            1e-5);

        UNIT_ASSERT_VALUES_EQUAL("newesthost", instances.Instances[5].Host);
        UNIT_ASSERT_DOUBLES_EQUAL(
            0,
            instances.Instances[5].BalancingScore,
            1e-5);
    }
}

}   // namespace NCloud::NBlockStore::NDiscovery
