#include "metrics.h"

#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/output.h>

namespace NCloud::NBlockStore::NStorage::NBlobMetrics {

////////////////////////////////////////////////////////////////////////////////

bool operator==(
    const TBlobLoadMetrics::TTabletMetric& lhs,
    const TBlobLoadMetrics::TTabletMetric& rhs)
{
    return lhs.ReadOperations.ByteCount == rhs.ReadOperations.ByteCount &&
           lhs.ReadOperations.Iops == rhs.ReadOperations.Iops &&
           lhs.WriteOperations.ByteCount == rhs.WriteOperations.ByteCount &&
           lhs.WriteOperations.Iops == rhs.WriteOperations.Iops;
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMetricTests)
{
    Y_UNIT_TEST(SumTest)
    {
        const TBlobLoadMetrics::TTabletOperations ssd_result = {
            {{42, 0}, {{300, 2}, {12000, 3}}},
            {{43, 0}, {{50, 1}, {0, 0}}}};
        const TBlobLoadMetrics::TTabletOperations hdd_result = {
            {{12, 0}, {{100, 2}, {32000, 2}}},
            {{42, 0}, {{50, 1}, {0, 0}}}};

        TBlobLoadMetrics metrics1;
        auto& metrics1Ssd = metrics1.PoolKind2TabletOps["ssd"];
        auto& metrics1Hdd = metrics1.PoolKind2TabletOps["hdd"];

        metrics1Ssd[std::make_pair(42, 0)].ReadOperations += {100, 1};
        metrics1Ssd[std::make_pair(42, 0)].WriteOperations += {1000, 1};

        metrics1Hdd[std::make_pair(42, 0)].ReadOperations += {50, 1};
        metrics1Hdd[std::make_pair(12, 0)].WriteOperations += {1000, 1};

        TBlobLoadMetrics metrics2;
        auto& metrics2Ssd = metrics2.PoolKind2TabletOps["ssd"];
        auto& metrics2Hdd = metrics2.PoolKind2TabletOps["hdd"];

        metrics2Ssd[std::make_pair(42, 0)].ReadOperations += {200, 1};
        metrics2Ssd[std::make_pair(43, 0)].ReadOperations += {50, 1};
        metrics2Ssd[std::make_pair(42, 0)].WriteOperations += {11000, 2};

        metrics2Hdd[std::make_pair(12, 0)].ReadOperations += {100, 2};
        metrics2Hdd[std::make_pair(12, 0)].WriteOperations += {31000, 1};

        TBlobLoadMetrics sumMetrics = metrics1;
        sumMetrics += metrics2;

        ASSERT_MAP_EQUAL(sumMetrics.PoolKind2TabletOps["ssd"], ssd_result);
        ASSERT_MAP_EQUAL(sumMetrics.PoolKind2TabletOps["hdd"], hdd_result);
    }

    Y_UNIT_TEST(OffsetTest)
    {
        const TBlobLoadMetrics::TTabletOperations ssd_result = {
            {{43, 1}, {{0, 0}, {30000, 1}}},
            {{43, 0}, {{0, 0}, {10000, 1}}},
            {{42, 0}, {{0, 0}, {0, 0}}}};
        const TBlobLoadMetrics::TTabletOperations hdd_result = {
            {{12, 0}, {{0, 0}, {0, 0}}},
            {{42, 0}, {{0, 0}, {0, 0}}}};

        TBlobLoadMetrics metrics1;
        auto& metrics1Ssd = metrics1.PoolKind2TabletOps["ssd"];
        auto& metrics1Hdd = metrics1.PoolKind2TabletOps["hdd"];

        metrics1Ssd[std::make_pair(42, 0)].ReadOperations += {100, 1};
        metrics1Ssd[std::make_pair(42, 1)].WriteOperations += {1000, 1};
        metrics1Ssd[std::make_pair(43, 0)].WriteOperations += {10000, 1};

        metrics1Hdd[std::make_pair(42, 0)].ReadOperations += {50, 1};
        metrics1Hdd[std::make_pair(12, 0)].WriteOperations += {1000, 1};

        TBlobLoadMetrics metrics2;
        auto& metrics2Ssd = metrics2.PoolKind2TabletOps["ssd"];
        auto& metrics2Hdd = metrics2.PoolKind2TabletOps["hdd"];

        metrics2Ssd[std::make_pair(42, 0)].ReadOperations += {100, 1};
        metrics2Ssd[std::make_pair(43, 0)].WriteOperations += {20000, 2};
        metrics2Ssd[std::make_pair(43, 1)].WriteOperations += {30000, 1};

        metrics2Hdd[std::make_pair(42, 0)].ReadOperations += {50, 1};
        metrics2Hdd[std::make_pair(12, 0)].WriteOperations += {1000, 1};

        TBlobLoadMetrics offsetMetrics = TakeDelta(metrics1, metrics2);

        ASSERT_MAP_EQUAL(offsetMetrics.PoolKind2TabletOps["ssd"], ssd_result);
        ASSERT_MAP_EQUAL(offsetMetrics.PoolKind2TabletOps["hdd"], hdd_result);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NBlobMetrics

using TBlobLoadMetrics =
    NCloud::NBlockStore::NStorage::NBlobMetrics::TBlobLoadMetrics;

IOutputStream& operator<<(
    IOutputStream& out,
    const TBlobLoadMetrics::TTabletMetric& rhs)
{
    out << "{" << rhs.ReadOperations.ByteCount << "," << rhs.ReadOperations.Iops
        << "," << rhs.WriteOperations.ByteCount << ","
        << rhs.WriteOperations.Iops << "}";
    return out;
}

IOutputStream& operator<<(
    IOutputStream& out,
    const std::pair<TBlobLoadMetrics::TChannel, TBlobLoadMetrics::TGroupId>&
        rhs)
{
    out << "{" << rhs.first << "," << rhs.second << "}";
    return out;
}
