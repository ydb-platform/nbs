#include "tablet.h"

#include <cloud/filestore/libs/storage/testlib/helpers.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_Quotas)
{
    Y_UNIT_TEST(ShouldCreateQuota)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        auto response = tablet.CreateQuota(1_GB, 100);
        const auto& quota = response->Record.GetQuota();

        UNIT_ASSERT(quota.GetQuotaId() != 0);
        UNIT_ASSERT_VALUES_EQUAL(1_GB, quota.GetMaxBytes());
        UNIT_ASSERT_VALUES_EQUAL(100u, quota.GetMaxNodes());
        UNIT_ASSERT(quota.GetCreationTimestampUs() != 0);
    }

    Y_UNIT_TEST(ShouldAllocateUniqueQuotaIds)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        auto quota1 = tablet.CreateQuota(1_GB)->Record.GetQuota();
        auto quota2 = tablet.CreateQuota(2_GB)->Record.GetQuota();

        UNIT_ASSERT(quota1.GetQuotaId() != quota2.GetQuotaId());
    }

    Y_UNIT_TEST(ShouldListQuotas)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        auto quota1 = tablet.CreateQuota(1_GB, 10)->Record.GetQuota();
        auto quota2 = tablet.CreateQuota(2_GB, 20)->Record.GetQuota();

        auto response = tablet.ListQuotas();
        UNIT_ASSERT_VALUES_EQUAL(2, response->Record.QuotasSize());

        THashMap<ui32, NProto::TQuota> quotaById;
        for (const auto& quota: response->Record.GetQuotas()) {
            quotaById[quota.GetQuotaId()] = quota;
        }

        UNIT_ASSERT(quotaById.contains(quota1.GetQuotaId()));
        UNIT_ASSERT_VALUES_EQUAL(
            1_GB,
            quotaById[quota1.GetQuotaId()].GetMaxBytes());
        UNIT_ASSERT_VALUES_EQUAL(
            10u,
            quotaById[quota1.GetQuotaId()].GetMaxNodes());

        UNIT_ASSERT(quotaById.contains(quota2.GetQuotaId()));
        UNIT_ASSERT_VALUES_EQUAL(
            2_GB,
            quotaById[quota2.GetQuotaId()].GetMaxBytes());
        UNIT_ASSERT_VALUES_EQUAL(
            20u,
            quotaById[quota2.GetQuotaId()].GetMaxNodes());
    }

    Y_UNIT_TEST(ShouldDeleteQuota)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        auto quota = tablet.CreateQuota(1_GB)->Record.GetQuota();

        tablet.DeleteQuota(quota.GetQuotaId());

        auto response = tablet.ListQuotas();
        UNIT_ASSERT_VALUES_EQUAL(0, response->Record.QuotasSize());
    }

    Y_UNIT_TEST(ShouldDeleteUnknownQuotaAsNoop)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        tablet.DeleteQuota(42);
    }

    Y_UNIT_TEST(ShouldPersistQuotasAcrossReboot)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        auto quota = tablet.CreateQuota(1_GB, 100)->Record.GetQuota();

        tablet.RebootTablet();

        auto response = tablet.ListQuotas();
        UNIT_ASSERT_VALUES_EQUAL(1, response->Record.QuotasSize());

        const auto& reloaded = response->Record.GetQuotas(0);
        UNIT_ASSERT_VALUES_EQUAL(quota.GetQuotaId(), reloaded.GetQuotaId());
        UNIT_ASSERT_VALUES_EQUAL(1_GB, reloaded.GetMaxBytes());
        UNIT_ASSERT_VALUES_EQUAL(100u, reloaded.GetMaxNodes());
    }
}

}   // namespace NCloud::NFileStore::NStorage
