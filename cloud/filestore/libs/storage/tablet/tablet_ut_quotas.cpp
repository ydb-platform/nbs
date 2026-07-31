#include "tablet.h"

#include <cloud/filestore/libs/storage/testlib/helpers.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>
#include <util/generic/size_literals.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_Quotas)
{
    Y_UNIT_TEST(ShouldSetAndListQuotas)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        auto quota1 = tablet.SetQuota(1, 1_GB, 10)->Record.GetQuota();
        UNIT_ASSERT_VALUES_EQUAL(1u, quota1.GetQuotaId());
        UNIT_ASSERT_VALUES_EQUAL(1_GB, quota1.GetMaxBytes());
        UNIT_ASSERT_VALUES_EQUAL(10u, quota1.GetMaxNodes());
        UNIT_ASSERT(quota1.GetCreationTimestampUs() != 0);

        tablet.SetQuota(2, 2_GB, 20);

        auto response = tablet.ListQuotas();
        UNIT_ASSERT_VALUES_EQUAL(2, response->Record.QuotasSize());

        THashMap<ui32, NProto::TQuota> quotaById;
        for (const auto& quota: response->Record.GetQuotas()) {
            quotaById[quota.GetQuotaId()] = quota;
        }

        UNIT_ASSERT_VALUES_EQUAL(1_GB, quotaById[1].GetMaxBytes());
        UNIT_ASSERT_VALUES_EQUAL(10u, quotaById[1].GetMaxNodes());
        UNIT_ASSERT_VALUES_EQUAL(2_GB, quotaById[2].GetMaxBytes());
        UNIT_ASSERT_VALUES_EQUAL(20u, quotaById[2].GetMaxNodes());
    }

    Y_UNIT_TEST(ShouldUpsertQuotaPreservingCreationTimestamp)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        auto created = tablet.SetQuota(1, 1_GB, 100)->Record.GetQuota();
        auto resetSame = tablet.SetQuota(1, 1_GB, 100)->Record.GetQuota();
        auto updated = tablet.SetQuota(1, 2_GB, 200)->Record.GetQuota();

        UNIT_ASSERT_VALUES_EQUAL(1_GB, resetSame.GetMaxBytes());
        UNIT_ASSERT_VALUES_EQUAL(2_GB, updated.GetMaxBytes());
        UNIT_ASSERT_VALUES_EQUAL(200u, updated.GetMaxNodes());

        UNIT_ASSERT_VALUES_EQUAL(
            created.GetCreationTimestampUs(),
            resetSame.GetCreationTimestampUs());
        UNIT_ASSERT_VALUES_EQUAL(
            created.GetCreationTimestampUs(),
            updated.GetCreationTimestampUs());
    }

    Y_UNIT_TEST(ShouldDeleteAndReuseQuotaId)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        tablet.DeleteQuota(1);   // noop, nothing exists yet

        tablet.SetQuota(1, 1_GB, 100);
        tablet.DeleteQuota(1);
        UNIT_ASSERT_VALUES_EQUAL(0, tablet.ListQuotas()->Record.QuotasSize());

        auto quota = tablet.SetQuota(1, 2_GB, 200)->Record.GetQuota();
        UNIT_ASSERT_VALUES_EQUAL(2_GB, quota.GetMaxBytes());
        UNIT_ASSERT_VALUES_EQUAL(200u, quota.GetMaxNodes());
    }

    Y_UNIT_TEST(ShouldRejectZeroQuotaId)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        tablet.AssertSetQuotaFailed(0, 1_GB, 100);
    }

    Y_UNIT_TEST(ShouldPersistQuotasAcrossReboot)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        auto quota = tablet.SetQuota(1, 1_GB, 100)->Record.GetQuota();

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
