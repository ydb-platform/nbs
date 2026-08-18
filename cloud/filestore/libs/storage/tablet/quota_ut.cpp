#include "quota.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash_set.h>
#include <util/generic/size_literals.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TQuota MakeQuota(ui32 quotaId, ui64 nodeId, ui64 maxBytes)
{
    NProto::TQuota quota;
    quota.SetQuotaId(quotaId);
    quota.SetMaxBytes(maxBytes);
    quota.AddNodeId(nodeId);

    return quota;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TQuotaStoreTest)
{
    Y_UNIT_TEST(ShouldReturnNullptrForUnknownQuota)
    {
        TQuotaStore store;

        UNIT_ASSERT(!store.FindQuota(42));
    }

    Y_UNIT_TEST(ShouldRemoveUnknownQuotaAsNoop)
    {
        TQuotaStore store;

        store.RemoveQuota(42);

        UNIT_ASSERT(!store.FindQuota(42));
    }

    Y_UNIT_TEST(ShouldStoreAndFindQuota)
    {
        TQuotaStore store;

        store.UpdateQuota(MakeQuota(1, 100, 1_GB));

        const auto* quota = store.FindQuota(1);
        UNIT_ASSERT(quota);
        UNIT_ASSERT_VALUES_EQUAL(1u, quota->GetQuotaId());
        UNIT_ASSERT_VALUES_EQUAL(1, quota->NodeIdSize());
        UNIT_ASSERT_VALUES_EQUAL(100u, quota->GetNodeId(0));
        UNIT_ASSERT_VALUES_EQUAL(1_GB, quota->GetMaxBytes());
    }

    Y_UNIT_TEST(ShouldSupportMultipleAttachedDirectories)
    {
        TQuotaStore store;

        auto quota = MakeQuota(1, 100, 1_GB);
        quota.AddNodeId(200);
        quota.AddNodeId(300);
        store.UpdateQuota(quota);

        const auto* found = store.FindQuota(1);
        UNIT_ASSERT(found);
        UNIT_ASSERT_VALUES_EQUAL(3, found->NodeIdSize());
        UNIT_ASSERT_VALUES_EQUAL(100u, found->GetNodeId(0));
        UNIT_ASSERT_VALUES_EQUAL(200u, found->GetNodeId(1));
        UNIT_ASSERT_VALUES_EQUAL(300u, found->GetNodeId(2));
    }

    Y_UNIT_TEST(ShouldOverwriteExistingQuotaOnUpdate)
    {
        TQuotaStore store;

        store.UpdateQuota(MakeQuota(1, 100, 1_GB));
        store.UpdateQuota(MakeQuota(1, 100, 2_GB));

        const auto* quota = store.FindQuota(1);
        UNIT_ASSERT(quota);
        UNIT_ASSERT_VALUES_EQUAL(2_GB, quota->GetMaxBytes());
    }

    Y_UNIT_TEST(ShouldRemoveQuota)
    {
        TQuotaStore store;

        store.UpdateQuota(MakeQuota(1, 100, 1_GB));
        UNIT_ASSERT(store.FindQuota(1));

        store.RemoveQuota(1);

        UNIT_ASSERT(!store.FindQuota(1));
    }

    Y_UNIT_TEST(ShouldNotAffectOtherQuotasOnRemove)
    {
        TQuotaStore store;

        store.UpdateQuota(MakeQuota(1, 100, 1_GB));
        store.UpdateQuota(MakeQuota(2, 200, 2_GB));

        store.RemoveQuota(1);

        UNIT_ASSERT(!store.FindQuota(1));
        UNIT_ASSERT(store.FindQuota(2));
    }

    Y_UNIT_TEST(ShouldRemoveUsageOnQuotaRemoval)
    {
        TQuotaStore store;

        store.UpdateQuota(MakeQuota(1, 100, 1_GB));
        store.UpdateQuota(MakeQuota(2, 200, 2_GB));
        store.UpdateUsage(1, 1_GB, 5);
        store.UpdateUsage(2, 2_GB, 10);

        store.RemoveQuota(1);

        UNIT_ASSERT(!store.FindUsage(1));
        UNIT_ASSERT(store.FindUsage(2));
    }

    Y_UNIT_TEST(ShouldGetAllQuotas)
    {
        TQuotaStore store;

        store.UpdateQuota(MakeQuota(1, 100, 1_GB));
        store.UpdateQuota(MakeQuota(2, 200, 2_GB));
        store.UpdateQuota(MakeQuota(3, 300, 3_GB));

        auto quotas = store.GetQuotas();
        UNIT_ASSERT_VALUES_EQUAL(3u, quotas.size());

        THashSet<ui32> quotaIds;
        for (const auto& quota: quotas) {
            quotaIds.insert(quota.GetQuotaId());
        }
        UNIT_ASSERT_VALUES_EQUAL(3u, quotaIds.size());
        UNIT_ASSERT(quotaIds.contains(1));
        UNIT_ASSERT(quotaIds.contains(2));
        UNIT_ASSERT(quotaIds.contains(3));
    }

    Y_UNIT_TEST(ShouldReturnNullptrForUnknownUsage)
    {
        TQuotaStore store;

        UNIT_ASSERT(!store.FindUsage(42));
    }

    Y_UNIT_TEST(ShouldAccumulateUsageFromDeltas)
    {
        TQuotaStore store;

        store.UpdateUsage(1, 1_GB, 1);
        store.UpdateUsage(1, 2_GB, 2);

        const auto* usage = store.FindUsage(1);
        UNIT_ASSERT(usage);
        UNIT_ASSERT_VALUES_EQUAL(3_GB, usage->UsedBytes);
        UNIT_ASSERT_VALUES_EQUAL(3u, usage->UsedNodes);
    }

    Y_UNIT_TEST(ShouldDecrementUsageOnNegativeDeltas)
    {
        TQuotaStore store;

        store.UpdateUsage(1, 3_GB, 3);
        store.UpdateUsage(1, -1_GB, -1);

        const auto* usage = store.FindUsage(1);
        UNIT_ASSERT(usage);
        UNIT_ASSERT_VALUES_EQUAL(2_GB, usage->UsedBytes);
        UNIT_ASSERT_VALUES_EQUAL(2u, usage->UsedNodes);
    }

    Y_UNIT_TEST(ShouldIgnoreUsageUpdatesForZeroQuotaId)
    {
        TQuotaStore store;

        store.UpdateUsage(0, 1_GB, 1);

        UNIT_ASSERT(!store.FindUsage(0));
    }

    Y_UNIT_TEST(ShouldNotAffectOtherQuotasUsageOnUpdate)
    {
        TQuotaStore store;

        store.UpdateUsage(1, 1_GB, 1);
        store.UpdateUsage(2, 2_GB, 2);

        const auto* usage1 = store.FindUsage(1);
        UNIT_ASSERT(usage1);
        UNIT_ASSERT_VALUES_EQUAL(1_GB, usage1->UsedBytes);
        UNIT_ASSERT_VALUES_EQUAL(1u, usage1->UsedNodes);

        const auto* usage2 = store.FindUsage(2);
        UNIT_ASSERT(usage2);
        UNIT_ASSERT_VALUES_EQUAL(2_GB, usage2->UsedBytes);
        UNIT_ASSERT_VALUES_EQUAL(2u, usage2->UsedNodes);
    }

    Y_UNIT_TEST(ShouldLoadUsageAsAbsoluteValue)
    {
        TQuotaStore store;

        store.UpdateUsage(1, 1_GB, 1);

        TQuotaUsage loaded;
        loaded.QuotaId = 1;
        loaded.UsedBytes = 5_GB;
        loaded.UsedNodes = 5;
        store.LoadUsage(loaded);

        const auto* usage = store.FindUsage(1);
        UNIT_ASSERT(usage);
        UNIT_ASSERT_VALUES_EQUAL(5_GB, usage->UsedBytes);
        UNIT_ASSERT_VALUES_EQUAL(5u, usage->UsedNodes);
    }

    Y_UNIT_TEST(ShouldGetAllUsages)
    {
        TQuotaStore store;

        store.UpdateUsage(1, 1_GB, 1);
        store.UpdateUsage(2, 2_GB, 2);
        store.UpdateUsage(3, 3_GB, 3);

        auto usages = store.GetUsages();
        UNIT_ASSERT_VALUES_EQUAL(3u, usages.size());

        THashSet<ui32> quotaIds;
        for (const auto& usage: usages) {
            quotaIds.insert(usage.QuotaId);
        }
        UNIT_ASSERT_VALUES_EQUAL(3u, quotaIds.size());
        UNIT_ASSERT(quotaIds.contains(1));
        UNIT_ASSERT(quotaIds.contains(2));
        UNIT_ASSERT(quotaIds.contains(3));
    }
}

}   // namespace NCloud::NFileStore::NStorage
