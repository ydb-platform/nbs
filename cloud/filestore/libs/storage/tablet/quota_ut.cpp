#include "quota.h"

#include <library/cpp/testing/unittest/registar.h>

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
}

}   // namespace NCloud::NFileStore::NStorage
