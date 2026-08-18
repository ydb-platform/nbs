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

    Y_UNIT_TEST(ShouldMarkEmptyDirectoryWithQuotaId)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto dirId =
            CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "dir"));

        tablet.SetNodeAttr(TSetNodeAttrArgs(dirId).SetQuotaId(1));

        // re-marking with the same quota is a harmless no-op success
        tablet.SetNodeAttr(TSetNodeAttrArgs(dirId).SetQuotaId(1));
    }

    Y_UNIT_TEST(ShouldRejectMarkingWithZeroQuotaId)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto dirId =
            CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "dir"));
        tablet.AssertSetNodeAttrFailed(TSetNodeAttrArgs(dirId).SetQuotaId(0));
    }

    Y_UNIT_TEST(ShouldRejectMarkingNonEmptyDirectory)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto dirId =
            CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "dir"));
        CreateNode(tablet, TCreateNodeArgs::File(dirId, "file"));

        tablet.AssertSetNodeAttrFailed(TSetNodeAttrArgs(dirId).SetQuotaId(1));
    }

    Y_UNIT_TEST(ShouldRejectMarkingNonDirectory)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto fileId =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "file"));
        tablet.AssertSetNodeAttrFailed(TSetNodeAttrArgs(fileId).SetQuotaId(1));

        auto sockId =
            CreateNode(tablet, TCreateNodeArgs::Sock(RootNodeId, "sock"));
        tablet.AssertSetNodeAttrFailed(TSetNodeAttrArgs(sockId).SetQuotaId(1));

        auto fifoId =
            CreateNode(tablet, TCreateNodeArgs::Fifo(RootNodeId, "fifo"));
        tablet.AssertSetNodeAttrFailed(TSetNodeAttrArgs(fifoId).SetQuotaId(1));

        auto symLinkId = CreateNode(
            tablet,
            TCreateNodeArgs::SymLink(RootNodeId, "symlink", "target"));
        tablet.AssertSetNodeAttrFailed(
            TSetNodeAttrArgs(symLinkId).SetQuotaId(1));

        auto charDevId =
            CreateNode(tablet, TCreateNodeArgs::CharDev(RootNodeId, "chardev"));
        tablet.AssertSetNodeAttrFailed(
            TSetNodeAttrArgs(charDevId).SetQuotaId(1));

        auto blockDevId = CreateNode(
            tablet,
            TCreateNodeArgs::BlockDev(RootNodeId, "blockdev"));
        tablet.AssertSetNodeAttrFailed(
            TSetNodeAttrArgs(blockDevId).SetQuotaId(1));
    }

    Y_UNIT_TEST(ShouldTrackAndReportQuotaUsage)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        tablet.SetQuota(1, 1_GB, 100);

        auto dirId =
            CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "dir"));
        tablet.SetNodeAttr(TSetNodeAttrArgs(dirId).SetQuotaId(1));

        // attaching the directory itself flips its QuotaId from 0 to 1,
        // which the UpdateNode usage hook treats as the node moving into
        // the new quota - so the attach point itself counts as +1 node
        auto usages = tablet.ListQuotas()->Record.GetUsages();
        UNIT_ASSERT_VALUES_EQUAL(1, usages.size());
        UNIT_ASSERT_VALUES_EQUAL(1u, usages[0].GetUsedNodes());
        UNIT_ASSERT_VALUES_EQUAL(0u, usages[0].GetUsedBytes());

        auto fileId = CreateNode(tablet, TCreateNodeArgs::File(dirId, "file"));
        tablet.SetNodeAttr(TSetNodeAttrArgs(fileId).SetSize(100));

        CreateNode(tablet, TCreateNodeArgs::Directory(dirId, "subdir"));

        THashMap<ui32, NProtoPrivate::TQuotaUsage> usageByQuotaId;
        {
            auto response = tablet.ListQuotas();
            for (const auto& usage: response->Record.GetUsages()) {
                usageByQuotaId[usage.GetQuotaId()] = usage;
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(1, usageByQuotaId.size());
        UNIT_ASSERT_VALUES_EQUAL(3u, usageByQuotaId[1].GetUsedNodes());
        UNIT_ASSERT_VALUES_EQUAL(100u, usageByQuotaId[1].GetUsedBytes());

        tablet.UnlinkNode(dirId, "file", false);

        usageByQuotaId.clear();
        {
            auto response = tablet.ListQuotas();
            for (const auto& usage: response->Record.GetUsages()) {
                usageByQuotaId[usage.GetQuotaId()] = usage;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(2u, usageByQuotaId[1].GetUsedNodes());
        UNIT_ASSERT_VALUES_EQUAL(0u, usageByQuotaId[1].GetUsedBytes());
    }

    Y_UNIT_TEST(ShouldPersistQuotaUsageAcrossReboot)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        tablet.SetQuota(1, 1_GB, 100);

        auto dirId =
            CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "dir"));
        tablet.SetNodeAttr(TSetNodeAttrArgs(dirId).SetQuotaId(1));
        CreateNode(tablet, TCreateNodeArgs::File(dirId, "file"));

        tablet.RebootTablet();

        auto usages = tablet.ListQuotas()->Record.GetUsages();
        UNIT_ASSERT_VALUES_EQUAL(1, usages.size());
        UNIT_ASSERT_VALUES_EQUAL(1u, usages[0].GetQuotaId());
        UNIT_ASSERT_VALUES_EQUAL(2u, usages[0].GetUsedNodes());
    }

    Y_UNIT_TEST(ShouldNotTrackUsageForUnknownQuotaId)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        // quota 99 is never defined via SetQuota - attaching a directory to
        // it and creating nodes under it should not produce a usage entry,
        // since there's no quota definition to attribute the usage to
        auto dirId =
            CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "dir"));
        tablet.SetNodeAttr(TSetNodeAttrArgs(dirId).SetQuotaId(99));
        CreateNode(tablet, TCreateNodeArgs::File(dirId, "file"));

        auto usages = tablet.ListQuotas()->Record.GetUsages();
        UNIT_ASSERT_VALUES_EQUAL(0, usages.size());
    }

    Y_UNIT_TEST(ShouldRemoveUsageWhenQuotaIsDeleted)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        tablet.SetQuota(1, 1_GB, 100);

        auto dirId =
            CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "dir"));
        tablet.SetNodeAttr(TSetNodeAttrArgs(dirId).SetQuotaId(1));
        CreateNode(tablet, TCreateNodeArgs::File(dirId, "file"));

        {
            auto usages = tablet.ListQuotas()->Record.GetUsages();
            UNIT_ASSERT_VALUES_EQUAL(1, usages.size());
        }

        tablet.DeleteQuota(1);

        {
            auto usages = tablet.ListQuotas()->Record.GetUsages();
            UNIT_ASSERT_VALUES_EQUAL(0, usages.size());
        }

        // re-creating the quota starts usage tracking from a clean slate,
        // rather than resurrecting the old, now-orphaned usage
        tablet.SetQuota(1, 1_GB, 100);
        {
            auto usages = tablet.ListQuotas()->Record.GetUsages();
            UNIT_ASSERT_VALUES_EQUAL(0, usages.size());
        }
    }

    Y_UNIT_TEST(ShouldRejectMarkingWithDifferentExistingQuotaId)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto dirId =
            CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "dir"));

        tablet.SetNodeAttr(TSetNodeAttrArgs(dirId).SetQuotaId(1));
        tablet.AssertSetNodeAttrFailed(TSetNodeAttrArgs(dirId).SetQuotaId(2));
    }
}

}   // namespace NCloud::NFileStore::NStorage
