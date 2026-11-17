#include "tablet.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/storage/tablet/model/block.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <cloud/storage/core/libs/features/features_config.h>

#include <library/cpp/testing/unittest/registar.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/core/base/logoblob.h>

#include <util/generic/size_literals.h>

namespace NCloud::NFileStore::NStorage {

using namespace NKikimr;
using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

void SetHasherTypeOverride(TIndexTabletClient& tablet, ui32 type)
{
    NProto::TStorageConfig patch;
    patch.SetRangeIdHasherTypeOverride(type);
    tablet.ChangeStorageConfig(std::move(patch));
    // the override is applied on tablet start only
    tablet.RebootTablet();
}

void TestReadWrite(ui32 hasherType, ui32 groupSize)
{
    TTestEnv env;
    ui32 nodeIdx = env.AddDynamicNode();
    ui64 tabletId = env.BootIndexTablet(nodeIdx);

    TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
    SetHasherTypeOverride(tablet, hasherType);
    // the persisted type is not changed
    UNIT_ASSERT_VALUES_EQUAL(
        1,
        GetFileSystemConfig(tablet).GetRangeIdHasherType());

    tablet.InitSession("client", "session");
    auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
    ui64 handle = CreateHandle(tablet, id);

    const auto hasher = CreateRangeIdHasher(hasherType);
    const ui32 rangeId = hasher->Calc(id, 0);
    UNIT_ASSERT_VALUES_EQUAL(rangeId, hasher->Calc(id, groupSize - 1));
    UNIT_ASSERT_VALUES_UNEQUAL(rangeId, hasher->Calc(id, groupSize));

    const ui64 size = 1_MB;
    const ui64 groupBytes = groupSize * 4_KB;

    // one group -> one blob
    tablet.WriteData(handle, 0, size, 'a');
    {
        auto stats = GetStorageStats(tablet);
        UNIT_ASSERT_VALUES_EQUAL(
            size / groupBytes,
            stats.GetMixedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(size / 4_KB, stats.GetMixedBlocksCount());
    }

    TString expected(size, 'a');

    // fresh blocks and fresh bytes on top
    tablet.WriteData(handle, 10 * 4_KB, 4_KB, 'b');
    memset(expected.begin() + 10 * 4_KB, 'b', 4_KB);
    tablet.WriteData(handle, 100, 10, 'c');
    memset(expected.begin() + 100, 'c', 10);
    UNIT_ASSERT_VALUES_EQUAL(expected, ReadData(tablet, handle, size));

    tablet.Flush();
    tablet.FlushBytes();
    UNIT_ASSERT_VALUES_EQUAL(expected, ReadData(tablet, handle, size));

    tablet.Compaction(rangeId);
    UNIT_ASSERT_VALUES_EQUAL(expected, ReadData(tablet, handle, size));

    // truncate + extend: deletion markers split by the group
    tablet.SetNodeAttr(TSetNodeAttrArgs(id).SetSize(size / 2 + 4_KB));
    tablet.SetNodeAttr(TSetNodeAttrArgs(id).SetSize(size));
    memset(expected.begin() + size / 2 + 4_KB, 0, size / 2 - 4_KB);
    UNIT_ASSERT_VALUES_EQUAL(expected, ReadData(tablet, handle, size));

    for (ui64 offset = 0; offset < size; offset += groupBytes) {
        const ui32 r = hasher->Calc(id, offset / 4_KB);
        tablet.Cleanup(r);
        tablet.Compaction(r);
    }
    UNIT_ASSERT_VALUES_EQUAL(expected, ReadData(tablet, handle, size));

    // alter / resize does not switch the running tablet to the persisted type
    tablet.UpdateConfig({.BlockCount = 2 * DefaultBlockCount});
    {
        const auto blobs = GetStorageStats(tablet).GetMixedBlobsCount();
        tablet.WriteData(handle, size, groupBytes, 'd');
        UNIT_ASSERT_VALUES_EQUAL(
            blobs + 1,
            GetStorageStats(tablet).GetMixedBlobsCount());
    }
    expected += TString(groupBytes, 'd');

    tablet.RebootTablet();
    tablet.RecoverSession();
    UNIT_ASSERT_VALUES_EQUAL(
        expected,
        ReadData(tablet, handle, size + groupBytes));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_RangeLayout)
{
    Y_UNIT_TEST(ShouldReadAndWriteWithRangeIdHasherTypeOverride2)
    {
        TestReadWrite(2, 128);
    }

    Y_UNIT_TEST(ShouldReadAndWriteWithRangeIdHasherTypeOverride3)
    {
        TestReadWrite(3, 256);
    }

    Y_UNIT_TEST(ShouldApplyRangeIdHasherTypeOverrideToNewTablet)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetRangeIdHasherTypeOverride(3);
        TTestEnv env({}, storageConfig);
        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");
        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        // 1 MiB = one group of type 3 -> one blob (four with type 1)
        tablet.WriteData(handle, 0, 1_MB, 'a');
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            GetStorageStats(tablet).GetMixedBlobsCount());
    }

    Y_UNIT_TEST(ShouldRejectUnknownRangeIdHasherTypeOverride)
    {
        TTestEnv env;
        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        NProto::TStorageConfig patch;
        patch.SetRangeIdHasherTypeOverride(7);
        tablet.SendChangeStorageConfigRequest(std::move(patch));
        auto response = tablet.RecvChangeStorageConfigResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_ARGUMENT,
            response->GetStatus(),
            response->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldBreakTabletWithUnknownRangeIdHasherType)
    {
        TTestEnv env;
        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TDynamicCountersPtr counters = new TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto invalidHasherType = counters->GetCounter(
            "AppCriticalEvents/InvalidRangeIdHasherType",
            true);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        // the features config is not validated by ChangeStorageConfig
        NCloud::NProto::TFeaturesConfig featuresConfigProto;
        auto* feature = featuresConfigProto.AddFeatures();
        feature->SetName("RangeIdHasherTypeOverride");
        feature->SetValue("7");
        feature->MutableWhitelist()->AddEntityIds(
            TString(DefaultFileSystemId));
        env.GetStorageConfig()->SetFeaturesConfig(
            NFeatures::TFeaturesConfig(featuresConfigProto));

        tablet.RebootTablet();
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(1, invalidHasherType->Val());

        tablet.SendCreateSessionRequest("client", "session");
        auto response = tablet.RecvCreateSessionResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
    }

    Y_UNIT_TEST(ShouldAddDataWithRangeIdHasherTypeOverride3)
    {
        TTestEnv env;
        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        SetHasherTypeOverride(tablet, 3);

        tablet.InitSession("client", "session");
        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        // 1 MiB = one group -> one blob (four with type 1)
        auto gbi = tablet.GenerateBlobIds(id, handle, 0, 1_MB)->Record;
        UNIT_ASSERT_VALUES_EQUAL(1, gbi.BlobsSize());

        const auto blobId =
            LogoBlobIDFromLogoBlobID(gbi.GetBlobs(0).GetBlobId());
        UNIT_ASSERT_VALUES_EQUAL(1_MB, blobId.BlobSize());

        auto evPut = std::make_unique<TEvBlobStorage::TEvPut>(
            blobId,
            TString(1_MB, 'a'),
            TInstant::Max(),
            NKikimrBlobStorage::UserData);
        const auto proxy =
            MakeBlobStorageProxyID(gbi.GetBlobs(0).GetBSGroupId());
        env.GetRuntime().Send(CreateEventForBSProxy(
            env.GetRuntime().AllocateEdgeActor(proxy.NodeId()),
            proxy,
            evPut.release(),
            blobId.Cookie()));

        tablet.AddData(
            id,
            handle,
            0,
            1_MB,
            TVector<NKikimr::TLogoBlobID>({blobId}),
            gbi.GetCommitId());

        UNIT_ASSERT_VALUES_EQUAL(
            TString(1_MB, 'a'),
            ReadData(tablet, handle, 1_MB));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            GetStorageStats(tablet).GetMixedBlobsCount());
    }
}

}   // namespace NCloud::NFileStore::NStorage
