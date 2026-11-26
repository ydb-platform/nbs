#include "tablet.h"

#include <cloud/filestore/libs/storage/tablet/model/block.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

NProtoPrivate::TUnsafeChangeTabletStateRequest MakeHasherTypeRequest(
    ui32 type)
{
    NProtoPrivate::TUnsafeChangeTabletStateRequest request;
    request.SetRangeIdHasherType(type);
    return request;
}

void TestReadWrite(ui32 hasherType, ui32 groupSize)
{
    TTestEnv env;
    ui32 nodeIdx = env.AddDynamicNode();
    ui64 tabletId = env.BootIndexTablet(nodeIdx);

    TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
    tablet.UnsafeChangeTabletState(MakeHasherTypeRequest(hasherType));
    UNIT_ASSERT_VALUES_EQUAL(
        hasherType,
        GetFileSystemConfig(tablet).GetRangeIdHasherType());

    tablet.InitSession("client", "session");
    auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
    ui64 handle = CreateHandle(tablet, id);

    const auto hasher = CreateRangeIdHasher(hasherType);
    const ui32 rangeId = hasher->Calc(id, 0);

    const ui64 size = 1_MB;
    const ui64 groupBytes = groupSize * 4_KB;

    // an aligned write is split into one blob per range
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
    tablet.Compaction(rangeId);
    UNIT_ASSERT_VALUES_EQUAL(expected, ReadData(tablet, handle, size));

    // truncate + extend: deletion markers are split by the group
    tablet.SetNodeAttr(TSetNodeAttrArgs(id).SetSize(size / 2 + 4_KB));
    tablet.SetNodeAttr(TSetNodeAttrArgs(id).SetSize(size));
    memset(expected.begin() + size / 2 + 4_KB, 0, size / 2 - 4_KB);

    for (ui64 offset = 0; offset < size; offset += groupBytes) {
        const ui32 r = hasher->Calc(id, offset / 4_KB);
        tablet.Cleanup(r);
        tablet.Compaction(r);
    }
    UNIT_ASSERT_VALUES_EQUAL(expected, ReadData(tablet, handle, size));

    // schemeshard is not aware of the type, alter keeps it
    tablet.UpdateConfig({.BlockCount = 2 * DefaultBlockCount});
    UNIT_ASSERT_VALUES_EQUAL(
        hasherType,
        GetFileSystemConfig(tablet).GetRangeIdHasherType());

    tablet.RebootTablet();
    tablet.RecoverSession();
    UNIT_ASSERT_VALUES_EQUAL(expected, ReadData(tablet, handle, size));
    UNIT_ASSERT_VALUES_EQUAL(
        hasherType,
        GetFileSystemConfig(tablet).GetRangeIdHasherType());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_RangeLayout)
{
    Y_UNIT_TEST(ShouldReadAndWriteWithRangeIdHasherType2)
    {
        TestReadWrite(2, 128);
    }

    Y_UNIT_TEST(ShouldReadAndWriteWithRangeIdHasherType3)
    {
        TestReadWrite(3, 256);
    }

    Y_UNIT_TEST(ShouldRejectRangeIdHasherTypeChange)
    {
        TTestEnv env;
        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        // unknown type
        auto response = tablet.AssertUnsafeChangeTabletStateFailed(
            MakeHasherTypeRequest(7));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());

        // a session is open
        tablet.InitSession("client", "session");
        response = tablet.AssertUnsafeChangeTabletStateFailed(
            MakeHasherTypeRequest(2));
        UNIT_ASSERT_VALUES_EQUAL(E_PRECONDITION_FAILED, response->GetStatus());

        // a node was created, even if it is deleted afterwards
        CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "a"));
        tablet.UnlinkNode(RootNodeId, "a", false /* unlinkDirectory */);
        tablet.DestroySession();
        response = tablet.AssertUnsafeChangeTabletStateFailed(
            MakeHasherTypeRequest(2));
        UNIT_ASSERT_VALUES_EQUAL(E_PRECONDITION_FAILED, response->GetStatus());

        // the same type is a no-op
        tablet.UnsafeChangeTabletState(MakeHasherTypeRequest(1));

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            GetFileSystemConfig(tablet).GetRangeIdHasherType());
    }

    Y_UNIT_TEST(ShouldRejectRangeIdHasherTypeOnUnconfiguredTablet)
    {
        TTestEnv env;
        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            {},
            false   // updateConfig
        );

        auto response = tablet.AssertUnsafeChangeTabletStateFailed(
            MakeHasherTypeRequest(2));
        UNIT_ASSERT_VALUES_EQUAL(E_PRECONDITION_FAILED, response->GetStatus());
    }

    Y_UNIT_TEST(ShouldRejectRangeIdHasherTypeIfRangeExceedsMaxBlobSize)
    {
        TTestEnv env;
        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        // 256 * 128_KB = 32_MB > MaxBlobSize (4_MB)
        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            {.BlockSize = 128_KB});

        auto response = tablet.AssertUnsafeChangeTabletStateFailed(
            MakeHasherTypeRequest(3));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());

        // the same type is not validated: 64 * 128_KB = 8_MB
        tablet.UnsafeChangeTabletState(MakeHasherTypeRequest(1));
    }
}

}   // namespace NCloud::NFileStore::NStorage
