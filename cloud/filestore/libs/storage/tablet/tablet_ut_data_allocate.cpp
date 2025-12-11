#include "tablet.h"

#include "tablet_schema.h"

#include <cloud/filestore/libs/storage/tablet/model/block.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

#include <memory>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TEnvironment: public NUnitTest::TBaseFixture
{
    const ui64 MaxBlocks = 64;

    TTestEnv Env;
    std::unique_ptr<TIndexTabletClient> Tablet;

    ui64 Id = 0;
    ui64 Handle = 0;

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        Env.CreateSubDomain("nfs");

        const ui32 nodeIdx = Env.CreateNode("nfs");
        const ui64 tabletId = Env.BootIndexTablet(nodeIdx);

        Tablet = std::make_unique<TIndexTabletClient>(
            Env.GetRuntime(),
            nodeIdx,
            tabletId,
            TFileSystemConfig{.BlockCount = MaxBlocks});
        Tablet->InitSession("client", "session");

        Id = CreateNode(*Tablet, TCreateNodeArgs::File(RootNodeId, "test_1"));
        Handle = CreateHandle(*Tablet, Id);
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {}

    void TestAllocate(
        ui64 offset,
        ui32 length,
        ui64 targetSize,
        ui32 flags,
        const TString& expected)
    {
        Tablet->AllocateData(Handle, offset, length, flags);

        const auto stat = Tablet->GetNodeAttr(Id)->Record.GetNode();
        UNIT_ASSERT_VALUES_EQUAL(stat.GetId(), Id);
        UNIT_ASSERT_VALUES_EQUAL(stat.GetSize(), targetSize);

        if (targetSize == 0) {
            return;
        }

        const auto response = Tablet->ReadData(Handle, 0, targetSize);
        const auto& buffer = response->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_Data_Allocate)
{
    using namespace NActors;

    using namespace NCloud::NStorage;

    Y_UNIT_TEST_F(ShouldReturnNotSupportedForInsertRangeFlag, TEnvironment)
    {
        const auto response = Tablet->AssertAllocateDataFailed(
            Handle,
            0,
            4_KB,
            ProtoFlag(NProto::TAllocateDataRequest::F_INSERT_RANGE));

        UNIT_ASSERT_VALUES_EQUAL(E_FS_NOTSUPP, response->GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldReturnNotSupportedForCollapseRangeFlag, TEnvironment)
    {
        const auto response = Tablet->AssertAllocateDataFailed(
            Handle,
            0,
            4_KB,
            ProtoFlag(NProto::TAllocateDataRequest::F_COLLAPSE_RANGE));

        UNIT_ASSERT_VALUES_EQUAL(E_FS_NOTSUPP, response->GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldReturnNotSupportedForSinglePunchHoleFlag, TEnvironment)
    {
        {
            const auto response = Tablet->AssertAllocateDataFailed(
                Handle,
                0,
                4_KB,
                ProtoFlag(NProto::TAllocateDataRequest::F_PUNCH_HOLE));

            UNIT_ASSERT_VALUES_EQUAL(
                E_FS_NOTSUPP,
                response->GetError().GetCode());
        }

        {
            const auto response = Tablet->AllocateData(
                Handle,
                0,
                4_KB,
                ProtoFlag(NProto::TAllocateDataRequest::F_PUNCH_HOLE) |
                    ProtoFlag(NProto::TAllocateDataRequest::F_KEEP_SIZE));

            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetError().GetCode());
        }
    }

    Y_UNIT_TEST_F(ShouldReturnErrorOnInvalidHandle, TEnvironment)
    {
        const auto response =
            Tablet->AssertAllocateDataFailed(Handle + 1, 0, 4_KB, 0);

        UNIT_ASSERT_VALUES_EQUAL(
            E_FS_BADHANDLE,
            response->GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldReturnErrorOnZeroLength, TEnvironment)
    {
        const auto response = Tablet->AssertAllocateDataFailed(Handle, 0, 0, 0);

        UNIT_ASSERT_VALUES_EQUAL(E_FS_INVAL, response->GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldReturnErrorOnBigSize, TEnvironment)
    {
        {
            // too big
            const auto response =
                Tablet->AssertAllocateDataFailed(Handle, 0, Max<ui64>(), 0);

            UNIT_ASSERT_VALUES_EQUAL(E_FS_FBIG, response->GetError().GetCode());
        }

        {
            // overflow
            const auto response = Tablet->AssertAllocateDataFailed(
                Handle,
                Max<ui64>() / 2 + 1,
                Max<ui32>(),
                0);

            UNIT_ASSERT_VALUES_EQUAL(
                E_FS_NOSPC,
                response->GetError().GetCode());
        }

        {
            // over the limit
            const auto response = Tablet->AssertAllocateDataFailed(
                Handle,
                8_KB,
                (MaxBlocks - 1) * 4_KB,
                0);

            UNIT_ASSERT_VALUES_EQUAL(
                E_FS_NOSPC,
                response->GetError().GetCode());
        }

        {
            // same way but no offset
            const auto response = Tablet->AssertAllocateDataFailed(
                Handle,
                0,
                (MaxBlocks + 1) * 4_KB,
                0);

            UNIT_ASSERT_VALUES_EQUAL(
                E_FS_NOSPC,
                response->GetError().GetCode());
        }
    }

    Y_UNIT_TEST_F(ShouldAllocateData, TEnvironment)
    {
        Tablet->AllocateData(Handle, 0, 8_KB, 0);

        auto stat = Tablet->GetNodeAttr(Id)->Record.GetNode();
        UNIT_ASSERT_VALUES_EQUAL(stat.GetId(), Id);
        UNIT_ASSERT_VALUES_EQUAL(stat.GetSize(), 8_KB);

        // posix like, no shrink
        Tablet->AllocateData(Handle, 0, 4_KB, 0);

        stat = Tablet->GetNodeAttr(Id)->Record.GetNode();
        UNIT_ASSERT_VALUES_EQUAL(stat.GetId(), Id);
        UNIT_ASSERT_VALUES_EQUAL(stat.GetSize(), 8_KB);

        // up to a limit
        Tablet->AllocateData(Handle, 8_KB, (MaxBlocks - 2) * 4_KB, 0);
        // same way but no offset
        Tablet->AllocateData(Handle, 0, MaxBlocks * 4_KB, 0);

        Tablet->WriteData(Handle, 0, 16_KB, 'a');

        TString expected(16_KB, 'a');
        {
            const auto response = Tablet->ReadData(Handle, 0, 16_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        // must not rewrite data
        Tablet->AllocateData(Handle, 4_KB, 8_KB, 0);

        {
            const auto response = Tablet->ReadData(Handle, 0, 16_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }
    }

    Y_UNIT_TEST_F(ShouldAllocateDataWithUnshareRange, TEnvironment)
    {
        const ui32 flags =
            ProtoFlag(NProto::TAllocateDataRequest::F_UNSHARE_RANGE);

        Tablet->AllocateData(Handle, 0, 8_KB, flags);

        auto stat = Tablet->GetNodeAttr(Id)->Record.GetNode();
        UNIT_ASSERT_VALUES_EQUAL(stat.GetId(), Id);
        UNIT_ASSERT_VALUES_EQUAL(stat.GetSize(), 8_KB);

        // posix like, no shrink
        Tablet->AllocateData(Handle, 0, 4_KB, flags);

        stat = Tablet->GetNodeAttr(Id)->Record.GetNode();
        UNIT_ASSERT_VALUES_EQUAL(stat.GetId(), Id);
        UNIT_ASSERT_VALUES_EQUAL(stat.GetSize(), 8_KB);

        // up to a limit
        Tablet->AllocateData(Handle, 8_KB, (MaxBlocks - 2) * 4_KB, flags);
        // same way but no offset
        Tablet->AllocateData(Handle, 0, MaxBlocks * 4_KB, flags);

        Tablet->WriteData(Handle, 0, 16_KB, 'a');

        TString expected(16_KB, 'a');
        {
            const auto response = Tablet->ReadData(Handle, 0, 16_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        // must not rewrite data
        Tablet->AllocateData(Handle, 4_KB, 8_KB, flags);

        {
            const auto response = Tablet->ReadData(Handle, 0, 16_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }
    }

    Y_UNIT_TEST_F(ShouldAllocateDataWithKeepSize, TEnvironment)
    {
        const ui32 flags = ProtoFlag(NProto::TAllocateDataRequest::F_KEEP_SIZE);

        // Nothing should be allocated
        TestAllocate(0, 4_KB, 0, flags, TString{});

        Tablet->AllocateData(Handle, 0, 16_KB, 0);

        auto stat = Tablet->GetNodeAttr(Id)->Record.GetNode();
        UNIT_ASSERT_VALUES_EQUAL(stat.GetId(), Id);
        UNIT_ASSERT_VALUES_EQUAL(stat.GetSize(), 16_KB);

        Tablet->WriteData(Handle, 0, 16_KB, 'a');

        TString expected(16_KB, 'a');
        {
            const auto response = Tablet->ReadData(Handle, 0, 16_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        // Nothing should be zeroed inside range

        // Aligned from start
        TestAllocate(0, 4_KB, 16_KB, flags, expected);

        // Aligned from aligned offset
        TestAllocate(4_KB, 4_KB, 16_KB, flags, expected);

        // Aligned from aligned offset till the end
        TestAllocate(8_KB, 8_KB, 16_KB, flags, expected);

        // Aligned from aligned offset over the end (size must not change)
        TestAllocate(12_KB, 8_KB, 16_KB, flags, expected);

        // Unaligned from start
        TestAllocate(0, 5_KB, 16_KB, flags, expected);

        // Unaligned from aligned offset
        TestAllocate(4_KB, 5_KB, 16_KB, flags, expected);

        // Unaligned from unaligned offset
        TestAllocate(8_KB, 2_KB, 16_KB, flags, expected);

        // Unaligned from unaligned offset till the end
        TestAllocate(10_KB, 10_KB, 16_KB, flags, expected);

        // Unaligned from aligned offset till the end
        TestAllocate(12_KB, 9_KB, 16_KB, flags, expected);

        // Unaligned from aligned offset over the end
        TestAllocate(12_KB, 10_KB, 16_KB, flags, expected);

        // Unligned from unaligned offset over the end
        TestAllocate(13_KB, 13_KB, 16_KB, flags, expected);
    }

    Y_UNIT_TEST_F(ShouldAllocateDataWithZero, TEnvironment)
    {
        const ui32 flags =
            ProtoFlag(NProto::TAllocateDataRequest::F_ZERO_RANGE);

        Tablet->AllocateData(Handle, 0, 16_KB, flags);

        auto stat = Tablet->GetNodeAttr(Id)->Record.GetNode();
        UNIT_ASSERT_VALUES_EQUAL(stat.GetId(), Id);
        UNIT_ASSERT_VALUES_EQUAL(stat.GetSize(), 16_KB);

        Tablet->WriteData(Handle, 0, 16_KB, 'a');

        TString expected(16_KB, 'a');
        {
            const auto response = Tablet->ReadData(Handle, 0, 16_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        {
            // Aligned from start
            memset(expected.begin(), 0, 4_KB);
            TestAllocate(0, 4_KB, 16_KB, flags, expected);
        }

        {
            // Aligned from aligned offset
            memset(expected.begin() + 4_KB, 0, 4_KB);
            TestAllocate(4_KB, 4_KB, 16_KB, flags, expected);
        }

        {
            // Aligned from aligned offset till the end
            memset(expected.begin() + 8_KB, 0, 8_KB);
            TestAllocate(8_KB, 8_KB, 16_KB, flags, expected);
        }

        {
            // Aligned from aligned offset over the end
            expected.ReserveAndResize(20_KB);
            memset(expected.begin() + 12_KB, 0, 8_KB);
            TestAllocate(12_KB, 8_KB, 20_KB, flags, expected);
        }

        Tablet->WriteData(Handle, 0, 20_KB, 'a');
        expected.assign(20_KB, 'a');

        {
            // Unaligned from start
            memset(expected.begin(), 0, 5_KB);
            TestAllocate(0, 5_KB, 20_KB, flags, expected);
        }

        {
            // Unaligned from aligned offset
            memset(expected.begin() + 4_KB, 0, 5_KB);
            TestAllocate(4_KB, 5_KB, 20_KB, flags, expected);
        }

        {
            // Unaligned from unaligned offset
            memset(expected.begin() + 8_KB, 0, 2_KB);
            TestAllocate(8_KB, 2_KB, 20_KB, flags, expected);
        }

        {
            // Unaligned from unaligned offset till the end
            memset(expected.begin() + 10_KB, 0, 10_KB);
            TestAllocate(10_KB, 10_KB, 20_KB, flags, expected);
        }

        Tablet->WriteData(Handle, 0, 21_KB, 'a');
        expected.assign(21_KB, 'a');

        {
            // Unaligned from aligned offset till the end
            memset(expected.begin() + 12_KB, 0, 9_KB);
            TestAllocate(12_KB, 9_KB, 21_KB, flags, expected);
        }

        {
            // Unaligned from aligned offset over the end
            expected.ReserveAndResize(22_KB);
            memset(expected.begin() + 12_KB, 0, 10_KB);
            TestAllocate(12_KB, 10_KB, 22_KB, flags, expected);
        }

        Tablet->AllocateData(Handle, 0, 24_KB, flags);
        Tablet->WriteData(Handle, 0, 24_KB, 'a');
        expected.assign(24_KB, 'a');

        {
            // Unligned from unaligned offset over the end
            expected.ReserveAndResize(26_KB);
            memset(expected.begin() + 13_KB, 0, 13_KB);
            TestAllocate(13_KB, 13_KB, 26_KB, flags, expected);
        }
    }

    Y_UNIT_TEST_F(ShouldAllocateDataWithZeroAndKeepSize, TEnvironment)
    {
        const ui32 flags =
            ProtoFlag(NProto::TAllocateDataRequest::F_ZERO_RANGE) |
            ProtoFlag(NProto::TAllocateDataRequest::F_KEEP_SIZE);

        // Nothing should be allocated
        TestAllocate(0, 4_KB, 0, flags, TString{});

        Tablet->AllocateData(Handle, 0, 16_KB, 0);

        auto stat = Tablet->GetNodeAttr(Id)->Record.GetNode();
        UNIT_ASSERT_VALUES_EQUAL(stat.GetId(), Id);
        UNIT_ASSERT_VALUES_EQUAL(stat.GetSize(), 16_KB);

        Tablet->WriteData(Handle, 0, 16_KB, 'a');

        TString expected(16_KB, 'a');
        {
            const auto response = Tablet->ReadData(Handle, 0, 16_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        {
            // Aligned from start
            memset(expected.begin(), 0, 4_KB);
            TestAllocate(0, 4_KB, 16_KB, flags, expected);
        }

        {
            // Aligned from aligned offset
            memset(expected.begin() + 4_KB, 0, 4_KB);
            TestAllocate(4_KB, 4_KB, 16_KB, flags, expected);
        }

        {
            // Aligned from aligned offset till the end
            memset(expected.begin() + 8_KB, 0, 8_KB);
            TestAllocate(8_KB, 8_KB, 16_KB, flags, expected);
        }

        {
            // Aligned from aligned offset over the end (size must not change)
            memset(expected.begin() + 12_KB, 0, 4_KB);
            TestAllocate(12_KB, 8_KB, 16_KB, flags, expected);
        }

        Tablet->WriteData(Handle, 0, 16_KB, 'a');
        memset(expected.begin(), 'a', 16_KB);

        {
            // Unaligned from start
            memset(expected.begin(), 0, 5_KB);
            TestAllocate(0, 5_KB, 16_KB, flags, expected);
        }

        {
            // Unaligned from aligned offset
            memset(expected.begin() + 4_KB, 0, 5_KB);
            TestAllocate(4_KB, 5_KB, 16_KB, flags, expected);
        }

        {
            // Unaligned from unaligned offset
            memset(expected.begin() + 8_KB, 0, 2_KB);
            TestAllocate(8_KB, 2_KB, 16_KB, flags, expected);
        }

        {
            // Unaligned from unaligned offset till the end
            memset(expected.begin() + 10_KB, 0, 6_KB);
            TestAllocate(10_KB, 10_KB, 16_KB, flags, expected);
        }

        Tablet->WriteData(Handle, 0, 16_KB, 'a');
        memset(expected.begin(), 'a', 16_KB);

        {
            // Unaligned from aligned offset till the end
            memset(expected.begin() + 12_KB, 0, 4_KB);
            TestAllocate(12_KB, 9_KB, 16_KB, flags, expected);
        }

        {
            // Unaligned from aligned offset over the end
            memset(expected.begin() + 12_KB, 0, 4_KB);
            TestAllocate(12_KB, 10_KB, 16_KB, flags, expected);
        }

        Tablet->WriteData(Handle, 0, 16_KB, 'a');
        memset(expected.begin(), 'a', 16_KB);

        {
            // Unligned from unaligned offset over the end
            memset(expected.begin() + 13_KB, 0, 3_KB);
            TestAllocate(13_KB, 13_KB, 16_KB, flags, expected);
        }
    }

    Y_UNIT_TEST_F(ShouldAllocateDataWithPunchAndKeepSize, TEnvironment)
    {
        const ui32 flags =
            ProtoFlag(NProto::TAllocateDataRequest::F_PUNCH_HOLE) |
            ProtoFlag(NProto::TAllocateDataRequest::F_KEEP_SIZE);

        // Nothing should be allocated
        TestAllocate(0, 4_KB, 0, flags, TString{});

        Tablet->AllocateData(Handle, 0, 16_KB, 0);

        auto stat = Tablet->GetNodeAttr(Id)->Record.GetNode();
        UNIT_ASSERT_VALUES_EQUAL(stat.GetId(), Id);
        UNIT_ASSERT_VALUES_EQUAL(stat.GetSize(), 16_KB);

        Tablet->WriteData(Handle, 0, 16_KB, 'a');

        TString expected(16_KB, 'a');
        {
            const auto response = Tablet->ReadData(Handle, 0, 16_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        {
            // Aligned from start
            memset(expected.begin(), 0, 4_KB);
            TestAllocate(0, 4_KB, 16_KB, flags, expected);
        }

        {
            // Aligned from aligned offset
            memset(expected.begin() + 4_KB, 0, 4_KB);
            TestAllocate(4_KB, 4_KB, 16_KB, flags, expected);
        }

        {
            // Aligned from aligned offset till the end
            memset(expected.begin() + 8_KB, 0, 8_KB);
            TestAllocate(8_KB, 8_KB, 16_KB, flags, expected);
        }

        {
            // Aligned from aligned offset over the end (size must not change)
            memset(expected.begin() + 12_KB, 0, 4_KB);
            TestAllocate(12_KB, 8_KB, 16_KB, flags, expected);
        }

        Tablet->WriteData(Handle, 0, 16_KB, 'a');
        memset(expected.begin(), 'a', 16_KB);

        {
            // Unaligned from start
            memset(expected.begin(), 0, 5_KB);
            TestAllocate(0, 5_KB, 16_KB, flags, expected);
        }

        {
            // Unaligned from aligned offset
            memset(expected.begin() + 4_KB, 0, 5_KB);
            TestAllocate(4_KB, 5_KB, 16_KB, flags, expected);
        }

        {
            // Unaligned from unaligned offset
            memset(expected.begin() + 8_KB, 0, 2_KB);
            TestAllocate(8_KB, 2_KB, 16_KB, flags, expected);
        }

        {
            // Unaligned from unaligned offset till the end
            memset(expected.begin() + 10_KB, 0, 6_KB);
            TestAllocate(10_KB, 10_KB, 16_KB, flags, expected);
        }

        Tablet->WriteData(Handle, 0, 16_KB, 'a');
        memset(expected.begin(), 'a', 16_KB);

        {
            // Unaligned from aligned offset till the end
            memset(expected.begin() + 12_KB, 0, 4_KB);
            TestAllocate(12_KB, 9_KB, 16_KB, flags, expected);
        }

        {
            // Unaligned from aligned offset over the end
            memset(expected.begin() + 12_KB, 0, 4_KB);
            TestAllocate(12_KB, 10_KB, 16_KB, flags, expected);
        }

        Tablet->WriteData(Handle, 0, 16_KB, 'a');
        memset(expected.begin(), 'a', 16_KB);

        {
            // Unligned from unaligned offset over the end
            memset(expected.begin() + 13_KB, 0, 3_KB);
            TestAllocate(13_KB, 13_KB, 16_KB, flags, expected);
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
