#include "tablet.h"
#include "tablet_schema.h"

#include <cloud/filestore/libs/storage/tablet/model/block.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

#include <memory>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TEnvironment
    : public NUnitTest::TBaseFixture
{
    TTestEnv Env;
    std::unique_ptr<TIndexTabletClient> Tablet;

    ui64 Id = 0;
    ui64 Handle = 0;

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        const ui32 nodeIdx = Env.AddDynamicNode();
        const ui64 tabletId = Env.BootIndexTablet(nodeIdx);

        Tablet = std::make_unique<TIndexTabletClient>(
            Env.GetRuntime(),
            nodeIdx,
            tabletId
        );
        Tablet->InitSession("client", "session");

        Id = CreateNode(*Tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        Handle = CreateHandle(*Tablet, Id);
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {}

    void WriteData(ui64 offset, ui64 length, char fill)
    {
        Tablet->WriteData(Handle, offset, length, fill);
    }

    TString ReadData(ui64 size, ui64 offset = 0)
    {
        return Tablet->ReadData(Handle, offset, size)->Record.GetBuffer();
    }

    void ZeroRange(ui64 offset, ui64 length)
    {
        Tablet->ZeroRange(Id, offset, length);
    }

    NProto::TNodeAttr GetNodeAttrs()
    {
        return Tablet->GetNodeAttr(Id)->Record.GetNode();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_Data_ZeroRange)
{
    Y_UNIT_TEST_F(ShouldZeroRange, TEnvironment)
    {
        // aligned
        WriteData(0, 8_KB, '1');
        // non aligned for each block
        WriteData(0, 1_KB, '2');
        WriteData(4_KB, 1_KB, '2');

        TString expected = TString(1_KB, '2') + TString(3_KB, '1');
        expected += expected;
        UNIT_ASSERT_EQUAL(ReadData(8_KB), expected);

        ZeroRange(3_KB, 4_KB);

        memset(&expected[3_KB], 0, 4_KB);
        UNIT_ASSERT_EQUAL(ReadData(4_KB), expected.substr(0, 4_KB));
        UNIT_ASSERT_VALUES_EQUAL(ReadData(4_KB, 4_KB), expected.substr(4_KB, 4_KB));
    }

    Y_UNIT_TEST_F(ShouldSimultaneouslyZeroRange, TEnvironment)
    {
        WriteData(0, 4_KB + 5, '1');
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs().GetSize(), 4_KB + 5);
        UNIT_ASSERT_VALUES_EQUAL(ReadData(8_KB), TString(4_KB + 5, '1'));

        ZeroRange(4_KB, 5);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs().GetSize(), 4_KB + 5);
        {
            const TString expected = TString(4_KB, '1') + TString(5, 0);
            UNIT_ASSERT_VALUES_EQUAL(ReadData(8_KB), expected);
        }

        WriteData(4_KB, 5, '1');

        ZeroRange(2_KB, 2_KB + 5);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs().GetSize(), 4_KB + 5);
        {
            const TString expected = TString(2_KB, '1') + TString(2_KB + 5, 0);
            UNIT_ASSERT_VALUES_EQUAL(ReadData(8_KB), expected);
        }

        ZeroRange(0, 4_KB + 5);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs().GetSize(), 4_KB + 5);
        UNIT_ASSERT_VALUES_EQUAL(ReadData(8_KB), TString(4_KB + 5, 0));

        WriteData(0, 18_KB, '1');
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs().GetSize(), 18_KB);
        UNIT_ASSERT_VALUES_EQUAL(ReadData(18_KB), TString(18_KB, '1'));

        ZeroRange(6_KB, 2_KB);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs().GetSize(), 18_KB);
        {
            const TString expected =
                TString(6_KB, '1') + TString(2_KB, 0) + TString(10_KB, '1');
            UNIT_ASSERT_VALUES_EQUAL(ReadData(18_KB), expected);
        }

        ZeroRange(14_KB, 4_KB);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs().GetSize(), 18_KB);
        {
            const TString expected =
                TString(6_KB, '1') + TString(2_KB, 0) +
                TString(6_KB, '1') + TString(4_KB, 0);
            UNIT_ASSERT_VALUES_EQUAL(ReadData(18_KB), expected);
        }

        ZeroRange(0_KB, 9_KB);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs().GetSize(), 18_KB);
        {
            const TString expected =
                TString(9_KB, 0) + TString(5_KB, '1') + TString(4_KB, 0);
            UNIT_ASSERT_VALUES_EQUAL(ReadData(18_KB), expected);
        }

        ZeroRange(4_KB, 12_KB);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs().GetSize(), 18_KB);
        UNIT_ASSERT_VALUES_EQUAL(ReadData(18_KB), TString(18_KB, 0));

        WriteData(4_KB, 12_KB, '1');

        ZeroRange(4_KB, 4_KB);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs().GetSize(), 18_KB);
        {
            const TString expected =
                TString(8_KB, 0) + TString(8_KB, '1') + TString(2_KB, 0);
            UNIT_ASSERT_VALUES_EQUAL(ReadData(18_KB), expected);
        }

        ZeroRange(7_KB, 9_KB);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs().GetSize(), 18_KB);
        UNIT_ASSERT_VALUES_EQUAL(ReadData(18_KB), TString(18_KB, 0));
    }
}

}   // namespace NCloud::NFileStore::NStorage
