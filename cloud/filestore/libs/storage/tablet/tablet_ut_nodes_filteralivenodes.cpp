#include "tablet.h"

#include "tablet_schema.h"

#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <library/cpp/testing/unittest/registar.h>

#include <memory>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TEnvironment: public NUnitTest::TBaseFixture
{
    TTestEnv Env;
    std::unique_ptr<TIndexTabletClient> Tablet;

    ui64 CurrentId = 2;
    TMap<ui64, TString> Ids;

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        Env.CreateSubDomain("nfs");

        const ui32 nodeIdx = Env.CreateNode("nfs");
        const ui64 tabletId = Env.BootIndexTablet(nodeIdx);

        Tablet = std::make_unique<TIndexTabletClient>(
            Env.GetRuntime(),
            nodeIdx,
            tabletId);
        Tablet->InitSession("client", "session");
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {}

    TSet<ui64> FilterAliveNodes(TStackVec<ui64, 16> nodes)
    {
        return Tablet->FilterAliveNodes(std::move(nodes))->Nodes;
    }

    void CreateNode()
    {
        const ui64 current = CurrentId++;
        Ids[current] = "test_" + std::to_string(current);
        ::NCloud::NFileStore::NStorage::CreateNode(
            *Tablet,
            TCreateNodeArgs::File(RootNodeId, Ids[current]));
    }

    void UnlinkNode(ui64 id, bool isDirectory)
    {
        Tablet->UnlinkNode(RootNodeId, Ids[id], isDirectory);
        Ids.erase(id);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_Nodes_FilterAliveNodes)
{
    Y_UNIT_TEST_F(Smoke, TEnvironment)
    {
        const auto res = FilterAliveNodes({1, 2, 3});
        UNIT_ASSERT_VALUES_EQUAL(1, res.size());
        UNIT_ASSERT_VALUES_EQUAL(1, *res.begin());
    }

    Y_UNIT_TEST_F(ShouldWork, TEnvironment)
    {
        CreateNode();

        {
            const auto res = FilterAliveNodes({1, 2, 3});
            UNIT_ASSERT_VALUES_EQUAL(2, res.size());
            UNIT_ASSERT_VALUES_EQUAL(1, *res.begin());
            UNIT_ASSERT_VALUES_EQUAL(2, *std::next(res.begin(), 1));
        }

        {
            const auto res = FilterAliveNodes({1});
            UNIT_ASSERT_VALUES_EQUAL(1, res.size());
            UNIT_ASSERT_VALUES_EQUAL(1, *res.begin());
        }

        {
            const auto res = FilterAliveNodes({2, 3});
            UNIT_ASSERT_VALUES_EQUAL(1, res.size());
            UNIT_ASSERT_VALUES_EQUAL(2, *res.begin());
        }

        CreateNode();

        {
            const auto res = FilterAliveNodes({1, 3, 5});
            UNIT_ASSERT_VALUES_EQUAL(2, res.size());
            UNIT_ASSERT_VALUES_EQUAL(1, *res.begin());
            UNIT_ASSERT_VALUES_EQUAL(3, *std::next(res.begin(), 1));
        }

        {
            const auto res = FilterAliveNodes({2, 4, 6});
            UNIT_ASSERT_VALUES_EQUAL(1, res.size());
            UNIT_ASSERT_VALUES_EQUAL(2, *res.begin());
        }

        {
            const auto res = FilterAliveNodes({1, 2, 3, 4, 5});
            UNIT_ASSERT_VALUES_EQUAL(3, res.size());
            UNIT_ASSERT_VALUES_EQUAL(1, *res.begin());
            UNIT_ASSERT_VALUES_EQUAL(2, *std::next(res.begin(), 1));
            UNIT_ASSERT_VALUES_EQUAL(3, *std::next(res.begin(), 2));
        }

        UnlinkNode(2, false);

        {
            const auto res = FilterAliveNodes({1, 2, 3, 4, 5});
            UNIT_ASSERT_VALUES_EQUAL(2, res.size());
            UNIT_ASSERT_VALUES_EQUAL(1, *res.begin());
            UNIT_ASSERT_VALUES_EQUAL(3, *std::next(res.begin(), 1));
        }

        CreateNode();

        {
            const auto res = FilterAliveNodes({1, 2, 3, 4, 5});
            UNIT_ASSERT_VALUES_EQUAL(3, res.size());
            UNIT_ASSERT_VALUES_EQUAL(1, *res.begin());
            UNIT_ASSERT_VALUES_EQUAL(3, *std::next(res.begin(), 1));
            UNIT_ASSERT_VALUES_EQUAL(4, *std::next(res.begin(), 2));
        }

        UnlinkNode(4, false);

        {
            const auto res = FilterAliveNodes({1, 2, 3, 4, 5});
            UNIT_ASSERT_VALUES_EQUAL(2, res.size());
            UNIT_ASSERT_VALUES_EQUAL(1, *res.begin());
            UNIT_ASSERT_VALUES_EQUAL(3, *std::next(res.begin(), 1));
        }

        UnlinkNode(3, false);

        {
            const auto res = FilterAliveNodes({1, 2, 3, 4, 5});
            UNIT_ASSERT_VALUES_EQUAL(1, res.size());
            UNIT_ASSERT_VALUES_EQUAL(1, *res.begin());
        }

        {
            const auto res = FilterAliveNodes({});
            UNIT_ASSERT_VALUES_EQUAL(0, res.size());
        }

        {
            const auto res = FilterAliveNodes({2, 4});
            UNIT_ASSERT_VALUES_EQUAL(0, res.size());
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
