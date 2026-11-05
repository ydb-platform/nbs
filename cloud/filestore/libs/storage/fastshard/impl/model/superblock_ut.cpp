#include <cloud/filestore/libs/storage/fastshard/impl/model/superblock.h>

#include <cloud/storage/core/libs/common/error.h>

#include <gtest/gtest.h>

using namespace NCloud;
using namespace NFileStore;
using namespace NFileStore::NStorage::NFastShard;
using namespace NCloud::NFastShard;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t PageSize = 4_KB;

////////////////////////////////////////////////////////////////////////////////

TVector<ui64> CollectPages(const TVector<TPageGroup>& groups)
{
    TVector<ui64> res;
    for (const auto& pg: groups) {
        for (ui64 i = 0; i < pg.Content.size(); ++i) {
            res.push_back(pg.FirstPageNo + i);
        }
    }

    return res;
}

void Flush(TVector<TPageGroup>& groups, IPageStore& pageStore)
{
    pageStore.CommitPages(CollectPages(groups));
    groups.clear();
}

////////////////////////////////////////////////////////////////////////////////

struct TFixture
{
    const ui64 FirstPageNo = 10;

    IPageStorePtr PageStore = CreateMemPageStore(PageSize);
    TSuperBlock SB;

    TFixture()
    {
        SB.Init(FirstPageNo, PageStore);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(SuperBlockTest, AllocatesNodeIds)
{
    TFixture fx;

    ui64 nodeId = 0;

    {
        TWriteContext writeContext;
        writeContext.Lsn = fx.PageStore->AllocateLsn();
        auto error = fx.SB.AllocateNodeId(writeContext, &nodeId);
        ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
        ASSERT_EQ(2U, nodeId);
        Flush(writeContext.PageGroups, *fx.PageStore);
    }

    {
        TWriteContext writeContext;
        writeContext.Lsn = fx.PageStore->AllocateLsn();
        auto error = fx.SB.AllocateNodeId(writeContext, &nodeId);
        ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
        ASSERT_EQ(3U, nodeId);
        Flush(writeContext.PageGroups, *fx.PageStore);
    }

    {
        TWriteContext writeContext;
        writeContext.Lsn = fx.PageStore->AllocateLsn();
        auto error = fx.SB.AllocateNodeId(writeContext, &nodeId);
        ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
        ASSERT_EQ(4U, nodeId);
        Flush(writeContext.PageGroups, *fx.PageStore);
    }

    TSuperBlock sb2;
    sb2.Init(fx.FirstPageNo, fx.PageStore);

    {
        TWriteContext writeContext;
        writeContext.Lsn = fx.PageStore->AllocateLsn();
        auto error = fx.SB.AllocateNodeId(writeContext, &nodeId);
        ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
        ASSERT_EQ(5U, nodeId);
        Flush(writeContext.PageGroups, *fx.PageStore);
    }
}

TEST(SuperBlockTest, AllocatesHandleIds)
{
    TFixture fx;

    ui64 handleId = 0;

    {
        TWriteContext writeContext;
        writeContext.Lsn = fx.PageStore->AllocateLsn();
        auto error = fx.SB.AllocateHandleId(writeContext, &handleId);
        ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
        ASSERT_EQ(2U, handleId);
        Flush(writeContext.PageGroups, *fx.PageStore);
    }

    {
        TWriteContext writeContext;
        writeContext.Lsn = fx.PageStore->AllocateLsn();
        auto error = fx.SB.AllocateHandleId(writeContext, &handleId);
        ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
        ASSERT_EQ(3U, handleId);
        Flush(writeContext.PageGroups, *fx.PageStore);
    }

    {
        TWriteContext writeContext;
        writeContext.Lsn = fx.PageStore->AllocateLsn();
        auto error = fx.SB.AllocateHandleId(writeContext, &handleId);
        ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
        ASSERT_EQ(4U, handleId);
        Flush(writeContext.PageGroups, *fx.PageStore);
    }

    TSuperBlock sb2;
    sb2.Init(fx.FirstPageNo, fx.PageStore);

    {
        TWriteContext writeContext;
        writeContext.Lsn = fx.PageStore->AllocateLsn();
        auto error = fx.SB.AllocateHandleId(writeContext, &handleId);
        ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
        ASSERT_EQ(5U, handleId);
        Flush(writeContext.PageGroups, *fx.PageStore);
    }
}

TEST(SuperBlockTest, CollectsStats)
{
    TFixture fx;

    const ui64 nodeCount = 20;
    const ui64 handleCount = 100;

    for (ui64 i = 0; i < nodeCount; ++i) {
        ui64 nodeId = 0;
        TWriteContext writeContext;
        writeContext.Lsn = fx.PageStore->AllocateLsn();
        auto error = fx.SB.AllocateNodeId(writeContext, &nodeId);
        ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
        ASSERT_EQ(i + 2, nodeId);
        Flush(writeContext.PageGroups, *fx.PageStore);
    }

    for (ui64 i = 0; i < handleCount; ++i) {
        ui64 handleId = 0;
        TWriteContext writeContext;
        writeContext.Lsn = fx.PageStore->AllocateLsn();
        auto error = fx.SB.AllocateHandleId(writeContext, &handleId);
        ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
        ASSERT_EQ(i + 2, handleId);
        Flush(writeContext.PageGroups, *fx.PageStore);
    }

    TFileSystemShardStats stats;
    auto error = fx.SB.CollectStats(&stats);
    ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
    ASSERT_EQ(nodeCount + 1, stats.LastNodeId);
    ASSERT_EQ(handleCount + 1, stats.LastHandleId);
}
