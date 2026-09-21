#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>
#include <cloud/filestore/libs/storage/fastshard/impl/hash_table_index/shard.h>
#include <cloud/filestore/libs/storage/fastshard/sn/factory/group_factory.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <silk/util/logger.h>

#include <gtest/gtest.h>

using namespace NCloud;
using namespace NFileStore;
using namespace NFileStore::NProto;
using namespace NFileStore::NStorage::NFastShard;
using namespace NCloud::NFastShard;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 ShardNo = 1;
constexpr size_t PageSize = 4_KB;
constexpr size_t PageCount = 128_MB / PageSize;
constexpr size_t NodesPerGroup = 64;

////////////////////////////////////////////////////////////////////////////////

struct TTempError
{
    NCloud::NProto::TError E;
    ui64 Ttl = 0;

    void Set(NCloud::NProto::TError e, ui64 ttl)
    {
        E = std::move(e);
        Ttl = ttl;
    }

    auto Get()
    {
        if (!Ttl) {
            return MakeError(S_OK);
        }

        --Ttl;
        return E;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestStorageGroup: IStorageGroup
{
    //
    // Deliberately not brace-initialized: TBuffer(size_t) is implicit, so
    // {PageCount} would create a single buffer instead of PageCount empty
    // pages.
    //

    TVector<TBuffer> Pages = TVector<TBuffer>(PageCount);
    TTempError ReadError;
    TTempError WriteError;
    ui64 LastLsn = 0;
    TVector<TLsnLink> WriteLinks;

    TResultOrError<ui64> Init() override
    {
        return LastLsn;
    }

    void TearDown() override
    {}

    NCloud::NProto::TError WriteLogRecord(
        NCloud::NProto::TDeviceRequestHeaders headers,
        TVector<TPageGroup> pageGroups,
        TLsnLink link) override
    {
        Y_UNUSED(headers);

        auto e = WriteError.Get();
        if (HasError(e)) {
            return e;
        }

        WriteLinks.push_back(link);
        for (auto& pg: pageGroups) {
            for (ui64 i = 0; i < pg.Content.size(); ++i) {
                Pages[pg.FirstPageNo + i] = std::move(pg.Content[i]);
            }
        }

        return {};
    }

    NCloud::NProto::TError ReadPages(
        NCloud::NProto::TDeviceRequestHeaders headers,
        const TVector<TPageGroupRef>& pageGroupRefs,
        TVector<TPageGroup>* pageGroups) override
    {
        Y_UNUSED(headers);

        auto e = ReadError.Get();
        if (HasError(e)) {
            return e;
        }

        for (const auto& pgr: pageGroupRefs) {
            auto& pg = pageGroups->emplace_back();
            pg.FirstPageNo = pgr.FirstPageNo;
            for (ui64 i = 0; i < pgr.PageCount; ++i) {
                pg.Content.push_back(Pages[pgr.FirstPageNo + i]);
                if (pg.Content.back().Empty()) {
                    pg.Content.back().Fill(0, PageSize);
                }
            }
        }

        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestStorageGroupFactory: IStorageGroupFactory
{
    std::shared_ptr<TTestStorageGroup> Group =
        std::make_shared<TTestStorageGroup>();

    IStorageGroupPtr MakeStorageGroup(
        const NProtoPrivate::TPersistentFastShardConfig& config,
        ui64 generation) override
    {
        Y_UNUSED(config, generation);

        return Group;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TStorageFixture
{
    NProtoPrivate::TPersistentFastShardConfig Config;
    std::shared_ptr<TTestStorageGroupFactory> Factory =
        std::make_shared<TTestStorageGroupFactory>();

    TStorageFixture()
    {
        Config.SetNodesPerGroup(NodesPerGroup);
        Config.SetExpectedGroupCapacity(64_MB);
        Config.SetPageSize(PageSize);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(HashTableIndexShardErrorTest, CreatesHandles)
{
    silk::Logger::setLevel(silk::LogLevel::DEBUG);

    TStorageFixture fx;

    auto shard = CreateHashTableIndexFileSystemShard(
        "fs0",
        ShardNo,
        1 /* generation */,
        fx.Factory,
        fx.Config);
    {
        auto e = shard->Init().GetValueSync();
        ASSERT_EQ(S_OK, e.GetCode()) << e.GetMessage();
    }

    const TString file1 = "file1";
    const ui32 mode = 0644;
    const ui32 expectedMode = S_IFREG | 0644;
    const ui64 uid = 111;
    const ui64 gid = 222;

    const ui32 create = ProtoFlag(TCreateHandleRequest::E_CREATE);
    const ui32 createExcl =
        create | ProtoFlag(TCreateHandleRequest::E_EXCLUSIVE);

    fx.Factory->Group->ReadError.Set(MakeError(E_REJECTED), 1 /* ttl */);

    {
        TCreateHandleRequest request;
        request.SetNodeId(RootNodeId);
        request.SetName(file1);
        request.SetMode(mode);
        request.SetUid(uid);
        request.SetGid(gid);
        request.SetFlags(createExcl);
        auto f = shard->CreateHandle(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(E_REJECTED, response.GetError().GetCode())
            << FormatError(response.GetError());
    }

    ui64 nodeId = 0;
    ui64 handle1 = 0;
    {
        TCreateHandleRequest request;
        request.SetNodeId(RootNodeId);
        request.SetName(file1);
        request.SetMode(mode);
        request.SetUid(uid);
        request.SetGid(gid);
        request.SetFlags(createExcl);
        auto f = shard->CreateHandle(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(S_OK, response.GetError().GetCode())
            << FormatError(response.GetError());
        EXPECT_EQ(uid, response.GetNodeAttr().GetUid());
        EXPECT_EQ(gid, response.GetNodeAttr().GetGid());
        EXPECT_EQ(
            static_cast<ui32>(E_REGULAR_NODE),
            response.GetNodeAttr().GetType());
        EXPECT_EQ(expectedMode, response.GetNodeAttr().GetMode());
        nodeId = response.GetNodeAttr().GetId();
        handle1 = response.GetHandle();
    }

    {
        TGetNodeAttrRequest request;
        request.SetNodeId(nodeId);
        auto f = shard->GetNodeAttr(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(S_OK, response.GetError().GetCode())
            << FormatError(response.GetError());
        EXPECT_EQ(nodeId, response.GetNode().GetId());
        EXPECT_EQ(uid, response.GetNode().GetUid());
        EXPECT_EQ(gid, response.GetNode().GetGid());
        EXPECT_EQ(
            static_cast<ui32>(E_REGULAR_NODE),
            response.GetNode().GetType());
        EXPECT_EQ(expectedMode, response.GetNode().GetMode());
    }

    ui64 handle2 = 0;
    {
        TCreateHandleRequest request;
        request.SetNodeId(RootNodeId);
        request.SetName(file1);
        request.SetMode(mode);
        request.SetUid(uid);
        request.SetGid(gid);
        request.SetFlags(create);
        auto f = shard->CreateHandle(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(S_OK, response.GetError().GetCode())
            << FormatError(response.GetError());
        EXPECT_EQ(nodeId, response.GetNodeAttr().GetId());
        EXPECT_EQ(uid, response.GetNodeAttr().GetUid());
        EXPECT_EQ(gid, response.GetNodeAttr().GetGid());
        EXPECT_EQ(
            static_cast<ui32>(E_REGULAR_NODE),
            response.GetNodeAttr().GetType());
        EXPECT_EQ(expectedMode, response.GetNodeAttr().GetMode());
        handle2 = response.GetHandle();
        EXPECT_NE(handle2, handle1);
    }

    {
        TDestroyHandleRequest request;
        request.SetHandle(handle1);
        auto f = shard->DestroyHandle(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(S_OK, response.GetError().GetCode())
            << FormatError(response.GetError());
    }

    {
        TDestroyHandleRequest request;
        request.SetHandle(handle1);
        auto f = shard->DestroyHandle(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(NCloud::E_FS_BADHANDLE, response.GetError().GetCode())
            << FormatError(response.GetError());
    }

    {
        TDestroyHandleRequest request;
        request.SetHandle(handle2);
        auto f = shard->DestroyHandle(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(S_OK, response.GetError().GetCode())
            << FormatError(response.GetError());
    }
}

TEST(HashTableIndexShardErrorTest, NumbersRecordsAboveTheStorageGroupLsn)
{
    TStorageFixture fx;
    fx.Factory->Group->LastLsn = 41;

    auto shard = CreateHashTableIndexFileSystemShard(
        "fs0",
        ShardNo,
        1 /* generation */,
        fx.Factory,
        fx.Config);
    {
        auto e = shard->Init().GetValueSync();
        ASSERT_EQ(S_OK, e.GetCode()) << e.GetMessage();
    }

    TCreateHandleRequest request;
    request.SetNodeId(RootNodeId);
    request.SetName("file1");
    request.SetMode(0644);
    request.SetFlags(ProtoFlag(TCreateHandleRequest::E_CREATE));
    auto response = shard->CreateHandle(request).GetValueSync();
    ASSERT_EQ(S_OK, response.GetError().GetCode())
        << FormatError(response.GetError());

    const auto& links = fx.Factory->Group->WriteLinks;
    ASSERT_FALSE(links.empty());
    EXPECT_EQ(42U, links.front().Lsn);
    EXPECT_EQ(41U, links.front().PrevLsn);
}

TEST(HashTableIndexShardErrorTest, LinksPastTheLsnOfAnOperationThatWroteNothing)
{
    TStorageFixture fx;

    auto shard = CreateHashTableIndexFileSystemShard(
        "fs0",
        ShardNo,
        1 /* generation */,
        fx.Factory,
        fx.Config);
    {
        auto e = shard->Init().GetValueSync();
        ASSERT_EQ(S_OK, e.GetCode()) << e.GetMessage();
    }

    auto create = [&](const TString& name)
    {
        TCreateHandleRequest request;
        request.SetNodeId(RootNodeId);
        request.SetName(name);
        request.SetMode(0644);
        request.SetFlags(
            ProtoFlag(TCreateHandleRequest::E_CREATE)
            | ProtoFlag(TCreateHandleRequest::E_EXCLUSIVE));
        return shard->CreateHandle(request).GetValueSync().GetError();
    };

    auto error = create("file1");
    ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);

    // The same name again: the op takes an lsn and writes nothing with it.
    error = create("file1");
    ASSERT_TRUE(HasError(error));

    error = create("file2");
    ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);

    const auto& links = fx.Factory->Group->WriteLinks;
    ASSERT_EQ(2U, links.size());
    // The chain is unbroken, and the lsn nobody wrote is not in it.
    EXPECT_EQ(links[0].Lsn, links[1].PrevLsn);
    EXPECT_GT(links[1].Lsn, links[0].Lsn + 1);
}
