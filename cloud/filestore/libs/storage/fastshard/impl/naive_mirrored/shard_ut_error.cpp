#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>
#include <cloud/filestore/libs/storage/fastshard/impl/naive_mirrored/shard.h>
#include <cloud/filestore/libs/storage/fastshard/testlib/silk_env.h>
#include <cloud/filestore/private/api/unsafe_protos/unsafe.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <silk/util/logger.h>

#include <gtest/gtest.h>

using namespace NCloud;
using namespace NFileStore;
using namespace NFileStore::NProto;
using namespace NStorage::NFastShard;

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
    TVector<TString> Pages{PageCount};
    TTempError ReadError;
    TTempError WriteError;

    NCloud::NProto::TError AcquireDevices() override
    {
        return {};
    }

    NCloud::NProto::TError ReleaseDevices() override
    {
        return {};
    }

    NCloud::NProto::TError WriteLogRecord(
        NCloud::NProto::TDeviceRequestHeaders headers,
        TVector<TPageGroup> pageGroups) override
    {
        Y_UNUSED(headers);

        auto e = WriteError.Get();
        if (HasError(e)) {
            return e;
        }

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
                if (pg.Content.back().empty()) {
                    pg.Content.back().resize(PageSize, 0);
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
        const NProtoPrivate::TPersistentFastShardConfig& config) override
    {
        Y_UNUSED(config);

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
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(NaiveMirroredShardErrorTest, CreatesHandles)
{
    silk::Logger::setLevel(silk::LogLevel::DEBUG);

    TStorageFixture fx;

    auto shard =
        CreateNaiveMirroredFileSystemShard(ShardNo, fx.Factory, fx.Config);

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
