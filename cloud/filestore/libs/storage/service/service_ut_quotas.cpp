#include "service.h"
#include "service_private.h"
#include "service_ut_helpers.h"
#include "service_ut_sharding.h"

#include <cloud/filestore/libs/storage/model/utils.h>
#include <cloud/filestore/libs/storage/testlib/service_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TStorageConfig MakeStorageConfigWithDirectoryCreationInShards()
{
    NProto::TStorageConfig config;
    config.SetDirectoryCreationInShardsEnabled(true);
    return config;
}

void SetQuota(
    TServiceClient& service,
    const TString& fsId,
    ui32 quotaId,
    ui64 maxBytes,
    ui64 maxNodes)
{
    NProtoPrivate::TSetQuotaRequest request;
    request.SetFileSystemId(fsId);
    request.SetQuotaId(quotaId);
    request.SetMaxBytes(maxBytes);
    request.SetMaxNodes(maxNodes);

    TString buf;
    google::protobuf::util::MessageToJsonString(request, &buf);
    service.ExecuteAction("setquota", buf);
}

void ForceRefreshAggregateQuotaUsage(TServiceClient& service, const TString& fsId)
{
    NProtoPrivate::TGetStorageStatsRequest request;
    request.SetFileSystemId(fsId);
    request.SetMode(NProtoPrivate::STATS_REQUEST_MODE_FORCE_FETCH_SHARDS);

    TString buf;
    google::protobuf::util::MessageToJsonString(request, &buf);
    service.ExecuteAction("GetStorageStats", buf);
}

////////////////////////////////////////////////////////////////////////////////

class TShardingModeFixtureBase: public NUnitTest::TBaseTestCase
{
public:
    THolder<TTestEnv> Env;
    THolder<TServiceClient> Service;
    TString FsId;

protected:
    void Init(NProto::TStorageConfig config, bool sharded)
    {
        TShardedFileSystemConfig fsConfig;
        if (sharded) {
            config.SetAutomaticShardCreationEnabled(true);
            config.SetAutomaticallyCreatedShardSize(
                fsConfig.ShardBlockCount * 4_KB);
            config.SetShardAllocationUnit(fsConfig.ShardBlockCount * 4_KB);
        }

        Env = MakeHolder<TTestEnv>(TTestEnvConfig{}, config);
        ui32 nodeIdx = Env->AddDynamicNode();
        Service = MakeHolder<TServiceClient>(Env->GetRuntime(), nodeIdx);

        if (sharded) {
            CreateFileSystem(*Service, fsConfig);
            FsId = fsConfig.FsId;
        } else {
            FsId = "test";
            Service->CreateFileStore(FsId, 1'000);
        }
    }
};

class TUnshardedFixture: public TShardingModeFixtureBase
{
public:
    void SetUp(NUnitTest::TTestContext&) override
    {
        Init(NProto::TStorageConfig{}, false);
    }
};

class TShardedFixture: public TShardingModeFixtureBase
{
public:
    void SetUp(NUnitTest::TTestContext&) override
    {
        Init(NProto::TStorageConfig{}, true);
    }
};

class TShardedWithDirCreationFixture: public TShardingModeFixtureBase
{
public:
    void SetUp(NUnitTest::TTestContext&) override
    {
        Init(MakeStorageConfigWithDirectoryCreationInShards(), true);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

#define SERVICE_TEST_DECL(name)                                                \
    void RunTest##name(TShardingModeFixtureBase& fixture)                      \
// SERVICE_TEST_DECL

#define SERVICE_TEST(name)                                                     \
    SERVICE_TEST_DECL(name);                                                   \
    Y_UNIT_TEST_F(name, TUnshardedFixture)                                     \
    {                                                                          \
        RunTest##name(*this);                                                  \
    }                                                                          \
    Y_UNIT_TEST_F(name##WithShards, TShardedFixture)                           \
    {                                                                          \
        RunTest##name(*this);                                                  \
    }                                                                          \
    Y_UNIT_TEST_F(                                                             \
        name##WithDirectoryCreationInShards,                                   \
        TShardedWithDirCreationFixture)                                        \
    {                                                                          \
        RunTest##name(*this);                                                  \
    }                                                                          \
    SERVICE_TEST_DECL(name)                                                    \
// SERVICE_TEST

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStorageServiceQuotasTest)
{
    SERVICE_TEST(ShouldInheritQuotaIdOnCreateNode)
    {
        auto& service = *fixture.Service;
        const auto& fsId = fixture.FsId;

        SetQuota(service, fsId, 42, 1_GB, 100);

        auto headers = service.InitSession(fsId, "client");

        const auto dirId =
            service
                .CreateNode(headers, TCreateNodeArgs::Directory(RootNodeId, "dir"))
                ->Record.GetNode()
                .GetId();
        service.SetNodeAttr(
            headers,
            fsId,
            TSetNodeAttrArgs(dirId).SetQuotaId(42));

        const auto fileId =
            service.CreateNode(headers, TCreateNodeArgs::File(dirId, "file"))
                ->Record.GetNode()
                .GetId();
        UNIT_ASSERT_VALUES_EQUAL(
            42u,
            service.GetNodeAttr(headers, fsId, fileId, "")
                ->Record.GetNode()
                .GetQuotaId());

        const auto subdirId =
            service
                .CreateNode(headers, TCreateNodeArgs::Directory(dirId, "subdir"))
                ->Record.GetNode()
                .GetId();
        UNIT_ASSERT_VALUES_EQUAL(
            42u,
            service.GetNodeAttr(headers, fsId, subdirId, "")
                ->Record.GetNode()
                .GetQuotaId());

        // grandchildren keep inheriting through the subdirectory, even
        // though it wasn't itself explicitly marked
        const auto grandchildId =
            service
                .CreateNode(
                    headers,
                    TCreateNodeArgs::File(subdirId, "grandchild"))
                ->Record.GetNode()
                .GetId();
        UNIT_ASSERT_VALUES_EQUAL(
            42u,
            service.GetNodeAttr(headers, fsId, grandchildId, "")
                ->Record.GetNode()
                .GetQuotaId());

        // unrelated siblings outside the quota'd subtree stay unquota'd
        const auto otherId =
            service.CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "other"))
                ->Record.GetNode()
                .GetId();
        UNIT_ASSERT_VALUES_EQUAL(
            0u,
            service.GetNodeAttr(headers, fsId, otherId, "")
                ->Record.GetNode()
                .GetQuotaId());
    }

    SERVICE_TEST(ShouldInheritQuotaIdOnCreateHandle)
    {
        auto& service = *fixture.Service;
        const auto& fsId = fixture.FsId;

        SetQuota(service, fsId, 42, 1_GB, 100);

        auto headers = service.InitSession(fsId, "client");

        const auto dirId =
            service
                .CreateNode(headers, TCreateNodeArgs::Directory(RootNodeId, "dir"))
                ->Record.GetNode()
                .GetId();
        service.SetNodeAttr(
            headers,
            fsId,
            TSetNodeAttrArgs(dirId).SetQuotaId(42));

        auto createHandleResponse = service.CreateHandle(
            headers,
            fsId,
            dirId,
            "file1",
            TCreateHandleArgs::CREATE)->Record;

        UNIT_ASSERT_VALUES_EQUAL(
            42u,
            createHandleResponse.GetNodeAttr().GetQuotaId());

        const auto fileId = createHandleResponse.GetNodeAttr().GetId();
        UNIT_ASSERT_VALUES_EQUAL(
            42u,
            service.GetNodeAttr(headers, fsId, fileId, "")
                ->Record.GetNode()
                .GetQuotaId());
    }

    SERVICE_TEST(ShouldRejectCreateNodeWhenQuotaNodesExceeded)
    {
        auto& service = *fixture.Service;
        const auto& fsId = fixture.FsId;

        // the dir's own attach point already counts as 1 node, so only 1
        // more node fits under this limit
        SetQuota(service, fsId, 42, 1_GB, 2);

        auto headers = service.InitSession(fsId, "client");

        const auto dirId =
            service
                .CreateNode(headers, TCreateNodeArgs::Directory(RootNodeId, "dir"))
                ->Record.GetNode()
                .GetId();
        service.SetNodeAttr(
            headers,
            fsId,
            TSetNodeAttrArgs(dirId).SetQuotaId(42));

        // exactly at the limit - allowed. This file may land on a shard,
        // while the dir's own attach point stays on main - force a refresh
        // so main's aggregate view picks up the shard's contribution before
        // the next check.
        service.CreateNode(headers, TCreateNodeArgs::File(dirId, "file1"));
        ForceRefreshAggregateQuotaUsage(service, fsId);

        // one more pushes past the limit - rejected
        service.AssertCreateNodeFailed(
            headers,
            TCreateNodeArgs::File(dirId, "file2"));
    }

    SERVICE_TEST(ShouldRejectGrowingFileBeyondQuotaBytes)
    {
        auto& service = *fixture.Service;
        const auto& fsId = fixture.FsId;

        SetQuota(service, fsId, 42, 100, 100);

        auto headers = service.InitSession(fsId, "client");

        const auto dirId =
            service
                .CreateNode(headers, TCreateNodeArgs::Directory(RootNodeId, "dir"))
                ->Record.GetNode()
                .GetId();
        service.SetNodeAttr(
            headers,
            fsId,
            TSetNodeAttrArgs(dirId).SetQuotaId(42));

        const auto fileId =
            service.CreateNode(headers, TCreateNodeArgs::File(dirId, "file"))
                ->Record.GetNode()
                .GetId();

        // exactly at the limit - allowed
        service.SetNodeAttr(headers, fsId, TSetNodeAttrArgs(fileId).SetSize(100));

        // shrinking is always fine, regardless of the limit
        service.SetNodeAttr(headers, fsId, TSetNodeAttrArgs(fileId).SetSize(50));

        // growing back past the limit - rejected
        service.AssertSetNodeAttrFailed(
            headers,
            fsId,
            TSetNodeAttrArgs(fileId).SetSize(101));
    }

    SERVICE_TEST(ShouldNotEnforceUnlimitedQuota)
    {
        auto& service = *fixture.Service;
        const auto& fsId = fixture.FsId;

        // MaxBytes == 0 means unlimited
        SetQuota(service, fsId, 42, 0, 100);

        auto headers = service.InitSession(fsId, "client");

        const auto dirId =
            service
                .CreateNode(headers, TCreateNodeArgs::Directory(RootNodeId, "dir"))
                ->Record.GetNode()
                .GetId();
        service.SetNodeAttr(
            headers,
            fsId,
            TSetNodeAttrArgs(dirId).SetQuotaId(42));

        const auto fileId =
            service.CreateNode(headers, TCreateNodeArgs::File(dirId, "file"))
                ->Record.GetNode()
                .GetId();
        // well within the quota's MaxNodes (100) but far more than
        // MaxBytes would allow if it weren't 0 (unlimited) - the fixtures'
        // filesystems are small (ShardBlockCount * 4KB ~= 4MB), so this
        // has to stay comfortably under that too
        service.SetNodeAttr(headers, fsId, TSetNodeAttrArgs(fileId).SetSize(1_MB));
    }

    SERVICE_TEST(ShouldRejectRenameAcrossQuotaDomains)
    {
        auto& service = *fixture.Service;
        const auto& fsId = fixture.FsId;

        SetQuota(service, fsId, 1, 1_GB, 100);
        SetQuota(service, fsId, 2, 1_GB, 100);

        auto headers = service.InitSession(fsId, "client");

        const auto dir1Id =
            service
                .CreateNode(headers, TCreateNodeArgs::Directory(RootNodeId, "dir1"))
                ->Record.GetNode()
                .GetId();
        service.SetNodeAttr(headers, fsId, TSetNodeAttrArgs(dir1Id).SetQuotaId(1));

        const auto dir2Id =
            service
                .CreateNode(headers, TCreateNodeArgs::Directory(RootNodeId, "dir2"))
                ->Record.GetNode()
                .GetId();
        service.SetNodeAttr(headers, fsId, TSetNodeAttrArgs(dir2Id).SetQuotaId(2));

        service.CreateNode(headers, TCreateNodeArgs::File(dir1Id, "file"));

        // moving a file across a quota boundary is rejected outright, like
        // a cross-device rename - no usage transfer, no partial recoloring
        service.AssertRenameNodeFailed(
            headers,
            dir1Id,
            "file",
            dir2Id,
            "file",
            0);
    }

    SERVICE_TEST(ShouldRejectRenameOfEmptyDirectoryAcrossQuotaDomains)
    {
        auto& service = *fixture.Service;
        const auto& fsId = fixture.FsId;

        SetQuota(service, fsId, 1, 1_GB, 100);
        SetQuota(service, fsId, 2, 1_GB, 100);

        auto headers = service.InitSession(fsId, "client");

        const auto dir1Id =
            service
                .CreateNode(headers, TCreateNodeArgs::Directory(RootNodeId, "dir1"))
                ->Record.GetNode()
                .GetId();
        service.SetNodeAttr(headers, fsId, TSetNodeAttrArgs(dir1Id).SetQuotaId(1));

        const auto dir2Id =
            service
                .CreateNode(headers, TCreateNodeArgs::Directory(RootNodeId, "dir2"))
                ->Record.GetNode()
                .GetId();
        service.SetNodeAttr(headers, fsId, TSetNodeAttrArgs(dir2Id).SetQuotaId(2));

        // an ordinary, empty subdirectory - inherits QuotaId 1 at creation,
        // never itself attached. Even though it's empty, crossing a quota
        // boundary is rejected outright, same as a non-empty one.
        service.CreateNode(headers, TCreateNodeArgs::Directory(dir1Id, "subdir"));

        service.AssertRenameNodeFailed(
            headers,
            dir1Id,
            "subdir",
            dir2Id,
            "subdir",
            0);
    }

    SERVICE_TEST(ShouldAllowRenameWithinTheSameQuotaDomain)
    {
        auto& service = *fixture.Service;
        const auto& fsId = fixture.FsId;

        SetQuota(service, fsId, 1, 1_GB, 100);

        auto headers = service.InitSession(fsId, "client");

        const auto dir1Id =
            service
                .CreateNode(headers, TCreateNodeArgs::Directory(RootNodeId, "dir1"))
                ->Record.GetNode()
                .GetId();
        service.SetNodeAttr(headers, fsId, TSetNodeAttrArgs(dir1Id).SetQuotaId(1));

        const auto dir2Id =
            service
                .CreateNode(headers, TCreateNodeArgs::Directory(RootNodeId, "dir2"))
                ->Record.GetNode()
                .GetId();
        service.SetNodeAttr(headers, fsId, TSetNodeAttrArgs(dir2Id).SetQuotaId(1));

        service.CreateNode(headers, TCreateNodeArgs::File(dir1Id, "file"));

        // both directories are under the same quota - moving between them
        // stays within the domain and is allowed
        service.RenameNode(headers, dir1Id, "file", dir2Id, "file", 0);

        UNIT_ASSERT_VALUES_EQUAL(
            1u,
            service.GetNodeAttr(headers, fsId, dir2Id, "file")
                ->Record.GetNode()
                .GetQuotaId());
    }

    SERVICE_TEST(ShouldAllowMovingAQuotaRootDirectoryAcrossQuotaDomains)
    {
        auto& service = *fixture.Service;
        const auto& fsId = fixture.FsId;

        SetQuota(service, fsId, 1, 1_GB, 100);
        SetQuota(service, fsId, 2, 1_GB, 100);

        auto headers = service.InitSession(fsId, "client");

        const auto rootAId =
            service
                .CreateNode(headers, TCreateNodeArgs::Directory(RootNodeId, "rootA"))
                ->Record.GetNode()
                .GetId();
        service.SetNodeAttr(headers, fsId, TSetNodeAttrArgs(rootAId).SetQuotaId(1));
        service.CreateNode(headers, TCreateNodeArgs::File(rootAId, "file"));

        const auto dir2Id =
            service
                .CreateNode(headers, TCreateNodeArgs::Directory(RootNodeId, "dir2"))
                ->Record.GetNode()
                .GetId();
        service.SetNodeAttr(headers, fsId, TSetNodeAttrArgs(dir2Id).SetQuotaId(2));

        // rootA is itself a quota root (attached, not inherited) - moving it
        // under a different domain is allowed even though it's non-empty,
        // and it keeps carrying its own QuotaId
        service.RenameNode(headers, RootNodeId, "rootA", dir2Id, "rootA", 0);

        UNIT_ASSERT_VALUES_EQUAL(
            1u,
            service.GetNodeAttr(headers, fsId, dir2Id, "rootA")
                ->Record.GetNode()
                .GetQuotaId());
    }
}

}   // namespace NCloud::NFileStore::NStorage
