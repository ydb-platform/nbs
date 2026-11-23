#include "service.h"
#include "service_private.h"
#include "service_ut_helpers.h"
#include "service_ut_sharding.h"

#include <cloud/filestore/libs/storage/model/utils.h>
#include <cloud/filestore/libs/storage/testlib/service_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/common/error.h>

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

        auto headers = service.InitSession(fsId, "client");

        service.SetQuota(headers, fsId, 42, 1_GB, 100);

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

        auto headers = service.InitSession(fsId, "client");

        service.SetQuota(headers, fsId, 42, 1_GB, 100);

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

        auto headers = service.InitSession(fsId, "client");

        // the dir's own attach point already counts as 1 node, so only 1
        // more node fits under this limit
        service.SetQuota(headers, fsId, 42, 1_GB, 2);

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

        auto headers = service.InitSession(fsId, "client");

        service.SetQuota(headers, fsId, 42, 100, 100);

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

        auto headers = service.InitSession(fsId, "client");

        // MaxBytes == 0 means unlimited
        service.SetQuota(headers, fsId, 42, 0, 100);

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

        auto headers = service.InitSession(fsId, "client");

        service.SetQuota(headers, fsId, 1, 1_GB, 100);
        service.SetQuota(headers, fsId, 2, 1_GB, 100);

        auto createDir = [&] (const TString& name, ui32 quotaId) {
            const auto dirId =
                service
                    .CreateNode(
                        headers,
                        TCreateNodeArgs::Directory(RootNodeId, name))
                    ->Record.GetNode()
                    .GetId();
            if (quotaId) {
                service.SetNodeAttr(
                    headers,
                    fsId,
                    TSetNodeAttrArgs(dirId).SetQuotaId(quotaId));
            }
            return dirId;
        };

        const auto dirAId = createDir("dirA", 1);
        // same quota as dirA
        const auto dirCId = createDir("dirC", 1);
        const auto dirBId = createDir("dirB", 2);
        const auto plainDirId = createDir("plain", 0);

        if (ExtractShardNo(dirAId)) {
            UNIT_ASSERT_VALUES_EQUAL(
                ExtractShardNo(dirAId),
                ExtractShardNo(dirBId));
            UNIT_ASSERT_VALUES_UNEQUAL(
                ExtractShardNo(dirAId),
                ExtractShardNo(dirCId));
        }

        service.CreateNode(headers, TCreateNodeArgs::File(dirAId, "file"));
        service.CreateNode(headers, TCreateNodeArgs::Directory(dirAId, "sub"));
        service.CreateNode(headers, TCreateNodeArgs::File(dirBId, "bfile"));
        service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "outside"));

        auto assertRejected = [&] (
            ui64 parentId,
            const TString& name,
            ui64 newParentId)
        {
            auto response = service.AssertRenameNodeFailed(
                headers,
                parentId,
                name,
                newParentId,
                name,
                0 /* flags */);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_XDEV,
                response->GetError().GetCode(),
                response->GetError().GetMessage());

            // the node stays where it was
            service.GetNodeAttr(headers, fsId, parentId, name);
            service.AssertGetNodeAttrFailed(headers, fsId, newParentId, name);
        };

        // quota 1 -> quota 2
        assertRejected(dirAId, "file", dirBId);
        // quota 1 -> no quota
        assertRejected(dirAId, "file", RootNodeId);
        assertRejected(dirAId, "file", plainDirId);
        // no quota -> quota 1
        assertRejected(RootNodeId, "outside", dirAId);
        // a directory, quota 1 -> quota 2
        assertRejected(dirAId, "sub", dirBId);
        // an attach point can't be moved into a quota'd directory, even of
        // the same quota - no nested quotas
        assertRejected(RootNodeId, "dirA", dirBId);
        assertRejected(RootNodeId, "dirA", dirCId);

        {
            auto response = service.AssertRenameNodeFailed(
                headers,
                dirAId,
                "file",
                dirBId,
                "bfile",
                NProto::TRenameNodeRequest::F_EXCHANGE);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_XDEV,
                response->GetError().GetCode(),
                response->GetError().GetMessage());

            service.GetNodeAttr(headers, fsId, dirAId, "file");
            service.GetNodeAttr(headers, fsId, dirBId, "bfile");
            service.AssertGetNodeAttrFailed(headers, fsId, dirAId, "bfile");
            service.AssertGetNodeAttrFailed(headers, fsId, dirBId, "file");
        }

        // moves that don't cross a quota boundary still work
        service.RenameNode(headers, dirAId, "file", dirCId, "file", 0);
        service.RenameNode(
            headers,
            RootNodeId,
            "outside",
            plainDirId,
            "outside",
            0);

        // the attach point belongs to quota 1 while its parent doesn't - it
        // can still be renamed within the unquota'd space
        service.RenameNode(headers, RootNodeId, "dirA", RootNodeId, "dirA2", 0);
        service.RenameNode(headers, RootNodeId, "dirA2", plainDirId, "dirA2", 0);
        UNIT_ASSERT_VALUES_EQUAL(
            1u,
            service.GetNodeAttr(headers, fsId, plainDirId, "dirA2")
                ->Record.GetNode()
                .GetQuotaId());
    }

    SERVICE_TEST(ShouldRejectSetQuotaWithZeroQuotaId)
    {
        auto& service = *fixture.Service;
        const auto& fsId = fixture.FsId;

        auto response =
            service.AssertSetQuotaFailed(THeaders{}, fsId, 0, 1_GB, 100);
        UNIT_ASSERT_VALUES_EQUAL(
            (ui32)E_ARGUMENT,
            response->GetError().GetCode());
    }

    SERVICE_TEST(ShouldFailQuotaRequestsForNonExistentFileSystem)
    {
        auto& service = *fixture.Service;

        auto response = service.AssertSetQuotaFailed(
            THeaders{},
            "nonexistent",
            42,
            1_GB,
            100);
        UNIT_ASSERT_VALUES_EQUAL(
            (ui32)E_NOT_FOUND,
            response->GetError().GetCode());
    }
}

}   // namespace NCloud::NFileStore::NStorage
