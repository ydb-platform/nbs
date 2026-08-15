#include "service.h"
#include "service_private.h"
#include "service_ut_helpers.h"
#include "service_ut_sharding.h"

#include <cloud/filestore/libs/storage/model/utils.h>
#include <cloud/filestore/libs/storage/testlib/service_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

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
}

}   // namespace NCloud::NFileStore::NStorage
