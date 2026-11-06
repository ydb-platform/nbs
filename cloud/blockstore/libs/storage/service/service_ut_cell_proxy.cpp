#include "service_ut.h"

#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/storage/api/cell_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/testlib/cell_manager_mock.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>
#include <cloud/blockstore/libs/ydbstats/ydbstats.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

ui32 CreateCellNode(TTestEnv& env, const TString& cellId)
{
    env.CreateSubDomain(cellId);
    NProto::TStorageServiceConfig config;
    config.SetSchemeShardDir("/local/" + cellId);
    return env.CreateBlockStoreNode(cellId,
                                    CreateTestStorageConfig(std::move(config)),
                                    CreateTestDiagnosticsConfig(),
                                    NYdbStats::CreateVolumesStatsUploaderStub(),
                                    CreateManuallyPreemptedVolumes(), cellId);
}

void InitializeCellProxy(TServiceClient& service, TTestActorRuntime& runtime,
                         const TString& cellId, ui32 nodeIdx)
{
    service.SendRequest(
        MakeCellProxyServiceId(),
        std::make_unique<TEvCellProxy::TEvInitializeRequest>(
            CreateCellManagerMock(runtime, {{cellId, nodeIdx}}),
            std::make_shared<NClient::TClientAppConfig>()));
    const auto response =
        service.RecvResponse<TEvCellProxy::TEvInitializeResponse>();
    UNIT_ASSERT_VALUES_EQUAL_C(S_OK, response->GetStatus(),
                               FormatError(response->GetError()));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceCellProxyTest)
{
    Y_UNIT_TEST(ShouldDescribeVolumeInAnotherCell)
    {
        TTestEnv env(1, 2, 4);
        const auto nodeA = CreateCellNode(env, "cell-a");
        const auto nodeB = CreateCellNode(env, "cell-b");
        auto& runtime = env.GetRuntime();
        TServiceClient serviceA(runtime, nodeA);
        TServiceClient serviceB(runtime, nodeB);
        // Resolve the same disk id in two cells to detect local fallback.
        serviceA.CreateVolume("volume", 1024);
        serviceB.CreateVolume("volume", 2048);
        InitializeCellProxy(serviceA, runtime, "cell-b", nodeB);

        serviceA.SendRequest(
            MakeCellProxyServiceId(),
            std::make_unique<TEvCellProxy::TEvDescribeVolumeRequest>("cell-b",
                                                                     "volume"));
        const auto response =
            serviceA.RecvResponse<TEvCellProxy::TEvDescribeVolumeResponse>();
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, response->GetStatus(),
                                   FormatError(response->GetError()));
        UNIT_ASSERT_VALUES_EQUAL("volume", response->Volume.GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(2048, response->Volume.GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(DefaultBlockSize,
                                 response->Volume.GetBlockSize());

        serviceA.CreateVolume("local-only");
        serviceA.SendRequest(
            MakeCellProxyServiceId(),
            std::make_unique<TEvCellProxy::TEvDescribeVolumeRequest>(
                "cell-b", "local-only"));
        const auto missing =
            serviceA.RecvResponse<TEvCellProxy::TEvDescribeVolumeResponse>();
        UNIT_ASSERT(IsNotFoundSchemeShardError(missing->GetError()));
    }

    Y_UNIT_TEST(ShouldExecuteActionInAnotherCell)
    {
        TTestEnv env(1, 2, 4);
        const auto nodeA = CreateCellNode(env, "cell-a");
        const auto nodeB = CreateCellNode(env, "cell-b");
        auto& runtime = env.GetRuntime();
        TServiceClient serviceA(runtime, nodeA);
        InitializeCellProxy(serviceA, runtime, "cell-b", nodeB);

        NProto::TExecuteActionRequest request;
        request.SetAction("getstorageconfig");
        request.SetInput("{}");
        serviceA.SendRequest(
            MakeCellProxyServiceId(),
            std::make_unique<TEvCellProxy::TEvExecuteActionRequest>(
                "cell-b", std::move(request)));
        const auto response =
            serviceA.RecvResponse<TEvCellProxy::TEvExecuteActionResponse>();
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, response->GetStatus(),
                                   FormatError(response->GetError()));
        NProto::TStorageServiceConfig config;
        UNIT_ASSERT_C(
            google::protobuf::util::JsonStringToMessage(response->Output,
                                                        &config)
                .ok(), response->Output);
        UNIT_ASSERT_VALUES_EQUAL("/local/cell-b", config.GetSchemeShardDir());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
