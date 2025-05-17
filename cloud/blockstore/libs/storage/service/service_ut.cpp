#include "service_ut.h"

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>
#include <cloud/blockstore/libs/ydbstats/ydbstats.h>
#include "cloud/blockstore/private/api/protos/volume.pb.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TDiagnosticsConfigPtr CreateTestDiagnosticsConfig()
{
    return std::make_shared<TDiagnosticsConfig>(NProto::TDiagnosticsConfig());
}

ui32 SetupTestEnv(TTestEnv& env)
{
    env.CreateSubDomain("nbs");

    auto storageConfig = CreateTestStorageConfig({});

    return env.CreateBlockStoreNode(
        "nbs",
        storageConfig,
        CreateTestDiagnosticsConfig());
}

ui32 SetupTestEnv(
    TTestEnv& env,
    NProto::TStorageServiceConfig storageServiceConfig,
    NProto::TFeaturesConfig featuresConfig)
{
    env.CreateSubDomain("nbs");

    auto storageConfig = CreateTestStorageConfig(
        std::move(storageServiceConfig),
        std::move(featuresConfig));

    return env.CreateBlockStoreNode(
        "nbs",
        storageConfig,
        CreateTestDiagnosticsConfig());
}

ui32 SetupTestEnvWithYdbStats(
    TTestEnv& env,
    ui32 diskCnt,
    ui32 rowCnt,
    TDuration statsUploadInterval,
    TDuration statsUploadRetryTimeout,
    NYdbStats::IYdbVolumesStatsUploaderPtr ydbStatsUploader)
{
    env.CreateSubDomain("nbs");

    NProto::TStorageServiceConfig storageServiceConfig;
    storageServiceConfig.SetStatsUploadDiskCount(diskCnt);
    storageServiceConfig.SetStatsUploadRowCount(rowCnt);
    storageServiceConfig.SetStatsUploadInterval(statsUploadInterval.MilliSeconds());
    storageServiceConfig.SetStatsUploadRetryTimeout(statsUploadRetryTimeout.MilliSeconds());

    auto storageConfig = CreateTestStorageConfig(
        std::move(storageServiceConfig),
        {});

    return env.CreateBlockStoreNode(
        "nbs",
        std::move(storageConfig),
        CreateTestDiagnosticsConfig(),
        std::move(ydbStatsUploader),
        CreateManuallyPreemptedVolumes());
}

ui32 SetupTestEnvWithMultipleMount(
    TTestEnv& env,
    TDuration inactivateTimeout)
{
    NProto::TStorageServiceConfig storageServiceConfig;
    storageServiceConfig.SetInactiveClientsTimeout(
        inactivateTimeout.MilliSeconds());
    return SetupTestEnv(env, std::move(storageServiceConfig));
}

ui32 SetupTestEnvWithAllowVersionInModifyScheme(TTestEnv& env)
{
    NProto::TStorageServiceConfig storageServiceConfig;
    storageServiceConfig.SetAllowVersionInModifyScheme(true);
    return SetupTestEnv(env, std::move(storageServiceConfig));
}

ui32 SetupTestEnvWithManuallyPreemptedVolumes(
    TTestEnv& env,
    NProto::TStorageServiceConfig storageServiceConfig,
    TVector<std::pair<TString, TInstant>> volumes)
{
    auto manuallyPreemptedVolumes = CreateManuallyPreemptedVolumes();

    for (const auto& v: volumes) {
        manuallyPreemptedVolumes->AddVolume(v.first, v.second);
    }

    return SetupTestEnvWithManuallyPreemptedVolumes(
        env,
        std::move(storageServiceConfig),
        std::move(manuallyPreemptedVolumes)
    );
}

ui32 SetupTestEnvWithManuallyPreemptedVolumes(
    TTestEnv& env,
    NProto::TStorageServiceConfig storageServiceConfig,
    TManuallyPreemptedVolumesPtr manuallyPreemptedVolumes)
{
    env.CreateSubDomain("nbs");

    auto storageConfig = CreateTestStorageConfig(
        std::move(storageServiceConfig),
        {});

    return env.CreateBlockStoreNode(
        "nbs",
        std::move(storageConfig),
        CreateTestDiagnosticsConfig(),
        NYdbStats::CreateVolumesStatsUploaderStub(),
        std::move(manuallyPreemptedVolumes));
}

TString GetBlockContent(char fill, size_t size)
{
    return TString(size, fill);
}

NKikimrBlockStore::TVolumeConfig GetVolumeConfig(
    TServiceClient& service,
    const TString& diskId)
{
    NCloud::NBlockStore::NPrivateProto::TDescribeVolumeRequest request;
    request.SetDiskId(diskId);
    TString buf;
    google::protobuf::util::MessageToJsonString(request, &buf);
    auto response = service.ExecuteAction("DescribeVolume", buf);
    NKikimrSchemeOp::TBlockStoreVolumeDescription pathDescr;
    UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
        response->Record.GetOutput(),
        &pathDescr
    ).ok());
    return pathDescr.GetVolumeConfig();
}

}   // namespace NCloud::NBlockStore::NStorage
