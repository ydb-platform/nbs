#include "ss_proxy.h"

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/volume_label.h>
#include <cloud/blockstore/libs/storage/core/volume_model.h>
#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>
#include <cloud/blockstore/libs/storage/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>

#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/libs/kikimr/helpers.h>

#include <contrib/ydb/core/tx/schemeshard/schemeshard.h>
#include <contrib/ydb/core/tx/tx_proxy/proxy.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NSchemeShard;

using TVolumeConfig = NKikimrBlockStore::TVolumeConfig;

namespace {

////////////////////////////////////////////////////////////////////////////////

void SetupTestEnv(
    TTestEnv& env,
    TStorageConfigPtr storageConfig)
{
    env.CreateSubDomain("nbs");
    env.CreateBlockStoreNode(
        "nbs",
        storageConfig,
        std::make_shared<TDiagnosticsConfig>(NProto::TDiagnosticsConfig()));

    auto& runtime = env.GetRuntime();
    ui32 nodeIdx = 0;

    auto ssProxyId = runtime.Register(
        CreateSSProxy(storageConfig).release(),
        nodeIdx,
        runtime.GetAppData(nodeIdx).UserPoolId,
        TMailboxType::Simple,
        0);
    runtime.EnableScheduleForActor(ssProxyId);
    runtime.RegisterService(MakeSSProxyServiceId(), ssProxyId, nodeIdx);
}

auto CreateStorageConfig(NProto::TStorageServiceConfig config = {})
{
    return CreateTestStorageConfig(std::move(config));
}

void SetupTestEnv(TTestEnv& env)
{
    SetupTestEnv(env, CreateStorageConfig());
}

void MkDir(TTestActorRuntime& runtime, const TString& path)
{
    TStringBuf dir;
    TStringBuf name;
    TStringBuf(path).RSplit('/', dir, name);

    TActorId sender = runtime.AllocateEdgeActor();

    NKikimrSchemeOp::TModifyScheme modifyScheme;
    modifyScheme.SetWorkingDir(TString{dir});
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpMkDir);

    auto* op = modifyScheme.MutableMkDir();
    op->SetName(TString{name});

    Send(
        runtime,
        MakeSSProxyServiceId(),
        sender,
        std::make_unique<TEvSSProxy::TEvModifySchemeRequest>(std::move(modifyScheme)));

    TAutoPtr<IEventHandle> handle;
    auto response = runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvModifySchemeResponse>(handle);
    UNIT_ASSERT_C(Succeeded(response), GetErrorReason(response));
}

void SendDescribeDir(TTestActorRuntime& runtime, const TString& path)
{
    TActorId sender = runtime.AllocateEdgeActor();

    Send(
        runtime,
        MakeSSProxyServiceId(),
        sender,
        std::make_unique<TEvSSProxy::TEvDescribeSchemeRequest>(path));

    TAutoPtr<IEventHandle> handle;
    auto response = runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvDescribeSchemeResponse>(handle);
    UNIT_ASSERT_C(Succeeded(response), GetErrorReason(response));
    UNIT_ASSERT_EQUAL(
        response->PathDescription.GetSelf().GetPathType(),
        NKikimrSchemeOp::EPathTypeDir);
}

void SendWaitTx(TTestActorRuntime& runtime, ui64 tabletId, ui64 txId)
{
    TActorId sender = runtime.AllocateEdgeActor();

    Send(
        runtime,
        MakeSSProxyServiceId(),
        sender,
        std::make_unique<TEvSSProxy::TEvWaitSchemeTxRequest>(tabletId, txId));

    TAutoPtr<IEventHandle> handle;
    auto response = runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvWaitSchemeTxResponse>(handle);
    UNIT_ASSERT_C(Succeeded(response), GetErrorReason(response));
}

void AddChannel(
    TVolumeConfig& volumeConfig,
    TString poolKind,
    EChannelDataKind dataKind)
{
    auto* cp = volumeConfig.AddExplicitChannelProfiles();
    cp->SetPoolKind(std::move(poolKind));
    cp->SetDataKind(static_cast<int>(dataKind));
}

void FillVolumeConfig(
    TVolumeConfig& volumeConfig,
    const TString& diskId,
    ui32 blockSize,
    ui64 blocksCount,
    ui32 numChannels)
{
    volumeConfig.SetDiskId(diskId);
    volumeConfig.SetBlockSize(blockSize);
    AddChannel(volumeConfig, "pool-kind-1", EChannelDataKind::System);
    AddChannel(volumeConfig, "pool-kind-1", EChannelDataKind::Log);
    AddChannel(volumeConfig, "pool-kind-1", EChannelDataKind::Index);
    while (numChannels--) {
        AddChannel(volumeConfig, "pool-kind-1", EChannelDataKind::Merged);
    }

    auto partition = volumeConfig.PartitionsSize()
        ? volumeConfig.MutablePartitions(0)
        : volumeConfig.AddPartitions();
    partition->SetBlockCount(blocksCount);
}

void CreateVolumeViaModifyScheme(
    TTestActorRuntime& runtime,
    TStorageConfigPtr config,
    const TString& diskId,
    const TString& relativeVolumeDir,
    const TString& volumeName,
    ui32 blockSize)
{
    TActorId sender = runtime.AllocateEdgeActor();

    const TString& rootDir = config->GetSchemeShardDir();

    {
        auto splitted = SplitPath(relativeVolumeDir);

        TString dirToCreate = rootDir;
        for (const auto& part : splitted) {
            dirToCreate += "/" + part;
            MkDir(runtime, dirToCreate);
        }
    }

    TString volumeDir = rootDir;
    if (relativeVolumeDir) {
        volumeDir += "/" + relativeVolumeDir;
    }

    NKikimrSchemeOp::TModifyScheme modifyScheme;
    modifyScheme.SetWorkingDir(volumeDir);
    modifyScheme.SetOperationType(
        NKikimrSchemeOp::ESchemeOpCreateBlockStoreVolume);

    auto& op = *modifyScheme.MutableCreateBlockStoreVolume();
    op.SetName(volumeName);

    FillVolumeConfig(
        *op.MutableVolumeConfig(),
        diskId,
        blockSize,
        1,  // blocksCount
        1  // numChannels
    );

    Send(
        runtime,
        MakeSSProxyServiceId(),
        sender,
        std::make_unique<TEvSSProxy::TEvModifySchemeRequest>(
            std::move(modifyScheme)));

    TAutoPtr<IEventHandle> handle;
    auto response =
        runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvModifySchemeResponse>(handle);
    UNIT_ASSERT_C(Succeeded(response), GetErrorReason(response));
}

void CreateVolumeViaModifySchemeDeprecated(
    TTestActorRuntime& runtime,
    TStorageConfigPtr config,
    const TString& diskId,
    ui32 blockSize = 1)
{
    TString relativeVolumeDir;
    TString volumeName;
    std::tie(relativeVolumeDir, volumeName) =
        DiskIdToVolumeDirAndNameDeprecated({}, diskId);

    CreateVolumeViaModifyScheme(
        runtime,
        config,
        diskId,
        relativeVolumeDir,
        volumeName,
        blockSize);
}

void CreateVolumeViaModifyScheme(
    TTestActorRuntime& runtime,
    TStorageConfigPtr config,
    const TString& diskId,
    ui32 blockSize = 1)
{
    TString relativeVolumeDir;
    TString volumeName;
    std::tie(relativeVolumeDir, volumeName) =
        DiskIdToVolumeDirAndName({}, diskId);

    CreateVolumeViaModifyScheme(
        runtime,
        config,
        diskId,
        relativeVolumeDir,
        volumeName,
        blockSize);
}

void SendCreateVolumeRequest(
    TTestActorRuntime& runtime,
    TActorId sender,
    const TString& diskId,
    ui32 blockSize = 1)
{
    const ui64 blocksCount = 1;
    const ui32 numChannels = 1;

    TVolumeConfig volumeConfig;
    FillVolumeConfig(
        volumeConfig,
        diskId,
        blockSize,
        blocksCount,
        numChannels);

    Send(
        runtime,
        MakeSSProxyServiceId(),
        sender,
        std::make_unique<TEvSSProxy::TEvCreateVolumeRequest>(
            std::move(volumeConfig)));

}

void CreateVolume(
    TTestActorRuntime& runtime,
    const TString& diskId,
    ui32 blockSize = 1)
{
    TActorId sender = runtime.AllocateEdgeActor();

    SendCreateVolumeRequest(runtime, sender, diskId, blockSize);

    TAutoPtr<IEventHandle> handle;
    auto response =
        runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvCreateVolumeResponse>(
            handle);
    UNIT_ASSERT_C(Succeeded(response), GetErrorReason(response));
}

void CreateVolumeWithFailure(
    TTestActorRuntime& runtime,
    const TString& diskId,
    ui32 blockSize = 1)
{
    TActorId sender = runtime.AllocateEdgeActor();

    SendCreateVolumeRequest(runtime, sender, diskId, blockSize);

    TAutoPtr<IEventHandle> handle;
    auto response =
        runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvCreateVolumeResponse>(
            handle);
    UNIT_ASSERT_C(!Succeeded(response), GetErrorReason(response));
}

void DescribeVolumeWithFailure(
    TTestActorRuntime& runtime,
    const TString& diskId)
{
    TActorId sender = runtime.AllocateEdgeActor();

    Send(
        runtime,
        MakeSSProxyServiceId(),
        sender,
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(diskId));

    TAutoPtr<IEventHandle> handle;
    auto response =
        runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvDescribeVolumeResponse>(handle);

    UNIT_ASSERT_C(!Succeeded(response), GetErrorReason(response));
}

struct TVolume {
    TString Path;
    TString MountToken;
};

TVolume DescribeVolume(
    TTestActorRuntime& runtime,
    const TString& diskId)
{
    TActorId sender = runtime.AllocateEdgeActor();

    Send(
        runtime,
        MakeSSProxyServiceId(),
        sender,
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(diskId));

    TAutoPtr<IEventHandle> handle;
    auto response =
        runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvDescribeVolumeResponse>(handle);

    UNIT_ASSERT_C(Succeeded(response), GetErrorReason(response));

    const auto& pathDescription = response->PathDescription;

    UNIT_ASSERT_EQUAL(
        NKikimrSchemeOp::EPathTypeBlockStoreVolume,
        pathDescription.GetSelf().GetPathType());

    const auto& volumeDescription =
        pathDescription.GetBlockStoreVolumeDescription();
    const auto& volumeConfig = volumeDescription.GetVolumeConfig();

    UNIT_ASSERT_VALUES_EQUAL(diskId, volumeConfig.GetDiskId());

    return { response->Path, volumeDescription.GetMountToken() };
}

TString DescribeVolumeAndReturnPath(
    TTestActorRuntime& runtime,
    const TString& diskId)
{
    return DescribeVolume(runtime, diskId).Path;
}

TString DescribeVolumeAndReturnMountToken(
    TTestActorRuntime& runtime,
    const TString& diskId)
{
    return DescribeVolume(runtime, diskId).MountToken;
}

using EOpType = TEvSSProxy::TModifyVolumeRequest::EOpType;

void ModifyVolume(
    TTestActorRuntime& runtime,
    EOpType opType,
    const TString& diskId,
    const TString& mountToken = {})
{
    TActorId sender = runtime.AllocateEdgeActor();

    Send(
        runtime,
        MakeSSProxyServiceId(),
        sender,
        std::make_unique<TEvSSProxy::TEvModifyVolumeRequest>(
            opType,
            diskId,
            mountToken,
            0));

    TAutoPtr<IEventHandle> handle;
    auto response =
        runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvModifyVolumeResponse>(handle);

    UNIT_ASSERT_C(Succeeded(response), GetErrorReason(response));
}

void AssignVolume(
    TTestActorRuntime& runtime,
    const TString& diskId,
    const TString& mountToken)
{
    ModifyVolume(runtime, EOpType::Assign, diskId, mountToken);
}

void DestroyVolume(
    TTestActorRuntime& runtime,
    const TString& diskId)
{
    ModifyVolume(runtime, EOpType::Destroy, diskId);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSSProxyTest)
{
    Y_UNIT_TEST(ShouldCreateDescribeDirectories)
    {
        NProto::TStorageServiceConfig configProto;

        configProto.SetSchemeShardDir("/local");
        {
            TTestEnv env;
            SetupTestEnv(env, CreateTestStorageConfig(configProto));

            MkDir(env.GetRuntime(), "/local/foo");
            MkDir(env.GetRuntime(), "/local/bar");

            SendDescribeDir(env.GetRuntime(), "/local/foo");
            SendDescribeDir(env.GetRuntime(), "/local/bar");
        }

        configProto.SetSchemeShardDir("/local/nbs");
        {
            TTestEnv env;
            SetupTestEnv(env, CreateTestStorageConfig(configProto));

            MkDir(env.GetRuntime(), "/local/nbs/foo");
            MkDir(env.GetRuntime(), "/local/nbs/bar");

            SendDescribeDir(env.GetRuntime(), "/local/nbs/foo");
            SendDescribeDir(env.GetRuntime(), "/local/nbs/bar");
        }
    }

    Y_UNIT_TEST(ShouldWaitForTransactions)
    {
        TTestEnv env;
        SetupTestEnv(env);

        SendWaitTx(env.GetRuntime(), env.GetSchemeShard(), 1);
    }

    Y_UNIT_TEST(ShouldDescribeVolumes)
    {
        TTestEnv env;
        auto& runtime = env.GetRuntime();
        auto config = CreateStorageConfig();
        SetupTestEnv(env, config);

        CreateVolumeViaModifySchemeDeprecated(runtime, config, "old-volume");
        CreateVolumeViaModifyScheme(runtime, config, "new-volume");

        UNIT_ASSERT_VALUES_EQUAL(
            "/local/nbs/old-volume",
            DescribeVolumeAndReturnPath(runtime, "old-volume"));

        UNIT_ASSERT_VALUES_EQUAL(
            "/local/nbs/_17A/new-volume",
            DescribeVolumeAndReturnPath(runtime, "new-volume"));
    }

    Y_UNIT_TEST(ShouldDescribeVolumesWithSchemeCache)
    {
        TTestEnv env;
        auto& runtime = env.GetRuntime();
        auto config = CreateStorageConfig(
            []() {
                NProto::TStorageServiceConfig config;
                config.SetUseSchemeCache(true);
                return config;
            }()
        );
        SetupTestEnv(env, config);

        CreateVolumeViaModifySchemeDeprecated(runtime, config, "old-volume");
        CreateVolumeViaModifyScheme(runtime, config, "new-volume");

        UNIT_ASSERT_VALUES_EQUAL(
            "/local/nbs/old-volume",
            DescribeVolumeAndReturnPath(runtime, "old-volume"));

        UNIT_ASSERT_VALUES_EQUAL(
            "/local/nbs/_17A/new-volume",
            DescribeVolumeAndReturnPath(runtime, "new-volume"));
    }

    Y_UNIT_TEST(ShouldDescribeVolumesInFallbackMode)
    {
        TString backupFilePath =
            "ShouldDescribeVolumesInFallbackMode.path_description_backup";

        NProto::TStorageServiceConfig configProto;
        configProto.SetPathDescriptionBackupFilePath(backupFilePath);

        {
            TTestEnv env;
            auto& runtime = env.GetRuntime();
            auto config = CreateTestStorageConfig(configProto);
            SetupTestEnv(env, config);

            CreateVolumeViaModifySchemeDeprecated(runtime, config, "old-volume");
            CreateVolumeViaModifyScheme(runtime, config, "new-volume");

            UNIT_ASSERT_VALUES_EQUAL(
                "/local/nbs/old-volume",
                DescribeVolumeAndReturnPath(runtime, "old-volume"));
            UNIT_ASSERT_VALUES_EQUAL(
                "/local/nbs/_17A/new-volume",
                DescribeVolumeAndReturnPath(runtime, "new-volume"));

            // Smoke check for background sync (15 seconds should be enough).
            runtime.AdvanceCurrentTime(TDuration::Seconds(15));
            runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(15));

            TActorId sender = runtime.AllocateEdgeActor();

            Send(
                runtime,
                MakeSSProxyServiceId(),
                sender,
                std::make_unique<TEvSSProxy::TEvBackupPathDescriptionsRequest>());

            TAutoPtr<IEventHandle> handle;
            auto response =
                runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvBackupPathDescriptionsResponse>(
                    handle);
            UNIT_ASSERT_C(Succeeded(response), GetErrorReason(response));
        }

        configProto.SetSSProxyFallbackMode(true);

        {
            TTestEnv env;
            auto& runtime = env.GetRuntime();
            auto config = CreateTestStorageConfig(configProto);
            SetupTestEnv(env, config);

            UNIT_ASSERT_VALUES_EQUAL(
                "/local/nbs/old-volume",
                DescribeVolumeAndReturnPath(runtime, "old-volume"));
            UNIT_ASSERT_VALUES_EQUAL(
                "/local/nbs/_17A/new-volume",
                DescribeVolumeAndReturnPath(runtime, "new-volume"));
            DescribeVolumeWithFailure(runtime, "unexisting");
        }
    }

    Y_UNIT_TEST(ShouldFailDescribeVolumeIfPathDoesNotExist)
    {
        TTestEnv env;
        SetupTestEnv(env);
        auto& runtime = env.GetRuntime();

        DescribeVolumeWithFailure(runtime, "volume");
    }

    Y_UNIT_TEST(ShouldFailDescribeVolumeIfPathIsNotBlockStoreVolume)
    {
        TTestEnv env;
        SetupTestEnv(env);
        auto& runtime = env.GetRuntime();

        MkDir(env.GetRuntime(), "/local/nbs/volume");

        DescribeVolumeWithFailure(runtime, "volume");
    }

    Y_UNIT_TEST(ShouldDescribeOldVolumeInCaseOfNameCollision)
    {
        TTestEnv env;
        auto& runtime = env.GetRuntime();
        auto config = CreateStorageConfig();
        SetupTestEnv(env, config);

        CreateVolumeViaModifySchemeDeprecated(runtime, config, "volume");
        CreateVolumeViaModifyScheme(runtime, config, "volume");

        UNIT_ASSERT_VALUES_EQUAL(
            "/local/nbs/volume",
            DescribeVolumeAndReturnPath(runtime, "volume"));
    }

    Y_UNIT_TEST(ShouldAssignVolumes)
    {
        TTestEnv env;
        auto& runtime = env.GetRuntime();
        auto config = CreateStorageConfig();
        SetupTestEnv(env, config);

        CreateVolumeViaModifySchemeDeprecated(runtime, config, "old-volume");
        CreateVolumeViaModifyScheme(runtime, config, "new-volume");

        AssignVolume(runtime, "old-volume", "old-token");
        AssignVolume(runtime, "new-volume", "new-token");

        UNIT_ASSERT_VALUES_EQUAL(
            "old-token",
            DescribeVolumeAndReturnMountToken(runtime, "old-volume"));

        UNIT_ASSERT_VALUES_EQUAL(
            "new-token",
            DescribeVolumeAndReturnMountToken(runtime, "new-volume"));
    }

    Y_UNIT_TEST(ShouldAssignToOldVolumeInCaseOfNameCollision)
    {
        TTestEnv env;
        auto& runtime = env.GetRuntime();
        auto config = CreateStorageConfig();
        SetupTestEnv(env, config);

        CreateVolumeViaModifySchemeDeprecated(runtime, config, "volume");
        CreateVolumeViaModifyScheme(runtime, config, "volume");

        UNIT_ASSERT_VALUES_EQUAL(
            "",
            DescribeVolumeAndReturnMountToken(runtime, "volume"));

        AssignVolume(runtime, "volume", "token");

        UNIT_ASSERT_VALUES_EQUAL(
            "token",
            DescribeVolumeAndReturnMountToken(runtime, "volume"));
    }

    Y_UNIT_TEST(ShouldDestroyVolumes)
    {
        TTestEnv env;
        auto& runtime = env.GetRuntime();
        auto config = CreateStorageConfig();
        SetupTestEnv(env, config);

        CreateVolumeViaModifySchemeDeprecated(runtime, config, "old-volume");
        CreateVolumeViaModifyScheme(runtime, config, "new-volume");

        DestroyVolume(runtime, "old-volume");

        DescribeVolumeWithFailure(runtime, "old-volume");

        UNIT_ASSERT_VALUES_EQUAL(
            "/local/nbs/_17A/new-volume",
            DescribeVolumeAndReturnPath(runtime, "new-volume"));

        DestroyVolume(runtime, "new-volume");

        DescribeVolumeWithFailure(runtime, "new-volume");
    }

    Y_UNIT_TEST(ShouldDestroyOldVolumeFirst)
    {
        TTestEnv env;
        auto& runtime = env.GetRuntime();
        auto config = CreateStorageConfig();
        SetupTestEnv(env, config);

        CreateVolumeViaModifySchemeDeprecated(runtime, config, "volume");
        CreateVolumeViaModifyScheme(runtime, config, "volume");

        DestroyVolume(runtime, "volume");

        UNIT_ASSERT_VALUES_EQUAL(
            "/local/nbs/_355/volume",
            DescribeVolumeAndReturnPath(runtime, "volume"));

        DestroyVolume(runtime, "volume");

        DescribeVolumeWithFailure(runtime, "volume");
    }

    Y_UNIT_TEST(ShouldCreateVolume)
    {
        TTestEnv env;
        SetupTestEnv(env);
        auto& runtime = env.GetRuntime();

        CreateVolume(runtime, "volume");

        UNIT_ASSERT_VALUES_EQUAL(
            "/local/nbs/_355/volume",
            DescribeVolumeAndReturnPath(runtime, "volume"));
    }

    Y_UNIT_TEST(ShouldCreateVolumeIdempotent)
    {
        TTestEnv env;
        SetupTestEnv(env);
        auto& runtime = env.GetRuntime();

        CreateVolume(runtime, "volume");
        CreateVolume(runtime, "volume");

        DestroyVolume(runtime, "volume");
        DescribeVolumeWithFailure(runtime, "volume");
    }

    Y_UNIT_TEST(ShouldCreateVolumeIdempotentWhenVolumeAtDeprecatedPathExists)
    {
        TTestEnv env;
        auto& runtime = env.GetRuntime();
        auto config = CreateStorageConfig();
        SetupTestEnv(env, config);

        CreateVolumeViaModifySchemeDeprecated(runtime, config, "volume");
        CreateVolume(runtime, "volume");

        DestroyVolume(runtime, "volume");
        DescribeVolumeWithFailure(runtime, "volume");
    }

    Y_UNIT_TEST(ShouldCreateVolumeRaceIdempotent)
    {
        TTestEnv env;
        SetupTestEnv(env);
        auto& runtime = env.GetRuntime();

        TActorId sender = runtime.AllocateEdgeActor();

        SendCreateVolumeRequest(runtime, sender, "volume");
        SendCreateVolumeRequest(runtime, sender, "volume");

        TAutoPtr<IEventHandle> handle1;
        auto response1 =
            runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvCreateVolumeResponse>(
                handle1);

        TAutoPtr<IEventHandle> handle2;
        auto response2 =
            runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvCreateVolumeResponse>(
                handle2);

        UNIT_ASSERT_C(
            SUCCEEDED(response1->GetStatus()), response1->GetErrorReason());
        UNIT_ASSERT_C(
            SUCCEEDED(response2->GetStatus()), response2->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldCreateVolumeRaceFailure)
    {
        TTestEnv env;
        SetupTestEnv(env);
        auto& runtime = env.GetRuntime();

        TActorId sender = runtime.AllocateEdgeActor();

        SendCreateVolumeRequest(
            runtime,
            sender,
            "volume",
            1);  // blockSize
        SendCreateVolumeRequest(
            runtime,
            sender,
            "volume",
            2);  // blockSize

        TAutoPtr<IEventHandle> handle1;
        auto response1 =
            runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvCreateVolumeResponse>(
                handle1);

        TAutoPtr<IEventHandle> handle2;
        auto response2 =
            runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvCreateVolumeResponse>(
                handle2);

        if (FAILED(response1->GetStatus())) {
            DoSwap(response1, response2);
        }

        UNIT_ASSERT_C(SUCCEEDED(response1->GetStatus()), response1->GetErrorReason());
        UNIT_ASSERT_C(FAILED(response2->GetStatus()), response2->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldCreateVolumeEvenWhenSmthButNotVolumeExistsAtDeprecatedPath)
    {
        TTestEnv env;
        SetupTestEnv(env);
        auto& runtime = env.GetRuntime();

        MkDir(env.GetRuntime(), "/local/nbs/volume");
        CreateVolume(runtime, "volume");
    }

    Y_UNIT_TEST(ShouldFailVolumeCreationWhenVolumeWithDifferentConfigExistsAtDeprecatedPath)
    {
        TTestEnv env;
        auto& runtime = env.GetRuntime();
        auto config = CreateStorageConfig();
        SetupTestEnv(env, config);

        const ui32 blockSize = 1;
        CreateVolumeViaModifySchemeDeprecated(
            runtime, config, "volume", blockSize);
        CreateVolumeWithFailure(runtime, "volume", blockSize + 1);

        DestroyVolume(runtime, "volume");
        DescribeVolumeWithFailure(runtime, "volume");
    }

    Y_UNIT_TEST(ShouldFailVolumeCreationIfDescribeSchemeFails)
    {
        TTestEnv env;
        SetupTestEnv(env);
        auto& runtime = env.GetRuntime();

        auto error = MakeError(E_FAIL, "Error");

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeSchemeRequest: {
                        auto response =
                            std::make_unique<TEvSSProxy::TEvDescribeSchemeResponse>(
                                error);
                        Send(
                            runtime,
                            event->Sender,
                            event->Recipient,
                            std::move(response));
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TActorId sender = runtime.AllocateEdgeActor();
        SendCreateVolumeRequest(runtime, sender, "volume");

        TAutoPtr<IEventHandle> handle;
        auto response =
            runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvCreateVolumeResponse>(
                handle);

        UNIT_ASSERT(response->GetStatus() == error.GetCode());
        UNIT_ASSERT(response->GetErrorReason() == error.GetMessage());
    }

    Y_UNIT_TEST(ShouldFailVolumeCreationIfDescribeSchemeReturnsWrongVolumeConfig)
    {
        TTestEnv env;
        SetupTestEnv(env);
        auto& runtime = env.GetRuntime();

        const ui32 blockSize = 1;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeSchemeResponse: {
                        auto* msg =
                            event->Get<TEvSSProxy::TEvDescribeSchemeResponse>();
                        if (FAILED(msg->GetError().GetCode())) {
                            break;
                        }
                        auto& pathDescription =
                            const_cast<NKikimrSchemeOp::TPathDescription&>(
                                msg->PathDescription);

                        auto& volumeDescription =
                            *pathDescription.MutableBlockStoreVolumeDescription();
                        auto& volumeConfig =
                            *volumeDescription.MutableVolumeConfig();
                        volumeConfig.SetBlockSize(blockSize + 1);
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        CreateVolumeWithFailure(runtime, "volume", blockSize);
    }

    Y_UNIT_TEST(ShouldFailVolumeCreationIfDescribeSchemeReturnsWrongPathType)
    {
        TTestEnv env;
        SetupTestEnv(env);
        auto& runtime = env.GetRuntime();

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeSchemeResponse: {
                        auto* msg =
                            event->Get<TEvSSProxy::TEvDescribeSchemeResponse>();
                        if (FAILED(msg->GetError().GetCode())) {
                            break;
                        }
                        auto& pathDescription =
                            const_cast<NKikimrSchemeOp::TPathDescription&>(
                                msg->PathDescription);

                        auto& self = *pathDescription.MutableSelf();
                        self.SetPathType(NKikimrSchemeOp::EPathTypeDir);
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        CreateVolumeWithFailure(runtime, "volume");
    }

    Y_UNIT_TEST(ShouldFailVolumeCreationIfDescribeVolumeAfterCreateReturnsStatusPathDoesNotExist)
    {
        TTestEnv env;
        SetupTestEnv(env);
        auto& runtime = env.GetRuntime();

        const auto notFound = MakeSchemeShardError(
            NKikimrScheme::StatusPathDoesNotExist,
            "path does not exist");

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case NKikimr::NSchemeShard::TEvSchemeShard::EvDescribeSchemeResult: {
                        auto record = event->Get<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>()->MutableRecord();
                        auto path = record->GetPathDescription().GetSelf().GetName();
                        if (path == "volume") {
                            record->SetStatus(NKikimrScheme::StatusPathDoesNotExist);
                        }
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TActorId sender = runtime.AllocateEdgeActor();

        SendCreateVolumeRequest(runtime, sender, "volume", 4_KB);

        TAutoPtr<IEventHandle> handle;
        auto response =
            runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvCreateVolumeResponse>(
                handle);
        auto error = response->GetError();
        UNIT_ASSERT_VALUES_EQUAL_C(
            notFound.GetCode(),
            error.GetCode(),
            error.GetMessage());
        UNIT_ASSERT_C(
            HasProtoFlag(error.GetFlags(), NProto::EF_SILENT),
            error.GetMessage());
    }

    Y_UNIT_TEST(ShouldFailDecsribeVolumeIfSSTimesout)
    {
        TTestEnv env;
        SetupTestEnv(env);
        auto& runtime = env.GetRuntime();

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvTxUserProxy::EvNavigate: {
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TActorId sender = runtime.AllocateEdgeActor();

        Send(
            runtime,
            MakeSSProxyServiceId(),
            sender,
            std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>("vol0"));

        // wait for background operations completion
        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        runtime.AdvanceCurrentTime(TDuration::Seconds(20));

        TAutoPtr<IEventHandle> handle;
        auto response =
            runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvDescribeVolumeResponse>(
                handle,
                TDuration::Seconds(1));

        UNIT_ASSERT_C(!Succeeded(response), GetErrorReason(response));

        UNIT_ASSERT_VALUES_EQUAL_C(
            E_TIMEOUT,
            response->GetStatus(),
            response->GetErrorReason()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "DescribeVolume timeout for volume: \"/local/nbs/vol0\"",
            response->GetErrorReason()
        );
    }

    Y_UNIT_TEST(ShouldRetryDescribeRequestIfWrongVolumeInfo)
    {
        TTestEnv env;
        auto& runtime = env.GetRuntime();
        auto config = CreateStorageConfig();
        SetupTestEnv(env, config);

        CreateVolumeViaModifySchemeDeprecated(runtime, config, "old-volume");

        auto error = MakeError(E_TIMEOUT, "DescribeVolume timeout for volume");

        ui32 count = 0;
        TActorId senderId;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvTxUserProxy::EvNavigate: {
                        senderId = event->Sender;
                        break;
                    }
                    case TEvSchemeShard::EvDescribeSchemeResult: {
                        auto* msg =
                            event->Get<TEvSchemeShard::TEvDescribeSchemeResult>();
                        if (event->Recipient != senderId || count != 0) {
                            break;
                        }
                        ++count;
                        auto* record = msg->MutableRecord();
                        const auto error =
                            MakeSchemeShardError(record->GetStatus(), record->GetReason());
                        if (FAILED(error.GetCode())) {
                            break;
                        }
                        auto* pathDescription =
                                record->MutablePathDescription();
                        auto* volumeDescription =
                            pathDescription->MutableBlockStoreVolumeDescription();
                        auto* volumeConfig = volumeDescription->MutableVolumeConfig();
                        volumeConfig->ClearPartitions();
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TActorId sender = runtime.AllocateEdgeActor();

        Send(
            runtime,
            MakeSSProxyServiceId(),
            sender,
            std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>("old-volume"));

        // wait for background operations completion
        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        runtime.AdvanceCurrentTime(TDuration::Seconds(20));

        TAutoPtr<IEventHandle> handle;
        auto response =
            runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvDescribeVolumeResponse>(handle);

        UNIT_ASSERT_C(Succeeded(response), GetErrorReason(response));

        const auto& pathDescription = response->PathDescription;

        UNIT_ASSERT_EQUAL(
            NKikimrSchemeOp::EPathTypeBlockStoreVolume,
            pathDescription.GetSelf().GetPathType());

        const auto& volumeDescription =
            pathDescription.GetBlockStoreVolumeDescription();
        const auto& volumeConfig = volumeDescription.GetVolumeConfig();

        UNIT_ASSERT_VALUES_EQUAL("old-volume", volumeConfig.GetDiskId());
        UNIT_ASSERT(count == 1);
    }

    Y_UNIT_TEST(ShouldReturnERejectedIfIfSchemeShardDetectsPathIdVersionMismatch)
    {
        TTestEnv env;
        auto& runtime = env.GetRuntime();
        auto config = CreateStorageConfig();
        SetupTestEnv(env, config);

        CreateVolumeViaModifySchemeDeprecated(runtime, config, "old-volume");

        TActorId sender = runtime.AllocateEdgeActor();

        Send(
            runtime,
            MakeSSProxyServiceId(),
            sender,
            std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>("old-volume"));

        // wait for background operations completion
        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        TAutoPtr<IEventHandle> handle;
        auto response =
            runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvDescribeVolumeResponse>(handle);

        UNIT_ASSERT_C(Succeeded(response), GetErrorReason(response));

        const auto& pathDescription = response->PathDescription;

        UNIT_ASSERT_EQUAL(
            NKikimrSchemeOp::EPathTypeBlockStoreVolume,
            pathDescription.GetSelf().GetPathType());

        const auto& pathId = pathDescription.GetSelf().GetPathId();
        const auto& pathVersion = pathDescription.GetSelf().GetPathVersion();

        TString volumeDir;
        TString volumeName;

        {
            TStringBuf dir;
            TStringBuf name;
            TStringBuf(response->Path).RSplit('/', dir, name);
            volumeDir = TString{dir};
            volumeName = TString{name};
        }

        NKikimrBlockStore::TVolumeConfig volumeConfig;
        volumeConfig.SetCloudId("cloud");

        NKikimrSchemeOp::TModifyScheme modifyScheme;
        modifyScheme.SetWorkingDir(volumeDir);
        modifyScheme.SetOperationType(
            NKikimrSchemeOp::ESchemeOpAlterBlockStoreVolume);

        auto* op = modifyScheme.MutableAlterBlockStoreVolume();
        op->SetName(volumeName);

        op->MutableVolumeConfig()->CopyFrom(volumeConfig);
        auto* applyIf = modifyScheme.MutableApplyIf()->Add();
        applyIf->SetPathId(pathId);
        applyIf->SetPathVersion(pathVersion + 1);

        Send(
            runtime,
            MakeSSProxyServiceId(),
            sender,
            std::make_unique<TEvSSProxy::TEvModifySchemeRequest>(std::move(modifyScheme)));

        // wait for background operations completion
        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        TAutoPtr<IEventHandle> modifyHandle;
        auto modifyResponse =
            runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvModifySchemeResponse>(modifyHandle);

        UNIT_ASSERT_C(FAILED(modifyResponse->GetStatus()), GetErrorReason(modifyResponse));
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, modifyResponse->GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldReturnConcurrentModificationErrorIfSchemeShardDetectsWrongVersion)
    {
        TTestEnv env;
        auto& runtime = env.GetRuntime();
        auto config = CreateStorageConfig();
        SetupTestEnv(env, config);

        CreateVolumeViaModifySchemeDeprecated(runtime, config, "old-volume");

        TActorId sender = runtime.AllocateEdgeActor();

        Send(
            runtime,
            MakeSSProxyServiceId(),
            sender,
            std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>("old-volume"));

        // wait for background operations completion
        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        TAutoPtr<IEventHandle> handle;
        auto response =
            runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvDescribeVolumeResponse>(handle);

        UNIT_ASSERT_C(Succeeded(response), GetErrorReason(response));

        const auto& pathDescription = response->PathDescription;

        UNIT_ASSERT_EQUAL(
            NKikimrSchemeOp::EPathTypeBlockStoreVolume,
            pathDescription.GetSelf().GetPathType());

        const auto& pathId = pathDescription.GetSelf().GetPathId();
        const auto& pathVersion = pathDescription.GetSelf().GetPathVersion();

        TString volumeDir;
        TString volumeName;

        {
            TStringBuf dir;
            TStringBuf name;
            TStringBuf(response->Path).RSplit('/', dir, name);
            volumeDir = TString{dir};
            volumeName = TString{name};
        }

        NKikimrBlockStore::TVolumeConfig volumeConfig;
        volumeConfig.SetCloudId("cloud");
        volumeConfig.SetVersion(10);

        NKikimrSchemeOp::TModifyScheme modifyScheme;
        modifyScheme.SetWorkingDir(volumeDir);
        modifyScheme.SetOperationType(
            NKikimrSchemeOp::ESchemeOpAlterBlockStoreVolume);

        auto* op = modifyScheme.MutableAlterBlockStoreVolume();
        op->SetName(volumeName);

        op->MutableVolumeConfig()->CopyFrom(volumeConfig);
        auto* applyIf = modifyScheme.MutableApplyIf()->Add();
        applyIf->SetPathId(pathId);
        applyIf->SetPathVersion(pathVersion + 1);

        Send(
            runtime,
            MakeSSProxyServiceId(),
            sender,
            std::make_unique<TEvSSProxy::TEvModifySchemeRequest>(std::move(modifyScheme)));

        // wait for background operations completion
        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        TAutoPtr<IEventHandle> modifyHandle;
        auto modifyResponse =
            runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvModifySchemeResponse>(modifyHandle);

        UNIT_ASSERT_C(FAILED(modifyResponse->GetStatus()), GetErrorReason(modifyResponse));
        UNIT_ASSERT_VALUES_EQUAL(E_ABORTED, modifyResponse->GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldReturnERejectedWhenSSisUnavailable)
    {
        TTestEnv env;
        auto& runtime = env.GetRuntime();
        auto config = CreateStorageConfig();
        SetupTestEnv(env, config);

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvTxUserProxy::EvNavigate: {
                        auto response =
                            std::make_unique<TEvSchemeShard::TEvDescribeSchemeResult>();
                        auto& rec = *response->MutableRecord();
                        rec.SetStatus(NKikimrScheme::StatusNotAvailable);
                        rec.SetReason("ss is gone");
                        Send(
                            runtime,
                            event->Sender,
                            event->Recipient,
                            std::move(response));
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    case TEvTxUserProxy::EvProposeTransaction: {
                        auto response =
                            std::make_unique<TEvTxUserProxy::TEvProposeTransactionStatus>(
                                TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError);
                        auto& rec = response->Record;
                        rec.SetSchemeShardStatus(NKikimrScheme::StatusNotAvailable);
                        rec.SetSchemeShardReason("ss is gone");
                        Send(
                            runtime,
                            event->Sender,
                            event->Recipient,
                            std::move(response));
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });


        TActorId sender = runtime.AllocateEdgeActor();

        {
            Send(
                runtime,
                MakeSSProxyServiceId(),
                sender,
                std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>("volume"));

            TAutoPtr<IEventHandle> describeHandle;
            auto describeResponse =
                runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvDescribeVolumeResponse>(
                    describeHandle);

            UNIT_ASSERT_VALUES_EQUAL(
                describeResponse->GetStatus(),
                E_REJECTED);
            UNIT_ASSERT_VALUES_EQUAL(
                "ss is gone",
                describeResponse->GetError().GetMessage());
        }

        {
            Send(
                runtime,
                MakeSSProxyServiceId(),
                sender,
                std::make_unique<TEvSSProxy::TEvDescribeSchemeRequest>("volume"));

            TAutoPtr<IEventHandle> describeHandle;
            auto describeResponse =
                runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvDescribeSchemeResponse>(
                    describeHandle);

            UNIT_ASSERT_VALUES_EQUAL(
                describeResponse->GetStatus(),
                E_REJECTED);
            UNIT_ASSERT_VALUES_EQUAL(
                "ss is gone",
                describeResponse->GetError().GetMessage());
        }

        {
            Send(
                runtime,
                MakeSSProxyServiceId(),
                sender,
                std::make_unique<TEvSSProxy::TEvModifyVolumeRequest>(
                    TEvSSProxy::TModifyVolumeRequest::EOpType::Destroy,
                    "volume",
                    "",
                    0));

            TAutoPtr<IEventHandle> modifyHandle;
            auto modifyResponse =
                runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvModifyVolumeResponse>(
                    modifyHandle);

            UNIT_ASSERT_VALUES_EQUAL(
                modifyResponse->GetStatus(),
                E_REJECTED);
        }
    }

    Y_UNIT_TEST(ShouldReturnERejectedForKnownTxProxyErrors)
    {
        TTestEnv env;
        auto& runtime = env.GetRuntime();
        auto config = CreateStorageConfig();
        SetupTestEnv(env, config);

        auto txStatus = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyNotReady;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvTxUserProxy::EvProposeTransaction: {
                        auto response =
                            std::make_unique<TEvTxUserProxy::TEvProposeTransactionStatus>(txStatus);
                        Send(
                            runtime,
                            event->Sender,
                            event->Recipient,
                            std::move(response));
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });


        TActorId sender = runtime.AllocateEdgeActor();

        {
            Send(
                runtime,
                MakeSSProxyServiceId(),
                sender,
                std::make_unique<TEvSSProxy::TEvModifyVolumeRequest>(
                    TEvSSProxy::TModifyVolumeRequest::EOpType::Destroy,
                    "volume",
                    "",
                    0));

            TAutoPtr<IEventHandle> modifyHandle;
            auto modifyResponse =
                runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvModifyVolumeResponse>(
                    modifyHandle);

            UNIT_ASSERT_VALUES_EQUAL(
                modifyResponse->GetStatus(),
                E_REJECTED);
        }

        txStatus = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest;

        {
            Send(
                runtime,
                MakeSSProxyServiceId(),
                sender,
                std::make_unique<TEvSSProxy::TEvModifyVolumeRequest>(
                    TEvSSProxy::TModifyVolumeRequest::EOpType::Destroy,
                    "volume",
                    "",
                    0));

            TAutoPtr<IEventHandle> modifyHandle;
            auto modifyResponse =
                runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvModifyVolumeResponse>(
                    modifyHandle);

            UNIT_ASSERT_VALUES_EQUAL(
                modifyResponse->GetStatus(),
                MAKE_TXPROXY_ERROR(txStatus));
        }
    }

    Y_UNIT_TEST(ShouldReturnERejectedIfSchemeShardIsUnavailableForDeprecatedPath)
    {
        TTestEnv env;
        auto& runtime = env.GetRuntime();
        auto config = CreateStorageConfig();
        SetupTestEnv(env, config);
        CreateVolumeViaModifySchemeDeprecated(runtime, config, "volume");

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvTxUserProxy::EvNavigate: {
                        auto request = event->Get<TEvTxUserProxy::TEvNavigate>()->Record;
                        if (request.GetDescribePath().GetPath() == "/local/nbs/volume") {
                            auto response =
                                std::make_unique<TEvSchemeShard::TEvDescribeSchemeResult>();
                            auto& record = *response->MutableRecord();
                            record.SetStatus(NKikimrScheme::StatusNotAvailable);
                            record.SetReason("ss is gone");
                            Send(
                                runtime,
                                event->Sender,
                                event->Recipient,
                                std::move(response));
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TActorId sender = runtime.AllocateEdgeActor();

        {
            Send(
                runtime,
                MakeSSProxyServiceId(),
                sender,
                std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>("volume"));

            TAutoPtr<IEventHandle> handle;
            auto response =
                runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvDescribeVolumeResponse>(
                    handle);

            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(
                "ss is gone",
                response->GetError().GetMessage());
        }
    }

    Y_UNIT_TEST(ShouldReturnERejectedIfVolumeTabletIdIsZero)
    {
        TTestEnv env;
        SetupTestEnv(env);
        auto& runtime = env.GetRuntime();

        CreateVolume(runtime, "vol0", 4096);

        runtime.SetEventFilter([&] (auto& runtime, auto& ev) {
                Y_UNUSED(runtime);
                switch (ev->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeSchemeResponse: {
                        using TEvent = TEvSSProxy::TEvDescribeSchemeResponse;
                        using TDescription = NKikimrSchemeOp::TPathDescription;
                        auto* msg =
                            ev->template Get<TEvent>();
                        TDescription& desc =
                            const_cast<TDescription&>(msg->PathDescription);
                        desc.
                            MutableBlockStoreVolumeDescription()->
                            SetVolumeTabletId(0);
                    }
                }
                return false;
            }
        );

        TActorId sender = runtime.AllocateEdgeActor();

        Send(
            runtime,
            MakeSSProxyServiceId(),
            sender,
            std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>("vol0"));

        TAutoPtr<IEventHandle> handle;
        auto* response =
            runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvDescribeVolumeResponse>(
                handle);

        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
    }

    Y_UNIT_TEST(ShouldReturnERejectedIfAnyOfPartitionIdsIsZero)
    {
        TTestEnv env;
        SetupTestEnv(env);
        auto& runtime = env.GetRuntime();

        CreateVolume(runtime, "vol0", 4096);

        runtime.SetEventFilter([&] (auto& runtime, auto& ev) {
                Y_UNUSED(runtime);
                switch (ev->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeSchemeResponse: {
                        using TEvent = TEvSSProxy::TEvDescribeSchemeResponse;
                        using TDescription = NKikimrSchemeOp::TPathDescription;
                        auto* msg =
                            ev->template Get<TEvent>();
                        auto& desc =
                            const_cast<TDescription&>(msg->PathDescription);
                        if (FAILED(msg->GetStatus()) ||
                            desc.GetSelf().GetPathType() !=
                            NKikimrSchemeOp::EPathTypeBlockStoreVolume)
                        {
                            break;
                        }
                        desc.
                            MutableBlockStoreVolumeDescription()->
                            MutablePartitions()->at(0).SetTabletId(0);
                    }
                }
                return false;
            }
        );

        TActorId sender = runtime.AllocateEdgeActor();

        Send(
            runtime,
            MakeSSProxyServiceId(),
            sender,
            std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>("vol0"));

        TAutoPtr<IEventHandle> handle;
        auto* response =
            runtime.GrabEdgeEventRethrow<TEvSSProxy::TEvDescribeVolumeResponse>(
                handle);

        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
