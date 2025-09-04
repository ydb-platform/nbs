#include "service_ut.h"

#include <contrib/ydb/core/protos/schemeshard/operations.pb.h>

#include <cloud/blockstore/config/storage.pb.h>

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceAlterTest)
{
    Y_UNIT_TEST(ShouldAlterVolume)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetAllowVersionInModifyScheme(true);
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(
            DefaultDiskId,
            512,
            DefaultBlockSize,
            "test_folder",
            "test_cloud"
        );

        ui64 creationTs = 0;
        {
            auto response = service.MountVolume();
            creationTs = response->Record.GetVolume().GetCreationTs();
            UNIT_ASSERT(creationTs > 0);
            UNIT_ASSERT(response->Record.GetVolume().GetAlterTs() == 0);
        }

        service.AlterVolume(DefaultDiskId, "project", "folder", "cloud");

        env.GetRuntime().DispatchEvents(TDispatchOptions(), TDuration::Seconds(2));

        service.SendStatVolumeRequest();

        auto response = service.RecvStatVolumeResponse();
        UNIT_ASSERT_C(
            SUCCEEDED(response->GetStatus()),
            response->GetErrorReason()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "project",
            response->Record.GetVolume().GetProjectId()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "folder",
            response->Record.GetVolume().GetFolderId()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "cloud",
            response->Record.GetVolume().GetCloudId()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            creationTs,
            response->Record.GetVolume().GetCreationTs()
        );
        UNIT_ASSERT(response->Record.GetVolume().GetAlterTs() > 0);
    }

    Y_UNIT_TEST(ShouldFailAlterVolumeWithRejectedCodeIfSchemeShardFailsWithVersionMismatch)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(
            DefaultDiskId,
            512,
            DefaultBlockSize,
            "test_folder",
            "test_cloud"
        );

        bool letItPass = false;

        env.GetRuntime().SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvModifySchemeRequest: {
                        if (letItPass) {
                            break;
                        }
                        letItPass = true;
                        const auto* original = reinterpret_cast<TEvSSProxy::TEvModifySchemeRequest::TPtr*>(&event);
                        auto modifyScheme = (*original)->Get()->ModifyScheme;
                        modifyScheme.MutableApplyIf()->Mutable(0)->SetPathVersion(0);
                        auto msg = std::make_unique<TEvSSProxy::TEvModifySchemeRequest>(std::move(modifyScheme));
                        env.GetRuntime().Send(
                            new IEventHandle(
                                event->Recipient,
                                event->Sender,
                                msg.release()),
                            nodeIdx);
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.SendAlterVolumeRequest(DefaultDiskId, "project", "folder", "cloud");
        auto response = service.RecvAlterVolumeResponse();
        UNIT_ASSERT_C(FAILED(response->GetStatus()), response->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldResizeVolume)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnvWithAllowVersionInModifyScheme(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(DefaultDiskId, DefaultBlocksCount);
        service.ResizeVolume(DefaultDiskId, DefaultBlocksCount * 2);
        service.DestroyVolume();
    }

    Y_UNIT_TEST(ShouldReturnErrorIfWaitReadyFailsAfterResize)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnvWithAllowVersionInModifyScheme(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(DefaultDiskId, DefaultBlocksCount);

        env.GetRuntime().SetObserverFunc( [nodeIdx, &runtime = env.GetRuntime() ] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvWaitReadyResponse: {
                        auto response = std::make_unique<TEvVolume::TEvWaitReadyResponse>(
                            MakeError(E_FAIL, "waitready failed")
                        );
                        runtime.Send(new IEventHandle(
                            event->Recipient,
                            event->Sender,
                            response.release(),
                            0, // flags
                            event->Cookie
                        ), nodeIdx);
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        service.SendResizeVolumeRequest(DefaultDiskId, DefaultBlocksCount * 2);
        auto response = service.RecvResizeVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_FAIL, response->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL("waitready failed", response->GetErrorReason());

        // retry should fail as well
        service.SendResizeVolumeRequest(DefaultDiskId, DefaultBlocksCount * 2);
        response = service.RecvResizeVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_FAIL, response->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL("waitready failed", response->GetErrorReason());

        service.DestroyVolume();
    }

    Y_UNIT_TEST(ShouldNotResizeVolumeIfNothingChanged)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnvWithAllowVersionInModifyScheme(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(DefaultDiskId, DefaultBlocksCount);

        ui32 resizeRequests = 0;
        env.GetRuntime().SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvModifySchemeRequest: {
                        ++resizeRequests;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.ResizeVolume(DefaultDiskId, DefaultBlocksCount * 2);
        UNIT_ASSERT_VALUES_EQUAL(1, resizeRequests);
        service.ResizeVolume(DefaultDiskId, DefaultBlocksCount * 2);
        UNIT_ASSERT_VALUES_EQUAL(1, resizeRequests);
        service.DestroyVolume();
    }

    void DoTestShouldResizeDiskRegistryVolume(
        const NProto::EStorageMediaKind mediaKind)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetAllocationUnitNonReplicatedSSD(100);
        config.SetAllocationUnitMirror2SSD(100);
        config.SetAllocationUnitMirror3SSD(100);
        config.SetAllowVersionInModifyScheme(true);
        ui32 nodeIdx = SetupTestEnv(env, config);

        TServiceClient service(env.GetRuntime(), nodeIdx);

        service.CreateVolume(
            DefaultDiskId,
            100_GB / DefaultBlockSize,
            DefaultBlockSize,
            "", // folderId
            "", // cloudId
            mediaKind
        );

        {
            auto request = service.CreateResizeVolumeRequest(
                DefaultDiskId,
                150_GB / DefaultBlockSize
            );
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvResizeVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
        }

        {
            auto request = service.CreateResizeVolumeRequest(
                DefaultDiskId,
                200_GB / DefaultBlockSize
            );
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvResizeVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        service.DestroyVolume();
    }

    Y_UNIT_TEST(ShouldResizeNonreplVolume)
    {
        DoTestShouldResizeDiskRegistryVolume(
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
    }

    Y_UNIT_TEST(ShouldResizeMirror2Volume)
    {
        DoTestShouldResizeDiskRegistryVolume(
            NProto::STORAGE_MEDIA_SSD_MIRROR2);
    }

    Y_UNIT_TEST(ShouldResizeMirror3Volume)
    {
        DoTestShouldResizeDiskRegistryVolume(
            NProto::STORAGE_MEDIA_SSD_MIRROR3);
    }

    Y_UNIT_TEST(ShouldValidateVolumeSize)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnvWithAllowVersionInModifyScheme(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(DefaultDiskId, DefaultBlocksCount);
        {
            auto request = service.CreateResizeVolumeRequest(
                DefaultDiskId,
                MaxVolumeBlocksCount + 1
            );
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvResizeVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
        }
        service.DestroyVolume();
    }

    Y_UNIT_TEST(ShouldAllowBiggerMultipartitionVolumes)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetBytesPerPartition(512_GB);
        config.SetMultipartitionVolumesEnabled(true);
        config.SetMaxPartitionsPerVolume(16);
        ui32 nodeIdx = SetupTestEnv(env, config);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(
            DefaultDiskId,
            1_TB / DefaultBlockSize // 2 partitions
        );

        {
            auto request = service.CreateResizeVolumeRequest(
                DefaultDiskId,
                16_TB / DefaultBlockSize
            );
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvResizeVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        {
            auto request = service.CreateResizeVolumeRequest(
                DefaultDiskId,
                32_TB / DefaultBlockSize
            );
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvResizeVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
        }

        service.DestroyVolume();
    }

    Y_UNIT_TEST(ShouldAllowBiggerNonreplicatedVolumes)
    {
        TTestEnvState state;
        for (ui32 i = 0; i < 512; ++i) {
            auto& device = state.DiskRegistryState->Devices.emplace_back();
            device.SetNodeId(0);
            device.SetBlocksCount(1_TB / DefaultBlockSize);
            device.SetDeviceUUID(Sprintf("uuid%u", i));
            device.SetDeviceName(Sprintf("dev%u", i));
            device.SetTransportId(Sprintf("transport%u", i));
            device.MutableRdmaEndpoint()->SetHost(Sprintf("rdma%u", i));
            device.MutableRdmaEndpoint()->SetPort(10020 + i);
            device.SetBlockSize(DefaultBlockSize);
        }

        TTestEnv env(1, 1, 4, 1, state);
        NProto::TStorageServiceConfig config;
        config.SetAllocationUnitNonReplicatedSSD(1024);
        ui32 nodeIdx = SetupTestEnv(env, config);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(
            DefaultDiskId,
            1_TB / DefaultBlockSize,
            DefaultBlockSize,
            "", // folderId
            "", // cloudId
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED
        );

        {
            auto request = service.CreateResizeVolumeRequest(
                DefaultDiskId,
                256_TB / DefaultBlockSize
            );
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvResizeVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        {
            auto request = service.CreateResizeVolumeRequest(
                DefaultDiskId,
                257_TB / DefaultBlockSize
            );
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvResizeVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
        }

        service.DestroyVolume();
    }

    Y_UNIT_TEST(ShouldResizeVolumeWithAutoComputedChannelsCount)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        bool detectedResizeVolumeRequest = false;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvModifySchemeRequest: {
                        detectedResizeVolumeRequest = true;
                        auto* msg = event->Get<TEvSSProxy::TEvModifySchemeRequest>();
                        const auto& alterRequest = msg->ModifyScheme.GetAlterBlockStoreVolume();
                        const auto& volumeConfig = alterRequest.GetVolumeConfig();
                        UNIT_ASSERT(volumeConfig.ExplicitChannelProfilesSize() > 4);
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.ResizeVolume(DefaultDiskId, 2*DefaultBlocksCount);
        UNIT_ASSERT(detectedResizeVolumeRequest);
    }

    Y_UNIT_TEST(ShouldFailAlterVolumeIfDescribeVolumeFails)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        auto error = MakeError(E_ARGUMENT, "Error");

        runtime.SetObserverFunc( [error, nodeIdx, &runtime] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeRequest: {
                        auto response = std::make_unique<TEvSSProxy::TEvDescribeVolumeResponse>(
                            error);
                        runtime.Send(
                            new IEventHandle(
                                event->Sender,
                                event->Recipient,
                                response.release(),
                                0, // flags
                                event->Cookie),
                            nodeIdx);
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.SendAlterVolumeRequest(DefaultDiskId, "project", "folder", "cloud");
        auto response = service.RecvAlterVolumeResponse();
        UNIT_ASSERT(response->GetStatus() == error.GetCode());
        UNIT_ASSERT(response->GetErrorReason() == error.GetMessage());
    }

    Y_UNIT_TEST(ShouldForbidResizeRequestsWithZeroBlocksCount)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        service.SendResizeVolumeRequest(DefaultDiskId, 0);
        auto response = service.RecvResizeVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
    }

    Y_UNIT_TEST(ShouldReturnOriginalErrorIfSchemeShardDetectsSpaceLimitViolation)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        ui32 errCode = MAKE_SCHEMESHARD_ERROR(NKikimrScheme::StatusPreconditionFailed);

        runtime.SetObserverFunc( [nodeIdx, errCode, &runtime] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvModifySchemeRequest: {
                        auto errStr = "New volume space is over a limit";
                        auto response =
                            std::make_unique<TEvSSProxy::TEvModifySchemeResponse>(
                                MakeError(errCode, errStr));
                        runtime.Send(
                            new IEventHandle(
                                event->Sender,
                                event->Recipient,
                                response.release(),
                                0, // flags
                                event->Cookie),
                            nodeIdx);
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.SendAlterVolumeRequest(DefaultDiskId, "project", "folder", "cloud");
        auto response = service.RecvAlterVolumeResponse();
        UNIT_ASSERT(response->GetStatus() == errCode);
    }

    Y_UNIT_TEST(ShouldChangeVolumePerformanceProfile)
    {
        TTestEnv env;
        ui32 nodeIdx;
        {
            NProto::TStorageServiceConfig storageServiceConfig;
            storageServiceConfig.SetThrottlingEnabled(true);
            storageServiceConfig.SetAllowVersionInModifyScheme(true);
            nodeIdx = SetupTestEnv(env, std::move(storageServiceConfig));
        }

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(DefaultDiskId, DefaultBlocksCount);

        NProto::TVolumePerformanceProfile pp;
        pp.SetMaxReadBandwidth(111'000'000);
        pp.SetMaxWriteBandwidth(222'000'000);
        pp.SetMaxReadIops(111);
        pp.SetMaxWriteIops(222);

        pp.SetBurstPercentage(20);
        pp.SetMaxPostponedWeight(333'000'000);

        pp.SetBoostTime(4444);
        pp.SetBoostRefillTime(44444);
        pp.SetBoostPercentage(30);

        pp.SetThrottlingEnabled(true);

        service.ResizeVolume(DefaultDiskId, DefaultBlocksCount * 2, pp);

        env.GetRuntime().DispatchEvents(TDispatchOptions(), TDuration::Seconds(2));

        service.SendStatVolumeRequest();

        auto response = service.RecvStatVolumeResponse();
        UNIT_ASSERT_C(
            SUCCEEDED(response->GetStatus()), response->GetErrorReason());

        const auto& pp2 = response->Record.GetVolume().GetPerformanceProfile();
        UNIT_ASSERT_VALUES_EQUAL(
            pp.GetMaxReadBandwidth(), pp2.GetMaxReadBandwidth());
        UNIT_ASSERT_VALUES_EQUAL(
            pp.GetMaxWriteBandwidth(), pp2.GetMaxWriteBandwidth());
        UNIT_ASSERT_VALUES_EQUAL(
            pp.GetMaxReadIops(), pp2.GetMaxReadIops());
        UNIT_ASSERT_VALUES_EQUAL(
            pp.GetMaxWriteIops(), pp2.GetMaxWriteIops());

        UNIT_ASSERT_VALUES_EQUAL(
            pp.GetBurstPercentage(), pp2.GetBurstPercentage());
        UNIT_ASSERT_VALUES_EQUAL(
            pp.GetMaxPostponedWeight(), pp2.GetMaxPostponedWeight());

        UNIT_ASSERT_VALUES_EQUAL(pp.GetBoostTime(), pp2.GetBoostTime());
        UNIT_ASSERT_VALUES_EQUAL(
            pp.GetBoostRefillTime(), pp2.GetBoostRefillTime());
        UNIT_ASSERT_VALUES_EQUAL(
            pp.GetBoostPercentage(), pp2.GetBoostPercentage());

        UNIT_ASSERT_VALUES_EQUAL(
            pp.GetThrottlingEnabled(), pp2.GetThrottlingEnabled());
    }

    Y_UNIT_TEST(ShouldNotDecreaseChannelsCount)
    {
        TTestEnv env(1, 1, 10);

        auto& runtime = env.GetRuntime();

        env.CreateSubDomain("nbs");
        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetAllocationUnitHDD(1);
        storageServiceConfig.SetFreshChannelCount(1);
        auto storageConfig =
            CreateTestStorageConfig(std::move(storageServiceConfig));
        TControlBoard controlBoard;
        storageConfig->Register(controlBoard);
        ui32 nodeIdx = env.CreateBlockStoreNode(
            "nbs",
            storageConfig,
            CreateTestDiagnosticsConfig()
        );
        TServiceClient service(runtime, nodeIdx);

        const ui32 blockCount =
            10ULL * 1024ULL * 1024ULL * 1024ULL / DefaultBlockSize;

        ui32 originalChannelsCount = 0;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvModifySchemeRequest: {
                        auto* msg = event->Get<TEvSSProxy::TEvModifySchemeRequest>();
                        if (msg->ModifyScheme.GetOperationType() ==
                            NKikimrSchemeOp::ESchemeOpCreateBlockStoreVolume)
                        {
                            const auto& createRequest = msg->ModifyScheme.GetCreateBlockStoreVolume();
                            const auto& volumeConfig = createRequest.GetVolumeConfig();
                            originalChannelsCount = volumeConfig.ExplicitChannelProfilesSize();
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.CreateVolume(DefaultDiskId, blockCount);
        UNIT_ASSERT_VALUES_EQUAL(14, originalChannelsCount);

        ui32 channelsCountOnResize = 0;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvModifySchemeRequest: {
                        auto* msg = event->Get<TEvSSProxy::TEvModifySchemeRequest>();
                        if (msg->ModifyScheme.GetOperationType() ==
                            NKikimrSchemeOp::ESchemeOpAlterBlockStoreVolume)
                        {
                            const auto& alterRequest = msg->ModifyScheme.GetAlterBlockStoreVolume();
                            const auto& volumeConfig = alterRequest.GetVolumeConfig();
                            channelsCountOnResize = volumeConfig.ExplicitChannelProfilesSize();
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TAtomic dummy;
        controlBoard.SetValue("BlockStore_AllocationUnitHDD", 10, dummy);
        UNIT_ASSERT_VALUES_EQUAL(10, storageConfig->GetAllocationUnitHDD());
        service.ResizeVolume(DefaultDiskId, blockCount + 1);
        UNIT_ASSERT(channelsCountOnResize);
        UNIT_ASSERT_VALUES_EQUAL(originalChannelsCount, channelsCountOnResize);
    }

    Y_UNIT_TEST(ShouldFailResizeIfBlocksCountIsDecreased)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        ui32 blocksCount = 1024;
        service.CreateVolume(DefaultDiskId, blocksCount);

        service.SendResizeVolumeRequest(DefaultDiskId, blocksCount - 1);
        auto response = service.RecvResizeVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
    }

    Y_UNIT_TEST(ShouldUpdateVolumeInfoAfterAlter)
    {
        TTestEnv env(1, 1, 10);
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        const ui64 blocksCount = 1024;
        service.CreateVolume(DefaultDiskId, blocksCount);

        {
            auto response = service.MountVolume(DefaultDiskId);
            UNIT_ASSERT(response->Record.GetVolume().GetBlocksCount() == blocksCount);
        }

        ui64 newBlocksCount = blocksCount * 2;
        service.ResizeVolume(DefaultDiskId, newBlocksCount);
        env.GetRuntime().DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        {
            auto response = service.MountVolume(DefaultDiskId);
            UNIT_ASSERT(response->Record.GetVolume().GetBlocksCount() == newBlocksCount);
        }
    }

    Y_UNIT_TEST(ShouldResizeVolumesWithSeparateMixedChannels)
    {
        TTestEnv env(1, 1, 10);
        ui32 nodeIdx;
        {
            NProto::TStorageServiceConfig config;
            config.SetAllocateSeparateMixedChannels(true);
            config.SetAllocationUnitHDD(1);
            config.SetMinChannelCount(4); // being explicit
            nodeIdx = SetupTestEnv(env, std::move(config));
        }

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        const ui64 blocksCount = 4_GB / DefaultBlockSize;
        service.CreateVolume(
            DefaultDiskId,
            blocksCount,
            DefaultBlockSize,
            "",
            "",
            NCloud::NProto::STORAGE_MEDIA_HYBRID
        );

        {
            auto response = service.DescribeVolume(DefaultDiskId);
            UNIT_ASSERT_VALUES_EQUAL(
                4,
                response->Record.GetVolume().GetMergedChannelsCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                response->Record.GetVolume().GetMixedChannelsCount()
            );
        }

        ui64 newBlocksCount = blocksCount * 2;
        service.ResizeVolume(DefaultDiskId, newBlocksCount);
        {
            auto response = service.DescribeVolume(DefaultDiskId);
            UNIT_ASSERT_VALUES_EQUAL(
                8,
                response->Record.GetVolume().GetMergedChannelsCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                response->Record.GetVolume().GetMixedChannelsCount()
            );
        }
    }

    Y_UNIT_TEST(ShouldNotChangePartitionsCountUponResize)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetBytesPerPartition(2_GB);
        config.SetMultipartitionVolumesEnabled(true);
        ui32 nodeIdx = SetupTestEnv(env, config);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume(DefaultDiskId, 1_GB / DefaultBlockSize);
        {
            auto response = service.DescribeVolume();
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                response->Record.GetVolume().GetPartitionsCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                1_GB / DefaultBlockSize,
                response->Record.GetVolume().GetBlocksCount()
            );
        }

        service.ResizeVolume(DefaultDiskId, 3_GB / DefaultBlockSize);
        {
            auto response = service.DescribeVolume();
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                response->Record.GetVolume().GetPartitionsCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                3_GB / DefaultBlockSize,
                response->Record.GetVolume().GetBlocksCount()
            );
        }
    }

    Y_UNIT_TEST(ShouldNotChangePartitionsCountForMultipartitionVolumeUponResize)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetBytesPerPartition(2_GB);
        config.SetMultipartitionVolumesEnabled(true);
        config.SetMaxPartitionsPerVolume(3);
        ui32 nodeIdx = SetupTestEnv(env, config);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume(DefaultDiskId, 3_GB / DefaultBlockSize);
        {
            auto response = service.DescribeVolume();
            UNIT_ASSERT_VALUES_EQUAL(
                2,
                response->Record.GetVolume().GetPartitionsCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                3_GB / DefaultBlockSize,
                response->Record.GetVolume().GetBlocksCount()
            );
        }

        service.ResizeVolume(DefaultDiskId, 10_GB / DefaultBlockSize);
        {
            auto response = service.DescribeVolume();
            UNIT_ASSERT_VALUES_EQUAL(
                2,
                response->Record.GetVolume().GetPartitionsCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                10_GB / DefaultBlockSize,
                response->Record.GetVolume().GetBlocksCount()
            );
        }
    }

    Y_UNIT_TEST(ShouldUpdateVolumeInfoAfterAlterOnAllHosts)
    {
        TTestEnv env(1, 2, 10);
        ui32 nodeIdx1 = SetupTestEnv(env);
        ui32 nodeIdx2 = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service1(runtime, nodeIdx1);
        TServiceClient service2(runtime, nodeIdx2);

        const ui64 blocksCount = 1024;
        service1.CreateVolume(DefaultDiskId, 1024);

        {
            auto response = service1.MountVolume(DefaultDiskId);
            UNIT_ASSERT(response->Record.GetVolume().GetBlocksCount() == blocksCount);
        }

        {
            auto response = service2.MountVolume(
                DefaultDiskId,
                "",
                "",
                NProto::IPC_GRPC,
                NProto::VOLUME_ACCESS_READ_ONLY,
                NProto::VOLUME_MOUNT_REMOTE);
            UNIT_ASSERT(response->Record.GetVolume().GetBlocksCount() == blocksCount);
        }

        {
            auto response = service1.MountVolume(DefaultDiskId);
            UNIT_ASSERT(response->Record.GetVolume().GetBlocksCount() == blocksCount);
        }

        ui64 newBlocksCount = blocksCount * 2;
        service1.ResizeVolume(DefaultDiskId, newBlocksCount);

        env.GetRuntime().DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = service1.MountVolume(DefaultDiskId);
            UNIT_ASSERT(response->Record.GetVolume().GetBlocksCount() == newBlocksCount);
        }

        {
            auto response = service2.MountVolume(
                DefaultDiskId,
                "",
                "",
                NProto::IPC_GRPC,
                NProto::VOLUME_ACCESS_READ_ONLY,
                NProto::VOLUME_MOUNT_REMOTE);
            UNIT_ASSERT(response->Record.GetVolume().GetBlocksCount() == newBlocksCount);
        }
    }

    Y_UNIT_TEST(ShouldAllocateFreshChannelOnAlterIfFeatureIsEnabledForCloud)
    {
        TTestEnv env(1, 2);

        ui32 nodeIdx1;
        ui32 nodeIdx2;
        {
            NProto::TStorageServiceConfig config;
            config.SetFreshChannelCount(0);

            NProto::TFeaturesConfig featuresConfig;
            auto* feature = featuresConfig.AddFeatures();
            feature->SetName("AllocateFreshChannel");
            auto* whitelist = feature->MutableWhitelist();
            *whitelist->AddCloudIds() = "cloud_id";

            nodeIdx1 = SetupTestEnv(env);
            nodeIdx2 = SetupTestEnv(env, config, featuresConfig);
        }

        auto& runtime = env.GetRuntime();

        TServiceClient service1(runtime, nodeIdx1);
        TServiceClient service2(runtime, nodeIdx2);

        constexpr ui32 blockCount = 1024;

        service1.CreateVolume(
            DefaultDiskId,
            blockCount,
            DefaultBlockSize,
            TString(),  // folderId
            "cloud_id"
        );

        {
            auto response = service1.DescribeVolume();
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                response->Record.GetVolume().GetFreshChannelsCount()
            );
        }

        service2.ResizeVolume(DefaultDiskId, blockCount + 1);

        {
            auto response = service2.DescribeVolume();
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                response->Record.GetVolume().GetFreshChannelsCount()
            );
        }
    }

    Y_UNIT_TEST(ShouldAlterVolumeWithEncryptionKeyHash)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetAllowVersionInModifyScheme(true);
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(
            DefaultDiskId,
            512,
            DefaultBlockSize,
            "folder"
        );

        ui64 creationTs = 0;
        {
            auto response = service.MountVolume();
            creationTs = response->Record.GetVolume().GetCreationTs();
            UNIT_ASSERT(creationTs > 0);
            UNIT_ASSERT(response->Record.GetVolume().GetAlterTs() == 0);
        }

        auto encryptionKeyHash = "encryption_key_hash";
        service.AlterVolume(DefaultDiskId, "", "folder", "", encryptionKeyHash);

        env.GetRuntime().DispatchEvents(TDispatchOptions(), TDuration::Seconds(2));

        service.SendStatVolumeRequest();

        auto response = service.RecvStatVolumeResponse();
        UNIT_ASSERT_C(
            SUCCEEDED(response->GetStatus()),
            response->GetErrorReason()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            encryptionKeyHash,
            response->Record.GetVolume().GetEncryptionDesc().GetKeyHash()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            creationTs,
            response->Record.GetVolume().GetCreationTs()
        );
        UNIT_ASSERT(response->Record.GetVolume().GetAlterTs() > 0);

        {
            service.AlterVolume(DefaultDiskId, "", "", "", "");

            env.GetRuntime().DispatchEvents(TDispatchOptions(), TDuration::Seconds(2));

            service.SendStatVolumeRequest();

            auto response = service.RecvStatVolumeResponse();
            UNIT_ASSERT_C(
                SUCCEEDED(response->GetStatus()),
                response->GetErrorReason()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                encryptionKeyHash,
                response->Record.GetVolume().GetEncryptionDesc().GetKeyHash()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                creationTs,
                response->Record.GetVolume().GetCreationTs()
            );
            UNIT_ASSERT(response->Record.GetVolume().GetAlterTs() > 0);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
