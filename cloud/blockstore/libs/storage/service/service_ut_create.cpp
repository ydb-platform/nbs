#include "service_ut.h"

#include <cloud/blockstore/libs/encryption/encryption_test.h>
#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/testlib/disk_registry_proxy_mock.h>
#include "cloud/blockstore/private/api/protos/volume.pb.h"

#include <cloud/storage/core/libs/common/helpers.h>

#include <contrib/ydb/core/tx/schemeshard/schemeshard.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceCreateVolumeTest)
{
    Y_UNIT_TEST(ShouldCreateVolume)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume();

        {
            auto response = service.DescribeVolume();
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                response->Record.GetVolume().GetTabletVersion()
            );
            UNIT_ASSERT(response->Record.GetVolume().GetCreationTs() > 0);
        }

        service.DestroyVolume();
    }

    Y_UNIT_TEST(ShouldReturnErrorIfWaitReadyFailsAfterCreation)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        ui32 status = E_FAIL;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvWaitReadyResponse: {
                        auto response = std::make_unique<TEvVolume::TEvWaitReadyResponse>(
                            MakeError(status, "waitready failed")
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

        service.SendCreateVolumeRequest();
        auto response = service.RecvCreateVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_FAIL, response->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            "waitready failed",
            response->GetErrorReason());

        // retry should fail as well
        service.SendCreateVolumeRequest();
        response = service.RecvCreateVolumeResponse();
        auto error = response->GetError();
        UNIT_ASSERT_VALUES_EQUAL(E_FAIL, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL("waitready failed", error.GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(0, error.GetFlags());
    }

    Y_UNIT_TEST(ShouldReturnSilentErrorIfDescribeFailsWithPathNotFoundAfterCreation)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        const auto notFoundStatus = NKikimrScheme::StatusPathDoesNotExist;
        bool describeRequested = false;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case NSchemeShard::TEvSchemeShard::EvDescribeSchemeResult: {
                        auto *record = event->Get<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>()->MutableRecord();
                        auto path = record->GetPathDescription().GetSelf().GetName();
                        if (path == "path_to_test_volume" && describeRequested) {
                            record->SetStatus(notFoundStatus);
                            describeRequested = false;
                        }
                    }
                    case TEvSSProxy::EvDescribeVolumeRequest: {
                        describeRequested = true;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        // but notFoundStatus should be treated in a slightly different way
        service.SendCreateVolumeRequest();
        auto response = service.RecvCreateVolumeResponse();
        auto error = response->GetError();
        UNIT_ASSERT_VALUES_EQUAL_C(
            MAKE_SCHEMESHARD_ERROR(notFoundStatus),
            error.GetCode(),
            error.GetMessage());
        ui32 flags = 0;
        SetProtoFlag(flags, NProto::EF_SILENT);
        UNIT_ASSERT_VALUES_EQUAL_C(flags, error.GetFlags(), error.GetMessage());
    }

    Y_UNIT_TEST(ShouldCreateVolumeWithDefaultTabletVersion)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetDefaultTabletVersion(2);
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume();

        {
            auto response = service.DescribeVolume();
            UNIT_ASSERT_VALUES_EQUAL(
                2,
                response->Record.GetVolume().GetTabletVersion()
            );
        }

        service.DestroyVolume();
    }

    Y_UNIT_TEST(ShouldValidateVolumeSize)
    {
        TTestEnvState state;
        for (ui32 i = 0; i < 512 * 3; ++i) {
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
        config.SetAllocationUnitMirror2SSD(1024);
        config.SetAllocationUnitMirror3SSD(1024);
        config.SetAllocationUnitNonReplicatedHDD(1024);
        ui32 nodeIdx = SetupTestEnv(env, config);

        TServiceClient service(env.GetRuntime(), nodeIdx);

        const auto mediaKinds = {
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            NProto::STORAGE_MEDIA_SSD_MIRROR2,
            NProto::STORAGE_MEDIA_SSD_MIRROR3,
            NProto::STORAGE_MEDIA_HDD_NONREPLICATED,
        };

        for (const auto mediaKind: mediaKinds) {
            const auto diskId = TStringBuilder() << DefaultDiskId
                << "-" << static_cast<int>(mediaKind);

            {
                auto request = service.CreateCreateVolumeRequest(diskId, 0);
                service.SendRequest(MakeStorageServiceId(), std::move(request));

                auto response = service.RecvCreateVolumeResponse();
                UNIT_ASSERT_VALUES_EQUAL_C(
                    E_ARGUMENT,
                    response->GetStatus(),
                    response->GetErrorReason()
                );
            }

            {
                auto request = service.CreateCreateVolumeRequest(
                    diskId,
                    MaxVolumeBlocksCount + 1
                );
                service.SendRequest(MakeStorageServiceId(), std::move(request));

                auto response = service.RecvCreateVolumeResponse();
                UNIT_ASSERT_VALUES_EQUAL_C(
                    E_ARGUMENT,
                    response->GetStatus(),
                    response->GetErrorReason()
                );
            }

            {
                auto request = service.CreateCreateVolumeRequest(
                    diskId,
                    1000_GB / DefaultBlockSize,
                    DefaultBlockSize,
                    "", // folderId
                    "", // cloudId
                    mediaKind
                );
                service.SendRequest(MakeStorageServiceId(), std::move(request));

                auto response = service.RecvCreateVolumeResponse();
                UNIT_ASSERT_VALUES_EQUAL_C(
                    E_ARGUMENT,
                    response->GetStatus(),
                    response->GetErrorReason()
                );
            }

            {
                auto request = service.CreateCreateVolumeRequest(
                    diskId,
                    1_TB / DefaultBlockSize,
                    DefaultBlockSize,
                    "", // folderId
                    "", // cloudId
                    mediaKind
                );
                service.SendRequest(MakeStorageServiceId(), std::move(request));

                auto response = service.RecvCreateVolumeResponse();
                UNIT_ASSERT_VALUES_EQUAL_C(
                    S_OK,
                    response->GetStatus(),
                    response->GetErrorReason()
                );
            }

            {
                auto request = service.CreateCreateVolumeRequest(
                    diskId + "2",
                    256_TB / DefaultBlockSize,
                    DefaultBlockSize,
                    "", // folderId
                    "", // cloudId
                    mediaKind
                );
                service.SendRequest(MakeStorageServiceId(), std::move(request));

                auto response = service.RecvCreateVolumeResponse();
                UNIT_ASSERT_VALUES_EQUAL_C(
                    S_OK,
                    response->GetStatus(),
                    response->GetErrorReason()
                );
            }

            {
                auto request = service.CreateCreateVolumeRequest(
                    diskId + "3",
                    257_TB / DefaultBlockSize,
                    DefaultBlockSize,
                    "", // folderId
                    "", // cloudId
                    mediaKind
                );
                service.SendRequest(MakeStorageServiceId(), std::move(request));

                auto response = service.RecvCreateVolumeResponse();
                UNIT_ASSERT_VALUES_EQUAL_C(
                    E_ARGUMENT,
                    response->GetStatus(),
                    response->GetErrorReason()
                );
            }
        }
    }

    Y_UNIT_TEST(ShouldAllowBigMultipartitionVolumes)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetBytesPerPartition(512_GB);
        config.SetMultipartitionVolumesEnabled(true);
        config.SetMaxPartitionsPerVolume(16);
        ui32 nodeIdx = SetupTestEnv(env, config);

        TServiceClient service(env.GetRuntime(), nodeIdx);

        {
            auto request = service.CreateCreateVolumeRequest(
                DefaultDiskId,
                128_TB / DefaultBlockSize
            );
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvCreateVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        {
            auto request = service.CreateCreateVolumeRequest(
                DefaultDiskId + "2",
                128_TB / DefaultBlockSize + 1
            );
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvCreateVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldValidateBlockSize)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);

        {
            // too small
            auto request = service.CreateCreateVolumeRequest(
                DefaultDiskId,
                DefaultBlocksCount,
                2_KB
            );
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvCreateVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
        }

        {
            // too big
            auto request = service.CreateCreateVolumeRequest(
                DefaultDiskId,
                DefaultBlocksCount,
                256_KB
            );
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvCreateVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
        }

        {
            // not a power of 2
            auto request = service.CreateCreateVolumeRequest(
                DefaultDiskId,
                DefaultBlocksCount,
                50000
            );
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvCreateVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
        }

        {
            auto request = service.CreateCreateVolumeRequest(
                DefaultDiskId,
                DefaultBlocksCount,
                64_KB
            );
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvCreateVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        {
            auto request = service.CreateCreateVolumeRequest(
                "nrd0",
                93_GB / 512,
                512,
                {}, // folderId
                {}, // cloudId
                NProto::STORAGE_MEDIA_SSD_NONREPLICATED
            );
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvCreateVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
        }

        {
            auto request = service.CreateCreateVolumeRequest(
                "local0",
                93_GB / 512,
                512,
                {}, // folderId
                {}, // cloudId
                NProto::STORAGE_MEDIA_SSD_LOCAL
            );
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvCreateVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        {
            auto request = service.CreateCreateVolumeRequest(
                "hdd_nrd0",
                93_GB / 512,
                512,
                {}, // folderId
                {}, // cloudId
                NProto::STORAGE_MEDIA_HDD_NONREPLICATED
            );
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvCreateVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldCreateVolumeWithAutoComputedChannelsCount)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);

        bool detectedCreateVolumeRequest = false;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvModifySchemeRequest: {
                        auto* msg = event->Get<TEvSSProxy::TEvModifySchemeRequest>();
                        if (msg->ModifyScheme.GetOperationType() ==
                            NKikimrSchemeOp::ESchemeOpCreateBlockStoreVolume)
                        {
                            detectedCreateVolumeRequest = true;
                            const auto& createRequest = msg->ModifyScheme.GetCreateBlockStoreVolume();
                            const auto& volumeConfig = createRequest.GetVolumeConfig();
                            UNIT_ASSERT(volumeConfig.ExplicitChannelProfilesSize() > 4);
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.CreateVolume();
        UNIT_ASSERT(detectedCreateVolumeRequest);
    }

    Y_UNIT_TEST(ShouldSetStoragePoolNameForNonReplicatedHddDisks)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);

        bool detectedCreateVolumeRequest = false;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvModifySchemeRequest: {
                        auto* msg =
                            event->Get<TEvSSProxy::TEvModifySchemeRequest>();
                        if (msg->ModifyScheme.GetOperationType() ==
                            NKikimrSchemeOp::ESchemeOpCreateBlockStoreVolume)
                        {
                            detectedCreateVolumeRequest = true;
                            const auto& createRequest =
                                msg->ModifyScheme.GetCreateBlockStoreVolume();
                            const auto& volumeConfig =
                                createRequest.GetVolumeConfig();
                            UNIT_ASSERT_VALUES_EQUAL(
                                "rot",
                                volumeConfig.GetStoragePoolName());
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.CreateVolume(
            "hdd_nrd0",
            93_GB / DefaultBlockSize,
            DefaultBlockSize,
            "", // folderId
            "", // cloudId
            NProto::STORAGE_MEDIA_HDD_NONREPLICATED);
        UNIT_ASSERT(detectedCreateVolumeRequest);
    }

    Y_UNIT_TEST(ShouldCreateNonReplicatedHddInsteadOfReplicatedIfFeatureIsEnabled)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        NProto::TFeaturesConfig featuresConfig;
        auto* feature = featuresConfig.AddFeatures();
        feature->SetName("UseNonReplicatedHDDInsteadOfReplicated");
        auto* whitelist = feature->MutableWhitelist();
        *whitelist->AddCloudIds() = "cloud_id";
        ui32 nodeIdx = SetupTestEnv(env, config, featuresConfig);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume(
            "hdd_nrd0",
            93_GB / DefaultBlockSize,
            DefaultBlockSize,
            "", // folderId
            "cloud_id");

        {
            auto response = service.DescribeVolume("hdd_nrd0");
            const auto& volume = response->Record.GetVolume();
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<int>(NProto::STORAGE_MEDIA_HDD_NONREPLICATED),
                static_cast<int>(volume.GetStorageMediaKind()));
            UNIT_ASSERT_VALUES_EQUAL(
                93_GB / DefaultDeviceBlockCount / DefaultDeviceBlockSize,
                volume.GetDevices().size());
        }
    }

    Y_UNIT_TEST(ShouldCreateVolumeWithMultiplePartitions)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetBytesPerPartition(2_GB);
        config.SetBytesPerPartitionSSD(1_GB);
        config.SetMultipartitionVolumesEnabled(true);
        config.SetBytesPerStripe(16_MB);
        config.SetMaxPartitionsPerVolume(2);
        ui32 nodeIdx = SetupTestEnv(env, config);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume("vol0", 3'000'000);
        {
            auto response = service.DescribeVolume("vol0");
            UNIT_ASSERT_VALUES_EQUAL(
                2,
                response->Record.GetVolume().GetPartitionsCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                3'006'464,
                response->Record.GetVolume().GetBlocksCount()
            );
        }

        service.CreateVolume("vol1", 2_GB / DefaultBlockSize);
        {
            auto response = service.DescribeVolume("vol1");
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                response->Record.GetVolume().GetPartitionsCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                2_GB / DefaultBlockSize,
                response->Record.GetVolume().GetBlocksCount()
            );
        }

        service.CreateVolume(
            "vol2",
            2_GB / DefaultBlockSize,
            DefaultBlockSize,
            "", // folderId
            "", // cloudId
            NCloud::NProto::STORAGE_MEDIA_SSD
        );

        {
            auto response = service.DescribeVolume("vol2");
            UNIT_ASSERT_VALUES_EQUAL(
                2,
                response->Record.GetVolume().GetPartitionsCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                2_GB / DefaultBlockSize,
                response->Record.GetVolume().GetBlocksCount()
            );
        }
    }

    Y_UNIT_TEST(ShouldCreateVolumeWithOnePartitionForSystemAndOverlayDisks)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetBytesPerPartition(2_GB);
        config.SetBytesPerPartitionSSD(1_GB);
        config.SetMultipartitionVolumesEnabled(true);
        config.SetBytesPerStripe(16_MB);
        config.SetMaxPartitionsPerVolume(2);
        ui32 nodeIdx = SetupTestEnv(env, config);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);

        {
            service.CreateVolume(
                "vol0",
                2_GB / DefaultBlockSize,
                DefaultBlockSize,
                TString(),  // folderId
                TString(),  // cloudId
                NCloud::NProto::STORAGE_MEDIA_SSD,
                NProto::TVolumePerformanceProfile(),
                TString(),  // placementGroupId
                0,          // placementPartitionIndex
                0,  // partitionsCount
                NProto::TEncryptionSpec()
            );

            auto response = service.DescribeVolume("vol0");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(2, response->Record.GetVolume().GetPartitionsCount());
        }
        {
            service.CreateVolume(
                "vol1",
                2_GB / DefaultBlockSize,
                DefaultBlockSize,
                TString(),  // folderId
                TString(),  // cloudId
                NCloud::NProto::STORAGE_MEDIA_SSD,
                NProto::TVolumePerformanceProfile(),
                TString(),  // placementGroupId
                0,          // placementPartitionIndex
                0,  // partitionsCount
                NProto::TEncryptionSpec(),
                true  // isSystem
            );

            auto response = service.DescribeVolume("vol1");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(1, response->Record.GetVolume().GetPartitionsCount());
        }
        {
            service.CreateVolume(
                "baseDisk",
                2_GB / DefaultBlockSize,
                DefaultBlockSize,
                "", // folderId
                "", // cloudId
                NCloud::NProto::STORAGE_MEDIA_SSD,
                NProto::TVolumePerformanceProfile(),
                TString(),  // placementGroupId
                0,          // placementPartitionIndex
                0,  // partitionsCount
                NProto::TEncryptionSpec(),
                true  // isSystem
            );

            service.CreateVolume(
                "vol2",
                2_GB / DefaultBlockSize,
                DefaultBlockSize,
                TString(),  // folderId
                TString(),  // cloudId
                NCloud::NProto::STORAGE_MEDIA_SSD,
                NProto::TVolumePerformanceProfile(),
                TString(),  // placementGroupId
                0,          // placementPartitionIndex
                0,  // partitionsCount
                NProto::TEncryptionSpec(),
                false,  // isSystem
                "baseDisk",
                "baseDiskCheckpointId"
            );

            auto response = service.DescribeVolume("vol2");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(1, response->Record.GetVolume().GetPartitionsCount());
        }
    }

    Y_UNIT_TEST(ShouldCreateSystemVolumeWithMultipartitionedBaseDisk)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetBytesPerPartition(2_GB);
        config.SetBytesPerPartitionSSD(1_GB);
        config.SetMultipartitionVolumesEnabled(true);
        config.SetBytesPerStripe(16_MB);
        config.SetMaxPartitionsPerVolume(2);
        ui32 nodeIdx = SetupTestEnv(env, config);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);

        {
            service.CreateVolume(
                "baseDisk",
                2_GB / DefaultBlockSize,
                DefaultBlockSize,
                "", // folderId
                "", // cloudId
                NCloud::NProto::STORAGE_MEDIA_SSD,
                NProto::TVolumePerformanceProfile(),
                TString(),  // placementGroupId
                0,          // placementPartitionIndex
                0,  // partitionsCount
                NProto::TEncryptionSpec()
            );

            auto response = service.DescribeVolume("baseDisk");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(2, response->Record.GetVolume().GetPartitionsCount());

            service.CreateVolume(
                "vol0",
                2_GB / DefaultBlockSize,
                DefaultBlockSize,
                TString(),  // folderId
                TString(),  // cloudId
                NCloud::NProto::STORAGE_MEDIA_SSD,
                NProto::TVolumePerformanceProfile(),
                TString(),  // placementGroupId
                0,          // placementPartitionIndex
                0,  // partitionsCount
                NProto::TEncryptionSpec(),
                true,  // isSystem
                "baseDisk",
                "baseDiskCheckpointId",
                0  // fillGeneration
            );

            response = service.DescribeVolume("vol0");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(1, response->Record.GetVolume().GetPartitionsCount());
        }
    }

    Y_UNIT_TEST(ShouldCreateEncryptedVolume)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);

        NProto::TEncryptionSpec encryptionSpec;
        encryptionSpec.SetMode(NProto::ENCRYPTION_AES_XTS);

        {
            service.CreateVolume(
                "vol0",
                2_GB / DefaultBlockSize,
                DefaultBlockSize,
                TString(),  // folderId
                TString(),  // cloudId
                NCloud::NProto::STORAGE_MEDIA_SSD,
                NProto::TVolumePerformanceProfile(),
                TString(),  // placementGroupId
                0,          // placementPartitionIndex
                0,  // partitionsCount
                encryptionSpec
            );

            auto response = service.DescribeVolume("vol0");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            auto desc = response->Record.GetVolume().GetEncryptionDesc();
            UNIT_ASSERT(NProto::ENCRYPTION_AES_XTS == desc.GetMode());
            UNIT_ASSERT_VALUES_EQUAL("", desc.GetKeyHash());
        }

        encryptionSpec.SetKeyHash("keyhash");
        {
            service.CreateVolume(
                "vol1",
                2_GB / DefaultBlockSize,
                DefaultBlockSize,
                TString(),  // folderId
                TString(),  // cloudId
                NCloud::NProto::STORAGE_MEDIA_SSD,
                NProto::TVolumePerformanceProfile(),
                TString(),  // placementGroupId
                0,          // placementPartitionIndex
                0,  // partitionsCount
                encryptionSpec
            );

            auto response = service.DescribeVolume("vol1");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            auto desc = response->Record.GetVolume().GetEncryptionDesc();
            UNIT_ASSERT(NProto::ENCRYPTION_AES_XTS == desc.GetMode());
            UNIT_ASSERT_VALUES_EQUAL("keyhash", desc.GetKeyHash());
        }

        encryptionSpec.MutableKeyPath()->SetFilePath("some correct path");
        {
            service.SendCreateVolumeRequest(
                "vol2",
                2_GB / DefaultBlockSize,
                DefaultBlockSize,
                TString(),  // folderId
                TString(),  // cloudId
                NCloud::NProto::STORAGE_MEDIA_SSD,
                NProto::TVolumePerformanceProfile(),
                TString(),  // placementGroupId
                0,          // placementPartitionIndex
                0,  // partitionsCount
                encryptionSpec
            );

            auto response = service.RecvCreateVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                response->GetStatus(),
                response->GetErrorReason()
            );
            UNIT_ASSERT_C(
                response->GetErrorReason().Contains("KeyPath not supported"),
                response->GetErrorReason()
            );
        }
    }

    Y_UNIT_TEST(ShouldNotCreateVolumeWithBaseDiskAndMultiplePartitions)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetBytesPerPartition(2_GB);
        config.SetBytesPerPartitionSSD(1_GB);
        config.SetMultipartitionVolumesEnabled(true);
        config.SetBytesPerStripe(16_MB);
        ui32 nodeIdx = SetupTestEnv(env, config);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        {
            service.SendCreateVolumeRequest(
                DefaultDiskId,
                DefaultBlocksCount,
                DefaultBlockSize,
                TString(),  // folderId
                TString(),  // cloudId
                NCloud::NProto::STORAGE_MEDIA_DEFAULT,
                NProto::TVolumePerformanceProfile(),
                TString(),  // placementGroupId
                0,          // placementPartitionIndex
                2,  // partitionsCount
                NProto::TEncryptionSpec(),
                true  // isSystem
            );

            auto response = service.RecvCreateVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
        }

        {
            service.SendCreateVolumeRequest(
                DefaultDiskId,
                DefaultBlocksCount,
                DefaultBlockSize,
                TString(),  // folderId
                TString(),  // cloudId
                NCloud::NProto::STORAGE_MEDIA_DEFAULT,
                NProto::TVolumePerformanceProfile(),
                TString(),  // placementGroupId
                0,          // placementPartitionIndex
                2,  // partitionsCount
                NProto::TEncryptionSpec(),
                false,  // isSystem
                "baseDiskId",
                "baseDiskCheckpointId"
            );

            auto response = service.RecvCreateVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldCreateVolumeWithMultiplePartitionsIfFeatureIsEnabledForCloud)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetBytesPerPartition(2_GB);
        config.SetBytesPerStripe(16_MB);
        NProto::TFeaturesConfig featuresConfig;
        auto* feature = featuresConfig.AddFeatures();
        feature->SetName("MultipartitionVolumes");
        auto* whitelist = feature->MutableWhitelist();
        *whitelist->AddCloudIds() = "cloud_id";
        ui32 nodeIdx = SetupTestEnv(env, config, featuresConfig);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume(
            DefaultDiskId,
            3'000'000,
            DefaultBlockSize,
            TString(),  // folderId
            "cloud_id"
        );

        {
            auto response = service.DescribeVolume();
            UNIT_ASSERT_VALUES_EQUAL(
                2,
                response->Record.GetVolume().GetPartitionsCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                3006464,
                response->Record.GetVolume().GetBlocksCount()
            );
        }
    }

    Y_UNIT_TEST(ShouldCreateVolumeWithFreshChannelIfFeatureIsEnabledForCloud)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        NProto::TFeaturesConfig featuresConfig;
        auto* feature = featuresConfig.AddFeatures();
        feature->SetName("AllocateFreshChannel");
        auto* whitelist = feature->MutableWhitelist();
        *whitelist->AddCloudIds() = "cloud_id";
        ui32 nodeIdx = SetupTestEnv(env, config, featuresConfig);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume(
            DefaultDiskId,
            1024,
            DefaultBlockSize,
            TString(),  // folderId
            "cloud_id"
        );

        {
            auto response = service.DescribeVolume();
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                response->Record.GetVolume().GetFreshChannelsCount()
            );
        }
    }

    Y_UNIT_TEST(ShouldCreateVolumeWithPartitionsCountSpecifiedInRequest)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetBytesPerPartition(999_TB);
        config.SetMultipartitionVolumesEnabled(true);
        config.SetBytesPerStripe(16_MB);
        ui32 nodeIdx = SetupTestEnv(env, config);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume(
            DefaultDiskId,
            3'000'000,
            DefaultBlockSize,
            TString(),  // folderId
            TString(),  // cloudId
            NCloud::NProto::STORAGE_MEDIA_DEFAULT,
            NProto::TVolumePerformanceProfile(),
            TString(),  // placementGroupId
            0,          // placementPartitionIndex
            2  // partitionsCount
        );

        {
            auto response = service.DescribeVolume();
            UNIT_ASSERT_VALUES_EQUAL(
                2,
                response->Record.GetVolume().GetPartitionsCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(
                3006464,
                response->Record.GetVolume().GetBlocksCount()
            );
        }
    }

    Y_UNIT_TEST(ShouldValidatePartitionsCountInRequest)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetBytesPerPartition(999_TB);
        config.SetMultipartitionVolumesEnabled(true);
        config.SetBytesPerStripe(16_MB);
        ui32 nodeIdx = SetupTestEnv(env, config);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);

        service.SendCreateVolumeRequest(
            DefaultDiskId,
            3'000'000,
            DefaultBlockSize,
            TString(),  // folderId
            TString(),  // cloudId
            NCloud::NProto::STORAGE_MEDIA_DEFAULT,
            NProto::TVolumePerformanceProfile(),
            TString(),  // placementGroupId
            0,          // placementPartitionIndex
            17  // partitionsCount
        );

        {
            auto response = service.RecvCreateVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldPassParamsToCreateVolume)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);

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

        service.CreateVolume(
            DefaultDiskId,
            512,
            8_KB,
            "test_folder",
            "test_cloud",
            NCloud::NProto::STORAGE_MEDIA_DEFAULT,
            pp
        );
        service.MountVolume();

        service.SendStatVolumeRequest();

        auto response = service.RecvStatVolumeResponse();
        UNIT_ASSERT_C(
            SUCCEEDED(response->GetStatus()), response->GetErrorReason());
        UNIT_ASSERT_VALUES_EQUAL(
            512, response->Record.GetVolume().GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            8_KB, response->Record.GetVolume().GetBlockSize());
        UNIT_ASSERT_VALUES_EQUAL(
            "test_folder", response->Record.GetVolume().GetFolderId());
        UNIT_ASSERT_VALUES_EQUAL(
            "test_cloud", response->Record.GetVolume().GetCloudId());

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

    Y_UNIT_TEST(ShouldDestroyVolumeIdempotent)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume();
        service.DestroyVolume();
        service.DestroyVolume();
    }

    Y_UNIT_TEST(ShouldDestroyVolumeRaceIdempotent)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume();
        service.SendDestroyVolumeRequest();
        service.SendDestroyVolumeRequest();
        auto response1 = service.RecvDestroyVolumeResponse();
        auto response2 = service.RecvDestroyVolumeResponse();

        UNIT_ASSERT_C(SUCCEEDED(response1->GetStatus()), response1->GetErrorReason());
        UNIT_ASSERT_C(SUCCEEDED(response2->GetStatus()), response2->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldNotReturnEmptyErrorReasonInCaseOfError)
    {
        TTestEnv env(1, 2);
        ui32 nodeIdx1 = SetupTestEnv(env);
        ui32 nodeIdx2 = SetupTestEnv(env);

        {
            TServiceClient service1(env.GetRuntime(), nodeIdx1);
            service1.CreateVolume(DefaultDiskId + "1");
            service1.DestroyVolume();

            TServiceClient service2(env.GetRuntime(), nodeIdx2);
            service2.SendAssignVolumeRequest();

            auto response = service2.RecvAssignVolumeResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
            UNIT_ASSERT(response->GetErrorReason());
        }

        {
            TServiceClient service(env.GetRuntime(), nodeIdx2);
            service.CreateVolume(DefaultDiskId + "2");
            service.DestroyVolume();

            service.SendAssignVolumeRequest();

            auto response = service.RecvAssignVolumeResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
            UNIT_ASSERT(response->GetErrorReason());
        }
    }

    Y_UNIT_TEST(ShouldRejectRequestsIfVolumeCannotBeResolved)
    {
        TTestEnv env(1, 3);
        ui32 nodeIdx1 = SetupTestEnv(env);
        ui32 nodeIdx2 = SetupTestEnv(env);
        ui32 nodeIdx3 = SetupTestEnv(env);

        TServiceClient service1(env.GetRuntime(), nodeIdx1);
        service1.CreateVolume();
        env.GetRuntime().DispatchEvents(TDispatchOptions(), TDuration::Seconds(5));

        TServiceClient service2(env.GetRuntime(), nodeIdx2);
        service2.DestroyVolume();

        // Note: nodeIdx2 creates a volume client while destroying the volume,
        // so even though volume is no longer in the schema request may succeed
        // since tablet is not guaranteed to be dead yet.
        // Use nodeIdx3 which guarantees a new resolve request.
        TServiceClient service3(env.GetRuntime(), nodeIdx3);
        service3.SendStatVolumeRequest();
        auto response = service3.RecvStatVolumeResponse();
        UNIT_ASSERT_C(FAILED(response->GetStatus()), response->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldRejectRequestsIfVolumeWasDestroyed)
    {
        TTestEnv env(1, 2);
        ui32 nodeIdx1 = SetupTestEnv(env);
        ui32 nodeIdx2 = SetupTestEnv(env);

        TServiceClient service1(env.GetRuntime(), nodeIdx1);
        service1.CreateVolume();
        env.GetRuntime().DispatchEvents(TDispatchOptions(), TDuration::Seconds(5));

        TServiceClient service2(env.GetRuntime(), nodeIdx2);
        service2.StatVolume();

        bool tabletDeadSeen = false;
        env.GetRuntime().SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvTablet::EvTabletDead: {
                        tabletDeadSeen = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service2.DestroyVolume();

        if (!tabletDeadSeen) {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTablet::EvTabletDead);
            env.GetRuntime().DispatchEvents(options);
        }

        service2.SendStatVolumeRequest();

        auto response = service2.RecvStatVolumeResponse();
        UNIT_ASSERT_C(FAILED(response->GetStatus()), response->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldValidateDiskId)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        Cerr << IsAsciiAlnum('0') << Endl;

        service.SendCreateVolumeRequest("");
        auto response = service.RecvCreateVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_ARGUMENT,
            response->GetStatus(),
            response->GetErrorReason());

        service.SendCreateVolumeRequest("xxx$qqqq");
        response = service.RecvCreateVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_ARGUMENT,
            response->GetStatus(),
            response->GetErrorReason());

        service.SendCreateVolumeRequest("a/b");
        response = service.RecvCreateVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_ARGUMENT,
            response->GetStatus(),
            response->GetErrorReason());

        service.SendCreateVolumeRequest("0-1_2@zZ.");
        response = service.RecvCreateVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetStatus(),
            response->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldRejectVolumeCreationWithBadTabletVersion)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);

        {
            auto request = service.CreateCreateVolumeRequest();
            request->Record.SetTabletVersion(MaxSupportedTabletVersion);
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvCreateVolumeResponse();
            UNIT_ASSERT(response->GetStatus() == S_OK);
        }

        {
            auto request = service.CreateCreateVolumeRequest();
            request->Record.SetTabletVersion(MaxSupportedTabletVersion + 1);
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvCreateVolumeResponse();
            UNIT_ASSERT(response->GetStatus() == E_ARGUMENT);
            UNIT_ASSERT(response->GetErrorReason());
        }
    }

    Y_UNIT_TEST(ShouldCreateSsdVolumes)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);

        service.CreateVolume(
            DefaultDiskId,
            DefaultBlocksCount,
            DefaultBlockSize,
            "", // folder id
            "", // cloud id
            NCloud::NProto::STORAGE_MEDIA_SSD);
        service.DestroyVolume();
    }

    Y_UNIT_TEST(ShouldCreateHybridVolumes)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);

        service.CreateVolume(
            DefaultDiskId,
            DefaultBlocksCount,
            DefaultBlockSize,
            "", // folder id
            "", // cloud id
            NCloud::NProto::STORAGE_MEDIA_HYBRID);
        service.DestroyVolume();
    }

    Y_UNIT_TEST(ShouldAllowNonexistentVolumeDestroy)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        runtime.SetObserverFunc( [nodeIdx, &runtime] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvModifySchemeRequest: {
                        auto response = std::make_unique<TEvSSProxy::TEvModifySchemeResponse>(
                            MakeError(
                                NKikimrScheme::StatusPathDoesNotExist,
                                "path does not exist"));
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

        service.SendDestroyVolumeRequest();
        auto response = service.RecvDestroyVolumeResponse();
        UNIT_ASSERT(response->GetStatus() == S_ALREADY);
    }

    Y_UNIT_TEST(ShouldReturnErrorOnFailedDestroyVolumeAttempt)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        auto error = MakeError(NKikimrScheme::StatusAccessDenied, "Access denied");

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvModifyVolumeRequest: {
                        auto response = std::make_unique<TEvSSProxy::TEvModifyVolumeResponse>(
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

        service.SendDestroyVolumeRequest();
        auto response = service.RecvDestroyVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            error.GetCode(),
            response->GetStatus(),
            response->GetErrorReason()
        );
        UNIT_ASSERT_VALUES_EQUAL(error.GetMessage(), response->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldNotifyDiskRegistryUponDiskRegistryVolumeDestruction)
    {
        TTestEnvState state;
        TTestEnv env(1, 1, 4, 1, state);
        NProto::TStorageServiceConfig config;
        config.SetAllocationUnitNonReplicatedSSD(1);
        config.SetAllocationUnitMirror2SSD(1);
        config.SetAllocationUnitMirror3SSD(1);
        config.SetAllocationUnitNonReplicatedHDD(1);
        ui32 nodeIdx = SetupTestEnv(env, config);

        TServiceClient service(env.GetRuntime(), nodeIdx);

        const auto mediaKinds = {
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            NProto::STORAGE_MEDIA_SSD_MIRROR2,
            NProto::STORAGE_MEDIA_SSD_MIRROR3,
            NProto::STORAGE_MEDIA_HDD_NONREPLICATED,
        };

        ui32 markedDisks = 0;

        for (const auto mediaKind: mediaKinds) {
            const auto diskId = TStringBuilder() << DefaultDiskId
                << "-" << static_cast<int>(mediaKind);

            service.CreateVolume(
                diskId,
                1_GB / DefaultBlockSize,
                DefaultBlockSize,
                "", // folderId
                "", // cloudId
                mediaKind
            );

            UNIT_ASSERT_VALUES_EQUAL(
                markedDisks,
                state.DiskRegistryState->DisksMarkedForCleanup.size()
            );

            service.DestroyVolume(diskId);
            ++markedDisks;

            UNIT_ASSERT_VALUES_EQUAL(
                markedDisks,
                state.DiskRegistryState->DisksMarkedForCleanup.size()
            );

            UNIT_ASSERT(
                state.DiskRegistryState->DisksMarkedForCleanup.contains(diskId)
            );
        }
    }

    Y_UNIT_TEST(ShouldAutomaticallyConvertDefaultStorageMediaKindToHdd)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        ui32 createBlockStoreVolumeEventCount = 0;

        std::array<NCloud::NProto::EStorageMediaKind, 4> sentMediaKinds{
            NCloud::NProto::STORAGE_MEDIA_HDD,
            NCloud::NProto::STORAGE_MEDIA_SSD,
            NCloud::NProto::STORAGE_MEDIA_HYBRID,
            NCloud::NProto::STORAGE_MEDIA_DEFAULT
        };

        std::array<NCloud::NProto::EStorageMediaKind, 4> expectedMediaKinds{
            NCloud::NProto::STORAGE_MEDIA_HDD,
            NCloud::NProto::STORAGE_MEDIA_SSD,
            NCloud::NProto::STORAGE_MEDIA_HYBRID,
            NCloud::NProto::STORAGE_MEDIA_HDD
        };

        // Run once before actual test to ensure the dir for the volume exists
        service.CreateVolume();
        service.DestroyVolume();

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvModifySchemeRequest: {
                        auto* msg = event->Get<TEvSSProxy::TEvModifySchemeRequest>();
                        if (msg->ModifyScheme.GetOperationType() ==
                            NKikimrSchemeOp::ESchemeOpCreateBlockStoreVolume)
                        {
                            const auto& createBlockStoreVolume =
                                msg->ModifyScheme.GetCreateBlockStoreVolume();
                            const auto& volumeConfig =
                                createBlockStoreVolume.GetVolumeConfig();
                            UNIT_ASSERT(createBlockStoreVolumeEventCount < 4);
                            UNIT_ASSERT(volumeConfig.GetStorageMediaKind() ==
                                (ui32)expectedMediaKinds[createBlockStoreVolumeEventCount]);
                            ++createBlockStoreVolumeEventCount;
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        for (size_t i = 0; i < sentMediaKinds.size(); ++i) {
            service.CreateVolume(
                DefaultDiskId,
                DefaultBlocksCount,
                DefaultBlockSize,
                TString(),
                TString(),
                sentMediaKinds[i]);
            UNIT_ASSERT(createBlockStoreVolumeEventCount == (i + 1));

            service.DestroyVolume();
        }
    }

    Y_UNIT_TEST(ShouldNotDestroyOnlineDisksWithDestroyIfBrokenRequestFlag)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        NProto::TError error;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvWaitReadyRequest: {
                        auto response = std::make_unique<TEvVolume::TEvWaitReadyResponse>(
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

        service.SendDestroyVolumeRequest(DefaultDiskId, true);
        {
            auto response = service.RecvDestroyVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, response->GetStatus());
        }

        service.DescribeVolume();

        error.SetCode(E_BS_RESOURCE_EXHAUSTED);

        service.SendDestroyVolumeRequest(DefaultDiskId, true);
        {
            auto response = service.RecvDestroyVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        service.SendDescribeVolumeRequest();
        {
            auto response = service.RecvDescribeVolumeResponse();
            UNIT_ASSERT_C(S_OK != response->GetStatus(), response->GetErrorReason());
        }
    }

    Y_UNIT_TEST(ShouldNotDestroyDisksWithInstanceClients)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();
        runtime.AdvanceCurrentTime(TDuration::Hours(1));
        TServiceClient service(runtime, nodeIdx);

        NProto::TVolumeClient client;
        client.SetClientId("c");
        client.SetInstanceId("i");

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvService::EvStatVolumeRequest: {
                        auto response =
                            std::make_unique<TEvService::TEvStatVolumeResponse>();
                        response->Record.MutableClients()->Add()->CopyFrom(client);
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

        service.CreateVolume();

        service.SendDestroyVolumeRequest(DefaultDiskId);
        {
            auto response = service.RecvDestroyVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        service.DescribeVolume();

        client.SetInstanceId("");

        service.SendDestroyVolumeRequest(DefaultDiskId);
        {
            auto response = service.RecvDestroyVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        service.SendDescribeVolumeRequest();
        {
            auto response = service.RecvDescribeVolumeResponse();
            UNIT_ASSERT_C(S_OK != response->GetStatus(), response->GetErrorReason());
        }

        service.CreateVolume();
        client.SetInstanceId("i");
        client.SetDisconnectTimestamp(runtime.GetCurrentTime().MicroSeconds());

        // clients with fresh disconnect timestamps should prevent disk
        // destruction
        service.SendDestroyVolumeRequest(DefaultDiskId);
        {
            auto response = service.RecvDestroyVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        service.DescribeVolume();

        client.SetDisconnectTimestamp(
            (runtime.GetCurrentTime() - TDuration::Seconds(61)).MicroSeconds());

        // clients who disconected a long time ago should not prevent disk
        // destruction
        service.SendDestroyVolumeRequest(DefaultDiskId);
        {
            auto response = service.RecvDestroyVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        service.SendDescribeVolumeRequest();
        {
            auto response = service.RecvDescribeVolumeResponse();
            UNIT_ASSERT_C(S_OK != response->GetStatus(), response->GetErrorReason());
        }
    }

    Y_UNIT_TEST(ShouldAllowTokenVersionInAssignVolume)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(DefaultDiskId);

        service.AssignVolume();

        // Assign volume with token version
        service.AssignVolume(DefaultDiskId, TString(), TString(), 1);

        // Assign volume with wrong token version
        service.SendAssignVolumeRequest(DefaultDiskId, TString(), TString(), 1);
        auto response = service.RecvAssignVolumeResponse();

        UNIT_ASSERT_VALUES_EQUAL(E_ABORTED, response->GetStatus());
    }

    Y_UNIT_TEST(ShouldCreateVolumeWithSettingsForVolumeChannels)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);

        bool detectedCreateVolumeRequest = false;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvModifySchemeRequest: {
                        auto* msg = event->Get<TEvSSProxy::TEvModifySchemeRequest>();
                        if (msg->ModifyScheme.GetOperationType() ==
                            NKikimrSchemeOp::ESchemeOpCreateBlockStoreVolume)
                        {
                            detectedCreateVolumeRequest = true;
                            const auto& createRequest = msg->ModifyScheme.GetCreateBlockStoreVolume();
                            const auto& volumeConfig = createRequest.GetVolumeConfig();
                            UNIT_ASSERT_VALUES_EQUAL(3, volumeConfig.VolumeExplicitChannelProfilesSize());
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.CreateVolume();
        UNIT_ASSERT(detectedCreateVolumeRequest);
    }

    Y_UNIT_TEST(ShoudSaveFillGeneration)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);

        auto request = service.CreateCreateVolumeRequest();
        request->Record.SetFillGeneration(1);
        service.SendRequest(MakeStorageServiceId(), std::move(request));

        auto response = service.RecvCreateVolumeResponse();
        UNIT_ASSERT_C(response->GetStatus() == S_OK, response->GetErrorReason());

        auto volumeConfig = GetVolumeConfig(service, DefaultDiskId);
        UNIT_ASSERT_VALUES_EQUAL(1, volumeConfig.GetFillGeneration());
    }

    Y_UNIT_TEST(ShouldCheckFillGenerationIdempotence)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);

        for (int i = 0; i < 2; i++) {
            auto request = service.CreateCreateVolumeRequest();
            request->Record.SetFillGeneration(3);
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvCreateVolumeResponse();
            UNIT_ASSERT_C(response->GetStatus() == S_OK, response->GetErrorReason());
        }

        {
            auto request = service.CreateCreateVolumeRequest();
            request->Record.SetFillGeneration(2);
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvCreateVolumeResponse();
            UNIT_ASSERT_C(response->GetStatus() == E_INVALID_STATE, response->GetStatus());
        }

        {
            auto request = service.CreateCreateVolumeRequest();
            request->Record.SetFillGeneration(4);
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvCreateVolumeResponse();
            UNIT_ASSERT_C(response->GetStatus() == E_ABORTED, response->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldCheckFillGenerationOnDestroy)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);

        auto createVolume = [&](const TString& diskId) {
            auto request = service.CreateCreateVolumeRequest();
            request->Record.SetDiskId(diskId);
            request->Record.SetFillGeneration(713);
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvCreateVolumeResponse();
            UNIT_ASSERT_C(response->GetStatus() == S_OK, response->GetErrorReason());
        };

        auto failToDestroy = [&] (const TString& diskId, ui64 fillGeneration) {
            service.SendDestroyVolumeRequest(
                diskId,
                false,
                false,
                fillGeneration);
            auto destroyResponse = service.RecvDestroyVolumeResponse();
            // Expect that volume is not destroyed, but request finished without error.
            UNIT_ASSERT_C(destroyResponse->GetStatus() == S_OK, destroyResponse->GetErrorReason());

            auto describeResponse = service.DescribeVolume(diskId);
            UNIT_ASSERT_C(describeResponse->GetStatus() == S_OK, describeResponse->GetErrorReason());
        };

        auto successfullyDestroy = [&] (const TString& diskId, ui64 fillGeneration) {
            auto destroyResponse = service.DestroyVolume(
                diskId,
                false,
                false,
                fillGeneration);
            UNIT_ASSERT_C(destroyResponse->GetStatus() == S_OK, destroyResponse->GetErrorReason());

            service.SendDescribeVolumeRequest(diskId);
            auto describeResponse = service.RecvDescribeVolumeResponse();
            UNIT_ASSERT_C(S_OK != describeResponse->GetStatus(), describeResponse->GetErrorReason());
        };

        createVolume("vol");
        failToDestroy("vol", 1);
        failToDestroy("vol", 712);
        successfullyDestroy("vol", 713);

        createVolume("vol_2");
        successfullyDestroy("vol_2", 714);

        createVolume("vol_3");
        successfullyDestroy("vol_3", 777);

        createVolume("vol_4");
        successfullyDestroy("vol_4", 0);
    }

    Y_UNIT_TEST(ShouldCreateVolumeWithDefaultEncryption)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        NProto::TFeaturesConfig featuresConfig;
        auto* feature = featuresConfig.AddFeatures();
        feature->SetName("EncryptionAtRestForDiskRegistryBasedDisks");
        feature->MutableWhitelist()->AddFolderIds("encrypted-folder");

        ui32 nodeIdx = SetupTestEnv(env, config, featuresConfig);

        TServiceClient service(env.GetRuntime(), nodeIdx);

        {
            service.CreateVolume(
                "vol0",
                93_GB / DefaultBlockSize,
                DefaultBlockSize,
                "encrypted-folder",
                TString(),   // cloudId
                NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                NProto::TVolumePerformanceProfile(),
                TString(),   // placementGroupId
                0,           // placementPartitionIndex
                0,           // partitionsCount
                NProto::TEncryptionSpec{});

            auto response = service.DescribeVolume("vol0");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            auto desc = response->Record.GetVolume().GetEncryptionDesc();
            UNIT_ASSERT_EQUAL(NProto::ENCRYPTION_AT_REST, desc.GetMode());
            UNIT_ASSERT_VALUES_EQUAL("", desc.GetKeyHash());
            UNIT_ASSERT_VALUES_UNEQUAL("", desc.GetEncryptionKey().GetKekId());
            UNIT_ASSERT_VALUES_UNEQUAL(
                "",
                desc.GetEncryptionKey().GetEncryptedDEK());
        }

        {
            service.CreateVolume(
                "baseDisk",
                93_GB / DefaultBlockSize,
                DefaultBlockSize,
                "",   // folderId
                "",   // cloudId
                NCloud::NProto::STORAGE_MEDIA_SSD,
                NProto::TVolumePerformanceProfile(),
                TString(),   // placementGroupId
                0,           // placementPartitionIndex
                0,           // partitionsCount
                NProto::TEncryptionSpec(),
                true   // isSystem
            );

            service.SendCreateVolumeRequest(
                "vol1",
                93_GB / DefaultBlockSize,
                DefaultBlockSize,
                "encrypted-folder",
                TString(),   // cloudId
                NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                NProto::TVolumePerformanceProfile(),
                TString(),   // placementGroupId
                0,           // placementPartitionIndex
                0,           // partitionsCount
                NProto::TEncryptionSpec{},
                false,   // isSystem
                "baseDisk",
                "baseDiskCheckpointId");

            auto response = service.RecvCreateVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_NOT_IMPLEMENTED,
                response->GetStatus(),
                response->GetErrorReason());
            UNIT_ASSERT_C(
                response->GetErrorReason().Contains(
                    "Encrypted overlay disks are not supported"),
                response->GetErrorReason());
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
