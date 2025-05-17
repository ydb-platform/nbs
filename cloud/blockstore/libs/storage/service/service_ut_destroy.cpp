#include "service_ut.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>

#include <chrono>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace std::chrono_literals;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceDestroyTest)
{
    Y_UNIT_TEST(ShouldBeAbleToDestroyOverlayDiskIfBaseDiskIsAlreadyDestroyed)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

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

            service.SendDestroyVolumeRequest("baseDisk");
            {
                auto response = service.RecvDestroyVolumeResponse();
                UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            }

            service.SendDescribeVolumeRequest("baseDisk");
            {
                auto response = service.RecvDescribeVolumeResponse();
                UNIT_ASSERT(FACILITY_FROM_CODE(response->GetStatus()) == FACILITY_SCHEMESHARD);
            }

            service.SendDestroyVolumeRequest("vol0");
            {
                auto response = service.RecvDestroyVolumeResponse();
                UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            }
        }
    }

    void CreateSimpleSsdDisk(TServiceClient& service, const TString& diskId)
    {
        service.CreateVolume(
            diskId,
            2_GB / DefaultBlockSize,
            DefaultBlockSize,
            "",         // folderId
            "",    // cloudId
            NCloud::NProto::STORAGE_MEDIA_SSD,
            NProto::TVolumePerformanceProfile(),
            TString(),  // placementGroupId
            0,          // placementPartitionIndex
            0,          // partitionsCount
            NProto::TEncryptionSpec()
        );
    }

    Y_UNIT_TEST(ShouldDestroyAnyDiskByDefault)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);

        {
            CreateSimpleSsdDisk(service, "without_prefix");
            auto response = service.DescribeVolume("without_prefix");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            CreateSimpleSsdDisk(service, "with_prefix");
            response = service.DescribeVolume("with_prefix");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            service.SendDestroyVolumeRequest("without_prefix");
            {
                auto response = service.RecvDestroyVolumeResponse();
                UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            }

            service.SendDestroyVolumeRequest("with_prefix");
            {
                auto response = service.RecvDestroyVolumeResponse();
                UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            }
        }
    }

    Y_UNIT_TEST(ShouldOnlyDestroyDisksWithSpecificDiskIdPrefixes)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        auto* prefixes = config.MutableDestructionAllowedOnlyForDisksWithIdPrefixes();
        prefixes->Add("with_prefix");
        prefixes->Add("with_another_prefix");
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);

        {
            CreateSimpleSsdDisk(service, "without_prefix");
            auto response = service.DescribeVolume("without_prefix");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            CreateSimpleSsdDisk(service, "with_prefix");
            response = service.DescribeVolume("with_prefix");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            CreateSimpleSsdDisk(service, "with_another_prefix");
            response = service.DescribeVolume("with_another_prefix");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            service.SendDestroyVolumeRequest("without_prefix");
            {
                auto response = service.RecvDestroyVolumeResponse();
                UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
            }

            service.SendDestroyVolumeRequest("with_prefix");
            {
                auto response = service.RecvDestroyVolumeResponse();
                UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            }

            service.SendDestroyVolumeRequest("with_another_prefix");
            {
                auto response = service.RecvDestroyVolumeResponse();
                UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            }
        }
    }

    Y_UNIT_TEST(ShouldDestroyVolumeWithSync)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetAllocationUnitNonReplicatedSSD(1);
        ui32 nodeIdx = SetupTestEnv(env, config);

        TServiceClient service(env.GetRuntime(), nodeIdx);

        service.CreateVolume(
            DefaultDiskId,
            1_GB / DefaultBlockSize,
            DefaultBlockSize,
            "", // folderId
            "", // cloudId
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED
        );

        TVector<NProto::TDeallocateDiskRequest> requests;

        bool syncDealloc = false;

        auto& runtime = env.GetRuntime();
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvDiskRegistry::EvDeallocateDiskRequest)
                {
                    auto* msg =
                        event->Get<TEvDiskRegistry::TEvDeallocateDiskRequest>();
                    if (msg->Record.GetDiskId() == DefaultDiskId) {
                        syncDealloc = msg->Record.GetSync();
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        {
            auto response = service.DestroyVolume(
                DefaultDiskId,
                false,   // destroyIfBroken
                true     // sync
            );
            UNIT_ASSERT(syncDealloc);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetError());
        }

        // Destroy a non-existent disk

        syncDealloc = false;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvService::EvStatVolumeResponse: {
                        auto* msg =
                            event->Get<TEvService::TEvStatVolumeResponse>();
                        *msg->Record.MutableError() =
                            MakeError(MAKE_SCHEMESHARD_ERROR(
                                NKikimrScheme::StatusPathDoesNotExist));
                        break;
                    }

                    case TEvDiskRegistry::EvDeallocateDiskRequest: {
                        auto* msg = event->Get<
                            TEvDiskRegistry::TEvDeallocateDiskRequest>();
                        if (msg->Record.GetDiskId() == DefaultDiskId) {
                            syncDealloc = msg->Record.GetSync();
                        }
                        break;
                    }

                    case TEvDiskRegistry::EvDeallocateDiskResponse: {
                        auto* msg = event->Get<
                            TEvDiskRegistry::TEvDeallocateDiskResponse>();
                        *msg->Record.MutableError() = MakeError(S_ALREADY);
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        {
            auto response = service.DestroyVolume(
                DefaultDiskId,
                false,   // destroyIfBroken
                true     // sync
            );
            UNIT_ASSERT(syncDealloc);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_ALREADY,
                response->GetStatus(),
                response->GetError());
            UNIT_ASSERT_VALUES_EQUAL(
                "volume not found",
                response->GetErrorReason());
        }
    }

    Y_UNIT_TEST(ShouldDestroyNonreplVolumeAfterSSError)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetAllocationUnitNonReplicatedSSD(1);
        ui32 nodeIdx = SetupTestEnv(env, config);

        env.GetRuntime().SetLogPriority(
            TBlockStoreComponents::VOLUME,
            NLog::PRI_DEBUG);

        env.GetRuntime().SetLogPriority(
            TBlockStoreComponents::SERVICE,
            NLog::PRI_DEBUG);

        TServiceClient service(env.GetRuntime(), nodeIdx);

        service.CreateVolume(
            DefaultDiskId,
            1_GB / DefaultBlockSize,
            DefaultBlockSize,
            "", // folderId
            "", // cloudId
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED
        );

        {
            auto response = service.StatVolume(DefaultDiskId);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetError());

            UNIT_ASSERT_EQUAL(
                NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                response->Record.GetVolume().GetStorageMediaKind());
        }

        auto prevEventFilterFunc = env.GetRuntime().SetEventFilter(
            [nodeIdx](auto& runtime, auto& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvModifyVolumeRequest: {
                        auto response = std::make_unique<
                            TEvSSProxy::TEvModifyVolumeResponse>(
                            MakeError(E_REJECTED));
                        runtime.Send(
                            new IEventHandle(
                                event->Sender,
                                event->Recipient,
                                response.release(),
                                0,   // flags
                                event->Cookie),
                            nodeIdx);
                        return true;
                    }
                }

                return false;
            });

        {
            service.SendDestroyVolumeRequest(
                DefaultDiskId,
                false,   // destroyIfBroken
                false    // sync
            );

            auto response = service.RecvDestroyVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                response->GetError());
        }

        env.GetRuntime().SetEventFilter(prevEventFilterFunc);

        {
            service.SendDestroyVolumeRequest(
                DefaultDiskId,
                false,   // destroyIfBroken
                false     // sync
            );

            auto response = service.RecvDestroyVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetError());
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
