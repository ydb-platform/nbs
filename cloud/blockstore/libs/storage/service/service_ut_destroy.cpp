#include "service_ut.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceDestroyTest)
{
    void CreateSsdDisk(TServiceClient& service, const TString& diskId)
    {
        service.CreateVolume(
            diskId,
            2_GB / DefaultBlockSize,
            DefaultBlockSize,
            "", // folderId
            "", // cloudId
            NCloud::NProto::STORAGE_MEDIA_SSD,
            NProto::TVolumePerformanceProfile(),
            TString(),  // placementGroupId
            0,          // placementPartitionIndex
            0,          // partitionsCount
            NProto::TEncryptionSpec()
        );
    }

    void CreateSsdSystemDisk(TServiceClient& service, const TString& diskId)
    {
        service.CreateVolume(
            diskId,
            2_GB / DefaultBlockSize,
            DefaultBlockSize,
            TString(),  // folderId
            TString(),  // cloudId
            NCloud::NProto::STORAGE_MEDIA_SSD,
            NProto::TVolumePerformanceProfile(),
            TString(),  // placementGroupId
            0,  // placementPartitionIndex
            0,  // partitionsCount
            NProto::TEncryptionSpec(),
            true // isSystem
        );
    }

    void CreateSsdOverlayDisk(
        TServiceClient& service,
        const TString& diskId,
        const TString& baseDiskId,
        const TString& baseDiskCheckpointId)
    {
        service.CreateVolume(
            diskId,
            2_GB / DefaultBlockSize,
            DefaultBlockSize,
            TString(),  // folderId
            TString(),  // cloudId
            NCloud::NProto::STORAGE_MEDIA_SSD,
            NProto::TVolumePerformanceProfile(),
            TString(),  // placementGroupId
            0,  // placementPartitionIndex
            0,  // partitionsCount
            NProto::TEncryptionSpec(),
            false,  // isSystem
            baseDiskId,
            baseDiskCheckpointId,
            0  // fillGeneration
        );
    }

    Y_UNIT_TEST(ShouldBeAbleToDestroyOverlayDiskIfBaseDiskIsAlreadyDestroyed)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);

        {
            CreateSsdDisk(service, "baseDisk");
            auto response = service.DescribeVolume("baseDisk");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            CreateSsdOverlayDisk(service, "vol0", "baseDisk", "baseDiskCheckpointId");
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

    Y_UNIT_TEST(ShouldDestroyAnyDiskByDefault)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);

        {
            CreateSsdDisk(service, "without_prefix");
            auto response = service.DescribeVolume("without_prefix");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            CreateSsdDisk(service, "with_prefix");
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

        {
            // should be able to destroy system disks
            CreateSsdSystemDisk(service, "system_without_prefix");
            auto response = service.DescribeVolume("system_without_prefix");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            CreateSsdSystemDisk(service, "system_with_prefix");
            response = service.DescribeVolume("system_with_prefix");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            service.SendDestroyVolumeRequest("system_without_prefix");
            {
                auto response = service.RecvDestroyVolumeResponse();
                UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            }

            service.SendDestroyVolumeRequest("system_with_prefix");
            {
                auto response = service.RecvDestroyVolumeResponse();
                UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            }
        }
    }

    Y_UNIT_TEST(ShouldOnlyDestroyDisksWithSpecificDiskIdPrefix)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetDestructionAllowedOnlyForDisksWithIdPrefix("with_prefix");
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);

        {
            CreateSsdDisk(service, "without_prefix");
            auto response = service.DescribeVolume("without_prefix");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            CreateSsdDisk(service, "with_prefix");
            response = service.DescribeVolume("with_prefix");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            service.SendDestroyVolumeRequest("without_prefix");
            {
                auto response = service.RecvDestroyVolumeResponse();
                UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
            }

            service.SendDestroyVolumeRequest("with_prefix");
            {
                auto response = service.RecvDestroyVolumeResponse();
                UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            }
        }

        {
            // should be able to destroy system disks
            CreateSsdSystemDisk(service, "system_without_prefix");
            auto response = service.DescribeVolume("system_without_prefix");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            CreateSsdSystemDisk(service, "system_with_prefix");
            response = service.DescribeVolume("system_with_prefix");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            service.SendDestroyVolumeRequest("system_without_prefix");
            {
                auto response = service.RecvDestroyVolumeResponse();
                UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            }

            service.SendDestroyVolumeRequest("system_with_prefix");
            {
                auto response = service.RecvDestroyVolumeResponse();
                UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            }
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
