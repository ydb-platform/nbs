#include "service_ut.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

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

    void CreateSimpleSsdDisk(
        TServiceClient& service,
        const TString& diskId,
        const TString& cloudId)
    {
        service.CreateVolume(
            diskId,
            2_GB / DefaultBlockSize,
            DefaultBlockSize,
            "",         // folderId
            cloudId,    // cloudId
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
            CreateSimpleSsdDisk(service, "disk_with_cloud_id", "cloud.id");
            auto response = service.DescribeVolume("disk_with_cloud_id");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            CreateSimpleSsdDisk(service, "disk_without_cloud_id", "");
            response = service.DescribeVolume("disk_without_cloud_id");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            service.SendDestroyVolumeRequest("disk_with_cloud_id");
            {
                auto response = service.RecvDestroyVolumeResponse();
                UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            }

            service.SendDestroyVolumeRequest("disk_without_cloud_id");
            {
                auto response = service.RecvDestroyVolumeResponse();
                UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            }
        }
    }

    Y_UNIT_TEST(ShouldNotDestroyDiskWithCloudId)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetDisableDiskWithCloudIdDestruction(true);
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);

        {
            CreateSimpleSsdDisk(service, "disk_with_cloud_id", "cloud.id");
            auto response = service.DescribeVolume("disk_with_cloud_id");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            CreateSimpleSsdDisk(service, "disk_without_cloud_id", "");
            response = service.DescribeVolume("disk_without_cloud_id");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            service.SendDestroyVolumeRequest("disk_with_cloud_id");
            {
                auto response = service.RecvDestroyVolumeResponse();
                UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
            }

            service.SendDestroyVolumeRequest("disk_without_cloud_id");
            {
                auto response = service.RecvDestroyVolumeResponse();
                UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            }
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
