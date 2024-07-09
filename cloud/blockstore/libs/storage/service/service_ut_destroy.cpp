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
                UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
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
}

}   // namespace NCloud::NBlockStore::NStorage
