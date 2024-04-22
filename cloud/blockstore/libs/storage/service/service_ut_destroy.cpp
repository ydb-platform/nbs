#include "service_ut.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceDestroyTest)
{
    Y_UNIT_TEST(ShouldDestroyOverlayDiskIfBaseDiskIsDestroyed)
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
}

}   // namespace NCloud::NBlockStore::NStorage
