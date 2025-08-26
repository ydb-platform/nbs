#include "volume_ut.h"

#include <cloud/blockstore/libs/common/request_checksum_helpers.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/volume_model.h>
#include <cloud/blockstore/libs/storage/model/composite_id.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/model/processing_blocks.h>
#include <cloud/blockstore/libs/storage/stats_service/stats_service_events_private.h>

#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NBlockStore::NStorage::NPartition;

using namespace NCloud::NStorage;

using namespace NTestVolume;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeChecksumsTest)
{
    void YdbBasedDiskShoudValidateChecksums(int version, int partitionCount)
    {
        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetEnableChecksumValidationForYdbBasedDisks(true);

        auto runtime = PrepareTestActorRuntime(std::move(storageServiceConfig));

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,       // maxBandwidth
            0,       // maxIops
            0,       // burstPercentage
            0,       // maxPostponedWeight
            false,   // throttlingEnabled
            version,
            NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_SSD,
            2048,       // block count per partition
            "vol0",     // diskId
            "cloud",    // cloudId
            "folder",   // folderId
            partitionCount,
            2   // blocksPerStripe
        );
        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        auto range = TBlockRange64::WithLength(1, 3);

        // Check that write blocks request with incorrect checksums is rejected
        {
            auto request = volume.CreateWriteBlocksRequest(
                range,
                clientInfo.GetClientId(),
                1);
            NProto::TChecksum checksum1;
            checksum1.SetByteCount(DefaultBlockSize);
            checksum1.SetChecksum(0xdead);
            request->Record.MutableChecksums()->Add(std::move(checksum1));

            NProto::TChecksum checksum2;
            checksum2.SetByteCount(DefaultBlockSize * 2);
            checksum2.SetChecksum(0xbeef);
            request->Record.MutableChecksums()->Add(std::move(checksum2));

            volume.SendToPipe(std::move(request));
            auto response =
                volume.RecvResponse<TEvService::TEvWriteBlocksResponse>();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
            UNIT_ASSERT(HasProtoFlag(
                response->GetError().GetFlags(),
                NProto::EF_CHECKSUM_MISMATCH));
        }

        // Check that write blocks request with correct checksums is accepted
        google::protobuf::RepeatedPtrField<NProto::TChecksum> writtenChecksums;
        {
            auto request = volume.CreateWriteBlocksRequest(
                range,
                clientInfo.GetClientId(),
                1);
            NProto::TChecksum checksum1;
            checksum1.SetByteCount(DefaultBlockSize);
            checksum1.SetChecksum(1108896639);
            request->Record.MutableChecksums()->Add(std::move(checksum1));

            NProto::TChecksum checksum2;
            checksum2.SetByteCount(DefaultBlockSize * 2);
            checksum2.SetChecksum(2956588462);
            request->Record.MutableChecksums()->Add(std::move(checksum2));
            writtenChecksums.CopyFrom(request->Record.GetChecksums());

            volume.SendToPipe(std::move(request));
            auto response =
                volume.RecvResponse<TEvService::TEvWriteBlocksResponse>();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
        }

        // We should read combined checksum from written blocks
        {
            auto response = volume.ReadBlocks(range, clientInfo.GetClientId());
            UNIT_ASSERT_VALUES_EQUAL(
                DefaultBlockSize * 3,
                response->Record.GetChecksum().GetByteCount());
            auto checksum = CombineChecksums(writtenChecksums);
            UNIT_ASSERT_VALUES_EQUAL(
                checksum.GetChecksum(),
                response->Record.GetChecksum().GetChecksum());
        }
    }

    Y_UNIT_TEST(YdbBasedDiskShoudValidateChecksums_Version1_Partition1)
    {
        YdbBasedDiskShoudValidateChecksums(1, 1);
    }
    Y_UNIT_TEST(YdbBasedDiskShoudValidateChecksums_Version1_Partition2)
    {
        YdbBasedDiskShoudValidateChecksums(1, 2);
    }
    Y_UNIT_TEST(YdbBasedDiskShoudValidateChecksums_Version2_Partition1)
    {
        YdbBasedDiskShoudValidateChecksums(2, 1);
    }
    Y_UNIT_TEST(YdbBasedDiskShoudValidateChecksums_Version2_Partition2)
    {
        YdbBasedDiskShoudValidateChecksums(2, 2);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
