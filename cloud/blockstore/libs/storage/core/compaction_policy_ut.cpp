#include "compaction_policy.h"

#include "config.h"

#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCompactionPolicyTest)
{
    Y_UNIT_TEST(TestDefaultPolicy)
    {
        auto policy = BuildDefaultCompactionPolicy(1);
        UNIT_ASSERT_DOUBLES_EQUAL(-1, policy->CalculateScore({}).Score, 1e-4);
        UNIT_ASSERT(policy->CalculateScore({}).Score > -1);
        UNIT_ASSERT_DOUBLES_EQUAL(
            3,
            policy->CalculateScore({4, 0, 0, 0, 0, 0, false, 0}).Score,
            1e-4);
        UNIT_ASSERT_DOUBLES_EQUAL(
            3,
            policy->CalculateScore({4, 1, 1, 2, 3, 4, false, 5}).Score,
            1e-4);
    }

    Y_UNIT_TEST(TestLoadOptimizationPolicy)
    {
        auto policy = BuildLoadOptimizationCompactionPolicy(
            {4_MB, 4_KB, 400, 15_MB, 1000, 15_MB, 70});

        UNIT_ASSERT_VALUES_EQUAL(0, policy->CalculateScore({}).Score);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            policy->CalculateScore({4, 0, 0, 0, 0, 0, false, 0}).Score);

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            policy->CalculateScore({70, 0, 0, 0, 0, 0, false, 0}).Score);

        auto compactionScore =
            policy->CalculateScore({71, 0, 0, 0, 0, 0, false, 0});
        UNIT_ASSERT_VALUES_EQUAL(71, compactionScore.Score);
        UNIT_ASSERT_EQUAL(
            compactionScore.Type,
            TCompactionScore::EType::BlobCount);

        /*
         *  15_MB / 4_KB = 3840
         *
         *  BlobCount = 20
         *  BlockCount = 2048
         *  UsedBlockCount = 1024
         *  AverageBlobSize = 2048 / 20 = 102.4
         *  MaxBlobs = 1024 / 102.4 = 10
         *  ReadRequestCount = 30
         *  ReadRequestBlobCount = 400
         *  ReadRequestBlockCount = 7680
         *  readCost = 400 / 400 + 7680/ 3840 = 3
         *  compactedReadCost = 30 / 400 + 7680 / 3840 = 2.075
         *  compactionCost = 10 / 400 + 1024 / 3840 + 1 / 1000 + 1024 / 3840 =
         * 0.58433 loadScore = 3 - 2.075 - 0.58433 = 0.34066
         *
         */

        compactionScore =
            policy->CalculateScore({20, 2048, 1024, 30, 400, 7500, false, 0});
        UNIT_ASSERT_DOUBLES_EQUAL(0.36566, compactionScore.Score, 1e-5);
        UNIT_ASSERT_EQUAL(compactionScore.Type, TCompactionScore::EType::Read);

        /*
         * UsedBlockCount = 512
         */

        compactionScore =
            policy->CalculateScore({20, 2048, 512, 30, 400, 7500, false, 0});
        UNIT_ASSERT_DOUBLES_EQUAL(0.64483, compactionScore.Score, 1e-5);
        UNIT_ASSERT_EQUAL(compactionScore.Type, TCompactionScore::EType::Read);
    }

    Y_UNIT_TEST(TestBuildLoadOptimizationPolicyConfig)
    {
        NProto::TPartitionConfig partitionConfig;
        partitionConfig.SetStorageMediaKind(NCloud::NProto::STORAGE_MEDIA_SSD);
        partitionConfig.SetBlockSize(4_KB);
        NProto::TStorageServiceConfig storageConfigProto;
        storageConfigProto.SetRealSSDUnitReadIops(2222);
        storageConfigProto.SetRealSSDUnitReadBandwidth(222);
        storageConfigProto.SetRealSSDUnitWriteIops(1111);
        storageConfigProto.SetRealSSDUnitWriteBandwidth(111);
        storageConfigProto.SetSSDMaxBlobsPerRange(66);
        storageConfigProto.SetSSDV2MaxBlobsPerRange(6);
        storageConfigProto.SetRealHDDUnitReadIops(222);
        storageConfigProto.SetRealHDDUnitReadBandwidth(22);
        storageConfigProto.SetRealHDDUnitWriteIops(111);
        storageConfigProto.SetRealHDDUnitWriteBandwidth(11);
        storageConfigProto.SetHDDMaxBlobsPerRange(44);
        storageConfigProto.SetHDDV2MaxBlobsPerRange(4);
        TStorageConfig storageConfig(
            storageConfigProto,
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig()));
        const ui32 siblingCount = 2;
        auto config = BuildLoadOptimizationCompactionPolicyConfig(
            partitionConfig,
            storageConfig,
            66 / 2   // maxBlobsPerRange
        );

        UNIT_ASSERT_VALUES_EQUAL(2222, config.MaxReadIops);
        UNIT_ASSERT_VALUES_EQUAL(222_MB, config.MaxReadBandwidth);
        UNIT_ASSERT_VALUES_EQUAL(1111, config.MaxWriteIops);
        UNIT_ASSERT_VALUES_EQUAL(111_MB, config.MaxWriteBandwidth);
        UNIT_ASSERT_VALUES_EQUAL(4_MB, config.MaxBlobSize);
        UNIT_ASSERT_VALUES_EQUAL(4_KB, config.BlockSize);
        UNIT_ASSERT_VALUES_EQUAL(33, config.MaxBlobsPerRange);

        partitionConfig.SetBlockSize(64_KB);
        partitionConfig.SetMaxBlocksInBlob(128);
        partitionConfig.SetStorageMediaKind(
            NCloud::NProto::STORAGE_MEDIA_HYBRID);

        config = BuildLoadOptimizationCompactionPolicyConfig(
            partitionConfig,
            storageConfig,
            44 / 2   // maxBlobsPerRange
        );
        UNIT_ASSERT_VALUES_EQUAL(222, config.MaxReadIops);
        UNIT_ASSERT_VALUES_EQUAL(22_MB, config.MaxReadBandwidth);
        UNIT_ASSERT_VALUES_EQUAL(111, config.MaxWriteIops);
        UNIT_ASSERT_VALUES_EQUAL(11_MB, config.MaxWriteBandwidth);
        UNIT_ASSERT_VALUES_EQUAL(8_MB, config.MaxBlobSize);
        UNIT_ASSERT_VALUES_EQUAL(64_KB, config.BlockSize);
        UNIT_ASSERT_VALUES_EQUAL(22, config.MaxBlobsPerRange);

        partitionConfig.SetTabletVersion(2);
        partitionConfig.SetStorageMediaKind(NCloud::NProto::STORAGE_MEDIA_SSD);

        auto maxBlobsPerRange =
            GetMaxBlobsPerRange(partitionConfig, storageConfig, siblingCount);

        UNIT_ASSERT_VALUES_EQUAL(3, maxBlobsPerRange);

        partitionConfig.SetStorageMediaKind(
            NCloud::NProto::STORAGE_MEDIA_HYBRID);

        maxBlobsPerRange =
            GetMaxBlobsPerRange(partitionConfig, storageConfig, siblingCount);

        UNIT_ASSERT_VALUES_EQUAL(2, maxBlobsPerRange);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
