#include "compaction_policy.h"

#include "config.h"
#include "proto_helpers.h"

#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <util/generic/cast.h>
#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

float RequestCost(
    ui32 maxIops,
    ui64 maxBandwidth,
    double bytes,
    double count)
{
    if (!bytes) {
        return 0;
    }

    return count / maxIops + bytes / maxBandwidth;
}

template <class T>
auto OrDefault(const T t, const T def)
{
    return t ? t : def;
}

////////////////////////////////////////////////////////////////////////////////

struct TDefaultPolicy
    : ICompactionPolicy
{
    const ui32 CompactionThreshold;
    const ui64 UsedBlocksThresholdForMixedBlocksCompaction;
    const bool MixedBlocksCountCompactionEnabled;

    TDefaultPolicy(
        ui32 compactionThreshold,
        ui64 usedBlocksThresholdForMixedBlocksCompaction,
        bool mixedBlocksCountCompactionEnabled)
        : CompactionThreshold(compactionThreshold)
        , UsedBlocksThresholdForMixedBlocksCompaction(
              usedBlocksThresholdForMixedBlocksCompaction)
        , MixedBlocksCountCompactionEnabled(mixedBlocksCountCompactionEnabled)
    {}

    TCompactionScore CalculateScore(const TRangeStat& stat) const override
    {
        // eps needed because we want the 'score > 0' condition to be equivalent
        // to the legacy 'BlobCount >= CompactionThreshold' condition
        const float eps = 1e-5;
        return float(stat.BlobCount) - CompactionThreshold + eps;
    }

    bool BackpressureEnabled() const override
    {
        return true;
    }

    ui64 GetUsedBlocksThresholdForMixedBlocksCompaction() const override
    {
        return UsedBlocksThresholdForMixedBlocksCompaction;
    }

    bool IsMixedBlocksCountCompactionEnabled() const override
    {
        return MixedBlocksCountCompactionEnabled;
    }
};

////////////////////////////////////////////////////////////////////////////////

const ui32 DefaultMaxReadIops = 100;
const ui64 DefaultMaxReadBandwidth = 30_MB;
const ui32 DefaultMaxWriteIops = 300;
const ui64 DefaultMaxWriteBandwidth = 30_MB;

struct TLoadOptimizationPolicy
    : ICompactionPolicy
{
    TLoadOptimizationCompactionPolicyConfig Config;
    ui64 UsedBlocksThresholdForMixedBlocksCompaction;
    bool MixedBlocksCountCompactionEnabled;

    TLoadOptimizationPolicy(
        const TLoadOptimizationCompactionPolicyConfig& config,
        const ui64 usedBlocksThresholdForMixedBlocksCompaction,
        const bool mixedBlocksCountCompactionEnabled)
        : Config(config)
        , UsedBlocksThresholdForMixedBlocksCompaction(
              usedBlocksThresholdForMixedBlocksCompaction)
        , MixedBlocksCountCompactionEnabled(mixedBlocksCountCompactionEnabled)
    {}

    TCompactionScore CalculateScore(const TRangeStat& stat) const override
    {
        if (!stat.BlobCount) {
            return 0;
        }

        if (stat.BlobCount > Config.MaxBlobsPerRange) {
            return stat.BlobCount;
        }

        auto readCost = RequestCost(
            OrDefault(Config.MaxReadIops, DefaultMaxReadIops),
            OrDefault(Config.MaxReadBandwidth, DefaultMaxReadBandwidth),
            stat.ReadRequestBlockCount * Config.BlockSize,
            stat.ReadRequestBlobCount
        );

        auto compactedReadCost = RequestCost(
            OrDefault(Config.MaxReadIops, DefaultMaxReadIops),
            OrDefault(Config.MaxReadBandwidth, DefaultMaxReadBandwidth),
            stat.ReadRequestBlockCount * Config.BlockSize,
            stat.ReadRequestCount
        );

        const auto dataSize = Min(
            stat.UsedBlockCount * Config.BlockSize,
            Config.MaxBlobSize
        );

        float compactionCost = 0;
        if (dataSize) {
            const auto averageBlobSize = stat.BlockCount * Config.BlockSize
                / double(stat.BlobCount);

            const auto maxBlobs = stat.BlockCount
                ? dataSize / averageBlobSize
                : double(stat.BlobCount);

            compactionCost = RequestCost(
                OrDefault(Config.MaxReadIops, DefaultMaxReadIops),
                OrDefault(Config.MaxReadBandwidth, DefaultMaxReadBandwidth),
                dataSize,
                maxBlobs
            ) + RequestCost(
                OrDefault(Config.MaxWriteIops, DefaultMaxWriteIops),
                OrDefault(Config.MaxWriteBandwidth, DefaultMaxWriteBandwidth),
                Min(stat.UsedBlockCount * Config.BlockSize, Config.MaxBlobSize),
                1
            );
        }
        return {readCost - compactedReadCost - compactionCost,
            TCompactionScore::EType::Read};
    }

    bool BackpressureEnabled() const override
    {
        return true;
    }

    ui64 GetUsedBlocksThresholdForMixedBlocksCompaction() const override
    {
        return UsedBlocksThresholdForMixedBlocksCompaction;
    }

    bool IsMixedBlocksCountCompactionEnabled() const override
    {
        return MixedBlocksCountCompactionEnabled;
    }
};

////////////////////////////////////////////////////////////////////////////////

bool IsMixedBlocksCountCompactionEnabled(
    const TStorageConfig& storageConfig,
    const NProto::TPartitionConfig& partitionConfig)
{
    const bool isSSD = partitionConfig.GetStorageMediaKind() ==
                       NCloud::NProto::STORAGE_MEDIA_SSD;
    const bool enabled =
        isSSD ? storageConfig.GetMixedBlocksCountCompactionEnabledSSD()
              : storageConfig.GetMixedBlocksCountCompactionEnabledHDD();
    const bool enabledByFeature =
        isSSD ? storageConfig.IsMixedBlocksCountCompactionSSDFeatureEnabled(
                    partitionConfig.GetCloudId(),
                    partitionConfig.GetFolderId(),
                    partitionConfig.GetDiskId())
              : storageConfig.IsMixedBlocksCountCompactionHDDFeatureEnabled(
                    partitionConfig.GetCloudId(),
                    partitionConfig.GetFolderId(),
                    partitionConfig.GetDiskId());
    return enabled || enabledByFeature;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ui32 GetMaxBlobsPerRange(
    const NProto::TPartitionConfig& partitionConfig,
    const TStorageConfig& storageConfig,
    const ui32 siblingCount)
{
    auto maxBlobsPerRange = IntegerCast<ui32>(
        partitionConfig.GetStorageMediaKind() == NCloud::NProto::STORAGE_MEDIA_SSD
            ? partitionConfig.GetTabletVersion() == 2
                ? storageConfig.GetSSDV2MaxBlobsPerRange()
                : storageConfig.GetSSDMaxBlobsPerRange()
            : partitionConfig.GetTabletVersion() == 2
                ? storageConfig.GetHDDV2MaxBlobsPerRange()
                : storageConfig.GetHDDMaxBlobsPerRange()
    );

    maxBlobsPerRange = Max(maxBlobsPerRange / siblingCount, 1u);
    return maxBlobsPerRange;
}

////////////////////////////////////////////////////////////////////////////////

ICompactionPolicyPtr BuildDefaultCompactionPolicy(
    ui32 compactionThreshold,
    ui64 usedBlocksThresholdForMixedBlocksCompaction,
    bool mixedBlocksCountCompactionEnabled)
{
    return std::make_shared<TDefaultPolicy>(
        compactionThreshold,
        usedBlocksThresholdForMixedBlocksCompaction,
        mixedBlocksCountCompactionEnabled);
}

ICompactionPolicyPtr BuildLoadOptimizationCompactionPolicy(
    const TLoadOptimizationCompactionPolicyConfig& config,
    const ui64 usedBlocksThresholdForMixedBlocksCompaction,
    const bool mixedBlocksCountCompactionEnabled)
{
    return std::make_shared<TLoadOptimizationPolicy>(
        config,
        usedBlocksThresholdForMixedBlocksCompaction,
        mixedBlocksCountCompactionEnabled);
}

TLoadOptimizationCompactionPolicyConfig BuildLoadOptimizationCompactionPolicyConfig(
    const NProto::TPartitionConfig& partitionConfig,
    const TStorageConfig& storageConfig,
    const ui32 maxBlobsPerRange)
{
    const auto blockSize = partitionConfig.GetBlockSize();
    const auto maxBlocksInBlob = partitionConfig.GetMaxBlocksInBlob()
        ? partitionConfig.GetMaxBlocksInBlob()
        : MaxBlocksCount;

    const auto maxReadIops =
        partitionConfig.GetStorageMediaKind() == NCloud::NProto::STORAGE_MEDIA_SSD
            ? storageConfig.GetRealSSDUnitReadIops()
            : storageConfig.GetRealHDDUnitReadIops();

    const auto maxReadBandwidth = IntegerCast<ui32>(
        partitionConfig.GetStorageMediaKind() == NCloud::NProto::STORAGE_MEDIA_SSD
            ? storageConfig.GetRealSSDUnitReadBandwidth() * 1_MB
            : storageConfig.GetRealHDDUnitReadBandwidth() * 1_MB
    );

    const auto maxWriteIops =
        partitionConfig.GetStorageMediaKind() == NCloud::NProto::STORAGE_MEDIA_SSD
            ? storageConfig.GetRealSSDUnitWriteIops()
            : storageConfig.GetRealHDDUnitWriteIops();

    const auto maxWriteBandwidth = IntegerCast<ui32>(
        partitionConfig.GetStorageMediaKind() == NCloud::NProto::STORAGE_MEDIA_SSD
            ? storageConfig.GetRealSSDUnitWriteBandwidth() * 1_MB
            : storageConfig.GetRealHDDUnitWriteBandwidth() * 1_MB
    );

    return {
        maxBlocksInBlob * blockSize,
        blockSize,
        maxReadIops,
        maxReadBandwidth,
        maxWriteIops,
        maxWriteBandwidth,
        maxBlobsPerRange
    };
}

ICompactionPolicyPtr BuildCompactionPolicy(
    const NProto::TPartitionConfig& partitionConfig,
    const TStorageConfig& storageConfig,
    const ui32 siblingCount)
{
    Y_ABORT_UNLESS(siblingCount > 0);

    const auto maxBlobsPerRange = GetMaxBlobsPerRange(
        partitionConfig,
        storageConfig,
        siblingCount);

    ui64 usedBlocksThresholdForMixedBlocksCompaction =
        GetWriteBlobThreshold(
            storageConfig,
            partitionConfig.GetStorageMediaKind()) /
        partitionConfig.GetBlockSize();
    if (partitionConfig.GetStorageMediaKind() !=
        NCloud::NProto::STORAGE_MEDIA_SSD)
    {
        usedBlocksThresholdForMixedBlocksCompaction = Max<ui64>(
            usedBlocksThresholdForMixedBlocksCompaction,
            storageConfig.GetCompactionMergedBlobThresholdHDD() /
                partitionConfig.GetBlockSize());
    }

    const bool mixedBlocksCountCompactionEnabled =
        IsMixedBlocksCountCompactionEnabled(storageConfig, partitionConfig);
    const NProto::ECompactionType ct =
        partitionConfig.GetStorageMediaKind() == NProto::STORAGE_MEDIA_SSD
            ? storageConfig.GetSSDCompactionType()
            : storageConfig.GetHDDCompactionType();

    switch (ct) {
        case NProto::ECompactionType::CT_DEFAULT: {
            return BuildDefaultCompactionPolicy(
                maxBlobsPerRange,
                usedBlocksThresholdForMixedBlocksCompaction,
                mixedBlocksCountCompactionEnabled);
        }

        case NProto::ECompactionType::CT_LOAD: {
            return BuildLoadOptimizationCompactionPolicy(
                BuildLoadOptimizationCompactionPolicyConfig(
                    partitionConfig,
                    storageConfig,
                    maxBlobsPerRange),
                usedBlocksThresholdForMixedBlocksCompaction,
                mixedBlocksCountCompactionEnabled);
        }

        default:
            Y_ABORT_UNLESS(0);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
