#include "compaction_policy.h"

#include "config.h"

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

    TDefaultPolicy(ui32 compactionThreshold)
        : CompactionThreshold(compactionThreshold)
    {
    }

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

    TLoadOptimizationPolicy(const TLoadOptimizationCompactionPolicyConfig& config)
        : Config(config)
    {
    }

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
};

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

ICompactionPolicyPtr BuildDefaultCompactionPolicy(ui32 compactionThreshold)
{
    return std::make_shared<TDefaultPolicy>(compactionThreshold);
}

ICompactionPolicyPtr BuildLoadOptimizationCompactionPolicy(
    const TLoadOptimizationCompactionPolicyConfig& config)
{
    return std::make_shared<TLoadOptimizationPolicy>(config);
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

    NProto::ECompactionType ct = NProto::ECompactionType::CT_DEFAULT;
    switch (partitionConfig.GetStorageMediaKind()) {
        case NCloud::NProto::STORAGE_MEDIA_SSD: {
            if (!storageConfig.GetSSDMaxBlobsPerUnit()) {
                ct = storageConfig.GetSSDCompactionType();
            }
            break;
        }

        default: {
            if (!storageConfig.GetHDDMaxBlobsPerUnit()) {
                ct = storageConfig.GetHDDCompactionType();
            }
            break;
        }
    }

    switch (ct) {
        case NProto::ECompactionType::CT_DEFAULT: {
            return BuildDefaultCompactionPolicy(maxBlobsPerRange);
        }

        case NProto::ECompactionType::CT_LOAD: {
            return BuildLoadOptimizationCompactionPolicy(
                BuildLoadOptimizationCompactionPolicyConfig(
                    partitionConfig,
                    storageConfig,
                    maxBlobsPerRange
                )
            );
        }

        default: Y_ABORT_UNLESS(0);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
