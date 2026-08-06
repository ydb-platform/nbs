#pragma once

#include "public.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TCompactionScore
{
    enum class EType
    {
        BlobCount = 0,
        Read = 1
    };

    float Score = 0;
    EType Type = EType::BlobCount;

    TCompactionScore() = default;

    TCompactionScore(float score, EType type)
        : Score(score)
        , Type(type)
    {
    };

    TCompactionScore(float score) : Score(score) {};
};

struct TRangeStat
{
    ui16 BlobCount = 0;
    ui16 BlockCount = 0;
    ui16 UsedBlockCount = 0;
    ui16 ReadRequestCount = 0;
    ui16 ReadRequestBlobCount = 0;
    ui16 ReadRequestBlockCount = 0;
    ui16 NewlyZeroedBlocks = 0; // In-memory only.
    bool Compacted = false;
    TCompactionScore CompactionScore;

    TRangeStat() = default;

    TRangeStat(
            ui16 blobCount,
            ui16 blockCount,
            ui16 usedBlockCount,
            ui16 readRequestCount,
            ui16 readRequestBlobCount,
            ui16 readRequestBlockCount,
            bool compacted,
            float score,
            TCompactionScore::EType scoreType =
                TCompactionScore::EType::BlobCount)
        : BlobCount(blobCount)
        , BlockCount(blockCount)
        , UsedBlockCount(usedBlockCount)
        , ReadRequestCount(readRequestCount)
        , ReadRequestBlobCount(readRequestBlobCount)
        , ReadRequestBlockCount(readRequestBlockCount)
        , Compacted(compacted)
        , CompactionScore(score, scoreType)
    {
    }

    ui16 GarbageBlockCount() const
    {
        if (UsedBlockCount > BlockCount) {
            // UsedBlockCount can temporarily exceed BlockCount. For example, we
            // may write a mixed/merged blob and then add a zero blob covering
            // the same blocks to fresh. After compaction, BlockCount drops but
            // UsedBlockCount stays the same. This mismatch is corrected on the
            // next flush.
            return 0;
        }

        return BlockCount - UsedBlockCount;
    }

    ui16 GarbageIgnoringZeroed() const
    {
        const auto garbageBlockCount = GarbageBlockCount();
        if (garbageBlockCount < NewlyZeroedBlocks) {
            return 0;
        }
        return garbageBlockCount - NewlyZeroedBlocks;
    }

    ui16 UsedBlocksIgnoringZeroed() const
    {
        return UsedBlockCount + NewlyZeroedBlocks;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct ICompactionPolicy
{
    virtual ~ICompactionPolicy() {}

    virtual TCompactionScore CalculateScore(const TRangeStat& stat) const = 0;
    virtual bool BackpressureEnabled() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

ui32 GetMaxBlobsPerRange(
    const NProto::TPartitionConfig& partitionConfig,
    const TStorageConfig& storageConfig,
    const ui32 siblingCount);

ICompactionPolicyPtr BuildDefaultCompactionPolicy(ui32 compactionThreshold);

struct TLoadOptimizationCompactionPolicyConfig
{
    ui32 MaxBlobSize;
    ui32 BlockSize;
    ui32 MaxReadIops;
    ui64 MaxReadBandwidth;
    ui32 MaxWriteIops;
    ui64 MaxWriteBandwidth;
    ui32 MaxBlobsPerRange;
};

ICompactionPolicyPtr BuildLoadOptimizationCompactionPolicy(
    const TLoadOptimizationCompactionPolicyConfig& config);

TLoadOptimizationCompactionPolicyConfig BuildLoadOptimizationCompactionPolicyConfig(
    const NProto::TPartitionConfig& partitionConfig,
    const TStorageConfig& storageConfig,
    const ui32 siblingCount);

ICompactionPolicyPtr BuildCompactionPolicy(
    const NProto::TPartitionConfig& partitionConfig,
    const TStorageConfig& storageConfig,
    const ui32 siblingCount);

}   // namespace NCloud::NBlockStore::NStorage
