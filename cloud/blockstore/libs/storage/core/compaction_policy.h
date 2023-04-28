#pragma once

#include "public.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TRangeStat
{
    ui16 BlobCount = 0;
    ui16 BlockCount = 0;
    ui16 UsedBlockCount = 0;
    ui16 ReadRequestCount = 0;
    ui16 ReadRequestBlobCount = 0;
    ui16 ReadRequestBlockCount = 0;
    bool Compacted = false;
    float Score = 0;

    TRangeStat() = default;

    TRangeStat(
            ui16 blobCount,
            ui16 blockCount,
            ui16 usedBlockCount,
            ui16 readRequestCount,
            ui16 readRequestBlobCount,
            ui16 readRequestBlockCount,
            bool compacted,
            float score)
        : BlobCount(blobCount)
        , BlockCount(blockCount)
        , UsedBlockCount(usedBlockCount)
        , ReadRequestCount(readRequestCount)
        , ReadRequestBlobCount(readRequestBlobCount)
        , ReadRequestBlockCount(readRequestBlockCount)
        , Compacted(compacted)
        , Score(score)
    {
    }

    ui16 GarbageBlockCount() const
    {
        if (UsedBlockCount > BlockCount) {
            // it means that some of these used blocks are still in fresh index
            return 0;
        }

        return BlockCount - UsedBlockCount;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct ICompactionPolicy
{
    virtual ~ICompactionPolicy() {}

    virtual float CalculateScore(const TRangeStat& stat) const = 0;
    virtual bool BackpressureEnabled() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

ICompactionPolicyPtr BuildDefaultCompactionPolicy(ui32 compactionThreshold);

struct TLoadOptimizationCompactionPolicyConfig
{
    ui32 MaxBlobSize;
    ui32 BlockSize;
    ui32 MaxReadIops;
    ui32 MaxReadBandwidth;
    ui32 MaxWriteIops;
    ui32 MaxWriteBandwidth;
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
