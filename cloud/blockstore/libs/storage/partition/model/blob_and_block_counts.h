#pragma once

#include <util/system/types.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

struct TBlobAndBlockCounts
{
    ui64 MixedBlobs = 0;
    ui64 MergedBlobs = 0;
    ui64 MixedBlocks = 0;
    ui64 MergedBlocks = 0;

    TBlobAndBlockCounts& operator+=(const TBlobAndBlockCounts& rhs)
    {
        MixedBlobs += rhs.MixedBlobs;
        MergedBlobs += rhs.MergedBlobs;
        MixedBlocks += rhs.MixedBlocks;
        MergedBlocks += rhs.MergedBlocks;
        return *this;
    }

    TBlobAndBlockCounts& operator-=(const TBlobAndBlockCounts& rhs)
    {
        MixedBlobs -= rhs.MixedBlobs;
        MergedBlobs -= rhs.MergedBlobs;
        MixedBlocks -= rhs.MixedBlocks;
        MergedBlocks -= rhs.MergedBlocks;
        return *this;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TPartitionBlobAndBlockCounts
{
    TBlobAndBlockCounts Index;
    TBlobAndBlockCounts Channel;

    TPartitionBlobAndBlockCounts& operator+=(
        const TPartitionBlobAndBlockCounts& rhs)
    {
        Index += rhs.Index;
        Channel += rhs.Channel;
        return *this;
    }

    TPartitionBlobAndBlockCounts& operator-=(
        const TPartitionBlobAndBlockCounts& rhs)
    {
        Index -= rhs.Index;
        Channel -= rhs.Channel;
        return *this;
    }
};

inline TPartitionBlobAndBlockCounts operator+(
    TPartitionBlobAndBlockCounts lhs, const TPartitionBlobAndBlockCounts& rhs)
{
    return lhs += rhs;
}

inline TPartitionBlobAndBlockCounts operator-(
    TPartitionBlobAndBlockCounts lhs, const TPartitionBlobAndBlockCounts& rhs)
{
    return lhs -= rhs;
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
