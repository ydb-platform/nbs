#pragma once

#include <util/generic/maybe.h>
#include <util/stream/input.h>

#include <memory>

////////////////////////////////////////////////////////////////////////////////

struct TSuperBlock
{
    ui32 BlockSize = 0;
    ui32 BlocksPerGroup = 0;
    ui32 InodesPerGroup = 0;

    bool MetadataCsumFeature = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TBlockRange
{
    ui64 FirstBlock = 0;
    ui64 LastBlock = 0;

    inline ui64 Length() const
    {
        return LastBlock - FirstBlock + 1;
    }
};

struct TGroupDescr
{
    ui32 GroupNum = 0;
    TBlockRange Blocks;

    ui64 BlockBitmap = 0;
    ui64 BlockBitmapCS = 0;
    ui64 InodeBitmap = 0;
    ui64 InodeBitmapCS = 0;
    ui32 FreeBlocks = 0;
    ui32 FreeInodes = 0;
    ui32 UnusedInodes = 0;

    TMaybe<ui64> PrimarySuperblock;
    TMaybe<ui64> BackupSuperblock;
    TMaybe<TBlockRange> GroupDescriptors;
    TMaybe<TBlockRange> ReservedGDTBlocks;
};

////////////////////////////////////////////////////////////////////////////////

struct IExt4MetaReader
{
    virtual ~IExt4MetaReader() = default;

    virtual TSuperBlock ReadSuperBlock(IInputStream& stream) = 0;
    virtual TMaybe<TGroupDescr> ReadGroupDescr(IInputStream& stream) = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IExt4MetaReader> CreateExt4MetaReader();
