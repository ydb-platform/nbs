#pragma once

#include <util/system/types.h>

////////////////////////////////////////////////////////////////////////////////

struct TSuperBlock
{
    // The size of a basic unit of space allocation in bytes. Typically,
    //    this is 4096 (4KB) but can range from 512 to 65536 bytes.
    ui64 BlockSize = 0;

    ui64 GroupCount = 0;
    ui64 BlocksPerGroup = 0;

    // Specifies the underlying disk sector size in bytes. Typically
    //    this is 512 or 4096 bytes. This determines the minimum I/O alignment,
    //    especially for direct I/O.
    ui64 SectorSize = 0;
};

struct TFreeList
{
    ui64 GroupNo = 0;
    ui64 Offset = 0;
    ui64 Count = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IDev
{
    virtual ~IDev() = default;
    virtual void
    Pwrite(const void* buffer, ui32 byteCount, i64 offset) const = 0;
    virtual void Pload(void* buffer, ui32 byteCount, i64 offset) const = 0;
};
