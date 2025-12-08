#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/byte_range.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct IBlockBuffer
{
    virtual ~IBlockBuffer() = default;

    virtual TStringBuf GetUnalignedHead() = 0;
    virtual TStringBuf GetBlock(size_t index) = 0;
    virtual TStringBuf GetUnalignedTail() = 0;
    virtual void SetBlock(size_t index, TStringBuf block) = 0;
    virtual void ClearBlock(size_t index) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IBlockBufferPtr CreateBlockBuffer(TByteRange byteRange);
IBlockBufferPtr CreateBlockBuffer(TByteRange byteRange, TString buffer);
IBlockBufferPtr CreateLazyBlockBuffer(TByteRange byteRange);

////////////////////////////////////////////////////////////////////////////////

void CopyFileData(
    const TString& logTag,
    const TByteRange origin,
    const TByteRange aligned,
    const ui64 fileSize,
    IBlockBuffer& buffer,
    TString* out);

}   // namespace NCloud::NFileStore::NStorage
