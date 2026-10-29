#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/buffer.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>

#include <optional>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct TEntry
{
    ui64 Key = 0;
    ui64 Seq = 0;
    TStringBuf Data;
};

struct TSuperblock
{
    ui64 Seq = 0;
    ui64 ErasedBelowKey = 0;
    TBuffer MetaBuffer;
};

NProto::TError ValidatePageSize(ui32 pageSize, ui32 minKeyBufferSize);

ui32 GetPageHeaderSize();
ui32 GetSerializedEntrySize(ui32 entryBufferSize);

// Lays out |entries| in a page of |pageSize| bytes. On success the entries
// point into |*outPageBuffer| instead of the buffers they came from.
NProto::TError MakePageBuffer(
    ui32 pageSize,
    TVector<TEntry>& entries,
    TBuffer* outPageBuffer,
    ui32* outDataSize);

// Returns the entries of a valid page, nothing for a page that holds anything
// else.
TVector<TEntry> ParsePageBuffer(TStringBuf pageBuffer, ui32 pageSize);

ui32 GetSuperblockMetaCapacity(ui32 pageSize);

TResultOrError<TBuffer> MakeSuperblockPage(
    const TSuperblock& superblock,
    ui32 pageSize);

std::optional<TSuperblock> ParseSuperblockPage(TStringBuf page, ui32 pageSize);

}   // namespace NCloud::NJournalled
