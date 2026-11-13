#pragma once

#include "public.h"
#include "block_data_ref.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/strbuf.h>
#include <util/generic/vector.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

using TSgList = TVector<TBlockDataRef>;

size_t SgListGetSize(const TSgList& sglist);
size_t SgListCopy(const TSgList& src, const TSgList& dst);
size_t SgListCopy(TBlockDataRef src, const TSgList& dst);
size_t SgListCopy(std::span<const TBlockDataRef> src, TBlockDataRef dst);
size_t SgListCopy(const TSgList& src, TBlockDataRef dst);

// Copy into a private, contiguous destination and return the CRC32C of each
// completed logical block. Source fragments may be empty or represent zeroes.
// The caller keeps the source accessible while copying; its contents need not
// be immutable. The destination must not alias the source or be modified by
// another consumer until copying and checksumming have finished.
// blockSize must be nonzero; source and destination sizes must match and be
// divisible by blockSize. An empty source/destination yields no checksums.
TVector<ui32> SgListCopyWithBlockChecksums(
    TBlockDataRefSpan src,
    TBlockDataRef dst,
    ui32 blockSize);

TResultOrError<TSgList> SgListNormalize(TBlockDataRef buffer, ui32 blockSize);
TResultOrError<TSgList> SgListNormalize(TSgList sglist, ui32 blockSize);

// Creates a TSgList that represents a part of the original one, specified by
// the offset and size.
TSgList CreateSgListSubRange(const TSgList& sgList, size_t offset, size_t size);

}   // namespace NCloud
