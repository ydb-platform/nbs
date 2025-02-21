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
size_t SgListCopy(const TSgList& src, TBlockDataRef dst);

TResultOrError<TSgList> SgListNormalize(TBlockDataRef buffer, ui32 blockSize);
TResultOrError<TSgList> SgListNormalize(TSgList sglist, ui32 blockSize);

class TSgListBlockRange
{
    const ui32 BlockSize;
    const TSgList::const_iterator End;

    TSgList::const_iterator It;
    ui64 Offset = 0;

public:
    TSgListBlockRange(const TSgList& sglist, ui32 blockSize);
    ~TSgListBlockRange() = default;

    TSgList Next(ui64 blockCount);
    [[nodiscard]] bool HasNext() const;
};

}   // namespace NCloud
