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

}   // namespace NCloud
