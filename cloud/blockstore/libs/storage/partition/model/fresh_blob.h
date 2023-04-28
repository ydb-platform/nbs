#pragma once

#include "public.h"

#include "block.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/common/guarded_sglist.h>
#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

struct TGuardHolder
{
private:
    struct TData
    {
        TGuardedSgList SgList;
        TGuardedSgList::TGuard Guard;

        TData(TGuardedSgList sgList)
            : SgList(std::move(sgList))
            , Guard(SgList.Acquire())
        {}
    };

    std::unique_ptr<TData> Data;

public:
    TGuardHolder(TGuardedSgList sgList)
        : Data(new TData(std::move(sgList)))
    {}

    bool Acquired() const
    {
        return Data->Guard;
    }

    const TSgList& GetSgList() const
    {
        return Data->Guard.Get();
    }
};

////////////////////////////////////////////////////////////////////////////////

TString BuildWriteFreshBlocksBlobContent(
    const TVector<TBlockRange32>& blockRanges,
    const TVector<TGuardHolder>& guardHolders);

TString BuildZeroFreshBlocksBlobContent(TBlockRange32 blockRange);

NProto::TError ParseFreshBlobContent(
    const ui64 commitId,
    const ui32 blockSize,
    const TString& buffer,
    TVector<TOwningFreshBlock>& result);

}   // namespace NCloud::NBlockStore::NStorage::NPartition
