#pragma once

#include "public.h"

#include "blob.h"
#include "block.h"

#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

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

NProto::TError ParseFreshBlobContent(
    ui64 commitId,
    ui32 blockSize,
    const TString& buffer,
    TVector<TOwningFreshBlock>& blocks,
    TBlobUpdatesByFresh& updates);

TString BuildFreshBlobContent(
    const TVector<TBlockRange32>& blockRanges,
    const TVector<TGuardHolder>& guardHolders,
    ui64 firstRequestDeletionId);

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
