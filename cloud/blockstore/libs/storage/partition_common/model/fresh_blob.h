#pragma once

#include "block.h"

#include <cloud/blockstore/libs/common/block_range.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/guarded_sglist.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TFreshBlob
{
    ui64 CommitId;
    TPartialBlobId BlobId;
    TString Data;

    TFreshBlob(ui64 commitId, TPartialBlobId blobId, TString data)
        : CommitId(commitId)
        , BlobId(blobId)
        , Data(std::move(data))
    {}
};

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
    ui64 commitId,
    TPartialBlobId blobId,
    ui32 blockSize,
    const TString& buffer,
    TVector<TOwningFreshBlock>& result);

}   // namespace NCloud::NBlockStore::NStorage
