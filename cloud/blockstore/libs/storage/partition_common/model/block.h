#pragma once

#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

struct TBlock
{
    ui32 BlockIndex;
    ui64 CommitId;

    // fresh blocks only
    bool IsStoredInDb;

    TBlock(ui32 blockIndex, ui64 commitId, bool isStoredInDb)
        : BlockIndex(blockIndex)
        , CommitId(commitId)
        , IsStoredInDb(isStoredInDb)
    {}

    bool operator ==(const TBlock& other) const
    {
        return BlockIndex == other.BlockIndex
            && CommitId == other.CommitId;
    }

    bool operator <(const TBlock& other) const
    {
        // order by BlockIndex ASC
        return BlockIndex < other.BlockIndex;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFreshBlock
{
    TBlock Meta;
    TStringBuf Content;
    TPartialBlobId BlobId;

    TFreshBlock(TBlock meta, TStringBuf content, TPartialBlobId blobId)
        : Meta(meta)
        , Content(content)
        , BlobId(blobId)
    {
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TOwningFreshBlock
{
    TBlock Meta;
    TString Content;
    TPartialBlobId BlobId;

    TOwningFreshBlock(TBlock meta, TString content, TPartialBlobId blobId)
        : Meta(meta)
        , Content(std::move(content))
        , BlobId(blobId)
    {
    }
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
