#pragma once

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

}   // namespace NCloud::NBlockStore::NStorage
