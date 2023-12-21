#pragma once

#include "public.h"

#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <ydb/core/base/logoblob.h>

#include <util/generic/vector.h>
#include <util/stream/output.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

inline NKikimr::TLogoBlobID MakeBlobId(ui64 tabletId, const TPartialBlobId& blobId)
{
    Y_DEBUG_ABORT_UNLESS(blobId.PartId() == 0);
    return NKikimr::TLogoBlobID(
        tabletId,
        blobId.Generation(),
        blobId.Step(),
        blobId.Channel(),
        blobId.BlobSize(),
        blobId.Cookie());
}

inline NKikimr::TLogoBlobID MakeBlobId(ui64 tabletId, ui64 commitId, ui64 uniqueId)
{
    return MakeBlobId(tabletId, TPartialBlobId(commitId, uniqueId));
}

inline TPartialBlobId MakePartialBlobId(const NKikimr::TLogoBlobID& blobId)
{
    return TPartialBlobId(
        blobId.Generation(),
        blobId.Step(),
        blobId.Channel(),
        blobId.BlobSize(),
        blobId.Cookie(),
        blobId.PartId());
}

////////////////////////////////////////////////////////////////////////////////

TString DumpBlobIds(ui64 tabletId, const TPartialBlobId& blobId);
TString DumpBlobIds(ui64 tabletId, const TVector<TPartialBlobId>& blobIds);

}   // namespace NCloud
