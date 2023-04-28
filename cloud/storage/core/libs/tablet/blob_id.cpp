#include "blob_id.h"

#include <util/stream/str.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

TString DumpBlobIds(ui64 tabletId, const TPartialBlobId& blobId)
{
    TStringStream out;
    out << MakeBlobId(tabletId, blobId);
    return out.Str();
}

TString DumpBlobIds(ui64 tabletId, const TVector<TPartialBlobId>& blobIds)
{
    TStringStream out;
    for (const auto& blobId: blobIds) {
        out << MakeBlobId(tabletId, blobId) << " ";
    }
    return out.Str();
}

}   // namespace NCloud
