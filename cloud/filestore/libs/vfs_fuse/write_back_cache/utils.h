#pragma once

#include "node_cache.h"

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <cloud/storage/core/protos/error.pb.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

class TUtils
{
public:
    static NCloud::NProto::TError ValidateReadDataRequest(
        const NProto::TReadDataRequest& request,
        const TString& expectedFileSystemId);

    static NCloud::NProto::TError ValidateWriteDataRequest(
        const NProto::TWriteDataRequest& request,
        const TString& expectedFileSystemId);

    static bool IsFullyCoveredByParts(
        const TVector<TCachedDataPart>& parts,
        ui64 byteCount);

    static NProto::TReadDataResponse BuildReadDataResponse(
        const TVector<TCachedDataPart>& parts);

    static void AugmentReadDataResponse(
        NProto::TReadDataResponse& response,
        const TCachedData& cachedData);
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
