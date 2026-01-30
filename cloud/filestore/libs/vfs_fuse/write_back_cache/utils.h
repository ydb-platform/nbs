#pragma once

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
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
