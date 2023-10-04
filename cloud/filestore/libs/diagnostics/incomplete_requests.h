#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/request.h>

#include <cloud/storage/core/protos/media.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <array>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

struct TIncompleteRequest
{
    NCloud::NProto::EStorageMediaKind MediaKind =
        NCloud::NProto::STORAGE_MEDIA_DEFAULT;
    EFileStoreRequest RequestType;
    TDuration RequestTime;
};

////////////////////////////////////////////////////////////////////////////////

struct IIncompleteRequestCollector
{
    virtual ~IIncompleteRequestCollector() = default;

    virtual void Collect(const TIncompleteRequest& request) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IIncompleteRequestProvider
{
    virtual ~IIncompleteRequestProvider() = default;

    virtual void Accept(IIncompleteRequestCollector& collector) = 0;
};

}   // namespace NCloud::NFileStore
