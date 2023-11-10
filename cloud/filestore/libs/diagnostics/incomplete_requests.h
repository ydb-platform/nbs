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
    const NCloud::NProto::EStorageMediaKind MediaKind;
    const EFileStoreRequest RequestType;
    const TDuration ExecutionTime;
    const TDuration TotalTime;

    TIncompleteRequest(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EFileStoreRequest requestType,
        TDuration executionTime,
        TDuration totalTime);
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
