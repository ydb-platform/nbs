#pragma once

#include "public.h"

#include <util/generic/fwd.h>
#include <util/generic/set.h>

namespace NCloud::NFileStore {

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

class TProfileLogRecord;

}   // namespace NProto

////////////////////////////////////////////////////////////////////////////////

class IRequestFilter
{
public:
    virtual ~IRequestFilter() = default;

    virtual NProto::TProfileLogRecord GetFilteredRecord(
        const NProto::TProfileLogRecord& record) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

IRequestFilterPtr CreateRequestFilterAccept();

IRequestFilterPtr CreateRequestFilterByFileSystemId(
    IRequestFilterPtr nextFilter,
    TString fileSystemId);

IRequestFilterPtr CreateRequestFilterByNodeId(
    IRequestFilterPtr nextFilter,
    ui64 nodeId);

IRequestFilterPtr CreateRequestFilterByHandle(
    IRequestFilterPtr nextFilter,
    ui64 handle);

IRequestFilterPtr CreateRequestFilterByRequestType(
    IRequestFilterPtr nextFilter,
    TSet<ui32> requestTypes);

IRequestFilterPtr CreateRequestFilterByRange(
    IRequestFilterPtr nextFilter,
    ui64 start,
    ui64 count,
    ui32 blockSize);

IRequestFilterPtr CreateRequestFilterSince(
    IRequestFilterPtr nextFilter,
    ui64 timestampMcs);

IRequestFilterPtr CreateRequestFilterUntil(
    IRequestFilterPtr nextFilter,
    ui64 timestampMcs);

}   // namespace NCloud::NFileStore
