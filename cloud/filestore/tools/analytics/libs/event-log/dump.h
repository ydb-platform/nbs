#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

class IOutputStream;

namespace NCloud::NFileStore {

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

class TProfileLogRecord;

}   // namespace NProto

////////////////////////////////////////////////////////////////////////////////

struct TRequestTypeInfo
{
    const ui32 Id;
    const TString Name;

    TRequestTypeInfo(ui32 id, TString name)
        : Id(id)
        , Name(std::move(name))
    {}
};

////////////////////////////////////////////////////////////////////////////////

TVector<ui32> GetItemOrder(const NProto::TProfileLogRecord& record);

void DumpRequest(
    const NProto::TProfileLogRecord& record,
    int i,
    IOutputStream* out);

void DumpDiscardedRequestCount(
    const NProto::TProfileLogRecord& record,
    IOutputStream* out);

TString RequestName(const ui32 requestType);
TVector<TRequestTypeInfo> GetRequestTypes();

}   // namespace NCloud::NFileStore
