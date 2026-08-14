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

// Translates legacy profile log request type ids, written by older versions
// via the removed EFileStoreFuseRequest enum (Flush = 1001, Fsync = 1002,
// FsyncDir = 1003), to the current EFileStoreRequest values. Returns other
// values unchanged.
// TODO(#6799): this function (and the support for the legacy values) should be
// deleted after all profile logs written by older versions have been
// rotated.
ui32 NormalizeRequestTypeLegacy(ui32 requestType);
TVector<TRequestTypeInfo> GetRequestTypes();

}   // namespace NCloud::NFileStore
