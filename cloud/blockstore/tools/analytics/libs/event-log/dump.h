#pragma once

#include <util/generic/vector.h>

class IOutputStream;

namespace NCloud::NBlockStore {

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

class TProfileLogRecord;

}   // namespace NProto

////////////////////////////////////////////////////////////////////////////////

enum class EItemType
{
    Request,
    BlockInfo,
    BlockCommitId,
    BlobUpdate
};

struct TItemDescriptor
{
    EItemType Type;
    int Index;

    TItemDescriptor(EItemType type, int index)
        : Type(type)
        , Index(index)
    {}
};

TVector<TItemDescriptor> GetItemOrder(const NProto::TProfileLogRecord& record);

void DumpRequest(
    const NProto::TProfileLogRecord& record,
    int i,
    IOutputStream* out);

void DumpBlockInfoList(
    const NProto::TProfileLogRecord& record,
    int i,
    IOutputStream* out);

void DumpBlockCommitIdList(
    const NProto::TProfileLogRecord& record,
    int i,
    IOutputStream* out);

void DumpBlobUpdateList(
    const NProto::TProfileLogRecord& record,
    int i,
    IOutputStream* out);

TString RequestName(const ui32 requestType);

}   // namespace NCloud::NBlockStore
