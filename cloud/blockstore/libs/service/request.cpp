#include "request.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DECLARE_METHOD(name, ...)    #name,

static const TString RequestNames[] = {
    BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)
};

#undef BLOCKSTORE_DECLARE_METHOD

////////////////////////////////////////////////////////////////////////////////

const TString& GetBlockStoreRequestName(EBlockStoreRequest request)
{
    if (request < EBlockStoreRequest::MAX) {
        return RequestNames[(int)request];
    }

    static const TString Unknown = "unknown";
    return Unknown;
}

TStringBuf GetSysRequestName(ESysRequestType requestType)
{
    switch (requestType) {
        case ESysRequestType::Compaction: return "Compaction";
        case ESysRequestType::Flush: return "Flush";
        case ESysRequestType::ConvertToMixedIndex: return "ConvertToMixedIndex";
        case ESysRequestType::ConvertToRangeMap: return "ConvertToRangeMap";
        case ESysRequestType::Cleanup: return "Cleanup";
        case ESysRequestType::Migration: return "Migration";
        case ESysRequestType::WriteDeviceBlocks: return "WriteDeviceBlocks";
        case ESysRequestType::ZeroDeviceBlocks: return "ZeroDeviceBlocks";
        case ESysRequestType::Resync: return "Resync";
        case ESysRequestType::ConfirmBlobs: return "ConfirmBlobs";
        default: return "unknown";
    }
}

TStringBuf GetPrivateRequestName(EPrivateRequestType requestType)
{
    switch (requestType) {
        case EPrivateRequestType::DescribeBlocks: return "DescribeBlocks";
        default: return "unknown";
    }
}

}   // namespace NCloud::NBlockStore

template <>
void Out<NCloud::NBlockStore::NProto::TReadBlocksLocalResponse>(
    IOutputStream& out,
    const NCloud::NBlockStore::NProto::TReadBlocksLocalResponse& value)
{
    out << value.ShortDebugString();

    out << " FailedInfo->FailedRanges: [";
    for (const auto& blob: value.FailInfo.FailedRanges) {
        out << blob << ", ";
    }
    out << "]";
}
