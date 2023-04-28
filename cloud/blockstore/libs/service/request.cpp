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
