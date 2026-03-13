#include "request.h"

#include <util/string/cast.h>

namespace NCloud::NBlockStore {

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

TWriteBlocksLocalRequest TWriteBlocksLocalRequest::Clone() const
{
    auto request = CreateDependentRequest();
    if (SglistOwner) {
        request.CopySglistIntoBuffers();
    }

    return request;
}

TWriteBlocksLocalRequest TWriteBlocksLocalRequest::CreateDependentRequest() const
{
    TWriteBlocksLocalRequest copiedRecord;

    // Copy all protobuf fields except Blocks (field 4)
    copiedRecord.MutableHeaders()->CopyFrom(this->GetHeaders());
    copiedRecord.SetDiskId(this->GetDiskId());
    copiedRecord.SetStartIndex(this->GetStartIndex());
    // Skip Blocks field - it's accessed via Sglist
    copiedRecord.SetFlags(this->GetFlags());
    copiedRecord.SetSessionId(this->GetSessionId());
    copiedRecord.SetBlockSize(this->GetBlockSize());
    copiedRecord.MutableChecksums()->CopyFrom(this->GetChecksums());

    // Copy non-protobuf fields
    copiedRecord.Sglist = Sglist;
    copiedRecord.BlocksCount = BlocksCount;
    copiedRecord.BlockSize = BlockSize;

    return copiedRecord;
}

void TWriteBlocksLocalRequest::CopySglistIntoBuffers()
{
    auto g = Sglist.Acquire();
    if (!g) {
        return;
    }

    const auto& sgList = g.Get();
    TSgList newSgList;
    newSgList.reserve(sgList.size());
    for (const auto& block: sgList) {
        auto& buffer = *MutableBlocks()->AddBuffers();
        buffer.ReserveAndResize(block.Size());
        memcpy(buffer.begin(), block.Data(), block.Size());
        newSgList.emplace_back(buffer.data(), buffer.size());
    }

    TGuardedSgList newGuardedSgList(std::move(newSgList));
    Sglist = newGuardedSgList;
    SglistOwner.emplace(std::move(newGuardedSgList));
}

}   // namespace NProto

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

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                     \
    template <>                                                  \
    TString GetBlockStoreRequestName<NProto::T##name##Request>() \
    {                                                            \
        return #name;                                            \
    }

BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD

////////////////////////////////////////////////////////////////////////////////

TString GetSysRequestName(ESysRequestType requestType)
{
    return ToString(requestType);
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
