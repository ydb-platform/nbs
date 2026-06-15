#include "request.h"

#include <util/string/cast.h>

namespace NCloud::NBlockStore {

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

TWriteBlocksLocalRequest::~TWriteBlocksLocalRequest()
{
    if (OwnsSglist && !Sglist.Empty()) {
        Sglist.Close();
    }
}

TWriteBlocksLocalRequest& TWriteBlocksLocalRequest::operator=(
    TWriteBlocksLocalRequest&& request) noexcept
{
    if (this == &request) {
        return *this;
    }
    if (OwnsSglist && !Sglist.Empty()) {
        Sglist.Close();
    }
    TWriteBlocksRequest::operator=(std::move(request));
    Sglist = std::move(request.Sglist);
    BlocksCount = request.BlocksCount;
    OwnsSglist = request.OwnsSglist;

    return *this;
}

TWriteBlocksLocalRequest TWriteBlocksLocalRequest::Clone() const
{
    auto request = CreateDependentRequest();
    if (OwnsSglist) {
        request.TakeOwnershipOfData();
    }

    return request;
}

TWriteBlocksLocalRequest
TWriteBlocksLocalRequest::CreateDependentRequest() const
{
    Y_ABORT_UNLESS(GetDescriptor()->field_count() == 8);

    TWriteBlocksLocalRequest copiedRecord;

    // Copy all protobuf fields except Blocks (field 4)
    copiedRecord.MutableHeaders()->CopyFrom(GetHeaders());
    copiedRecord.SetDiskId(GetDiskId());
    copiedRecord.SetStartIndex(GetStartIndex());
    copiedRecord.SetFlags(GetFlags());
    copiedRecord.SetSessionId(GetSessionId());
    copiedRecord.SetBlockSize(GetBlockSize());
    copiedRecord.MutableChecksums()->CopyFrom(GetChecksums());

    // Copy non-protobuf fields
    copiedRecord.Sglist = Sglist;
    copiedRecord.BlocksCount = BlocksCount;

    return copiedRecord;
}

void TWriteBlocksLocalRequest::TakeOwnershipOfData()
{
    // After std::move, GuardedObject is null and Acquire() would abort.
    if (Sglist.Empty()) {
        return;
    }

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

    Sglist = TGuardedSgList(std::move(newSgList));
    OwnsSglist = true;
}

TWriteBlocksLocalRequest CopyRequest(const TWriteBlocksLocalRequest& request)
{
    return request.CreateDependentRequest();
}

TWriteBlocksRequest CopyRequest(const TWriteBlocksRequest& request)
{
    return request;
}

TZeroBlocksRequest CopyRequest(const TZeroBlocksRequest& request)
{
    return request;
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
