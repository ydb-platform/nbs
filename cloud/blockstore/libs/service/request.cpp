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

#define COPY_WRITE_BLOCKS_STRUCT_FIELD(field)                    \
    copiedRecord.Mutable##field()->CopyFrom(this->Get##field())  \
// COPY_WRITE_BLOCKS_FIELD

#define COPY_WRITE_BLOCKS_FIELD(field)                           \
    copiedRecord.Set##field(this->Get##field())                  \
// COPY_WRITE_BLOCKS_FIELD

    COPY_WRITE_BLOCKS_STRUCT_FIELD(Headers);
    COPY_WRITE_BLOCKS_FIELD(DiskId);
    COPY_WRITE_BLOCKS_FIELD(StartIndex);
    COPY_WRITE_BLOCKS_FIELD(Flags);
    COPY_WRITE_BLOCKS_FIELD(SessionId);
    COPY_WRITE_BLOCKS_FIELD(BlockSize);
    COPY_WRITE_BLOCKS_STRUCT_FIELD(Checksums);

#undef COPY_WRITE_BLOCKS_STRUCT_FIELD
#undef COPY_WRITE_BLOCKS_FIELD

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
