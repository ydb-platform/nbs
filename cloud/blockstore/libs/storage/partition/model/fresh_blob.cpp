#include "fresh_blob.h"

#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/ysaveload.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

using TFreshBlobFormatVersion = ui8;
using TProtobufSize = ui32;

static constexpr TFreshBlobFormatVersion FreshBlobFormatVersion = 1;
static constexpr bool IsStoredInDb = false;

namespace {

////////////////////////////////////////////////////////////////////////////////

TString BuildFreshBlobContent(
    const TVector<TBlockRange32>& blockRanges,
    const TVector<TGuardHolder>& guardHolders,
    bool isZero)
{
    TString result;
    TStringOutput so(result);

    Save(&so, FreshBlobFormatVersion);

    size_t contentSize = 0;

    for (const auto& guardHolder: guardHolders) {
        contentSize += SgListGetSize(guardHolder.GetSgList());
    }

    NProto::TFreshBlobMeta meta;
    for (const auto blockRange: blockRanges) {
        meta.AddStartIndices(blockRange.Start);
        meta.AddEndIndices(blockRange.End);
    }
    meta.SetIsZero(isZero);

    const TProtobufSize protoSize = meta.ByteSize();
    Save(&so, protoSize);

    size_t offset = result.size();

    result.ReserveAndResize(offset + protoSize + contentSize);
    Y_PROTOBUF_SUPPRESS_NODISCARD meta.SerializeToArray(
        const_cast<char*>(result.data()) + offset,
        protoSize);

    offset += protoSize;

    for (const auto& guardHolder: guardHolders) {
        const auto& sgList = guardHolder.GetSgList();
        const size_t sgListSize = SgListGetSize(sgList);
        SgListCopy(sgList, {result.data() + offset, sgListSize});
        offset += sgListSize;
    }

    return result;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TString BuildWriteFreshBlocksBlobContent(
    const TVector<TBlockRange32>& blockRanges,
    const TVector<TGuardHolder>& guardHolders)
{
    return BuildFreshBlobContent(blockRanges, guardHolders, false);
}

TString BuildZeroFreshBlocksBlobContent(TBlockRange32 blockRange)
{
    return BuildFreshBlobContent({blockRange}, {}, true);
}

NProto::TError ParseFreshBlobContent(
    const ui64 commitId,
    const ui32 blockSize,
    const TString& buffer,
    TVector<TOwningFreshBlock>& result)
{
    TStringInput si(buffer);

    TFreshBlobFormatVersion version = 0;
    Load(&si, version);
    if (version != FreshBlobFormatVersion) {
        return MakeError(E_FAIL, "wrong fresh blob version");
    }

    TProtobufSize protoSize = 0;
    Load(&si, protoSize);

    size_t offset = sizeof(version) + sizeof(protoSize);

    NProto::TFreshBlobMeta meta;
    if (!meta.ParseFromArray(buffer.data() + offset, protoSize)) {
        return MakeError(E_FAIL, "failed to parse fresh blob meta protobuf");
    }
    if (meta.StartIndicesSize() != meta.EndIndicesSize()) {
        return MakeError(E_FAIL, "StartIndicesSize != EndIndicesSize");
    }

    offset += protoSize;

    for (ui32 i = 0; i < meta.StartIndicesSize(); ++i) {
        auto start = meta.GetStartIndices(i);
        auto end = meta.GetEndIndices(i);

        for (auto blockIndex:
             xrange(TBlockRange32::MakeClosedInterval(start, end)))
        {
            auto block = TBlock(blockIndex, commitId, IsStoredInDb);

            if (meta.GetIsZero()) {
                result.emplace_back(block, TString());
                continue;
            }

            if (offset + blockSize > buffer.size()) {
                return MakeError(
                    E_FAIL,
                    TStringBuilder()
                        << "not enough blocks in fresh blob; #offset=" << offset
                        << " #blockSize=" << blockSize
                        << " #buffer.size()=" << buffer.size());
            }

            TString content(buffer.data() + offset, blockSize);
            result.emplace_back(block, std::move(content));

            offset += blockSize;
        }
    }

    Y_DEBUG_ABORT_UNLESS(offset == buffer.size());

    return {};
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
