#include "fresh_blob.h"

#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/ysaveload.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

using TFreshBlobFormatVersion = ui8;
using TProtobufSize = ui32;

static constexpr TFreshBlobFormatVersion FreshBlobFormatVersion = 1;

////////////////////////////////////////////////////////////////////////////////

NProto::TError ParseFreshBlobContent(
    ui64 commitId,
    ui32 blockSize,
    const TString& buffer,
    TVector<TOwningFreshBlock>& blocks,
    TBlobUpdatesByFresh& updates)
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

    NProto::TFreshBlobMeta2 meta;
    if (!meta.ParseFromArray(buffer.data() + offset, protoSize)) {
        return MakeError(E_FAIL, "failed to parse fresh blob meta protobuf");
    }
    if (meta.StartIndicesSize() != meta.EndIndicesSize()) {
        return MakeError(E_FAIL, "StartIndicesSize != EndIndicesSize");
    }
    if (meta.StartIndicesSize() != meta.DeletionIdsSize()) {
        return MakeError(E_FAIL, "StartIndicesSize != DeletionIdsSize");
    }

    offset += protoSize;

    for (ui32 i = 0; i < meta.StartIndicesSize(); ++i) {
        auto start = meta.GetStartIndices(i);
        auto end = meta.GetEndIndices(i);
        auto deletionId = meta.GetDeletionIds(i);

        auto blockRange = TBlockRange32::MakeClosedInterval(start, end);

        for (auto blockIndex: xrange(blockRange)) {
            auto block = TBlock(blockIndex, commitId, InvalidCommitId, false);

            if (offset + blockSize > buffer.size()) {
                return MakeError(
                    E_FAIL,
                    TStringBuilder()
                        << "not enough blocks in fresh blob; #offset=" << offset
                        << " #blockSize=" << blockSize
                        << " #buffer.size()=" << buffer.size());
            }

            TString content(buffer.data() + offset, blockSize);
            blocks.emplace_back(block, std::move(content));

            offset += blockSize;
        }

        if (!updates.emplace(blockRange, commitId, deletionId).second) {
            return MakeError(E_FAIL, "multiple deletions found");
        }
    }

    Y_DEBUG_ABORT_UNLESS(offset == buffer.size());

    return {};
}

TString BuildFreshBlobContent(
    const TVector<TBlockRange32>& blockRanges,
    const TVector<TGuardHolder>& guardHolders,
    ui64 firstRequestDeletionId)
{
    TString result;
    TStringOutput so(result);

    Save(&so, FreshBlobFormatVersion);

    size_t contentSize = 0;

    for (const auto& guardHolder: guardHolders) {
        contentSize += SgListGetSize(guardHolder.GetSgList());
    }

    NProto::TFreshBlobMeta2 meta;
    ui64 deletionId = firstRequestDeletionId;
    for (const auto& blockRange: blockRanges) {
        meta.AddStartIndices(blockRange.Start);
        meta.AddEndIndices(blockRange.End);
        meta.AddDeletionIds(deletionId++);
    }

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

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
