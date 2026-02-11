#include "fresh_blocks_writer_actor.h"
#include "cloud/blockstore/libs/storage/core/block_handler.h"

#include <cloud/blockstore/libs/storage/core/forward_helpers.h>

namespace NCloud::NBlockStore::NStorage::NFreshBlocksWriter {

using namespace NPartition;

namespace  {

////////////////////////////////////////////////////////////////////////////////

class TReadFreshBlocksVisitor final: public IFreshBlocksIndexVisitor
{
private:
    const TBlockRange64 ReadRange;
    const ui64 BlockSize;

    IReadBlocksHandlerPtr ReadBlocksHandler;

    TVector<ui64> BlockCommitIds;

public:
    explicit TReadFreshBlocksVisitor(
            TBlockRange64 readRange,
            ui64 blockSize,
            IReadBlocksHandlerPtr readBlocksHandler)
        : ReadRange(readRange)
        , BlockSize(blockSize)
        , ReadBlocksHandler(std::move(readBlocksHandler))
        , BlockCommitIds(ReadRange.Size(), 0)
    {}

    ui64& GetBlockCommitId(ui32 blockIndex)
    {
        Y_DEBUG_ABORT_UNLESS(ReadRange.Contains(blockIndex));
        return BlockCommitIds[blockIndex - ReadRange.Start];
    }

    bool Visit(const TFreshBlock& block) override
    {
        auto& blockCommitId = GetBlockCommitId(block.Meta.BlockIndex);

        if (block.Meta.CommitId > blockCommitId) {
            blockCommitId = block.Meta.CommitId;

            TBlockDataRef blockData = TBlockDataRef::CreateZeroBlock(BlockSize);
            if (!block.Content.empty()) {
                blockData = TBlockDataRef(block.Content.data(), block.Content.size());
            }

            ReadBlocksHandler->SetBlock(
                block.Meta.BlockIndex,
                blockData,
                false);
        }

        return true;
    }

    static std::pair<TVector<ui64>, NProto::TReadBlocksResponse> Finalize(
        TReadFreshBlocksVisitor&& visitor)
    {
        NProto::TReadBlocksResponse resp;
        visitor.ReadBlocksHandler->GetResponse(resp);

        return std::make_pair(
            std::move(visitor.BlockCommitIds),
            std::move(resp));
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TFreshBlocksWriterActor::HandleReadFreshBlocks(
    const TEvFreshBlocksWriter::TEvReadFreshBlocksRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto& record = msg->Record;

    auto range = TBlockRange32::WithLength(
        record.GetStartIndex(),
        record.GetBlocksCount());

    auto readHandler = CreateReadBlocksHandler(
        ConvertRangeSafe(range),
        PartitionConfig.GetBlockSize(),
        false);

    TReadFreshBlocksVisitor visitor(
        ConvertRangeSafe(range),
        PartitionConfig.GetBlockSize(),
        readHandler);
    FreshBlocksState->FindFreshBlocks(visitor, range, record.CommitId);

    auto [commitIds, rbResponse] =
        TReadFreshBlocksVisitor::Finalize(std::move(visitor));

    auto response =
        std::make_unique<TEvFreshBlocksWriter::TEvReadFreshBlocksResponse>();
    static_cast<NProto::TReadBlocksResponse&>(response->Record) =
        std::move(rbResponse);
    response->Record.BlockMarkCommitIds = std::move(commitIds);

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage::NFreshBlocksWriter
