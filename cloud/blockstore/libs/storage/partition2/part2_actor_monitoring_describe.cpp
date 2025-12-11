#include "part2_actor.h"

#include <cloud/blockstore/libs/storage/core/probes.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/string.h>
#include <util/stream/str.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

using namespace NCloud::NStorage;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

using namespace NMonitoringUtils;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDescribeRangeVisitor final
    : public IFreshBlockVisitor
    , public IMergedBlockVisitor
{
private:
    TTxPartition::TDescribeRange& Args;

public:
    TDescribeRangeVisitor(TTxPartition::TDescribeRange& args)
        : Args(args)
    {}

    void Visit(const TBlock& block, TStringBuf blockContent) override
    {
        Y_UNUSED(blockContent);
        AddBlock(block, {}, 0);
    }

    void Visit(
        const TBlock& block,
        const TPartialBlobId& blobId,
        ui16 blobOffset) override
    {
        AddBlock(block, blobId, blobOffset);
    }

private:
    void
    AddBlock(const TBlock& block, const TPartialBlobId& blobId, ui16 blobOffset)
    {
        Y_ABORT_UNLESS(Args.BlockRange.Contains(block.BlockIndex));
        Args.Blocks.emplace_back(block, blobId, blobOffset);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDescribeBlobVisitor final: public IMergedBlockVisitor
{
private:
    TTxPartition::TDescribeBlob& Args;

public:
    TDescribeBlobVisitor(TTxPartition::TDescribeBlob& args)
        : Args(args)
    {}

    void Visit(
        const TBlock& block,
        const TPartialBlobId& blobId,
        ui16 blobOffset) override
    {
        Y_UNUSED(blobId);
        Args.Blocks.emplace_back(block, Args.BlobId, blobOffset);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareDescribeRange(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TDescribeRange& args)
{
    Y_UNUSED(ctx);

    TPartitionDatabase db(tx.DB);

    TDescribeRangeVisitor visitor(args);

    if (!State->InitIndex(db, args.BlockRange)) {
        return false;
    }

    State->FindFreshBlocks(args.BlockRange, visitor);

    return State->FindMergedBlocks(db, args.BlockRange, visitor);
}

void TPartitionActor::ExecuteDescribeRange(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TDescribeRange& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TPartitionActor::CompleteDescribeRange(
    const TActorContext& ctx,
    TTxPartition::TDescribeRange& args)
{
    Sort(
        args.Blocks,
        [](const auto& l, const auto& r)
        {
            // order by (BlockIndex ASC, MinCommitId DESC, BlobOffset ASC)
            return l.Block.BlockIndex < r.Block.BlockIndex ||
                   (l.Block.BlockIndex == r.Block.BlockIndex &&
                    (l.Block.MinCommitId > r.Block.MinCommitId ||
                     (l.Block.MinCommitId == r.Block.MinCommitId &&
                      l.BlobOffset < r.BlobOffset)));
        });

    TStringStream out;
    DumpDefaultHeader(out, *Info(), SelfId().NodeId(), *DiagnosticsConfig);
    DumpDescribeHeader(out, *Info());

    HTML (out) {
        TABLE_SORTABLE()
        {
            TABLEHEAD () {
                TABLER () {
                    TABLED () {
                        out << "# Block";
                    }
                    TABLED () {
                        out << "MinCommitId";
                    }
                    TABLED () {
                        out << "MaxCommitId";
                    }
                    TABLED () {
                        out << "BlobId";
                    }
                    TABLED () {
                        out << "Offset";
                    }
                }
            }
            TABLEBODY()
            {
                auto dump = [&](const auto& ref)
                {
                    TABLER () {
                        TABLED_CLASS("view")
                        {
                            DumpBlockIndex(
                                out,
                                *Info(),
                                ref.Block.BlockIndex,
                                ref.Block.MinCommitId);
                        }
                        TABLED () {
                            DumpCommitId(out, ref.Block.MinCommitId);
                        }
                        TABLED () {
                            DumpCommitId(out, ref.Block.MaxCommitId);
                        }
                        TABLED_CLASS("view")
                        {
                            DumpBlobId(out, *Info(), ref.BlobId);
                        }
                        TABLED () {
                            DumpBlobOffset(out, ref.BlobOffset);
                        }
                    }
                };

                size_t count = 0;
                for (const auto& ref: args.Blocks) {
                    dump(ref);
                    if (++count == State->GetMaxBlocksInBlob()) {
                        break;
                    }
                }
            }
        }
    }

    GenerateBlobviewJS(out);

    SendHttpResponse(ctx, *args.RequestInfo, std::move(out.Str()));
}

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareDescribeBlob(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TDescribeBlob& args)
{
    Y_UNUSED(ctx);

    TPartitionDatabase db(tx.DB);

    TMaybe<TBlockList> blockList;
    if (!db.ReadBlockList(args.BlobId, blockList)) {
        return false;
    }

    if (!blockList) {
        return true;
    }

    auto blocks = blockList->GetBlocks();
    auto blobRange = TBlockRange32::MakeClosedInterval(
        blocks.front().BlockIndex,
        blocks.back().BlockIndex);
    if (!State->InitIndex(db, blobRange)) {
        return false;
    }

    TDescribeBlobVisitor visitor(args);
    return State->FindMergedBlocks(db, {{{args.BlobId, 0}}, 0}, visitor);
}

void TPartitionActor::ExecuteDescribeBlob(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TDescribeBlob& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TPartitionActor::CompleteDescribeBlob(
    const TActorContext& ctx,
    TTxPartition::TDescribeBlob& args)
{
    using namespace NMonitoringUtils;

    TStringStream out;
    DumpDefaultHeader(out, *Info(), SelfId().NodeId(), *DiagnosticsConfig);
    DumpDescribeHeader(out, *Info());

    HTML (out) {
        TABLE_SORTABLE()
        {
            TABLEHEAD () {
                TABLER () {
                    TABLED () {
                        out << "# Block";
                    }
                    TABLED () {
                        out << "MinCommitId";
                    }
                    TABLED () {
                        out << "MaxCommitId";
                    }
                    TABLED () {
                        out << "Offset";
                    }
                }
            }
            TABLEBODY()
            {
                auto dump = [&](const auto& ref)
                {
                    TABLER () {
                        TABLED_CLASS("view")
                        {
                            DumpBlockIndex(
                                out,
                                *Info(),
                                ref.Block.BlockIndex,
                                ref.Block.MinCommitId);
                        }
                        TABLED () {
                            DumpCommitId(out, ref.Block.MinCommitId);
                        }
                        TABLED () {
                            DumpCommitId(out, ref.Block.MaxCommitId);
                        }
                        TABLED () {
                            DumpBlobOffset(out, ref.BlobOffset);
                        }
                    }
                };

                size_t count = 0;
                for (const auto& ref: args.Blocks) {
                    dump(ref);
                    if (++count == State->GetMaxBlocksInBlob()) {
                        break;
                    }
                }
            }
        }
    }
    GenerateBlobviewJS(out);

    SendHttpResponse(ctx, *args.RequestInfo, std::move(out.Str()));
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleHttpInfo_Describe(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    if (const auto& range = params.Get("range")) {
        TBlockRange32 blockRange;
        if (TBlockRange32::TryParse(range, blockRange)) {
            ExecuteTx<TDescribeRange>(ctx, std::move(requestInfo), blockRange);
        } else {
            TString message = "invalid range specified: " + range.Quote();
            RejectHttpRequest(ctx, *requestInfo, std::move(message));
        }
        return;
    }

    if (const auto& blob = params.Get("blob")) {
        TLogoBlobID blobId;
        TString errorExplanation;
        if (TLogoBlobID::Parse(blobId, blob, errorExplanation)) {
            ExecuteTx<TDescribeBlob>(
                ctx,
                std::move(requestInfo),
                MakePartialBlobId(blobId));
        } else {
            TStringStream out;
            out << "invalid blob specified: " << blob.Quote()
                << "(" + errorExplanation + ")";
            RejectHttpRequest(ctx, *requestInfo, std::move(out.Str()));
        }
        return;
    }

    TStringStream out;
    DumpDefaultHeader(out, *Info(), SelfId().NodeId(), *DiagnosticsConfig);
    DumpDescribeHeader(out, *Info());

    SendHttpResponse(ctx, *requestInfo, std::move(out.Str()));
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
