#include "part_actor.h"

#include <cloud/blockstore/libs/storage/core/probes.h>

#include <contrib/ydb/core/base/logoblob.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <util/generic/string.h>
#include <util/stream/str.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

using namespace NMonitoringUtils;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDescribeRangeVisitor final
    : public IFreshBlocksIndexVisitor
    , public IBlocksIndexVisitor
    , public IMixedBlocksIndexVisitor
{
private:
    TTxPartition::TDescribeRange& Args;

public:
    TDescribeRangeVisitor(TTxPartition::TDescribeRange& args)
        : Args(args)
    {}

    bool Visit(const TFreshBlock& block) override
    {
        Args.MarkBlock(
            block.Meta.BlockIndex,
            block.Meta.CommitId,
            {},
            block.Content ? 0 : InvalidBlobOffset
        );

        return true;
    }

    bool Visit(
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset) override
    {
        Args.MarkBlock(blockIndex, commitId, blobId, blobOffset);
        return true;
    }

    bool VisitBlock(
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset,
        ui8 compactionRangeCount) override
    {
        Y_UNUSED(compactionRangeCount);

        Args.MarkBlock(blockIndex, commitId, blobId, blobOffset);
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDescribeBlobVisitor final
    : public IFreshBlocksIndexVisitor
    , public IExtendedBlocksIndexVisitor
{
private:
    TTxPartition::TDescribeBlob& Args;

public:
    TDescribeBlobVisitor(TTxPartition::TDescribeBlob& args)
        : Args(args)
    {}

    bool Visit(const TFreshBlock& block) override
    {
        if (block.BlobId != Args.BlobId) {
            return true;
        }

        Args.MarkBlock(
            block.Meta.BlockIndex,
            block.Meta.CommitId,
            0, // blobOffset
            0  // checksum
        );
        return true;
    }

    bool Visit(
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset,
        ui32 checksum) override
    {
        if (blobId != Args.BlobId) {
            return true;
        }

        Args.MarkBlock(blockIndex, commitId, blobOffset, checksum);
        return true;
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

    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    bool ready = true;

    TDescribeRangeVisitor visitor(args);
    if (!args.BlockFilter || args.BlockFilter.find('f') != TString::npos) {
        State->FindFreshBlocks(
            visitor,
            args.BlockRange,
            /*maxCommitId=*/Max<ui64>());
    }
    if (!args.BlockFilter || args.BlockFilter.find('m') != TString::npos) {
        ready &= db.FindMixedBlocks(
            visitor,
            args.BlockRange,
            true    // precharge
        );
    }
    if (!args.BlockFilter || args.BlockFilter.find('M') != TString::npos) {
        ready &= db.FindMergedBlocks(
            visitor,
            args.BlockRange,
            true,   // precharge
            State->GetMaxBlocksInBlob()
        );
    }

    return ready;
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
    using namespace NMonitoringUtils;

    auto cmp = [] (
        const TTxPartition::TDescribeRange::TBlockMark& l,
        const TTxPartition::TDescribeRange::TBlockMark& r)
    {
        if (l.BlockIndex != r.BlockIndex) {
            return l.BlockIndex < r.BlockIndex;
        }

        if (l.CommitId != r.CommitId) {
            // last entries go first
            return l.CommitId > r.CommitId;
        }

        return l.BlobOffset < r.BlobOffset;
    };

    Sort(args.BlockMarks, cmp);

    TStringStream out;
    DumpDefaultHeader(out, *Info(), SelfId().NodeId(), *DiagnosticsConfig);
    DumpDescribeHeader(out, *Info());

    HTML(out) {
        const auto& cm = State->GetCompactionMap();
        const auto groupStart =
            cm.GetGroupStart(args.BlockRange.Start, State->GetBlockSize());

        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                TABLER() {
                    TABLED() { out << "GroupStart"; }
                    TABLED() {
                        out << groupStart;
                    }
                }
            }
        }

        auto outputStat = [&] (const auto& rangeStat, ui32 blockIndex) {
            TABLE_CLASS("table table-condensed") {
                TABLEBODY() {
                    TABLER() {
                        TABLED() { out << "BlockIndex"; }
                        TABLED() {
                            out << blockIndex;
                        }
                    }
                    TABLER() {
                        TABLED() { out << "BlobCount"; }
                        TABLED() { out << rangeStat.BlobCount; }
                    }
                    TABLER() {
                        TABLED() { out << "BlockCount"; }
                        TABLED() { out << rangeStat.BlockCount; }
                    }
                    TABLER() {
                        TABLED() { out << "UsedBlockCount"; }
                        TABLED() { out << rangeStat.UsedBlockCount; }
                    }
                    TABLER() {
                        TABLED() { out << "ReadRequestCount"; }
                        TABLED() { out << rangeStat.ReadRequestCount; }
                    }
                    TABLER() {
                        TABLED() { out << "ReadRequestBlobCount"; }
                        TABLED() { out << rangeStat.ReadRequestBlobCount; }
                    }
                    TABLER() {
                        TABLED() { out << "ReadRequestBlockCount"; }
                        TABLED() { out << rangeStat.ReadRequestBlockCount; }
                    }
                    TABLER() {
                        TABLED() { out << "Compacted"; }
                        TABLED() { out << rangeStat.Compacted; }
                    }
                    TABLER() {
                        TABLED() { out << "Score"; }
                        TABLED() { out << rangeStat.CompactionScore.Score; }
                    }
                }
            }
        };

        TAG(TH3) { out << "RangeStat (current)"; }
        outputStat(cm.Get(args.BlockRange.Start), args.BlockRange.Start);
        TAG(TH3) { out << "RangeStat (top by garbage)"; }
        const auto& topByGarbage = cm.GetTopByGarbageBlockCount();
        outputStat(topByGarbage.Stat, topByGarbage.BlockIndex);

        TABLE_SORTABLE() {
            TABLEHEAD() {
                TABLER() {
                    TABLED() { out << "# Block"; }
                    TABLED() { out << "CommitId"; }
                    TABLED() { out << "BlobId"; }
                    TABLED() { out << "Offset"; }
                }
            }
            TABLEBODY() {
                auto dump = [&] (const TTxPartition::TDescribeRange::TBlockMark& mark) {
                    TABLER() {
                        TABLED_CLASS("view") { DumpBlockIndex(out, *Info(), mark.BlockIndex, mark.CommitId); }
                        TABLED() { DumpCommitId(out, mark.CommitId); }
                        TABLED_CLASS("view") { DumpBlobId(out, *Info(), mark.BlobId); }
                        TABLED() { DumpBlobOffset(out, mark.BlobOffset); }
                    }
                };

                size_t count = 0;
                for (const auto& mark: args.BlockMarks) {
                    dump(mark);
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

    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    TDescribeBlobVisitor visitor(args);
    State->FindFreshBlocks(
        visitor,
        TBlockRange32::Max(),
        Max<ui64>()  // maxCommitId
    );
    return db.FindBlocksInBlobsIndex(
        visitor,
        State->GetMaxBlocksInBlob(),
        args.BlobId);
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
    if (!args.HttpInfo) {
        auto response = std::make_unique<TEvVolume::TEvDescribeBlobResponse>();
        for (const auto& mark: args.BlockMarks) {
            auto* block = response->Record.AddBlocks();
            block->SetBlockIndex(mark.BlockIndex);
            block->SetCommitId(mark.CommitId);
            block->SetBlobOffset(mark.BlobOffset);
            block->SetChecksum(mark.Checksum);
        }
        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        return;
    }

    using namespace NMonitoringUtils;

    TStringStream out;
    DumpDefaultHeader(out, *Info(), SelfId().NodeId(), *DiagnosticsConfig);
    DumpDescribeHeader(out, *Info());

    HTML(out) {
        TABLE_SORTABLE() {
            TABLEHEAD() {
                TABLER() {
                    TABLED() { out << "# Block"; }
                    TABLED() { out << "Offset"; }
                    TABLED() { out << "Checksum"; }
                }
            }
            TABLEBODY() {
                using TMark = TTxPartition::TDescribeBlob::TBlockMark;
                auto dump = [&] (const TMark& mark) {
                    TABLER() {
                        TABLED_CLASS("view") {
                            DumpBlockIndex(
                                out,
                                *Info(),
                                mark.BlockIndex,
                                mark.CommitId);
                        }
                        TABLED() {
                            DumpBlobOffset(out, mark.BlobOffset);
                        }
                        TABLED() {
                            out << mark.Checksum;
                        }
                    }
                };

                size_t count = 0;
                for (const auto& mark: args.BlockMarks) {
                    dump(mark);
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

void TPartitionActor::HandleDescribeBlob(
    const TEvVolume::TEvDescribeBlobRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "DescribeBlob",
        requestInfo->CallContext->RequestId);

    const auto blobId = LogoBlobIDFromLogoBlobID(msg->Record.GetBlobId());
    if (!blobId) {
        auto response = std::make_unique<TEvVolume::TEvDescribeBlobResponse>(
            MakeError(E_ARGUMENT, "invalid blob id in DescribeBlob request"));
        NCloud::Reply(ctx, *requestInfo, std::move(response));
        return;
    }

    ExecuteTx(
        ctx,
        CreateTx<TDescribeBlob>(
            std::move(requestInfo),
            MakePartialBlobId(blobId)));
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
            ExecuteTx(
                ctx,
                CreateTx<TDescribeRange>(
                    std::move(requestInfo),
                    blockRange,
                    params.Get("blockfilter")));
        } else {
            TString message = "invalid range specified: " + range.Quote();
            RejectHttpRequest(
                ctx,
                *requestInfo,
                std::move(message));
        }
        return;
    }

    if (const auto& blob = params.Get("blob")) {
        TLogoBlobID blobId;
        TString errorExplanation;
        if (TLogoBlobID::Parse(blobId, blob, errorExplanation)) {
            ExecuteTx(
                ctx,
                CreateTx<TDescribeBlob>(
                    std::move(requestInfo),
                    MakePartialBlobId(blobId),
                    true  // httpInfo
                )
            );
        } else {
            TStringBuilder message;
            message << "invalid blob specified: " + blob.Quote() +
                "(" + errorExplanation + ")";
            RejectHttpRequest(
                ctx,
                *requestInfo,
                std::move(message));
        }
        return;
    }

    TStringStream out;
    DumpDefaultHeader(out, *Info(), SelfId().NodeId(), *DiagnosticsConfig);
    DumpDescribeHeader(out, *Info());

    SendHttpResponse(ctx, *requestInfo, std::move(out.Str()));
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
