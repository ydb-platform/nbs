#include "part_actor.h"

#include <cloud/blockstore/libs/storage/core/probes.h>

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

template <bool Index>
class TCheckIndexVisitor final
    : public IBlocksIndexVisitor
    , public IExtendedBlocksIndexVisitor
{
private:
    TTxPartition::TCheckIndex& Args;

public:
    TCheckIndexVisitor(TTxPartition::TCheckIndex& args)
        : Args(args)
    {}

    bool Visit(
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset) override
    {
        if (Index) {
            Args.MarkBlock_Index(blockIndex, commitId, blobId, blobOffset);
        } else {
            Args.MarkBlock_Blobs(blockIndex, commitId, blobId, blobOffset);
        }

        return true;
    }

    bool Visit(
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset,
        ui32 checksum) override
    {
        Y_UNUSED(checksum);

        return Visit(blockIndex, commitId, blobId, blobOffset);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void CheckIndexIntegrity(TTxPartition::TCheckIndex& args, T&& errorHandler)
{
    auto comparer = [] (
        const TTxPartition::TCheckIndex::TBlockMark& l,
        const TTxPartition::TCheckIndex::TBlockMark& r)
    {
        if (l.BlockIndex == r.BlockIndex) {
            if (l.CommitId == r.CommitId) {
                return l.BlobOffset < r.BlobOffset;
            } else {
                // last entries goes first
                return l.CommitId > r.CommitId;
            }
        } else {
            return l.BlockIndex < r.BlockIndex;
        }
    };

    Sort(args.BlockMarks_Index, comparer);
    Sort(args.BlockMarks_Blobs, comparer);

    using TBlockMarkList = TVector<TTxPartition::TCheckIndex::TBlockMark>;

    auto advance = [] (
        TBlockMarkList::iterator& it,
        TBlockMarkList::iterator end)
    {
        // skip older versions
        ui32 blockIndex = it->BlockIndex;
        do {
            ++it;
        } while (it != end && blockIndex == it->BlockIndex);
    };

    auto l = args.BlockMarks_Index.begin(), lend = args.BlockMarks_Index.end();
    auto r = args.BlockMarks_Blobs.begin(), rend = args.BlockMarks_Blobs.end();

    for (;;) {
        if (l == lend) {
            while (r != rend) {
                if (!errorHandler(*r, "missing")) {
                    break;
                }
                advance(r, rend);
            }
            break;
        }

        if (r == rend) {
            while (l != lend) {
                if (!IsDeletionMarker(l->BlobId)) {
                    if (!errorHandler(*l, "phantom")) {
                        break;
                    }
                }
                advance(l, lend);
            }
            break;
        }

        if (l->BlockIndex < r->BlockIndex) {
            if (!IsDeletionMarker(l->BlobId)) {
                if (!errorHandler(*l, "phantom")) {
                    break;
                }
            }
            advance(l, lend);
        } else if (l->BlockIndex > r->BlockIndex) {
            if (!errorHandler(*r, "missing")) {
                break;
            }
            advance(r, rend);
        } else {
            // find unmatched entries
            if (l->BlobId != r->BlobId) {
                if (!errorHandler(*r, "mismatched")) {
                    break;
                }
            }
            advance(l, lend);
            advance(r, rend);
        }
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareCheckIndex(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TCheckIndex& args)
{
    Y_UNUSED(ctx);

    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    TCheckIndexVisitor<true> visitorIndex(args);
    auto ready = db.FindMixedBlocks(
        visitorIndex,
        args.BlockRange,
        true    // precharge
    );
    ready &= db.FindMergedBlocks(
        visitorIndex,
        args.BlockRange,
        true,   // precharge
        State->GetMaxBlocksInBlob()
    );

    TCheckIndexVisitor<false> visitorBlobs(args);
    ready &= db.FindBlocksInBlobsIndex(
        visitorBlobs,
        State->GetMaxBlocksInBlob(),
        args.BlockRange);

    return ready;
}

void TPartitionActor::ExecuteCheckIndex(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TCheckIndex& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TPartitionActor::CompleteCheckIndex(
    const TActorContext& ctx,
    TTxPartition::TCheckIndex& args)
{
    using namespace NMonitoringUtils;

    TStringStream out;
    DumpCheckHeader(out, *Info());

    HTML(out) {
        TABLE_SORTABLE() {
            TABLEHEAD() {
                TABLER() {
                    TABLED() { out << "# Block"; }
                    TABLED() { out << "CommitId"; }
                    TABLED() { out << "BlobId"; }
                    TABLED() { out << "Offset"; }
                    TABLED() { out << "Error"; }
                }
            }
            TABLEBODY() {
                size_t rowsCount = 0;

                auto errorHandler = [&] (
                    const TTxPartition::TCheckIndex::TBlockMark& mark,
                    TStringBuf error)
                {
                    TABLER() {
                        TABLED_CLASS("view") {
                            DumpBlockIndex(out, *Info(), mark.BlockIndex, mark.CommitId);
                        }
                        TABLED() {
                            DumpCommitId(out, mark.CommitId);
                        }
                        TABLED_CLASS("view") {
                            DumpBlobId(out, *Info(), mark.BlobId);
                        }
                        TABLED() {
                            DumpBlobOffset(out, mark.BlobOffset);
                        }
                        TABLED() {
                            out << error;
                        }
                    }

                    return ++rowsCount < State->GetMaxBlocksInBlob();
                };

                CheckIndexIntegrity(args, errorHandler);
            }
        }
    }

    auto response = std::make_unique<NMon::TEvRemoteHttpInfoRes>(out.Str());

    LWTRACK(
        ResponseSent_Partition,
        args.RequestInfo->CallContext->LWOrbit,
        "HttpInfo",
        args.RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    RemoveTransaction(*args.RequestInfo);
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleHttpInfo_Check(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    if (const auto& range = params.Get("range")) {
        TBlockRange32 blockRange;
        if (TBlockRange32::TryParse(range, blockRange)) {
            AddTransaction(
                *requestInfo,
                ETransactionType::CheckIndex,
                [](const NActors::TActorContext&, TRequestInfo&) {});

            ExecuteTx<TCheckIndex>(
                ctx,
                std::move(requestInfo),
                blockRange);
        } else {
            TString message = "invalid range specified: " + range.Quote();
            SendHttpResponse(
                ctx,
                *requestInfo,
                std::move(message),
                EAlertLevel::DANGER);
        }
        return;
    }

    using namespace NMonitoringUtils;

    TStringStream out;
    DumpDefaultHeader(out, *Info(), SelfId().NodeId(), *DiagnosticsConfig);
    DumpCheckHeader(out, *Info());

    SendHttpResponse(ctx, *requestInfo, std::move(out.Str()));
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
