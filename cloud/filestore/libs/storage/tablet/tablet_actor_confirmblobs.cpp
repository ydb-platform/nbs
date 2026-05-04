#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/service/context.h>

#include <cloud/storage/core/libs/kikimr/helpers.h>
#include <cloud/storage/core/libs/tablet/blob_id.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

#include <util/generic/algorithm.h>
#include <util/generic/cast.h>
#include <util/generic/hash_set.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/system/datetime.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TBlobRequest
{
    TPartialBlobId BlobId;
    TActorId Proxy;

    TBlobRequest(const TPartialBlobId& blobId, TActorId proxy)
        : BlobId(blobId)
        , Proxy(proxy)
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TConfirmBlobsActor final: public TActorBootstrapped<TConfirmBlobsActor>
{
private:
    const ui64 StartCycleCount = GetCycleCount();
    const TString LogTag;
    const ui64 TabletId;
    const TActorId Tablet;
    const TVector<TBlobRequest> Requests;

    size_t RequestsCompleted = 0;

    NProto::TError Error;
    TVector<TPartialBlobId> UnrecoverableBlobs;

public:
    TConfirmBlobsActor(
        ui64 tabletId,
        const TActorId& tablet,
        TVector<TBlobRequest> requests);

    void Bootstrap(const TActorContext& ctx);

private:
    void NotifyAndDie(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleGetResult(
        const TEvBlobStorage::TEvGetResult::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TConfirmBlobsActor::TConfirmBlobsActor(
    ui64 tabletId,
    const TActorId& tablet,
    TVector<TBlobRequest> requests)
    : LogTag(TStringBuilder() << "[t:" << tabletId << "]")
    , TabletId(tabletId)
    , Tablet(tablet)
    , Requests(std::move(requests))
{}

void TConfirmBlobsActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    for (size_t i = 0; i < Requests.size(); ++i) {
        auto blobId = NCloud::MakeBlobId(TabletId, Requests[i].BlobId);
        auto request = std::make_unique<TEvBlobStorage::TEvGet>(
            blobId,
            0,   // shift
            0,   // size
            TInstant::Max(),
            NKikimrBlobStorage::FastRead,
            true,   // mustRestoreFirst
            true    // isIndexOnly
        );

        SendToBSProxy(
            ctx,
            Requests[i].Proxy,
            request.release(),
            i   // cookie
        );
    }
}

void TConfirmBlobsActor::NotifyAndDie(const TActorContext& ctx)
{
    auto ev = std::make_unique<TEvIndexTabletPrivate::TEvConfirmBlobsCompleted>(
        Error,
        StartCycleCount,
        std::move(UnrecoverableBlobs));
    NCloud::Send(ctx, Tablet, std::move(ev));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TConfirmBlobsActor::HandleGetResult(
    const TEvBlobStorage::TEvGetResult::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (msg->Status != NKikimrProto::OK) {
        Error = MakeError(E_REJECTED, msg->ErrorReason);
        NotifyAndDie(ctx);
        return;
    }

    if (msg->ResponseSz != 1) {
        Error = MakeError(E_FAIL, "response size is invalid");
        NotifyAndDie(ctx);
        return;
    }

    if (NCloud::IsUnrecoverable(msg->Responses[0].Status)) {
        ui32 requestIndex = ev->Cookie;
        TABLET_VERIFY(requestIndex < Requests.size());

        const auto& blobId = Requests[requestIndex].BlobId;
        UnrecoverableBlobs.push_back(blobId);
    }

    TABLET_VERIFY(RequestsCompleted < Requests.size());
    if (++RequestsCompleted < Requests.size()) {
        return;
    }

    NotifyAndDie(ctx);
}

void TConfirmBlobsActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TConfirmBlobsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvBlobStorage::TEvGetResult, HandleGetResult);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::TABLET_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::ConfirmBlobs(const TActorContext& ctx)
{
    if (UnconfirmedData.empty()) {
        BlobsConfirmed(ctx);
        return;
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::TABLET,
        "%s ConfirmBlobs: starting confirmation for %zu entries",
        LogTag.c_str(),
        UnconfirmedData.size());

    TVector<TBlobRequest> requests;

    for (const auto& [_, entry]: UnconfirmedData) {
        for (const auto& blobIdProto: entry.Data.GetBlobIds()) {
            auto blobId = LogoBlobIDFromLogoBlobID(blobIdProto);
            auto partialBlobId = TPartialBlobId(
                blobId.Generation(),
                blobId.Step(),
                blobId.Channel(),
                blobId.BlobSize(),
                blobId.Cookie(),
                blobId.PartId());

            auto proxy = Info()->BSProxyIDForChannel(
                partialBlobId.Channel(),
                partialBlobId.Generation());
            requests.emplace_back(partialBlobId, proxy);
        }
    }

    // Should not have records without blob ids
    if (requests.empty()) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::TABLET,
            "%s ConfirmBlobs: request list is empty for %zu unconfirmed "
            "entries",
            LogTag.c_str(),
            UnconfirmedData.size());

        UnconfirmedData.clear();
        BlobsConfirmed(ctx);
        return;
    }

    auto actor = NCloud::Register<TConfirmBlobsActor>(
        ctx,
        TabletID(),
        SelfId(),
        std::move(requests));

    WorkerActors.insert(actor);
}

void TIndexTabletActor::HandleConfirmBlobsCompleted(
    const TEvIndexTabletPrivate::TEvConfirmBlobsCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    const auto& error = msg->GetError();

    WorkerActors.erase(ev->Sender);

    if (HasError(error)) {
        const auto errorMessage = FormatError(error);
        ReportConfirmBlobsFailed(
            TStringBuilder()
            << "tabletId: " << TabletID() << ", error: " << errorMessage);

        BecomeAux(ctx, STATE_BROKEN);
        return;
    }

    THashSet<ui64> unrecoverableCommitIdsSet;
    unrecoverableCommitIdsSet.reserve(msg->UnrecoverableBlobs.size());

    if (!msg->UnrecoverableBlobs.empty()) {
        TStringStream ss;
        ss << "Unrecoverable blobs: ";
        for (const auto& blobId: msg->UnrecoverableBlobs) {
            ss << "[CommitId=" << blobId.CommitId() << ", BlobId=" << blobId
               << "] ";
            unrecoverableCommitIdsSet.insert(blobId.CommitId());
        }

        LOG_INFO(
            ctx,
            TFileStoreComponents::TABLET,
            "%s %s",
            LogTag.c_str(),
            ss.Str().c_str());
    }

    TVector<ui64> unrecoverableCommitIds;
    TVector<ui64> recoverableCommitIds;
    unrecoverableCommitIds.reserve(unrecoverableCommitIdsSet.size());
    recoverableCommitIds.reserve(UnconfirmedData.size());

    for (const auto& [commitId, _]: UnconfirmedData) {
        if (unrecoverableCommitIdsSet.contains(commitId)) {
            unrecoverableCommitIds.push_back(commitId);
        } else {
            recoverableCommitIds.push_back(commitId);
        }
    }

    if (!unrecoverableCommitIds.empty()) {
        // This Tx doesn't read from db and can't have page faults, so it's safe
        // to rely that it will succeed before any other write operations.
        ExecuteTx<TDeleteUnconfirmedData>(
            ctx,
            CreateRequestInfo(
                SelfId(),
                0 /* cookie */,
                MakeIntrusive<TCallContext>()),
            std::move(unrecoverableCommitIds));
    }

    if (recoverableCommitIds.empty()) {
        return BlobsConfirmed(ctx);
    }

    // Recovery must replay confirmations in commitId order to preserve write
    // order for overlapping ranges.
    Sort(recoverableCommitIds);

    for (ui64 commitId: recoverableCommitIds) {
        // TODO(#5353) Support out of order insertion to unblock here
        // imeadeately
        ConfirmData(commitId, ctx);
    }
}

void TIndexTabletActor::BlobsConfirmed(const TActorContext& ctx)
{
    LOG_INFO(
        ctx,
        TFileStoreComponents::TABLET,
        "%s ConfirmBlobs: recovery confirmation completed",
        LogTag.c_str());

    UnconfirmedRecoveryReady = true;
}

}   // namespace NCloud::NFileStore::NStorage
