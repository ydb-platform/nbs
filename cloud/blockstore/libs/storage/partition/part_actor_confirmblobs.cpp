#include "part_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/core/public.h>

#include <cloud/storage/core/libs/common/alloc.h>
#include <cloud/storage/core/libs/tablet/blob_id.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

#include <util/system/datetime.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TRequest
{
    TPartialBlobId BlobId;
    TActorId Proxy;

    TRequest(const TPartialBlobId& blobId, TActorId proxy)
        : BlobId(blobId)
        , Proxy(proxy)
    {}
};

class TConfirmBlobsActor final
    : public TActorBootstrapped<TConfirmBlobsActor>
{
private:
    const ui64 StartCycleCount = GetCycleCount();
    const ui64 TabletId = 0;
    const TActorId Tablet;
    const TVector<TRequest> Requests;

    size_t RequestsCompleted = 0;

    NProto::TError Error;
    TVector<TPartialBlobId> UnrecoverableBlobs;

public:
    TConfirmBlobsActor(
        ui64 tabletId,
        const TActorId& tablet,
        TVector<TRequest> requests);

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
        TVector<TRequest> requests)
    : TabletId(tabletId)
    , Tablet(tablet)
    , Requests(std::move(requests))
{}

void TConfirmBlobsActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    for (size_t i = 0; i < Requests.size(); ++i) {
        auto request = std::make_unique<TEvBlobStorage::TEvGet>(
            MakeBlobId(TabletId, Requests[i].BlobId),
            0,  // shift
            0,  // size
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
    auto ev = std::make_unique<TEvPartitionPrivate::TEvConfirmBlobsCompleted>(
        std::move(Error),
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

    if (IsUnrecoverable(msg->Responses[0].Status)) {
        ui32 requestIndex = ev->Cookie;
        Y_ABORT_UNLESS(requestIndex < Requests.size());

        const auto& blobId = Requests[requestIndex].BlobId;
        UnrecoverableBlobs.push_back(blobId);
    }

    Y_DEBUG_ABORT_UNLESS(RequestsCompleted < Requests.size());
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
                TBlockStoreComponents::PARTITION_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::ConfirmBlobs(const TActorContext& ctx)
{
    if (State->GetUnconfirmedBlobs().empty()) {
        BlobsConfirmed(ctx);
        return;
    }

    LOG_INFO(ctx, TBlockStoreComponents::PARTITION,
        "[%lu][d:%s] ConfirmBlobs: register actor",
        TabletID(),
        PartitionConfig.GetDiskId().c_str());

    TVector<TRequest> requests;

    for (const auto& [commitId, blobs]: State->GetUnconfirmedBlobs()) {
        for (const auto& blob: blobs) {
            auto blobId = MakePartialBlobId(commitId, blob.UniqueId);
            auto proxy = Info()->BSProxyIDForChannel(
                blobId.Channel(), blobId.Generation()
            );
            requests.emplace_back(blobId, proxy);
        }
    }

    auto actor = NCloud::Register<TConfirmBlobsActor>(
        ctx,
        TabletID(),
        SelfId(),
        std::move(requests));

    Actors.Insert(actor);
}

void TPartitionActor::HandleConfirmBlobsCompleted(
    const TEvPartitionPrivate::TEvConfirmBlobsCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (FAILED(msg->GetStatus())) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "]"
            << " ConfirmBlobs failed: " << msg->GetStatus()
            << " reason: " << msg->GetError().GetMessage().Quote());

        ReportConfirmBlobsError();
        Suicide(ctx);
        return;
    }

    Actors.Erase(ev->Sender);

    LOG_INFO(ctx, TBlockStoreComponents::PARTITION,
        "[%lu][d:%s] ConfirmBlobs: start tx",
        TabletID(),
        PartitionConfig.GetDiskId().c_str());

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        CreateRequestId(),
        MakeIntrusive<TCallContext>());

    AddTransaction(
        *requestInfo,
        ETransactionType::ConfirmBlobs,
        [](const NActors::TActorContext&, TRequestInfo&) {});

    ExecuteTx<TConfirmBlobs>(
        ctx,
        requestInfo,
        msg->StartCycleCount,
        std::move(msg->UnrecoverableBlobs));
}

bool TPartitionActor::PrepareConfirmBlobs(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TConfirmBlobs& args)
{
    Y_UNUSED(tx);
    Y_UNUSED(args);

    LOG_INFO(ctx, TBlockStoreComponents::PARTITION,
        "[%lu][d:%s] ConfirmBlobs: prepare tx",
        TabletID(),
        PartitionConfig.GetDiskId().c_str());

    return true;
}

void TPartitionActor::ExecuteConfirmBlobs(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TConfirmBlobs& args)
{
    LOG_INFO(ctx, TBlockStoreComponents::PARTITION,
        "[%lu][d:%s] ConfirmBlobs: execute tx",
        TabletID(),
        PartitionConfig.GetDiskId().c_str());

    TPartitionDatabase db(tx.DB);
    State->ConfirmBlobs(db, args.UnrecoverableBlobs);
}

void TPartitionActor::CompleteConfirmBlobs(
    const TActorContext& ctx,
    TTxPartition::TConfirmBlobs& args)
{
    LOG_INFO(ctx, TBlockStoreComponents::PARTITION,
        "[%lu][d:%s] ConfirmBlobs: complete tx",
        TabletID(),
        PartitionConfig.GetDiskId().c_str());

    BlobsConfirmed(ctx);

    const auto duration =
        CyclesToDurationSafe(GetCycleCount() - args.StartCycleCount);

    PartCounters->RequestCounters.ConfirmBlobs.AddRequest(
        duration.MicroSeconds());

    IProfileLog::TSysReadWriteRequest request;
    request.RequestType = ESysRequestType::ConfirmBlobs;
    request.Duration = duration;

    for (const auto& [_, blobs]: State->GetConfirmedBlobs()) {
        for (const auto& blob: blobs) {
            request.Ranges.push_back(ConvertRangeSafe(blob.BlockRange));
        }
    }

    IProfileLog::TRecord record;
    record.DiskId = State->GetConfig().GetDiskId();
    record.Ts = ctx.Now() - duration;
    record.Request = request;
    ProfileLog->Write(std::move(record));

    RemoveTransaction(*args.RequestInfo);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
