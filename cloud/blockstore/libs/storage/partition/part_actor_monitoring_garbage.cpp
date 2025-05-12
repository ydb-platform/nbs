#include "part_actor.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/str.h>
#include <util/string/split.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NKikimr;

using namespace NMonitoringUtils;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class THttpGarbageActor final
    : public TActorBootstrapped<THttpGarbageActor>
{
public:
    enum EAction
    {
        AddGarbage,
        CollectGarbage,
    };

private:
    const TRequestInfoPtr RequestInfo;

    const TActorId Tablet;
    const ui64 TabletId;
    const EAction Action;
    const TVector<TPartialBlobId> BlobIds;

public:
    THttpGarbageActor(
        TRequestInfoPtr requestInfo,
        const TActorId& tablet,
        ui64 tabletId,
        EAction action,
        TVector<TPartialBlobId> blobIds = {});

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleAddGarbageResponse(
        const TEvPartitionPrivate::TEvAddGarbageResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleCollectGarbageResponse(
        const TEvPartitionPrivate::TEvCollectGarbageResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleAddGarbageRequest(
        const TEvPartitionPrivate::TEvAddGarbageRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleCollectGarbageRequest(
        const TEvPartitionPrivate::TEvCollectGarbageRequest::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

THttpGarbageActor::THttpGarbageActor(
        TRequestInfoPtr requestInfo,
        const TActorId& tablet,
        ui64 tabletId,
        EAction action,
        TVector<TPartialBlobId> blobIds)
    : RequestInfo(std::move(requestInfo))
    , Tablet(tablet)
    , TabletId(tabletId)
    , Action(action)
    , BlobIds(std::move(blobIds))
{}

void THttpGarbageActor::Bootstrap(const TActorContext& ctx)
{
    switch (Action) {
        case AddGarbage: {
            auto request = std::make_unique<TEvPartitionPrivate::TEvAddGarbageRequest>(
                MakeIntrusive<TCallContext>(),
                BlobIds);

            NCloud::SendWithUndeliveryTracking(ctx, Tablet, std::move(request));
            break;
        }
        case CollectGarbage: {
            auto request =
                std::make_unique<TEvPartitionPrivate::TEvCollectGarbageRequest>();

            NCloud::SendWithUndeliveryTracking(ctx, Tablet, std::move(request));
            break;
        }
        default:
            Y_ABORT("Invalid action");
    }

    Become(&TThis::StateWork);
}

void THttpGarbageActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto alertType = EAlertLevel::SUCCESS;
    TStringStream msg;
    if (FAILED(error.GetCode())) {
        msg << "[" << TabletId << "] ";
        msg << "Operation completed with error : " << FormatError(error);
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION, msg.Str());
        alertType = EAlertLevel::DANGER;
    } else {
        msg << "Operation successfully completed";
    }

    LWTRACK(
        ResponseSent_Partition,
        RequestInfo->CallContext->LWOrbit,
        "HttpInfo",
        RequestInfo->CallContext->RequestId);

    TStringStream out;
    BuildTabletNotifyPageWithRedirect(out, msg.Str(), TabletId, alertType);

    NCloud::Reply(
        ctx,
        *RequestInfo,
        std::make_unique<NMon::TEvRemoteHttpInfoRes>(std::move(out.Str())));

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void THttpGarbageActor::HandleAddGarbageResponse(
    const TEvPartitionPrivate::TEvAddGarbageResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    ReplyAndDie(ctx, msg->GetError());
}

void THttpGarbageActor::HandleCollectGarbageResponse(
    const TEvPartitionPrivate::TEvCollectGarbageResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    ReplyAndDie(ctx, msg->GetError());
}

void THttpGarbageActor::HandleAddGarbageRequest(
    const TEvPartitionPrivate::TEvAddGarbageRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void THttpGarbageActor::HandleCollectGarbageRequest(
    const TEvPartitionPrivate::TEvCollectGarbageRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

STFUNC(THttpGarbageActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvPartitionPrivate::TEvAddGarbageResponse, HandleAddGarbageResponse);
        HFunc(TEvPartitionPrivate::TEvCollectGarbageResponse, HandleCollectGarbageResponse);

        HFunc(TEvPartitionPrivate::TEvAddGarbageRequest, HandleAddGarbageRequest);
        HFunc(TEvPartitionPrivate::TEvCollectGarbageRequest, HandleCollectGarbageRequest);

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

void TPartitionActor::HandleHttpInfo_AddGarbage(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    TStringBuilder errors;

    TVector<ui32> allowedChannels = State->GetChannelsByKind([](auto kind) {
        return kind == EChannelDataKind::Mixed
            || kind == EChannelDataKind::Merged;
    });

    TVector<TPartialBlobId> blobIds;
    for (const auto& it:
         StringSplitter(params.Get("blobs")).SplitBySet(" ,;").SkipEmpty())
    {
        TString part(it);

        TLogoBlobID blobId;
        TString errorExplanation;
        if (TLogoBlobID::Parse(blobId, part, errorExplanation)) {
            if (blobId.TabletID() != TabletID()) {
                errorExplanation = "tablet does not match";
            } else if (Find(allowedChannels, blobId.Channel()) == allowedChannels.end()) {
                errorExplanation = "channel does not match";
            } else if (blobId.Generation() >= Executor()->Generation()) {
                errorExplanation = "generation does not match";
            } else {
                blobIds.push_back(MakePartialBlobId(blobId));
                continue;
            }
        }

        errors << "Invalid blob " << part.Quote() << ": " << errorExplanation << Endl;
    }

    if (!blobIds || errors) {
        TStringStream out;
        HTML(out) {
            if (errors) {
                out << "Could not parse some blob IDs: "
                    << "<pre>" << errors << "</pre>";
            } else {
                out << "You should specify some blob IDs to add";
            }
        }

        SendHttpResponse(ctx, *requestInfo, std::move(out.Str()));
        return;
    }

    SortUnique(blobIds);
    NCloud::Register<THttpGarbageActor>(
        ctx,
        std::move(requestInfo),
        SelfId(),
        TabletID(),
        THttpGarbageActor::AddGarbage,
        std::move(blobIds));
}

void TPartitionActor::HandleHttpInfo_CollectGarbage(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    const auto& type = params.Get("type");
    if (type == "hard") {
        State->CollectGarbageHardRequested = true;
    }

    NCloud::Register<THttpGarbageActor>(
        ctx,
        std::move(requestInfo),
        SelfId(),
        TabletID(),
        THttpGarbageActor::CollectGarbage);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
