#include "service_actor.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/core/disk_counters.h>

#include <cloud/storage/core/libs/common/alloc.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/mon/mon.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TVolumeLivenessCheckActor final
    : public TActorBootstrapped<TVolumeLivenessCheckActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TVector<TString> VolumesToCheck;

    ui32 ReplyCount = 0;
    TVector<TString> DeletedVolumes;
    TVector<TString> LiveVolumes;

public:
    TVolumeLivenessCheckActor(
        TRequestInfoPtr requestInfo,
        TVector<TString> volumesToCheck);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleDescribeResponse(
        const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TVolumeLivenessCheckActor::TVolumeLivenessCheckActor(
        TRequestInfoPtr requestInfo,
        TVector<TString> volumesToCheck)
    : RequestInfo(std::move(requestInfo))
    , VolumesToCheck(std::move(volumesToCheck))
{}

void TVolumeLivenessCheckActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    if (VolumesToCheck.empty()) {
        auto response =
            std::make_unique<TEvService::TEvRunVolumesLivenessCheckResponse>();

        NCloud::Reply(ctx, *RequestInfo, std::move(response));
        Die(ctx);
        return;
    }

    for (ui32 i = 0; i < VolumesToCheck.size(); ++i) {
        auto request = std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(
            RequestInfo->CallContext,
            VolumesToCheck[i]);

        NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request), i);
    }
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TVolumeLivenessCheckActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvSSProxy::TEvDescribeVolumeResponse, HandleDescribeResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

void TVolumeLivenessCheckActor::HandleDescribeResponse(
    const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& error = msg->GetError();

    bool deleted = false;

    if (FAILED(error.GetCode()) &&
        FACILITY_FROM_CODE(error.GetCode()) == FACILITY_SCHEMESHARD)
    {
        const auto status =
            static_cast<NKikimrScheme::EStatus>(STATUS_FROM_CODE(error.GetCode()));
        deleted = status == NKikimrScheme::StatusPathDoesNotExist;
    }

    if (deleted) {
        DeletedVolumes.emplace_back(VolumesToCheck[ev->Cookie]);
    } else {
        LiveVolumes.emplace_back(VolumesToCheck[ev->Cookie]);
    }

    if (++ReplyCount == VolumesToCheck.size()) {
        auto response =
            std::make_unique<TEvService::TEvRunVolumesLivenessCheckResponse>(
                std::move(DeletedVolumes),
                std::move(LiveVolumes));

        NCloud::Reply(ctx, *RequestInfo, std::move(response));
        Die(ctx);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TServiceActor::RunVolumesLivenessCheck(
    const TActorContext& ctx,
    TRequestInfoPtr requestInfo)
{
    const auto& preemptedVolumes = *State.GetManuallyPreemptedVolumes();

    TVector<TString> volumesToCheck = preemptedVolumes.GetVolumes();

    auto actor = std::make_unique<TVolumeLivenessCheckActor>(
        std::move(requestInfo),
        std::move(volumesToCheck));
    ctx.Register(
        actor.release(),
        TMailboxType::Revolving,
        AppData(ctx)->BatchPoolId);
    IsVolumeLivenessCheckRunning = true;
}

void TServiceActor::HandleRunVolumesLivenessCheckResponse(
    const TEvService::TEvRunVolumesLivenessCheckResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto& preemptedVolumes = *State.GetManuallyPreemptedVolumes();
    for (const auto& v: msg->DeletedVolumes) {
        preemptedVolumes.RemoveVolume(v);
        LOG_DEBUG_S(ctx, TBlockStoreComponents::SERVICE,
            "Deleting from manually preempted volumes list: " << v.Quote());
    }

    for (const auto& v: msg->LiveVolumes) {
        if (preemptedVolumes.GetVolume(v).has_value()) {
            preemptedVolumes.AddVolume(v, ctx.Now());
        }
    }

    if (msg->DeletedVolumes || msg->LiveVolumes) {
        RunManuallyPreemptedVolumesSync(ctx);
    }

    ScheduleLivenessCheck(
        ctx,
        Config->GetManuallyPreemptedVolumeLivenessCheckPeriod());

    IsVolumeLivenessCheckRunning = false;
}

void TServiceActor::ScheduleLivenessCheck(
    const TActorContext& ctx,
    TDuration timeout)
{
    ctx.Schedule(
        ctx.Now() + timeout,
        new TEvService::TEvRunVolumesLivenessCheckRequest());
}

void TServiceActor::HandleRunVolumesLivenessCheck(
    const TEvService::TEvRunVolumesLivenessCheckRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (IsVolumeLivenessCheckRunning) {
        auto response =
            std::make_unique<TEvService::TEvRunVolumesLivenessCheckResponse>(
                MakeError(S_ALREADY, "Volumes liveness check is running"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender ? ev->Sender : MakeStorageServiceId(),
        ev->Cookie,
        msg->CallContext);

    if (Config->GetDisableManuallyPreemptedVolumesTracking()) {
        auto response =
            std::make_unique<TEvService::TEvRunVolumesLivenessCheckResponse>();
        NCloud::Reply(ctx, *requestInfo, std::move(response));
        return;
    }

    RunVolumesLivenessCheck(ctx, std::move(requestInfo));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
