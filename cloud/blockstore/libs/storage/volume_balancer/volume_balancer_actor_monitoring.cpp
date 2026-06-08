#include "volume_balancer_actor.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/string.h>
#include <util/stream/str.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TVolumeBalancerActor::HandleHttpInfo(
    const NMon::TEvHttpInfo::TPtr& ev,
    const TActorContext& ctx)
{
    using THttpHandler = void (TVolumeBalancerActor::*)(
        const TActorContext&,
        const TCgiParameters&,
        TRequestInfoPtr);

    using THttpHandlers = THashMap<TString, THttpHandler>;

    static const THttpHandlers postActions{{
        {"advisoryPush", &TVolumeBalancerActor::HandleHttpInfo_AdvisoryPush},
        {"advisoryPull", &TVolumeBalancerActor::HandleHttpInfo_AdvisoryPull},
    }};

    const auto& request = ev->Get()->Request;

    TString uri { request.GetUri() };
    LOG_DEBUG(ctx, TBlockStoreComponents::VOLUME_BALANCER,
        "HTTP request: %s", uri.c_str());

    TStringStream out;

    if constexpr (Y_IS_DEBUG_BUILD) {
        if (request.GetMethod() == HTTP_METHOD_POST) {
            const auto& params = request.GetPostParams();
            const auto& action = params.Get("action");
            const auto& diskId = params.Get("Volume");

            LOG_DEBUG(
                ctx,
                TBlockStoreComponents::VOLUME_BALANCER,
                "HTTP request action: %s disk: %s",
                action.c_str(),
                diskId.c_str());

            auto requestInfo = CreateRequestInfo(
                ev->Sender,
                ev->Cookie,
                MakeIntrusive<TCallContext>());

            if (auto* handler = postActions.FindPtr(action)) {
                std::invoke(
                    *handler,
                    this,
                    ctx,
                    params,
                    std::move(requestInfo));
            } else {
                LOG_WARN(
                    ctx,
                    TBlockStoreComponents::VOLUME_BALANCER,
                    "No handler for HTTP post action: %s",
                    action.c_str());
                RejectHttpRequest(ctx, *requestInfo, "Unrecognized action");
            }
            return;
        }
    }

    if (State) {
        State->RenderHtml(out, ctx.Now());
    }

    NCloud::Reply(
        ctx,
        *ev,
        std::make_unique<NMon::TEvHttpInfoRes>(out.Str()));
}

void TVolumeBalancerActor::HandleHttpInfo_AdvisoryPush(
    const NActors::TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    const auto& action = params.Get("action");
    const auto& diskId = params.Get("Volume");

    LOG_WARN(
        ctx,
        TBlockStoreComponents::VOLUME_BALANCER,
        "Unimplemented handler for HTTP post action: %s, disk %s",
        action.c_str(),
        diskId.c_str());

    if (!diskId) {
        SendHttpResponse(
            ctx,
            *requestInfo,
            TStringBuilder() << "No diskId provided",
            NMonitoringUtils::EAlertLevel::DANGER);
        return;
    }

    if (!IsBalancerEnabled()) {
        SendHttpResponse(
            ctx,
            *requestInfo,
            TStringBuilder() << "Balancer is disabled",
            NMonitoringUtils::EAlertLevel::INFO);
        return;
    }

    if (IsMaxInProgressLimitReached()) {
        SendHttpResponse(
            ctx,
            *requestInfo,
            TStringBuilder() << "MaxInProgressLimit reached",
            NMonitoringUtils::EAlertLevel::INFO);
        return;
    }

    SendVolumeToHive(ctx, diskId);

    SendHttpResponse(
        ctx,
        *requestInfo,
        TStringBuilder() << "Advisory Push submitted",
        NMonitoringUtils::EAlertLevel::SUCCESS);
}

void TVolumeBalancerActor::HandleHttpInfo_AdvisoryPull(
    const NActors::TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    const auto& action = params.Get("action");
    const auto& diskId = params.Get("Volume");

    LOG_WARN(
        ctx,
        TBlockStoreComponents::VOLUME_BALANCER,
        "HTTP post action: %s, disk %s",
        action.c_str(),
        diskId.c_str());

    if (!diskId) {
        SendHttpResponse(
            ctx,
            *requestInfo,
            TStringBuilder() << "No diskId provided",
            NMonitoringUtils::EAlertLevel::DANGER);
        return;
    }

    if (!IsBalancerEnabled()) {
        SendHttpResponse(
            ctx,
            *requestInfo,
            TStringBuilder() << "Balancer is disabled",
            NMonitoringUtils::EAlertLevel::INFO);
        return;
    }

    if (IsMaxInProgressLimitReached()) {
        SendHttpResponse(
            ctx,
            *requestInfo,
            TStringBuilder() << "MaxInProgressLimit reached",
            NMonitoringUtils::EAlertLevel::INFO);
        return;
    }

    PullVolumeFromHive(ctx, diskId);

    SendHttpResponse(
        ctx,
        *requestInfo,
        TStringBuilder() << "Advisory Pull submitted",
        NMonitoringUtils::EAlertLevel::SUCCESS);
}

void TVolumeBalancerActor::RejectHttpRequest(
    const TActorContext& ctx,
    TRequestInfo& requestInfo,
    TString message)
{
    LOG_ERROR_S(ctx, TBlockStoreComponents::VOLUME_BALANCER, message);

    TStringStream out;
    NMonitoringUtils::BuildNotifyPageWithRedirect(
        out,
        std::move(message),
        "../blockstore/balancer",
        NMonitoringUtils::EAlertLevel::DANGER);

    NCloud::Reply(
        ctx,
        requestInfo,
        std::make_unique<NMon::TEvHttpInfoRes>(std::move(out.Str())));
}

void TVolumeBalancerActor::SendHttpResponse(
    const TActorContext& ctx,
    TRequestInfo& requestInfo,
    TStringStream message,
    const NMonitoringUtils::EAlertLevel alertLevel)
{
    TStringStream out;
    BuildNotifyPageWithRedirect(
        out,
        message.Str(),
        TStringBuilder() << "../blockstore/balancer",
        alertLevel);

    auto response = std::make_unique<NMon::TEvHttpInfoRes>(out.Str());
    NCloud::Reply(ctx, requestInfo, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
