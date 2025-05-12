#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/stream/str.h>
#include <util/string/cast.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NMonitoringUtils;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class THttpFindVolumeActor final
    : public TActorBootstrapped<THttpFindVolumeActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString DiskId;
    const ui32 ClientCount;

public:
    THttpFindVolumeActor(
        TRequestInfoPtr requestInfo,
        TString diskId,
        ui32 clientCount);

    void Bootstrap(const TActorContext& ctx);

private:
    void Notify(const TActorContext& ctx, TString html);

    TString HandleError(
        const TActorContext& ctx,
        const NProto::TError& error,
        const TString& path);

    TString BuildHtmlResponse(ui64 tabletId, const TString& path);

private:
    STFUNC(StateWork);

    void HandleDescribeResponse(
        const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

THttpFindVolumeActor::THttpFindVolumeActor(
        TRequestInfoPtr requestInfo,
        TString diskId,
        ui32 clientCount)
    : RequestInfo(std::move(requestInfo))
    , DiskId(std::move(diskId))
    , ClientCount(clientCount)
{}

void THttpFindVolumeActor::Bootstrap(const TActorContext& ctx)
{
    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(DiskId));

    Become(&TThis::StateWork);
}

void THttpFindVolumeActor::Notify(const TActorContext& ctx, TString html)
{
    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "HttpInfo",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(
        ctx,
        *RequestInfo,
        std::make_unique<NMon::TEvHttpInfoRes>(std::move(html)));
}

TString THttpFindVolumeActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error,
    const TString& path)
{
    TStringStream out;
    out << "Could not resolve path " << path.Quote()
        << " for volume " << DiskId.Quote()
        << ": " << FormatError(error);

    LOG_ERROR(ctx, TBlockStoreComponents::SERVICE, out.Str());
    return out.Str();
}

TString THttpFindVolumeActor::BuildHtmlResponse(
    ui64 tabletId,
    const TString& path)
{
    TStringStream out;

    HTML(out) {
        TAG(TH3) { out << "Volume"; }
        TABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Volume"; }
                    TABLEH() { out << "Tablet ID"; }
                    TABLEH() { out << "Clients"; }
                }
            }
            TABLER() {
                TABLED() {
                    out << path;
                }

                TABLED() {
                    out << "<a href='../tablets?TabletID="
                        << tabletId
                        << "'>"
                        << tabletId
                        << "</a>";
                }

                TABLED() {
                    out << "<a href='../blockstore/service?Volume="
                        << DiskId
                        << "&action=listclients'>"
                        << ClientCount
                        << "</a>";
                }
            }
        }
    }

    return out.Str();
}

////////////////////////////////////////////////////////////////////////////////

void THttpFindVolumeActor::HandleDescribeResponse(
    const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& error = msg->GetError();

    TString out;

    if (FAILED(error.GetCode())) {
        Notify(ctx, HandleError(ctx, error, msg->Path));
    } else {
        const auto& pathDescr = msg->PathDescription;
        const auto& volumeTabletID =
            pathDescr.GetBlockStoreVolumeDescription().GetVolumeTabletId();
        Notify(ctx, BuildHtmlResponse(volumeTabletID, msg->Path));
    }

    Die(ctx);
}

STFUNC(THttpFindVolumeActor::StateWork)
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TServiceActor::HandleHttpInfo_Search(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    auto diskId = params.Get("Volume");
    if (!diskId) {
        RejectHttpRequest(ctx, *requestInfo, "No Volume is given");
        return;
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "Search volume for id: %s",
        diskId.Quote().data());

    ui32 clientCount = 0;
    auto volume = State.GetVolume(diskId);
    if (volume) {
        clientCount = volume->ClientInfos.Size();
    }

    NCloud::Register<THttpFindVolumeActor>(
        ctx,
        std::move(requestInfo),
        std::move(diskId),
        clientCount);
}

}   // namespace NCloud::NBlockStore::NStorage
