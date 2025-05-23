#include "service_actor.h"

#include <cloud/blockstore/libs/diagnostics/diag_down_graph.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore//libs//diagnostics/hostname.h>
#include <cloud/blockstore/libs/rdma/iface/client.h>
#include <cloud/blockstore/libs/storage/core/monitoring_utils.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/media.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/stream/str.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NMonitoringUtils;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

void BuildSearchButton(IOutputStream& out)
{
    out <<
        "<form method=\"GET\" id=\"volSearch\" name=\"volSearch\">\n"
        "Volume: <input type=\"text\" id=\"Volume\" name=\"Volume\"/>\n"
        "<input class=\"btn btn-primary\" type=\"submit\" value=\"Search\"/>\n"
        "<input type='hidden' name='action' value='search'/>"
        "</form>\n";
}

void BuildVolumePreemptionButton(IOutputStream& out, const TVolumeInfo& volume)
{
    TString buttonName = "Push";
    if (volume.BindingType == NProto::BINDING_REMOTE) {
        if (volume.PreemptionSource == NProto::SOURCE_INITIAL_MOUNT) {
            buttonName = "Freeze";
        } else {
            buttonName = "Pull";
        }
    }

    out << "<form method='POST' name='volPreempt" << volume.DiskId << "'>\n";
    out << "<input class='btn btn-primary' type='button' value='" << buttonName << "'"
        << " data-toggle='modal' data-target='#toggle-preemption" << volume.DiskId << "'/>";
    out << "<input type='hidden' name='action' value='togglePreemption'/>";
    out << "<input type='hidden' name='type' value='" << buttonName << "'/>";
    out << "<input type='hidden' name='Volume' value='" << volume.DiskId << "'/>";
    out << "</form>\n";

    BuildConfirmActionDialog(
        out,
        TStringBuilder() << "toggle-preemption" << volume.DiskId,
        "toggle-preemption",
        TStringBuilder() << "Are you sure you want to " << buttonName << " volume?",
        TStringBuilder() << "togglePreemption(\"" << volume.DiskId << "\");");
}

void GenerateServiceActionsJS(IOutputStream& out)
{
    out << R"___(
        <script type='text/javascript'>
        function togglePreemption(diskId) {
            document.forms['volPreempt'+diskId].submit();
        }
        </script>
    )___";
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TServiceActor::HandleHttpInfo(
    const NMon::TEvHttpInfo::TPtr& ev,
    const TActorContext& ctx)
{
    using THttpHandler = void(TServiceActor::*)(
        const NActors::TActorContext&,
        const TCgiParameters&,
        TRequestInfoPtr);

    using THttpHandlers = THashMap<TString, THttpHandler>;

    static const THttpHandlers postActions {{
        {"unmount",           &TServiceActor::HandleHttpInfo_Unmount       },
        {"togglePreemption",  &TServiceActor::HandleHttpInfo_VolumeBinding },
    }};

    static const THttpHandlers getActions {{
        {"listclients",       &TServiceActor::HandleHttpInfo_Clients       },
        {"search",            &TServiceActor::HandleHttpInfo_Search        },
    }};

    const auto& request = ev->Get()->Request;
    TString uri{request.GetUri()};
    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "HTTP request: %s", uri.c_str());

    const auto& params =
        (request.GetMethod() != HTTP_METHOD_POST)
        ? request.GetParams()
        : request.GetPostParams();

    const auto& action = params.Get("action");
    const auto& diskId = params.Get("Volume");

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "HTTP request action: %s disk: %s", action.c_str(), diskId.c_str());

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        MakeIntrusive<TCallContext>());

    LWTRACK(
        RequestReceived_Service,
        requestInfo->CallContext->LWOrbit,
        "HttpInfo",
        requestInfo->CallContext->RequestId);

    if (auto* handler = postActions.FindPtr(action)) {
        if (request.GetMethod() != HTTP_METHOD_POST) {
            RejectHttpRequest(ctx, *requestInfo, "Wrong HTTP method");
            return;
        }

        std::invoke(*handler, this, ctx, params, std::move(requestInfo));
        return;
    }

    if (auto* handler = getActions.FindPtr(action)) {
        std::invoke(*handler, this, ctx, params, std::move(requestInfo));
        return;
    }

    TStringStream out;
    RenderHtmlInfo(out);

    SendHttpResponse(ctx, *requestInfo, std::move(out));
}

void TServiceActor::RenderHtmlInfo(IOutputStream& out) const
{
    HTML(out) {
        TAG(TH3) { out << "Search Volume"; }
        BuildSearchButton(out);

        TAG (TH3) {
            out << "Volumes ";
            out << "<a href='"
                << GetMonitoringVolumeUrlWithoutDiskId(*DiagnosticsConfig);
            for (auto it = State.GetVolumes().begin();
                 it != State.GetVolumes().end();
                 ++it)
            {
                out << it->second->DiskId;

                auto next = it;
                ++next;

                if (next != State.GetVolumes().end()) {
                    out << "|";
                }
            }
            out << "'>[dashboard]</a>";
        }

        RenderDownDisks(out);
        RenderVolumeList(out);

        if (RdmaClient) {
            TAG(TH3) { out << "RdmaClient"; }
            RdmaClient->DumpHtml(out);
        }

        TAG(TH3) { out << "Config"; }
        Config->DumpHtml(out);

        if (Counters) {
            TAG(TH3) { out << "Counters"; }
            Counters->OutputHtml(out);
        }
        GenerateServiceActionsJS(out);
    }
}

void TServiceActor::RenderDownDisks(IOutputStream& out) const
{
    HTML(out)
    {
        TABLE_SORTABLE_CLASS("table table-bordered")
        {
            TABLEHEAD()
            {
                TABLER()
                {
                    TABLEH() { out << "Volume"; }
                    TABLEH() { out << "Downtime"; }

                }
            }

            auto addVolumeRow =
                [&](const TString& diskId, const TVolumeInfo& volume)
            {
                auto history = VolumeStats->GetDowntimeHistory(diskId);
                bool hasDowntimes = false;
                for (const auto& [_, state]: history) {
                    if (state == EDowntimeStateChange::DOWN) {
                        hasDowntimes = true;
                        break;
                    }
                }
                if (hasDowntimes) {
                    TABLER() {
                        TABLEH() {
                            out << "<a href='../tablets?TabletID="
                                << volume.TabletId << "'>" << diskId << "</a>";
                        }
                        TABLEH() {
                            TSvgWithDownGraph svg(out);
                            for (const auto& [time, state]: history) {
                                svg.AddEvent(
                                    time,
                                    state == EDowntimeStateChange::DOWN);
                            }
                        }
                    }
                }
            };

            for (const auto& p: State.GetVolumes()) {
                addVolumeRow(p.first, *p.second);
            }
        }
    }
}

void TServiceActor::RenderVolumeList(IOutputStream& out) const
{
    auto status = VolumeStats->GatherVolumePerfStatuses();
    THashMap<TString, ui32> statusMap(status.begin(), status.end());

    HTML(out) {
        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Volume"; }
                    TABLEH() { out << "Tablet"; }
                    TABLEH() { out << "Size"; }
                    TABLEH() { out << "PartitionCount"; }
                    TABLEH() { out << "MediaKind"; }
                    TABLEH() { out << "HasStorageConfigPatch"; }
                    TABLEH() { out << "Status"; }
                    TABLEH() { out << "Clients"; }
                    TABLEH() { out << "Meets perf guarantees"; }
                    TABLEH() { out << "Action"; }
                }
            }

            for (const auto& p: State.GetVolumes()) {
                const auto& volume = *p.second;
                if (!volume.VolumeInfo.Defined()) {
                    continue;
                }
                const auto& diskId = volume.VolumeInfo->GetDiskId();
                TABLER() {
                    TABLED() {
                        out << "<a href='../tablets?TabletID="
                            << volume.TabletId
                            << "'>"
                            << diskId
                            << "</a>";
                    }
                    TABLED() {
                        out << "<a href='../tablets?TabletID="
                            << volume.TabletId
                            << "'>"
                            << volume.TabletId
                            << "</a>";
                    }
                    TABLED() {
                        out << FormatByteSize(
                            volume.VolumeInfo->GetBlocksCount() * volume.VolumeInfo->GetBlockSize());
                    }
                    TABLED() {
                        out << volume.VolumeInfo->GetPartitionsCount();
                    }
                    TABLED() {
                        out << MediaKindToString(volume.VolumeInfo->GetStorageMediaKind());
                    }
                    TABLED() {
                        if (VolumeStats->HasStorageConfigPatch(diskId)) {
                            out << "patched";
                        }
                    }
                    TABLED() {
                        TString statusText = "Online";
                        TString cssClass = "label-info";

                        if (volume.GetLocalMountClientInfo()) {
                            if (volume.BindingType != NProto::BINDING_REMOTE) {
                                statusText = "Mounted";
                            } else {
                                switch (volume.PreemptionSource) {
                                    case NProto::SOURCE_INITIAL_MOUNT: {
                                        statusText = "Mounted (initial)";
                                        break;
                                    }
                                    case NProto::SOURCE_BALANCER: {
                                        statusText = "Mounted (preempted)";
                                        break;
                                    }
                                    case NProto::SOURCE_MANUAL: {
                                        statusText = "Mounted (manually preempted)";
                                        break;
                                    }
                                    default: {
                                        statusText = "Mounted (not set)";
                                    }
                                }
                            }
                            cssClass = "label-success";
                        } else if (volume.IsMounted()) {
                            statusText = "Mounted (remote)";
                        } else if (volume.GetTabletReportedLocalMount()) {
                            statusText = "Mounted (preempted)";
                        }

                        SPAN_CLASS("label " + cssClass) {
                            out << statusText;
                        }
                    }
                    TABLED() {
                        out << "<a href='../blockstore/service?Volume="
                            << volume.VolumeInfo->GetDiskId() << "&action=listclients'>"
                            << volume.ClientInfos.Size()
                            << "</a>";
                    }
                    TABLED() {
                        TString statusText = "Unknown";
                        TString cssClass = "label-default";

                        auto it = statusMap.find(volume.VolumeInfo->GetDiskId());
                        if (it != statusMap.end()) {
                            if (!it->second) {
                                statusText = "Yes";
                                cssClass = "label-success";
                            } else {
                                statusText = TStringBuilder() <<
                                    "No(" <<
                                    it->second
                                    << " s)";
                                cssClass = "label-warning";
                            }
                        }

                        SPAN_CLASS("label " + cssClass) {
                            out << statusText;
                        }
                    }
                    TABLED() {
                        if (volume.GetLocalMountClientInfo() &&
                            !IsDiskRegistryLocalMediaKind(
                                volume.StorageMediaKind))
                        {
                            BuildVolumePreemptionButton(out, volume);
                        }
                    }
                }
            }
        }
    }
}

void TServiceActor::RejectHttpRequest(
    const TActorContext& ctx,
    TRequestInfo& requestInfo,
    TString message)
{
    LOG_ERROR_S(ctx, TBlockStoreComponents::SERVICE, message);

    TStringStream out;
    BuildNotifyPageWithRedirect(
        out,
        std::move(message),
        "../blockstore/service",
        EAlertLevel::DANGER);

    LWTRACK(
        ResponseSent_Service,
        requestInfo.CallContext->LWOrbit,
        "HttpInfo",
        requestInfo.CallContext->RequestId);

    NCloud::Reply(
        ctx,
        requestInfo,
        std::make_unique<NMon::TEvHttpInfoRes>(std::move(out.Str())));
}

void TServiceActor::SendHttpResponse(
    const TActorContext& ctx,
    TRequestInfo& requestInfo,
    TStringStream message)
{
    LWTRACK(
        ResponseSent_Service,
        requestInfo.CallContext->LWOrbit,
        "HttpInfo",
        requestInfo.CallContext->RequestId);

    NCloud::Reply(
        ctx,
        requestInfo,
        std::make_unique<NMon::TEvHttpInfoRes>(std::move(message.Str())));
}


}   // namespace NCloud::NBlockStore::NStorage
