#include "mon.h"

#include <cloud/blockstore/libs/cells/iface/inbound_activity.h>
#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/hostname.h>
#include <cloud/blockstore/libs/kikimr/components.h>

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/core/base/appdata.h>
#include <contrib/ydb/core/mon/mon.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/mon.h>

#include <library/cpp/html/pcdata/pcdata.h>
#include <library/cpp/monlib/service/pages/index_mon_page.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/string_utils/quote/quote.h>

#include <util/generic/utility.h>
#include <util/stream/str.h>

namespace NCloud::NBlockStore::NCells {

using namespace NActors;
using namespace NKikimr;
using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

// the mon disk search never waits longer than this, and falls back to it when
// no describe timeout is configured - a zero would fire the result timer at
// once while the describes kept running on their own (grpc) default
constexpr TDuration MonitoringSearchTimeout = TDuration::Seconds(10);

////////////////////////////////////////////////////////////////////////////////

struct TEvPrivate
{
    enum EEv
    {
        EvSearchCompleted = TBlockStorePrivateEvents::CELLS_START,
        EvEnd
    };

    struct TEvSearchCompleted
        : public TEventLocal<TEvSearchCompleted, EvSearchCompleted>
    {
        const TActorId ReplyTo;
        const ui64 Cookie;
        const TString DiskId;
        const TVector<TCellDescribeResult> Results;

        TEvSearchCompleted(
                TActorId replyTo,
                ui64 cookie,
                TString diskId,
                TVector<TCellDescribeResult> results)
            : ReplyTo(replyTo)
            , Cookie(cookie)
            , DiskId(std::move(diskId))
            , Results(std::move(results))
        {}
    };
};

////////////////////////////////////////////////////////////////////////////////

void RenderSearchForm(IOutputStream& out)
{
    // the same form as the search on the service page
    HTML(out) {
        TAG(TH3) { out << "Search Volume"; }
        out << "<form method='GET'>"
            << "Volume: <input type='text' name='Volume'/> "
            << "<input class='btn btn-primary' type='submit' value='Search'/>"
            << "<input type='hidden' name='action' value='search'/>"
            << "</form>";
    }
}

// a link to the disk's page on a remote host
void RenderRemoteVolumeLink(
    IOutputStream& out,
    const TString& fqdn,
    const TString& diskId,
    const TDiagnosticsConfig& diagnosticsConfig)
{
    // a reachable url under the deployment's hostname scheme (bastion,
    // viewer, ...), html-escaped as it embeds the fqdn; the id is url-encoded
    // before html-escaping, or a '#'/'&' in it would truncate or split the
    // Volume query parameter
    out << "<a href='"
        << EncodeHtmlPcdata(
               GetExternalHostUrl(fqdn, EHostService::Nbs, diagnosticsConfig))
        << "blockstore/service?action=search&amp;Volume="
        << EncodeHtmlPcdata(CGIEscapeRet(diskId)) << "'>"
        << EncodeHtmlPcdata(fqdn) << "</a>";
}

void RenderSearchResultTable(
    IOutputStream& out,
    const TVector<TCellDescribeResult>& results,
    const TDiagnosticsConfig& diagnosticsConfig,
    const TString& diskId)
{
    HTML(out) {
        TAG(TH4) { out << "Search result for " << EncodeHtmlPcdata(diskId); }
        TABLE_CLASS("table table-condensed") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Cell"; }
                    TABLEH() { out << "Disk found on"; }
                }
            }
            TABLEBODY() {
                for (const auto& result: results) {
                    TABLER() {
                        TABLED() {
                            out << (result.CellId
                                        ? EncodeHtmlPcdata(*result.CellId)
                                        : TString("local"));
                        }
                        TABLED() {
                            switch (result.Status) {
                                case ECellDescribeStatus::Found:
                                    if (result.CellId) {
                                        RenderRemoteVolumeLink(
                                            out,
                                            result.Fqdn,
                                            diskId,
                                            diagnosticsConfig);
                                    } else {
                                        // the local disk is on this same node;
                                        // link relative to /blockstore/Cells so
                                        // the Viewer node prefix is preserved
                                        out << "<a href='service?action=search"
                                               "&amp;Volume="
                                            << EncodeHtmlPcdata(
                                                   CGIEscapeRet(diskId))
                                            << "'>"
                                            << EncodeHtmlPcdata(result.Fqdn)
                                            << "</a>";
                                    }
                                    break;
                                case ECellDescribeStatus::MigrationDestination:
                                    out << "migration destination copy on "
                                        << EncodeHtmlPcdata(result.Fqdn);
                                    break;
                                case ECellDescribeStatus::NotFound:
                                    out << "not found";
                                    break;
                                case ECellDescribeStatus::Unavailable:
                                    out << "unavailable (not connected)";
                                    break;
                                case ECellDescribeStatus::Failed:
                                    out << "lookup failed: "
                                        << EncodeHtmlPcdata(
                                               FormatError(result.Error));
                                    break;
                            }
                        }
                    }
                }
            }
        }
    }
}

void RenderMounts(
    IOutputStream& out,
    const TVector<TCellMountStatus>& mounts,
    const TDiagnosticsConfig& diagnosticsConfig)
{
    HTML(out) {
        TAG(TH3) { out << "Remote mounts"; }
        TABLE_SORTABLE_CLASS("table table-condensed") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "DiskId"; }
                    TABLEH() { out << "ClientId"; }
                    TABLEH() { out << "Cell"; }
                    TABLEH() { out << "Host"; }
                    TABLEH() { out << "Data transport"; }
                }
            }
            TABLEBODY() {
                for (const auto& mount: mounts) {
                    TABLER() {
                        TABLED() { out << EncodeHtmlPcdata(mount.DiskId); }
                        TABLED() { out << EncodeHtmlPcdata(mount.ClientId); }
                        TABLED() { out << EncodeHtmlPcdata(mount.CellId); }
                        TABLED() {
                            RenderRemoteVolumeLink(
                                out,
                                mount.Host,
                                mount.DiskId,
                                diagnosticsConfig);
                        }
                        TABLED() { out << mount.DataTransport; }
                    }
                }
            }
        }
    }
}

void RenderConfig(IOutputStream& out, const TCellsConfig& config)
{
    HTML(out) {
        TAG(TH3) { out << "Cells config"; }
    }
    config.DumpHtml(out);

    // the collapse id goes into a CSS selector as is, so it is numbered
    // rather than taken from the cell id
    ui32 index = 0;
    for (const auto& [cellId, cellConfig]: config.GetCells()) {
        HTML(out) {
            COLLAPSED_BUTTON_CONTENT(
                "cell-config-" + ToString(index++),
                "Cell " + EncodeHtmlPcdata(cellId))
            {
                cellConfig->DumpHtml(out);

                TABLE_CLASS("table table-condensed") {
                    TABLEHEAD() {
                        TABLER() {
                            TABLEH() { out << "Host"; }
                            TABLEH() { out << "GrpcPort"; }
                            TABLEH() { out << "SecureGrpcPort"; }
                            TABLEH() { out << "RdmaPort"; }
                        }
                    }
                    TABLEBODY() {
                        for (const auto& [fqdn, host]: cellConfig->GetHosts())
                        {
                            Y_UNUSED(fqdn);
                            TABLER() {
                                TABLED() {
                                    out << EncodeHtmlPcdata(host.GetFqdn());
                                }
                                TABLED() { out << host.GetGrpcPort(); }
                                TABLED() { out << host.GetSecureGrpcPort(); }
                                TABLED() { out << host.GetRdmaPort(); }
                            }
                        }
                    }
                }
            }
        }
    }
}

void RenderOutbound(
    IOutputStream& out,
    const THashMap<TString, TVector<TCellHostStatus>>& hostStatuses)
{
    HTML(out) {
        TAG(TH3) { out << "Outbound host status"; }
    }
    ui32 index = 0;
    for (const auto& [cellId, statuses]: hostStatuses) {
        HTML(out) {
            COLLAPSED_BUTTON_CONTENT(
                "cell-outbound-" + ToString(index++),
                "Cell " + EncodeHtmlPcdata(cellId))
            {
                TABLE_CLASS("table table-condensed") {
                    TABLEHEAD() {
                        TABLER() {
                            TABLEH() { out << "Host"; }
                            TABLEH() { out << "Alive"; }
                            TABLEH() { out << "Warm"; }
                            TABLEH() { out << "Connections"; }
                        }
                    }
                    TABLEBODY() {
                        for (const auto& status: statuses) {
                            TABLER() {
                                TABLED() {
                                    out << EncodeHtmlPcdata(status.Fqdn);
                                }
                                TABLED() {
                                    out << (status.Alive ? "yes" : "no");
                                }
                                TABLED() {
                                    out << (status.Warm ? "yes" : "no");
                                }
                                TABLED() { out << status.Connections; }
                            }
                        }
                    }
                }
            }
        }
    }
}

void RenderInbound(
    IOutputStream& out,
    const TVector<TCellInboundActivity::TRow>& rows)
{
    HTML(out) {
        TAG(TH3) { out << "Inbound inter-cell connections"; }
        TABLE_SORTABLE_CLASS("table table-condensed") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Peer"; }
                    TABLEH() { out << "DiskId"; }
                    TABLEH() { out << "ClientId"; }
                    TABLEH() { out << "Mounts"; }
                    TABLEH() { out << "Unmounts"; }
                    TABLEH() { out << "Describes"; }
                }
            }
            TABLEBODY() {
                for (const auto& row: rows) {
                    TABLER() {
                        TABLED() { out << EncodeHtmlPcdata(row.Peer); }
                        TABLED() { out << EncodeHtmlPcdata(row.DiskId); }
                        TABLED() { out << EncodeHtmlPcdata(row.ClientId); }
                        TABLED() { out << row.Mounts; }
                        TABLED() { out << row.Unmounts; }
                        TABLED() { out << row.Describes; }
                    }
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

// The /blockstore/Cells page. Renders the manager's snapshot on a plain open;
// delegates a disk search to the manager's SearchVolume and renders the result
// once its future completes, so the mon thread never waits on RPCs.
class TCellsMonActor final
    : public TActorBootstrapped<TCellsMonActor>
{
private:
    const ICellManagerPtr CellManager;
    const TDiagnosticsConfigPtr DiagnosticsConfig;

public:
    TCellsMonActor(
            ICellManagerPtr cellManager,
            TDiagnosticsConfigPtr diagnosticsConfig)
        : CellManager(std::move(cellManager))
        , DiagnosticsConfig(std::move(diagnosticsConfig))
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        auto* mon = AppData(ctx)->Mon;
        if (mon) {
            auto* root = mon->RegisterIndexPage("blockstore", "BlockStore");
            mon->RegisterActorPage(
                root, "Cells", "Cells", false, ctx.ActorSystem(), SelfId());
        }
        Become(&TThis::StateWork);
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(NMon::TEvHttpInfo, HandleHttpInfo);
            HFunc(TEvPrivate::TEvSearchCompleted, HandleSearchCompleted);
            default:
                break;
        }
    }

private:
    void HandleHttpInfo(
        const NMon::TEvHttpInfo::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto& params = ev->Get()->Request.GetParams();

        if (params.Get("action") == "search" && params.Get("Volume")) {
            StartSearch(ctx, ev->Sender, ev->Cookie, params.Get("Volume"));
            return;
        }

        TStringStream out;
        RenderCellsPage(
            out,
            *CellManager->Config,
            CellManager->GetSnapshot(),
            *DiagnosticsConfig);

        ctx.Send(
            ev->Sender,
            new NMon::TEvHttpInfoRes(out.Str()),
            0,
            ev->Cookie);
    }

    void StartSearch(
        const TActorContext& ctx,
        TActorId replyTo,
        ui64 cookie,
        const TString& diskId)
    {
        auto timeout = CellManager->Config->GetDescribeVolumeTimeout();
        if (!timeout || timeout > MonitoringSearchTimeout) {
            timeout = MonitoringSearchTimeout;
        }

        auto future =
            CellManager->SearchVolume(diskId, timeout);

        auto* actorSystem = ctx.ActorSystem();
        const auto self = SelfId();
        future.Subscribe(
            [actorSystem, self, replyTo, cookie, diskId] (const auto& f)
            {
                actorSystem->Send(
                    self,
                    new TEvPrivate::TEvSearchCompleted(
                        replyTo,
                        cookie,
                        diskId,
                        f.GetValue()));
            });
    }

    void HandleSearchCompleted(
        const TEvPrivate::TEvSearchCompleted::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();

        TStringStream out;
        RenderCellsSearchResult(
            out, msg->Results, *DiagnosticsConfig, msg->DiskId);

        ctx.Send(
            msg->ReplyTo,
            new NMon::TEvHttpInfoRes(out.Str()),
            0,
            msg->Cookie);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void RenderCellsPage(
    IOutputStream& out,
    const TCellsConfig& config,
    const TCellsSnapshot& snapshot,
    const TDiagnosticsConfig& diagnosticsConfig)
{
    RenderSearchForm(out);
    RenderMounts(out, snapshot.Mounts, diagnosticsConfig);
    RenderConfig(out, config);
    RenderOutbound(out, snapshot.HostStatuses);
    RenderInbound(out, snapshot.InboundActivity);
}

void RenderCellsSearchResult(
    IOutputStream& out,
    const TVector<TCellDescribeResult>& results,
    const TDiagnosticsConfig& diagnosticsConfig,
    const TString& diskId)
{
    RenderSearchForm(out);
    RenderSearchResultTable(out, results, diagnosticsConfig, diskId);
}

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateCellsMonActor(
    ICellManagerPtr cellManager,
    TDiagnosticsConfigPtr diagnosticsConfig)
{
    return std::make_unique<TCellsMonActor>(
        std::move(cellManager),
        std::move(diagnosticsConfig));
}

}   // namespace NCloud::NBlockStore::NCells
