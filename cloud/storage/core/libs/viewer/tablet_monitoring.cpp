#include "tablet_monitoring.h"

#include <library/cpp/monlib/service/pages/templates.h>

namespace NCloud::NStorage {

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

void DumpChannel(
    IOutputStream& out,
    const TChannelMonInfo& channelInfo,
    const ui64 tabletId,
    const TTabletChannelInfo& channel,
    const TGetMonitoringYDBGroupUrl& getGroupUrl,
    const TGetMonitoringDashboardYDBGroupUrl& getDashboardUrl,
    const TBuildReassignChannelButton& buildReassignButton,
    ui64 hiveTabletId)
{
    HTML(out) {
        TABLER() {
            TABLED() { out << "Channel: " << channel.Channel; }
            TABLED() { out << "StoragePool: " << channel.StoragePool; }

            auto latestEntry = channel.LatestEntry();
            if (!latestEntry) {
                return;
            }

            TABLED() { out << "Id: " << latestEntry->GroupID; }
            TABLED() { out << "Gen: " << latestEntry->FromGeneration; }
            TABLED() { out << "PoolKind: " << channelInfo.PoolKind; }
            TABLED() { out << "DataKind: " << channelInfo.DataKind; }
            TABLED() {
                TStringBuf label;
                TStringBuf color;
                if (channelInfo.SystemWritable) {
                    if (channelInfo.Writable) {
                        color = "green";
                        label = "Writable";
                    } else {
                        color = "yellow";
                        label = "SystemWritable";
                    }
                } else {
                    if (channelInfo.Writable) {
                        color = "pink";
                        label = "WeirdState";
                    } else {
                        color = "orange";
                        label = "Readonly";
                    }
                }

                SPAN_CLASS_STYLE(
                    "label",
                    TStringBuilder() << "background-color: " << color)
                {
                    out << label;
                    const auto freePercentage =
                        static_cast<ui32>(channelInfo.FreeSpaceShare * 100);
                    if (freePercentage) {
                        out << " free=" << freePercentage << "%";
                    }
                }
            }
            TABLED() {
                out << "<a href='"
                    << "../actors/blobstorageproxies/blobstorageproxy"
                    << latestEntry->GroupID
                    << "'>Status</a>";
            }
            const auto groupUrl = getGroupUrl(
                latestEntry->GroupID,
                channel.StoragePool,
                channelInfo.DataKind);
            if (groupUrl) {
                TABLED()
                {
                    out << "<a href='" << groupUrl << "'>Graphs</a>";
                    const auto dashboardGroupUrl =
                        getDashboardUrl(latestEntry->GroupID);
                    if (!dashboardGroupUrl.empty()) {
                        out << "<br>" << "<a href='" << dashboardGroupUrl
                            << "'>Group dashboard</a>";
                    }
                }
            }
            TABLED() {
                buildReassignButton(
                    out,
                    hiveTabletId,
                    tabletId,
                    channel.Channel);
            }
        }
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void DumpChannels(
    IOutputStream& out,
    const TVector<TChannelMonInfo>& channelInfos,
    const TTabletStorageInfo& storage,
    const TGetMonitoringYDBGroupUrl& getGroupUrl,
    const TGetMonitoringDashboardYDBGroupUrl& getDashboardUrl,
    const TBuildReassignChannelButton& buildReassignButton,
    ui64 hiveTabletId)
{
    HTML(out) {
        DIV() {
            out << "<p><a href='app?TabletID=" << hiveTabletId
                << "&page=Groups"
                << "&tablet_id=" << storage.TabletID
                << "'>Channel history</a></p>";
        }

        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                for (const auto& channel: storage.Channels) {
                    TChannelMonInfo channelInfo;
                    // we need this check for legacy volumes
                    // see NBS-752
                    if (channel.Channel < channelInfos.size()) {
                        channelInfo = channelInfos[channel.Channel];
                    }

                    DumpChannel(
                        out,
                        channelInfo,
                        storage.TabletID,
                        channel,
                        getGroupUrl,
                        getDashboardUrl,
                        buildReassignButton,
                        hiveTabletId);
                }
            }
        }
    }
}

}   // namespace NCloud::NStorage
