#include "tablet_monitoring.h"

#include <library/cpp/monlib/service/pages/templates.h>

namespace NCloud::NStorage {

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

void setWritable(TString& label, TString& color, bool writable, bool systemWritable) {
    if (systemWritable) {
        if (writable) {
            color = "green";
            label = "Writable";
        } else {
            color = "yellow";
            label = "SystemWritable";
        }
    } else {
        if (writable) {
            color = "pink";
            label = "WeirdState";
        } else {
            color = "orange";
            label = "Readonly";
        }
    }
}

void DumpChannel(
    IOutputStream& out,
    const TChannelMonInfo& channelInfo,
    const ui64 tabletId,
    const TTabletChannelInfo& channel,
    const TGetMonitoringYDBGroupUrl& getGroupUrl,
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
                TString label;
                TString color;
                setWritable(label, color, channelInfo.Writable, channelInfo.SystemWritable);

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
            const auto groupUrl =
                getGroupUrl(latestEntry->GroupID, channel.StoragePool);
            if (groupUrl) {
                TABLED() {
                    out << "<a href='" << groupUrl << "'>Graphs</a>";
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
                        buildReassignButton,
                        hiveTabletId);
                }
            }
        }
    }
}

void DumpChannelsXml(
    NXml::TNode root,
    const TVector<TChannelMonInfo>& channelInfos,
    const NKikimr::TTabletStorageInfo& storage,
    const TGetMonitoringYDBGroupUrl& getGroupUrl,
    const TBuildReassignChannelButtonXml& buildReassignButton,
    ui64 hiveTabletId)
{
    root.AddChild("hive_tablet_id", hiveTabletId);
    root.AddChild("storage_tablet_id", storage.TabletID);
    auto channels = root.AddChild("channels", " ");
    for (const auto& channel : storage.Channels) {
        TChannelMonInfo channelInfo;
        // we need this check for legacy volumes
        // see NBS-752
        if (channel.Channel < channelInfos.size()) {
            channelInfo = channelInfos[channel.Channel];
        }

        auto latestEntry = channel.LatestEntry();
        if (!latestEntry) {
            continue;
        }

        auto cd = channels.AddChild("cd", " ");
        cd.AddChild("channel", channel.Channel);
        cd.AddChild("storage_pool", channel.StoragePool);
        cd.AddChild("group_id", latestEntry->GroupID);
        cd.AddChild("generation", latestEntry->FromGeneration);
        cd.AddChild("pool_kind", channelInfo.PoolKind);
        cd.AddChild("data_kind", channelInfo.DataKind);
        if (channelInfo.SystemWritable) {
            cd.AddChild("system_writable");
        }
        if (channelInfo.Writable) {
            cd.AddChild("writable");
        }
        cd.AddChild("free", static_cast<ui32>(channelInfo.FreeSpaceShare * 100));
        const auto groupUrl = getGroupUrl(latestEntry->GroupID, channel.StoragePool);
        if (groupUrl) {
            cd.AddChild("group_url", groupUrl);
        }
        buildReassignButton(
                    cd,
                    hiveTabletId,
                    storage.TabletID,
                    channel.Channel);
    }
}

}   // namespace NCloud::NStorage
