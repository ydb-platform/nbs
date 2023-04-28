#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/storage/core/monitoring_utils.h>
#include <cloud/blockstore/libs/storage/disk_common/monitoring_utils.h>
#include <cloud/storage/core/libs/common/format.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/stream/str.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NMonitoringUtils;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

void BuildVolumeReallocateButton(
    IOutputStream& out,
    ui64 tabletId,
    const TString& diskId)
{
    out << Sprintf(R"html(
        <form method='POST' name='volRealloc%s'>
            <input
                class='btn btn-primary'
                type='button'
                value='Reallocate'
                title="Initiate update volume config"
                data-toggle='modal'
                data-target='#fire-reallocate%s'
            />

            <input type='hidden' name='action' value='volumeRealloc'/>
            <input type='hidden' name='DiskID' value='%s'/>
            <input type='hidden' name='TabletID' value='%lu'/>
        </form>
    )html", diskId.c_str(), diskId.c_str(), diskId.c_str(), tabletId);

    BuildConfirmActionDialog(
        out,
        Sprintf("fire-reallocate%s", diskId.c_str()),
        "Fire Reallocate",
        Sprintf("Are you sure you want to reallocate volume %s?", diskId.c_str()),
        Sprintf("fireVolumeRealloc(\"%s\")", diskId.c_str())
    );
}

void BuildDeviceReplaceButton(
    IOutputStream& out,
    ui64 tabletId,
    const TString& diskId,
    const TString& deviceId)
{
    out << Sprintf(R"html(
        <form method='POST' name='devReplace%s'>
            <input
                class='btn btn-primary'
                type='button'
                value='Replace'
                title="Replace device"
                data-toggle='modal'
                data-target='#fire-replace%s'
            />

            <input type='hidden' name='action' value='replaceDevice'/>
            <input type='hidden' name='DeviceUUID' value='%s'/>
            <input type='hidden' name='DiskID' value='%s'/>
            <input type='hidden' name='TabletID' value='%lu'/>
        </form>
    )html", deviceId.c_str(), deviceId.c_str(), deviceId.c_str(), diskId.c_str(), tabletId);

    BuildConfirmActionDialog(
        out,
        Sprintf("fire-replace%s", deviceId.c_str()),
        "Replace Device",
        Sprintf(R"(Are you sure you want to replace device "%s" for volume "%s"?)",
            deviceId.c_str(),
            diskId.c_str()),
        Sprintf(R"(fireReplaceDevice("%s"))"
            , deviceId.c_str())
    );
}

void GenerateDiskRegistryActionsJS(IOutputStream& out)
{
    out << R"html(
        <script type='text/javascript'>
        function fireVolumeRealloc(diskId) {
            document.forms['volRealloc'+diskId].submit();
        }
        </script>
    )html";
}

void GenerateVolumeActionsJS(IOutputStream& out)
{
    out << R"html(
        <script type='text/javascript'>
        function fireReplaceDevice(deviceId) {
            document.forms['devReplace'+deviceId].submit();
        }
        </script>
    )html";
}

void DumpDiskLink(IOutputStream& out, const ui64 tabletId, const TString& diskId)
{
    out << "<a href='?action=disk&TabletID="
        << tabletId
        << "&DiskID="
        << diskId
        << "'>"
        << diskId
        << "</a>";
}

void DumpDeviceLink(IOutputStream& out, ui64 tabletId, TStringBuf uuid)
{
    out << "<a href='?action=dev&TabletID="
        << tabletId
        << "&DeviceUUID="
        << uuid
        << "'>"
        << uuid
        << "</a>";
}

template <typename C>
void DumpSize(IOutputStream& out, const C& c)
{
    const auto size = std::size(c);
    if (size) {
        out << " <font color=gray>" << size << "</font>";
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleHttpInfo_RenderDeviceHtmlInfo(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    const auto& deviceId = params.Get("DeviceUUID");

    if (!deviceId) {
        RejectHttpRequest(
            ctx,
            *requestInfo,
            "No device id is given");
        return;
    }

    TStringStream out;

    RenderDeviceHtmlInfo(out, deviceId);

    SendHttpResponse(ctx, *requestInfo, std::move(out.Str()));
}

void TDiskRegistryActor::RenderDeviceHtmlInfo(
    IOutputStream& out,
    const TString& id) const
{
    HTML(out) {
        TAG(TH3) { out << "Device " << id.Quote(); }

        const auto& device = State->GetDevice(id);

        DIV() { out << "Name: " << device.GetDeviceName(); }
        DIV() { out << "S/N: " << device.GetSerialNumber(); }
        DIV() { out << "Block size: " << device.GetBlockSize(); }
        DIV() {
            const auto bytes = device.GetBlocksCount() * device.GetBlockSize();
            out << "Blocks: " << device.GetBlocksCount()
                << " <font color=gray>" << device.GetUnadjustedBlockCount()
                << "</font>"
                << " (" << FormatByteSize(bytes) << ")";
        }
        DIV() { out << "Transport id: " << device.GetTransportId(); }

        DIV() {
            if (device.HasRdmaEndpoint()) {
                auto e = device.GetRdmaEndpoint();
                out << "Rdma endpoint: " << e.GetHost() << ":" << e.GetPort();
            }
        }

        DIV() {
            out << "Pool: ";
            if (device.GetPoolName()) {
                out << device.GetPoolName();
            } else {
                out << "<font color=gray>default</font>";
            }
        }

        DIV() {
            auto agentId = State->GetAgentId(device.GetNodeId());

            if (agentId) {
                out << "NodeId: <a href='?action=agent&TabletID="
                    << TabletID()
                    << "&AgentID=" << agentId << "'>"
                    << agentId << "</a>"
                    << " <font color=gray>#"
                    << device.GetNodeId()
                    << "</font>";
            } else {
                out << "NodeId: " << device.GetNodeId();
            }
        }

        DIV() {
            out << "Rack: ";
            out << device.GetRack();
        }

        DIV() { out << "State: "; DumpState(out, device.GetState()); }
        DIV() {
            out << "State Timestamp: "
                << TInstant::MicroSeconds(device.GetStateTs());
        }
        DIV() { out << "State Message: " << device.GetStateMessage(); }

        if (auto diskId = State->FindDisk(id)) {
            DIV() {
                out << "Disk: ";
                DumpDiskLink(out, TabletID(), diskId);
            }
        }
    }
}

void TDiskRegistryActor::HandleHttpInfo_RenderAgentHtmlInfo(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    const auto& agentId = params.Get("AgentID");

    if (!agentId) {
        RejectHttpRequest(
            ctx,
            *requestInfo,
            "No agent id is given");
        return;
    }

    TStringStream out;

    RenderAgentHtmlInfo(out, agentId);

    SendHttpResponse(ctx, *requestInfo, std::move(out.Str()));
}

void TDiskRegistryActor::RenderAgentHtmlInfo(
    IOutputStream& out,
    const TString& id) const
{
    auto* agent = State->FindAgent(id);
    if (!agent) {
        return;
    }

    HTML(out) {
        TAG(TH3) {
            out << "Agent " << id.Quote();
        }

        DIV() {
            TString rack;
            for (const auto& device: agent->GetDevices()) {
                if (device.GetRack()) {
                    rack = device.GetRack();
                    break;
                }
            }
            out << "Rack: " << rack;
        }

        DIV() { out << "State: "; DumpState(out, agent->GetState()); }
        DIV() {
            out << "State Timestamp: "
                << TInstant::MicroSeconds(agent->GetStateTs());
        }
        DIV() { out << "State Message: " << agent->GetStateMessage(); }
        DIV() {
            out << "CMS Timestamp: "
                << TInstant::MicroSeconds(agent->GetCmsTs());
        }
        DIV() {
            out << "Work Timestamp: "
                << TInstant::Seconds(agent->GetWorkTs());
        }

        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "UUID"; }
                    TABLEH() { out << "Name"; }
                    TABLEH() { out << "S/N"; }
                    TABLEH() { out << "State"; }
                    TABLEH() { out << "State Ts"; }
                    TABLEH() { out << "State Message"; }
                    TABLEH() { out << "Block size"; }
                    TABLEH() { out << "Block count"; }
                    TABLEH() { out << "Size"; }
                    TABLEH() { out << "Transport id"; }
                    TABLEH() { out << "Rdma endpoint"; }
                    TABLEH() { out << "DiskId"; }
                    TABLEH() { out << "Pool"; }
                }
            }

            for (const auto& device: agent->GetDevices()) {
                TABLER() {
                    TABLED() {
                        DumpDeviceLink(out, TabletID(), device.GetDeviceUUID());
                    }
                    TABLED() { out << device.GetDeviceName(); }
                    TABLED() { out << device.GetSerialNumber(); }
                    TABLED() { DumpState(out, device.GetState()); }
                    TABLED() {
                        out << TInstant::MicroSeconds(device.GetStateTs());
                    }
                    TABLED() { out << device.GetStateMessage(); }
                    TABLED() { out << device.GetBlockSize(); }
                    TABLED() {
                        out << device.GetBlocksCount()
                            << " (" << device.GetUnadjustedBlockCount() << ")";
                    }
                    TABLED() {
                        const auto bytes =
                            device.GetBlockSize() * device.GetBlocksCount();
                        out << FormatByteSize(bytes);
                    }
                    TABLED() { out << device.GetTransportId(); }
                    TABLED() {
                        if (device.HasRdmaEndpoint()) {
                            auto e = device.GetRdmaEndpoint();
                            out << e.GetHost() << ":" << e.GetPort();
                        }
                    }
                    TABLED() {
                        if (auto id = State->FindDisk(device.GetDeviceUUID())) {
                            DumpDiskLink(out, TabletID(), id);
                        }
                    }
                    TABLED() { out << device.GetPoolName(); }
                }
            }
        }
    }
}

void TDiskRegistryActor::HandleHttpInfo_RenderDiskHtmlInfo(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    const auto& diskId = params.Get("DiskID");

    if (!diskId) {
        RejectHttpRequest(
            ctx,
            *requestInfo,
            "No disk id is given");
        return;
    }

    TStringStream out;

    RenderDiskHtmlInfo(out, diskId);

    SendHttpResponse(ctx, *requestInfo, std::move(out.Str()));
}

void TDiskRegistryActor::RenderDiskHtmlInfo(
    IOutputStream& out,
    const TString& id) const
{
    HTML(out) {
        TDiskInfo info;
        auto error = State->GetDiskInfo(id, info);
        if (HasError(error)) {
            DIV() {
                out << "Error: " << FormatError(error);
            }
            return;
        }

        const bool replaceDeviceAllowed = Config->IsReplaceDeviceFeatureEnabled(
            info.CloudId,
            info.FolderId);

        TAG(TH3) {
            out << "Disk " << id.Quote();
            if (info.MasterDiskId) {
                out << " (Replica of " << info.MasterDiskId << ")";
            }
        }

        DIV() {
            out << "Cloud Id: " << info.CloudId
                << " Folder Id: " << info.FolderId;
            if (info.UserId) {
                out << " User Id: " << info.UserId;
            }
        }

        DIV() {
            out << "<a href='../blockstore/service?action=search&Volume="
                << (info.MasterDiskId ? info.MasterDiskId : id)
                << "'>"
                << "Search this volume"
                << "</a>";
        }
        DIV() { out << "State: "; DumpState(out, info.State); }
        DIV() { out << "State timestamp: " << info.StateTs; }

        if (info.PlacementGroupId) {
            DIV() { out << "PlacementGroupId: " << info.PlacementGroupId; }
        }
        if (info.PlacementPartitionIndex) {
            DIV()
            {
                out << "PlacementPartitionIndex: "
                    << info.PlacementPartitionIndex;
            }
        }

        if (State->IsAcquireInProgress(id)) {
            DIV() { out << "Acquire in progress"; }
        }

        const bool isNonrepl = info.Replicas.empty();

        auto makeHeaderCells = [&] (const ui32 replicaNo) {
            TABLEH() {
                out << "Device";
                if (replicaNo) {
                    out << " (Replica " << replicaNo << ")";
                }
            }
            TABLEH() { out << "Node"; }
            TABLEH() { out << "State"; }
            TABLEH() { out << "State Timestamp"; }
            if (isNonrepl) {
                TABLEH() { out << "Action"; }
            } else {
                TABLEH() { out << "DataState"; }
            }
        };

        auto makeDeviceCells = [&] (const NProto::TDeviceConfig& device) {
            TABLED() {
                DumpDeviceLink(
                    out,
                    TabletID(),
                    device.GetDeviceUUID());
            }
            TABLED() {
                const auto* agent =
                    State->FindAgent(device.GetNodeId());

                if (agent) {
                    out << "<a href='?action=agent&TabletID="
                        << TabletID()
                        << "&AgentID=" << agent->GetAgentId()
                        << "'>"
                        << agent->GetAgentId() << "</a>"
                        << " <font color=gray>#"
                        << device.GetNodeId()
                        << "</font>";
                    if (agent->GetState()
                            != NProto::AGENT_STATE_ONLINE)
                    {
                        out << " ";
                        DumpState(out, agent->GetState());
                    }
                } else {
                    out << device.GetNodeId();
                }
            }
            TABLED() {
                DumpState(out, device.GetState());
            }
            TABLED() {
                out << TInstant::MicroSeconds(device.GetStateTs());
            }
            TABLED() {

                if (isNonrepl && replaceDeviceAllowed) {
                    BuildDeviceReplaceButton(
                        out,
                        TabletID(),
                        id,
                        device.GetDeviceUUID());
                }

                if (!isNonrepl) {
                    auto it = Find(info.DeviceReplacementIds, device.GetDeviceUUID());
                    if (it == info.DeviceReplacementIds.end()) {
                        out << "<font color=green>Ready</font>";
                    } else {
                        out << "<font color=blue>Fresh</font>";
                    }
                }
            }
        };

        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    makeHeaderCells(0);
                    for (ui32 i = 0; i < info.Replicas.size(); ++i) {
                        makeHeaderCells(i + 1);
                    }
                }
                for (ui32 i = 0; i < info.Devices.size(); ++i) {
                    TABLER() {
                        makeDeviceCells(info.Devices[i]);
                        for (const auto& replica: info.Replicas) {
                            makeDeviceCells(i < replica.size()
                                ? replica[i] : NProto::TDeviceConfig());
                        }
                    }
                }
            }
        }

        if (info.Migrations) {
            TAG(TH3) { out << "Active migrations"; DumpSize(out, info.Migrations); }

            TABLE_SORTABLE_CLASS("table table-bordered") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "SourceDeviceId"; }
                        TABLEH() { out << "TargetDeviceId"; }
                        TABLEH() { out << "Node"; }
                    }

                    auto dumpLink = [&] (const auto& uuid) {
                        DumpDeviceLink(out, TabletID(), uuid);
                    };

                    for (const auto& migration: info.Migrations) {
                        TABLER() {
                            TABLED() {
                                dumpLink(migration.GetSourceDeviceId());
                            }
                            TABLED() {
                                const auto& d = migration.GetTargetDevice();
                                dumpLink(d.GetDeviceUUID());
                            }
                            TABLED() {
                                const auto& d = migration.GetTargetDevice();
                                auto* agent = State->FindAgent(d.GetNodeId());
                                if (agent) {
                                    out << " <a href='?action=agent&TabletID="
                                        << TabletID()
                                        << "&AgentID=" << d.GetAgentId() << "'>"
                                        << d.GetAgentId() << "</a>"
                                        << " <font color=gray>#"
                                        << d.GetNodeId()
                                        << "</font>";
                                }
                            }
                        }
                    }
                }
            }
        }

        if (info.FinishedMigrations) {
            TAG(TH3) {
                out << "Canceled/finished migrations";
                DumpSize(out, info.FinishedMigrations);
            }

            TABLE_SORTABLE_CLASS("table table-bordered") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "DeviceId"; }
                        TABLEH() { out << "SeqNo"; }
                    }

                    for (const auto& [uuid, seqNo]: info.FinishedMigrations) {
                        TABLER() {
                            TABLED() { DumpDeviceLink(out, TabletID(), uuid); }
                            TABLED() { out << seqNo; }
                        }
                    }
                }
            }
        }

        GenerateVolumeActionsJS(out);
    }
}

void TDiskRegistryActor::RenderMigrationList(IOutputStream& out) const
{
    auto const migrations = State->BuildMigrationList();

    HTML(out) {
        TAG(TH3) { out << "Migration List"; DumpSize(out, migrations); }

        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Disk"; }
                    TABLEH() { out << "Source Device"; }
                }
            }

            for (const auto& [diskId, uuid]: migrations) {
                TABLER() {
                    TABLED() { DumpDiskLink(out, TabletID(), diskId); }
                    TABLED() { DumpDeviceLink(out, TabletID(), uuid); }
                }
            }
        }
    }
}

void TDiskRegistryActor::RenderDiskList(IOutputStream& out) const
{
    const auto ids = State->GetDiskIds();

    HTML(out) {
        TAG(TH3) { out << "Disks"; DumpSize(out, ids); }

        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Disk"; }
                    TABLEH() { out << "State"; }
                    TABLEH() { out << "State Timestamp"; }
                    TABLEH() { out << "Action"; }
                }
            }

            for (const auto& id: ids) {
                TDiskInfo diskInfo;
                State->GetDiskInfo(id, diskInfo);

                TABLER() {
                    TABLED() { DumpDiskLink(out, TabletID(), id); }
                    TABLED() { DumpState(out, diskInfo.State); }
                    TABLED() { out << diskInfo.StateTs; }
                    TABLED() {
                        BuildVolumeReallocateButton(out, TabletID(), id);
                    }
                }
            }
        }
    }
}

void TDiskRegistryActor::RenderBrokenDiskList(IOutputStream& out) const
{
    HTML(out) {
        TAG(TH3) { out << "BrokenDisks"; DumpSize(out, State->GetBrokenDisks()); }

        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Disk"; }
                    TABLEH() { out << "Destruction scheduled"; }
                    TABLEH() { out << "Destruction started"; }
                }
            }

            for (const auto& x: State->GetBrokenDisks()) {
                TABLER() {
                    TABLED() { DumpDiskLink(out, TabletID(), x.DiskId); }
                    TABLED() { out << x.TsToDestroy; }
                    auto it = Find(DisksBeingDestroyed, x.DiskId);

                    if (it != DisksBeingDestroyed.end()) {
                        TABLED() { out << BrokenDisksDestructionStartTs; }
                    } else {
                        TABLED() { out << "destruction not started"; }
                    }
                }
            }
        }
    }
}

void TDiskRegistryActor::RenderDisksToNotify(IOutputStream& out) const
{
    HTML(out) {
        TAG(TH3) {
            out << "DisksToNotify";
            DumpSize(out, State->GetDisksToNotify());
        }

        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Disk"; }
                    TABLEH() { out << "Notification scheduled"; }
                }
            }

            for (const auto& p: State->GetDisksToNotify()) {
                const auto& diskId = p.first;

                TABLER() {
                    TABLED() {
                        DumpDiskLink(out, TabletID(), p.first);
                        out << " <font color=gray>" << p.second << "</font>";
                    }

                    auto it = FindIf(DisksBeingNotified, [&] (auto& x) {
                        return x.DiskId == diskId;
                    });

                    if (it != DisksBeingNotified.end()) {
                        TABLED() { out << DisksNotificationStartTs; }
                    } else {
                        TABLED() { out << "notification not started"; }
                    }
                }
            }
        }
    }
}

void TDiskRegistryActor::RenderErrorNotifications(IOutputStream& out) const
{
    HTML(out) {
        TAG(TH3) {
            out << "Error notifications";
            DumpSize(out, State->GetErrorNotifications());
        }

        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Disk"; }
                    TABLEH() { out << "Notification scheduled"; }
                }
            }

            for (const auto& diskId: State->GetErrorNotifications()) {
                TABLER() {
                    TABLED() { DumpDiskLink(out, TabletID(), diskId); }

                    if (FindPtr(ErrorNotifications, diskId)) {
                        TABLED() { out << UsersNotificationStartTs; }
                    } else {
                        TABLED() { out << "notification not started"; }
                    }
                }
            }
        }
    }
}

void TDiskRegistryActor::RenderRacks(IOutputStream& out) const
{
    static const char svg[] = "svg";
    static const char rect[] = "rect";
    static const char text[] = "text";

    auto racks = State->GatherRacksInfo();

    HTML(out) {
        TAG(TH3) { out << "Racks"; DumpSize(out, racks); }

        ui64 totalFreeBytes = 0;
        ui64 totalBytes = 0;
        ui64 minFreeBytesAmongRacks = racks ? Max<ui64>() : 0;
        ui64 maxFreeBytesAmongRacks = 0;
        ui64 totalBytesInTheLeastOccupiedRack = 0;
        ui64 totalBytesInTheMostOccupiedRack = 0;

        for (const auto& rack: racks) {
            totalFreeBytes += rack.FreeBytes;
            totalBytes += rack.TotalBytes;
            if (rack.FreeBytes < minFreeBytesAmongRacks) {
                minFreeBytesAmongRacks = rack.FreeBytes;
                totalBytesInTheMostOccupiedRack = rack.TotalBytes;
            }
            if (rack.FreeBytes > maxFreeBytesAmongRacks) {
                maxFreeBytesAmongRacks = rack.FreeBytes;
                totalBytesInTheLeastOccupiedRack = rack.TotalBytes;
            }
        }

        auto makeText = [&] (ui32 x, ui32 y, TString value) {
            TAG_ATTRS(
                TTag<text>,
                {
                    {"x", ToString(x)},
                    {"y", ToString(y)},
                    {"fill", "black"}
                }
            ) {
                out << value;
            }
        };

        auto makeRect = [&] (ui32 x, ui32 y, ui32 w, ui32 h, TString color) {
            TAG_ATTRS(
                TTag<rect>,
                {
                    {"x", ToString(x)},
                    {"y", ToString(y)},
                    {"width", ToString(w)},
                    {"height", ToString(h)},
                    {"fill", color}
                }
            ) {}
        };

        const auto freeColor = "rgb(47, 245, 53)";
        const auto dirtyColor = "rgb(99, 99, 85)";
        const auto occupiedColor = "rgb(232, 47, 245)";
        const auto warningColor = "rgb(234, 245, 34)";
        const auto unavailableColor = "rgb(242, 152, 17)";
        const auto brokenColor = "rgb(245, 47, 47)";

        auto makeBar =
            [&] (ui64 val, ui64 total, TString valcolor, TString restcolor)
            {
                TAG_ATTRS(
                        TTag<svg>,
                        {
                            {"width", "200"},
                            {"height", "30"}
                        })
                {
                    auto valw = 200 * double(val) / Max(total, 1UL);
                    makeRect(0, 0, valw, 30, valcolor);
                    makeRect(valw, 0, 200 - valw, 30, restcolor);
                }
            };

        TABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Stat"; }
                    TABLEH() { out << "Free"; }
                    TABLEH() { out << "Total"; }
                    TABLEH() { out << "Free / Occupied"; }
                }

                TABLER() {
                    TABLED() { out << "Overall"; }
                    TABLED() { out << FormatByteSize(totalFreeBytes); }
                    TABLED() { out << FormatByteSize(totalBytes); }
                    TABLED() {
                        makeBar(
                            totalFreeBytes,
                            totalBytes,
                            freeColor,
                            occupiedColor
                        );
                    }
                }

                TABLER() {
                    TABLED() { out << "Most Occupied Rack"; }
                    TABLED() { out << FormatByteSize(minFreeBytesAmongRacks); }
                    TABLED() {
                        out << FormatByteSize(totalBytesInTheMostOccupiedRack);
                    }
                    TABLED() {
                        makeBar(
                            minFreeBytesAmongRacks,
                            totalBytesInTheMostOccupiedRack,
                            freeColor,
                            occupiedColor
                        );
                    }
                }

                TABLER() {
                    TABLED() { out << "Least Occupied Rack"; }
                    TABLED() { out << FormatByteSize(maxFreeBytesAmongRacks); }
                    TABLED() {
                        out << FormatByteSize(totalBytesInTheLeastOccupiedRack);
                    }
                    TABLED() {
                        makeBar(
                            maxFreeBytesAmongRacks,
                            totalBytesInTheLeastOccupiedRack,
                            freeColor,
                            occupiedColor
                        );
                    }
                }
            }
        }

        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Rack"; }
                    TABLEH() { out << "Free"; }
                    TABLEH() { out << "Warning"; }
                    TABLEH() { out << "Total"; }
                    TABLEH() { out << "Free / Other"; }
                    TABLEH() { out << "Groups"; }
                    TABLEH() { out << "Visualization"; }
                }
            }

            for (const auto& rack: racks) {
                TABLER() {
                    TABLED() { out << rack.Name; }
                    TABLED() { out << FormatByteSize(rack.FreeBytes); }
                    TABLED() { out << FormatByteSize(rack.WarningBytes); }
                    TABLED() { out << FormatByteSize(rack.TotalBytes); }
                    TABLED() {
                        makeBar(
                            rack.FreeBytes,
                            rack.TotalBytes,
                            freeColor,
                            occupiedColor
                        );
                    }
                    TABLED() {
                        UL() {
                            for (const auto& [placementGroup, placementPartitions]: rack.PlacementGroups) {
                                LI() { out << placementGroup; }
                                if (!placementPartitions.empty()) {
                                    UL() {
                                        for (const auto& placementPartition: placementPartitions) {
                                            LI() { out << placementPartition; }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    TABLED() {
                        const auto agentHeight = 200;
                        const auto agentWidth = 90;
                        const auto agentGap = 10;
                        const auto textHeight = 15;

                        const auto imageWidth =
                            agentWidth * rack.AgentInfos.size()
                            + agentGap * (rack.AgentInfos.size() - 1);
                        const auto imageHeight = agentHeight + 2 * textHeight;

                        TAG_ATTRS(
                                TTag<svg>,
                                {
                                    {"width", ToString(imageWidth)},
                                    {"height", ToString(imageHeight)}
                                })
                        {
                            ui32 curX = 0;

                            for (const auto& agent: rack.AgentInfos) {
                                ui32 curY = textHeight;

                                {
                                    auto label = agent.AgentId;
                                    auto idx = label.find('.');
                                    if (idx != TString::npos) {
                                        label = label.substr(0, idx);
                                    }
                                    makeText(curX, curY, label);
                                    curY += textHeight;
                                }

                                const double realHeight = agent.TotalDevices;

                                const auto r = agentHeight / realHeight;

                                const auto occupiedDevices = agent.TotalDevices
                                    - agent.FreeDevices
                                    - agent.DirtyDevices
                                    - agent.WarningDevices
                                    - agent.UnavailableDevices
                                    - agent.BrokenDevices;

                                makeRect(
                                    curX,
                                    curY,
                                    agentWidth,
                                    agent.FreeDevices * r,
                                    freeColor
                                );
                                curY += agent.FreeDevices * r;
                                makeRect(
                                    curX,
                                    curY,
                                    agentWidth,
                                    agent.DirtyDevices * r,
                                    dirtyColor
                                );
                                curY += agent.DirtyDevices * r;
                                makeRect(
                                    curX,
                                    curY,
                                    agentWidth,
                                    occupiedDevices * r,
                                    occupiedColor
                                );
                                curY += occupiedDevices * r;
                                makeRect(
                                    curX,
                                    curY,
                                    agentWidth,
                                    agent.WarningDevices * r,
                                    warningColor
                                );
                                curY += agent.WarningDevices * r;
                                makeRect(
                                    curX,
                                    curY,
                                    agentWidth,
                                    agent.UnavailableDevices * r,
                                    unavailableColor
                                );
                                curY += agent.UnavailableDevices * r;
                                makeRect(
                                    curX,
                                    curY,
                                    agentWidth,
                                    agent.BrokenDevices * r,
                                    brokenColor
                                );

                                curX += agentWidth + agentGap;
                            }
                        }
                    }
                }
            }
        }
    }
}

void TDiskRegistryActor::RenderPlacementGroupList(IOutputStream& out) const
{
    auto brokenGroups = State->GatherBrokenGroupsInfo(
        Now(),
        Config->GetPlacementGroupAlertPeriod());

    HTML(out) {
        TAG(TH3) {
            out << "PlacementGroups";
            DumpSize(out, State->GetPlacementGroups());
        }

        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "GroupId"; }
                    TABLEH() { out << "Stats"; }
                    TABLEH() { out << "Disks"; }
                }
            }

            for (const auto& x: State->GetPlacementGroups()) {
                TABLER() {
                    TABLED() {
                        auto it = brokenGroups.find(x.first);

                        if (it != brokenGroups.end()) {
                            if (it->second.RecentlyBrokenDiskCount == 1) {
                                out << "<font color=yellow>&#9632;&nbsp;</font>";
                            }

                            if (it->second.RecentlyBrokenDiskCount > 1) {
                                out << "<font color=red>&#9632;&nbsp;</font>";
                            }
                        }

                        out << x.first;
                    }
                    TABLED() {
                        if (x.second.Full) {
                            out << "<font color=red>GROUP IS FULL</font>, ";
                        }

                        out << "BiggestDisk: " << x.second.BiggestDiskId
                            << " ("
                            << FormatByteSize(x.second.BiggestDiskSize) << ")";
                    }
                    TABLED() {
                        TABLE_SORTABLE_CLASS("table table-bordered") {
                            TABLER() {
                                TABLED() { out << "ConfigVersion"; }
                                TABLED() { out << x.second.Config.GetConfigVersion(); }
                            }

                            TABLER() {
                                TABLED() { out << "Disks"; }
                                TABLED() {
                                    TABLE_SORTABLE_CLASS("table table-bordered") {
                                        TABLEHEAD() {
                                            TABLER() {
                                                TABLEH() { out << "DiskId"; }
                                                TABLEH() { out << "Racks"; }
                                            }
                                        }
                                        for (const auto& d: x.second.Config.GetDisks()) {
                                            TABLER() {
                                                TABLED() { DumpDiskLink(out, TabletID(), d.GetDiskId()); }
                                                TABLED() {
                                                    for (ui32 i = 0; i < d.DeviceRacksSize(); ++i) {
                                                        const auto& rack = d.GetDeviceRacks(i);
                                                        if (i) {
                                                            out << ", ";
                                                        }

                                                        out << rack;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

void TDiskRegistryActor::RenderAgentList(TInstant now, IOutputStream& out) const
{
    HTML(out) {
        TAG(TH3) { out << "Agents"; DumpSize(out, State->GetAgents()); }

        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Agent"; }
                    TABLEH() { out << "Node"; }
                    TABLEH() { out << "SeqNo"; }
                    TABLEH() { out << "State"; }
                    TABLEH() { out << "Dedicated"; }
                    TABLEH() { out << "State timestamp"; }
                    TABLEH() { out << "State message"; }
                }

                for (const auto& config: State->GetAgents()) {
                    TABLER() {
                        TABLED() {
                            out << "<a href='?action=agent&TabletID="
                                << TabletID()
                                << "&AgentID="
                                << config.GetAgentId()
                                << "'>"
                                << config.GetAgentId()
                                << "</a>";
                        }
                        TABLED() {
                            out << config.GetNodeId();
                        }
                        TABLED() {
                            out << config.GetSeqNumber();
                        }
                        TABLED() {
                            DumpState(out, config.GetState());

                            auto it = AgentRegInfo.find(config.GetAgentId());

                            const bool connected = it != AgentRegInfo.end()
                                && it->second.Connected;

                            if (config.GetState()
                                    != NProto::AGENT_STATE_UNAVAILABLE)
                            {
                                if (!connected) {
                                    out << " <font color=gray>disconnected</font>";
                                }
                            } else if (connected) {
                                out << " <font color=gray>connected</font>";
                            }
                        }
                        TABLED() {
                            out << (config.GetDedicatedDiskAgent()
                                    ? "true" : "false");
                        }
                        TABLED() {
                            out << TInstant::MicroSeconds(config.GetStateTs());
                        }
                        TABLED() { out << config.GetStateMessage(); }
                    }
                }
            }
        }

        TAG(TH3) { out << "AgentList Parameters"; }

        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Name"; }
                    TABLEH() { out << "Value"; }
                }

                TABLER() {
                    TABLED() {
                        out << "RejectAgentTimeout(" << now << ")";
                    }
                    TABLED() {
                        out << State->GetRejectAgentTimeout(now);
                    }
                }
            }
        }
    }
}

void TDiskRegistryActor::RenderConfig(IOutputStream& out) const
{
    HTML(out) {
        const auto& config = State->GetConfig();
        TAG(TH4) { out << "Version: " << config.GetVersion(); }
        TAG(TH4) { out << "Known agents"; }
        UL() {
            for (const auto& agent: config.GetKnownAgents()) {
                LI() {
                    // TODO: check if present
                    out << "<a href='?action=agent&TabletID="
                        << TabletID()
                        << "&AgentID="
                        << agent.GetAgentId()
                        << "'>"
                        << agent.GetAgentId()
                        << "</a>";
                    UL() {
                        for (const auto& device: agent.GetDevices()) {
                            LI() {
                                DumpDeviceLink(
                                    out,
                                    TabletID(),
                                    device.GetDeviceUUID());
                            }
                        }
                    }
                }
            }
        }
        TAG(TH4) { out << "Device overrides"; }
        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "DiskId"; }
                    TABLEH() { out << "DeviceId"; }
                    TABLEH() { out << "BlocksCount"; }
                }
            }
            for (const auto& o: config.GetDeviceOverrides()) {
                TABLER() {
                    TABLED() {
                        DumpDiskLink(out, TabletID(), o.GetDiskId());
                    }
                    TABLED() {
                        DumpDeviceLink(out, TabletID(), o.GetDevice());
                    }
                    TABLED() {
                        out << o.GetBlocksCount();
                    }
                }
            }
        }

        TAG(TH4) { out << "Device Pools"; }
        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Name"; }
                    TABLEH() { out << "Allocation Unit"; }
                    TABLEH() { out << "Kind"; }
                }
            }
            for (const auto& pool: config.GetDevicePoolConfigs()) {
                TABLER() {
                    TABLED() {
                        if (pool.GetName().empty()) {
                            out << "<font color=gray>default</font>";
                        } else {
                            out << pool.GetName();
                        }
                    }
                    TABLED() {
                        out << FormatByteSize(pool.GetAllocationUnit())
                            << " <font color=gray>" << pool.GetAllocationUnit()
                            << " B</font>";
                    }
                    TABLED() {
                        switch (pool.GetKind()) {
                            case NProto::DEVICE_POOL_KIND_DEFAULT:
                            case NProto::DEVICE_POOL_KIND_GLOBAL:
                                out << "global";
                                break;
                            case NProto::DEVICE_POOL_KIND_LOCAL:
                                out << "local";
                                break;
                            default:
                                out << "? " << static_cast<int>(pool.GetKind());
                                break;
                        }
                    }
                }
            }
        }
    }
}

void TDiskRegistryActor::RenderDirtyDeviceList(IOutputStream& out) const
{
    const auto devices = State->GetDirtyDevices();

    HTML(out) {
        TAG(TH3) { out << "Dirty devices"; DumpSize(out, devices); }

        UL() {
            for (const auto& device: devices) {
                LI() {
                    DumpDeviceLink(out, TabletID(), device.GetDeviceUUID());
                    out << " (#" << device.GetNodeId() << " )";
                }
            }
        }
    }
}

void TDiskRegistryActor::RenderSuspendedDeviceList(IOutputStream& out) const
{
    const auto uuids = State->GetSuspendedDevices();
    if (uuids.empty()) {
        return;
    }

    HTML(out) {
        TAG(TH3) { out << "Suspended devices"; }

        UL() {
            for (const auto& uuid: uuids) {
                LI() {
                    DumpDeviceLink(out, TabletID(), uuid);
                    auto config = State->GetDevice(uuid);
                    if (config.GetNodeId() != 0) {
                        out << " (#" << config.GetNodeId() << " )";
                    }
                }
            }
        }
    }
}

void TDiskRegistryActor::RenderHtmlInfo(TInstant now, IOutputStream& out) const
{
    using namespace NMonitoringUtils;

    DumpDefaultHeader(out, *Info(), SelfId().NodeId(), *DiagnosticsConfig);

    HTML(out) {
        if (State) {
            RenderState(out);

            RenderDiskList(out);

            RenderMigrationList(out);

            RenderBrokenDiskList(out);

            RenderDisksToNotify(out);

            RenderErrorNotifications(out);

            RenderPlacementGroupList(out);

            RenderRacks(out);

            RenderAgentList(now, out);

            TAG(TH3) { out << "Config"; }
            RenderConfig(out);

            RenderDirtyDeviceList(out);

            RenderSuspendedDeviceList(out);
        } else {
            TAG(TH3) { out << "Initialization in progress..."; }
        }

        TAG(TH3) { out << "Storage Config"; }
        Config->DumpHtml(out);

        GenerateDiskRegistryActionsJS(out);
    }
}


void TDiskRegistryActor::RenderState(IOutputStream& out) const
{
    using namespace NMonitoringUtils;

    HTML(out) {
        TAG(TH3) {
            bool colorRed =
                CurrentState == STATE_READ_ONLY ||
                CurrentState == STATE_RESTORE;
            out << "Current state: ";
            if (colorRed) {
                out << "<font color=red>";
            }
            out << GetStateName(CurrentState);
            if (CurrentState == STATE_RESTORE) {
                out << " Need to kill tablet</font>";
            }
            if (colorRed) {
                out << "</font>";
            }
        }
    }
}

void TDiskRegistryActor::HandleHttpInfo(
    const NMon::TEvRemoteHttpInfo::TPtr& ev,
    const TActorContext& ctx)
{
    using THttpHandler = void(TDiskRegistryActor::*)(
        const NActors::TActorContext&,
        const TCgiParameters&,
        TRequestInfoPtr);

    using THttpHandlers = THashMap<TString, THttpHandler>;

    static const THttpHandlers postActions {{
        {"volumeRealloc", &TDiskRegistryActor::HandleHttpInfo_VolumeRealloc  },
        {"replaceDevice", &TDiskRegistryActor::HandleHttpInfo_ReplaceDevice  },
    }};

    static const THttpHandlers getActions {{
        {"dev",    &TDiskRegistryActor::HandleHttpInfo_RenderDeviceHtmlInfo  },
        {"agent",  &TDiskRegistryActor::HandleHttpInfo_RenderAgentHtmlInfo   },
        {"disk",   &TDiskRegistryActor::HandleHttpInfo_RenderDiskHtmlInfo    },
    }};

    auto* msg = ev->Get();

    LOG_DEBUG(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] HTTP request: %s",
        TabletID(),
        msg->Query.Quote().data());

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        MakeIntrusive<TCallContext>(),
        std::move(ev->TraceId));

    auto methodType = GetHttpMethodType(*msg);
    auto params = GetHttpMethodParameters(*msg);
    const auto& action = params.Get("action");

    if (auto* handler = postActions.FindPtr(action)) {
        if (methodType != HTTP_METHOD_POST) {
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
    RenderHtmlInfo(ctx.Now(), out);

    SendHttpResponse(ctx, *requestInfo, std::move(out.Str()));
}

void TDiskRegistryActor::RejectHttpRequest(
    const TActorContext& ctx,
    TRequestInfo& requestInfo,
    TString message)
{
    LOG_ERROR_S(ctx, TBlockStoreComponents::DISK_REGISTRY, message);

    SendHttpResponse(ctx, requestInfo, std::move(message), EAlertLevel::DANGER);
}

void TDiskRegistryActor::SendHttpResponse(
    const TActorContext& ctx,
    TRequestInfo& requestInfo,
    TString message,
    EAlertLevel alertLevel)
{
    TStringStream out;
    BuildTabletNotifyPageWithRedirect(out, message, TabletID(), alertLevel);

    SendHttpResponse(ctx, requestInfo, std::move(out.Str()));
}

void TDiskRegistryActor::SendHttpResponse(
    const TActorContext& ctx,
    TRequestInfo& requestInfo,
    TString message)
{
    NCloud::Reply(
        ctx,
        requestInfo,
        std::make_unique<NMon::TEvRemoteHttpInfoRes>(std::move(message)));
}

}   // namespace NCloud::NBlockStore::NStorage
