#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/storage/core/monitoring_utils.h>
#include <cloud/blockstore/libs/storage/disk_common/monitoring_utils.h>
#include <cloud/blockstore/libs/storage/disk_registry/model/user_notification.h>
#include <cloud/storage/core/libs/common/format.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/stream/str.h>
#include <util/string/join.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NMonitoringUtils;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

EDeviceStateFlags GetDeviceStateFlags(
    const TDiskRegistryState& state,
    const TString& deviceUUID)
{
    EDeviceStateFlags deviceStateFlags = EDeviceStateFlags::NONE;
    if (state.IsDirtyDevice(deviceUUID)) {
        deviceStateFlags |= EDeviceStateFlags::DIRTY;
    }
    if (state.IsSuspendedDevice(deviceUUID)) {
        deviceStateFlags |= EDeviceStateFlags::SUSPENDED;
    }
    return deviceStateFlags;
}

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
                value='Reallocate volume'
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

void BuildChangeDeviceStateButton(
    IOutputStream& out,
    ui64 tabletId,
    const TString& deviceUUID)
{
    out << Sprintf(
        R"(<form name="devtSateChange%s" method="post">
            <br>
            <label for="NewState">Change device state to:</label>
            <select name="NewState">
                <option value="%s">Online</option>
                <option value="%s">Warning</option>
            </select>
            <input type="submit" value="Change state">
            <input type='hidden' name='action' value='changeDeviceState'/>
            <input type='hidden' name='DeviceUUID' value='%s'/>
            <input type='hidden' name='TabletID' value='%lu'/>
            </form>)",
        deviceUUID.c_str(),
        EDeviceState_Name(NProto::DEVICE_STATE_ONLINE).c_str(),
        EDeviceState_Name(NProto::DEVICE_STATE_WARNING).c_str(),
        deviceUUID.c_str(),
        tabletId);
}

void BuildChangeAgentStateButton(
    IOutputStream& out,
    ui64 tabletId,
    const TString& agentId)
{
    out << Sprintf(
        R"(<form name="agentStateChange%s" method="post">
            <br>
            <label for="NewState">Change agent state to:</label>
            <select name="NewState">
                <option value="%s">Online</option>
                <option value="%s">Warning</option>
            </select>
            <input type="submit" value="Change state">
            <input type='hidden' name='action' value='changeAgentState'/>
            <input type='hidden' name='AgentID' value='%s'/>
            <input type='hidden' name='TabletID' value='%lu'/>
            </form>)",
        agentId.c_str(),
        EAgentState_Name(NProto::AGENT_STATE_ONLINE).c_str(),
        EAgentState_Name(NProto::AGENT_STATE_WARNING).c_str(),
        agentId.c_str(),
        tabletId);
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

void OnShowRecentPGCheckboxClickedActionsJS(IOutputStream& out)
{
    out << R"html(
        <script type='text/javascript'>
        function onShowRecentPGCheckboxClicked() {
            var checkBox = document.getElementById("showRecentPGCheckbox");

            document.getElementById("TotalPGTable").hidden = checkBox.checked;
            document.getElementById("RecentPGTable").hidden = !checkBox.checked;
        }
        </script>
    )html";
}

void DumpSize(IOutputStream& out, size_t size)
{
    if (size) {
        out << " <font color=gray>" << size << "</font>";
    }
}

template <typename C>
    requires requires(const C& c) { std::size(c); }
void DumpSize(IOutputStream& out, const C& c)
{
    DumpSize(out, std::size(c));
}

void DumpActionLink(
    IOutputStream& out,
    const ui64 tabletId,
    const TString& action,
    const TString& label,
    const ui32 cnt)
{
    out << "<h3><a href='?action=" << action << "&TabletID="
        << tabletId
        << "'>"
        << label;
    out << " <font color=gray>" << cnt << "</font>";
    out << "</a></h3>";
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

void DumpSquare(IOutputStream& out, const TStringBuf& color)
{
    const char* utfBlackSquare = "&#9632";
    const char* nonBreakingSpace = "&nbsp";
    out << "<font color="
        << color
        << ">"
        << utfBlackSquare
        << nonBreakingSpace
        << "</font>";
}

using DiskInfoArray = ::google::protobuf::RepeatedPtrField<NProto::TPlacementGroupConfig_TDiskInfo>;
auto GetSortedDisksView(
    const DiskInfoArray& disks,
    const THashSet<TString>& brokenDisks)
{
    using TDiskIterator = decltype(disks.begin());

    TVector<TDiskIterator> diskIndices(disks.size());
    std::iota(diskIndices.begin(), diskIndices.end(), disks.begin());

    auto sortByFailureThenByPartition =
        [&](const TDiskIterator& d1, const TDiskIterator& d2)
    {
        auto makeComparableTuple = [&brokenDisks](const TDiskIterator& d){
            return std::make_tuple(
                !brokenDisks.contains(d->GetDiskId()),
                d->GetPlacementPartitionIndex(),
                d->GetDiskId()
            );
        };
        return makeComparableTuple(d1) < makeComparableTuple(d2);
    };

    std::sort(diskIndices.begin(), diskIndices.end(), sortByFailureThenByPartition);
    return diskIndices;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

template <typename TDevices>
void TDiskRegistryActor::RenderDevicesWithDetails(
    IOutputStream& out,
    const TDevices& devices,
    const TString& title,
    const TVector<TAdditionalColumn>& additionalColumns) const
{
    HTML(out) {
        if (title) {
            TAG(TH3) { out << title; }
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
                    TABLEH() { out << "Physical offset"; }
                    TABLEH() { out << "Transport id"; }
                    TABLEH() { out << "Rdma endpoint"; }
                    TABLEH() { out << "DiskId"; }
                    TABLEH() { out << "Pool"; }
                    for (const auto& additionalColumn: additionalColumns) {
                        TABLEH() { additionalColumn.TitleInserter(out); }
                    }
                }
            }

            for (size_t index = 0; index < static_cast<size_t>(devices.size());
                 ++index)
            {
                const auto& device = devices[index];
                TABLER() {
                    TABLED() {
                        DumpDeviceLink(out, TabletID(), device.GetDeviceUUID());
                    }
                    TABLED() { out << device.GetDeviceName(); }
                    TABLED() { out << device.GetSerialNumber(); }
                    TABLED() {
                        DumpDeviceState(
                            out,
                            device.GetState(),
                            GetDeviceStateFlags(
                                *State,
                                device.GetDeviceUUID()));
                    }
                    TABLED() {
                        out << TInstant::MicroSeconds(device.GetStateTs());
                    }
                    TABLED() { out << device.GetStateMessage(); }
                    TABLED() { out << device.GetBlockSize(); }
                    TABLED() {
                        out << device.GetBlocksCount();
                        if (device.GetUnadjustedBlockCount()) {
                            out << " (" << device.GetUnadjustedBlockCount() << ")";
                        }
                    }
                    TABLED() {
                        const auto bytes =
                            device.GetBlockSize() * device.GetBlocksCount();
                        out << FormatByteSize(bytes);
                    }
                    TABLED() {
                        out << device.GetPhysicalOffset() << " ("
                            << FormatByteSize(device.GetPhysicalOffset())
                            << ")";
                    }
                    TABLED() { out << device.GetTransportId(); }
                    TABLED() {
                        const auto& e = device.GetRdmaEndpoint();
                        if (e.GetHost() || e.GetPort()) {
                            out << e.GetHost() << ":" << e.GetPort();
                        }
                    }
                    TABLED() {
                        if (auto id = State->FindDisk(device.GetDeviceUUID())) {
                            DumpDiskLink(out, TabletID(), id);
                        }
                    }
                    TABLED() { out << device.GetPoolName(); }
                    for (const auto& additionalColumn: additionalColumns) {
                        TABLED() { additionalColumn.DataInserter(index, out); }
                    }
                }
            }
        }
    }
}

void TDiskRegistryActor::HandleHttpInfo_RenderBrokenDeviceList(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    Y_UNUSED(params);

    TStringStream out;
    RenderBrokenDeviceListDetailed(out);
    SendHttpResponse(ctx, *requestInfo, std::move(out.Str()));
}

void TDiskRegistryActor::RenderBrokenDeviceList(IOutputStream& out) const
{
    auto brokenDevices = State->GetBrokenDevices();
    if (brokenDevices.empty()) {
        return;
    }

    DumpActionLink(
        out,
        TabletID(),
        "RenderBrokenDeviceList",
        "Broken devices",
        brokenDevices.size());
}

void TDiskRegistryActor::RenderBrokenDeviceListDetailed(
    IOutputStream& out) const
{
    auto brokenDevices = State->GetBrokenDevices();
    if (brokenDevices.empty()) {
        return;
    }

    RenderDevicesWithDetails(out, brokenDevices, "Broken Devices");
}

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
                const auto& e = device.GetRdmaEndpoint();
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

        DIV() {
            out << "State: ";
            DumpDeviceState(
                out,
                device.GetState(),
                GetDeviceStateFlags(*State, id));
        }
        DIV() {
            out << "State Timestamp: "
                << TInstant::MicroSeconds(device.GetStateTs());
        }
        DIV() { out << "State Message: " << device.GetStateMessage(); }

        if (Config->GetEnableToChangeStatesFromDiskRegistryMonpage()) {
            if (device.GetState() != NProto::EDeviceState::DEVICE_STATE_ERROR ||
                Config->GetEnableToChangeErrorStatesFromDiskRegistryMonpage())
            {
                DIV()
                {
                    BuildChangeDeviceStateButton(
                        out,
                        TabletID(),
                        device.GetDeviceUUID());
                }
            }
        }

        if (auto diskId = State->FindDisk(id)) {
            DIV() {
                out << "Disk: ";
                DumpDiskLink(out, TabletID(), diskId);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

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

        DIV() {
            out << "State: ";

            auto it = AgentRegInfo.find(agent->GetAgentId());
            const bool connected =
                it != AgentRegInfo.end() && it->second.Connected;
            DumpAgentState(out, agent->GetState(), connected);
        }
        DIV() {
            out << "State Timestamp: "
                << TInstant::MicroSeconds(agent->GetStateTs());
        }
        DIV() {
            if (Config->GetEnableToChangeStatesFromDiskRegistryMonpage()) {
                if (agent->GetState() !=
                    NProto::EAgentState::AGENT_STATE_UNAVAILABLE)
                {
                    BuildChangeAgentStateButton(
                        out,
                        TabletID(),
                        agent->GetAgentId());
                }
            }

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

        auto dcomp = [] (const auto& d) {
            return std::make_pair(d.GetDeviceName(), d.GetPhysicalOffset());
        };

        auto devices = agent->GetDevices();
        SortBy(devices, dcomp);

        RenderDevicesWithDetails(out, devices, {});

        if (agent->UnknownDevicesSize()) {
            auto unknownDevices = agent->GetUnknownDevices();
            SortBy(unknownDevices, dcomp);
            RenderDevicesWithDetails(
                out,
                unknownDevices,
                "Unknown devices");
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

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

        TDiskInfo masterDiskInfo;
        if (info.MasterDiskId) {
            State->GetDiskInfo(info.MasterDiskId, masterDiskInfo);
        }

        const bool replaceDeviceAllowed = Config->IsReplaceDeviceFeatureEnabled(
            info.CloudId,
            info.FolderId,
            info.MasterDiskId ? info.MasterDiskId : id);

        TAG(TH3) {
            out << "Disk " << id.Quote();
            if (info.MasterDiskId) {
                out << " (Replica of " << info.MasterDiskId << ")";
            }
            if (info.CheckpointId) {
                out << " (Shadow disk of " << info.SourceDiskId
                    << " for checkpoint " << info.CheckpointId.Quote() << ")";
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
        DIV() {
            out << "Volume kind: "
                << NProto::EStorageMediaKind_Name(info.MediaKind);
        }
        DIV() { out << "State: "; DumpDiskState(out, info.State); }
        DIV() { out << "State timestamp: " << info.StateTs; }
        if (info.MigrationStartTs) {
            DIV() {
                out << "Migration start timestamp: " << info.MigrationStartTs;
            }
        }

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

        DIV()
        {
            BuildVolumeReallocateButton(out, TabletID(), id);
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

                        auto it = AgentRegInfo.find(agent->GetAgentId());
                        const bool connected =
                            it != AgentRegInfo.end() && it->second.Connected;
                        DumpAgentState(out, agent->GetState(), connected);
                    }
                } else {
                    out << device.GetNodeId();
                }
            }
            TABLED() {
                EDeviceStateFlags flags =
                    GetDeviceStateFlags(*State, device.GetDeviceUUID());
                if (FindPtr(
                        info.MasterDiskId ? masterDiskInfo.DeviceReplacementIds
                                          : info.DeviceReplacementIds,
                        device.GetDeviceUUID()) != nullptr)
                {
                    flags |= EDeviceStateFlags::FRESH;
                }

                if (AnyOf(
                        info.LaggingDevices,
                        [&uuid = device.GetDeviceUUID()](
                            const TLaggingDevice& laggingDevice)
                        {
                            return uuid == laggingDevice.Device.GetDeviceUUID();
                        }))
                {
                    flags |= EDeviceStateFlags::LAGGING;
                }

                DumpDeviceState(out, device.GetState(), flags);
            }
            TABLED() {
                out << TInstant::MicroSeconds(device.GetStateTs());
            }
            if (isNonrepl) {
                TABLED() {
                    if (replaceDeviceAllowed) {
                        BuildDeviceReplaceButton(
                            out,
                            TabletID(),
                            id,
                            device.GetDeviceUUID());
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

                    for (const auto& [uuid, seqNo, _]: info.FinishedMigrations) {
                        TABLER() {
                            TABLED() { DumpDeviceLink(out, TabletID(), uuid); }
                            TABLED() { out << seqNo; }
                        }
                    }
                }
            }
        }

        GenerateVolumeActionsJS(out);

        TAG(TH3) {
            out << "History";
        }

        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Timestamp"; }
                    TABLEH() { out << "Message"; }
                }

                for (const auto& hi: info.History) {
                    TABLER() {
                        TABLED() {
                            out << TInstant::MicroSeconds(hi.GetTimestamp())
                                << " (" << hi.GetTimestamp() << ")";
                        }
                        TABLED() {
                            PRE() {
                                out << hi.GetMessage();
                            }
                        }
                    }
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

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
                }
            }

            for (const auto& id: ids) {
                TDiskInfo diskInfo;
                State->GetDiskInfo(id, diskInfo);

                TABLER() {
                    TABLED() { DumpDiskLink(out, TabletID(), id); }
                    TABLED() { DumpDiskState(out, diskInfo.State); }
                    TABLED() { out << diskInfo.StateTs; }
                }
            }
        }
    }
}

void TDiskRegistryActor::RenderMirroredDiskList(IOutputStream& out) const
{
    const auto ids = State->GetMirroredDiskIds();

    HTML(out) {
        TAG(TH3) { out << "MirroredDisks"; DumpSize(out, ids); }

        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Disk"; }
                    TABLEH() { out << "ReplicaCount"; }
                    TABLEH() { out << "CellStates"; }
                    TABLEH() { out << "DeviceStates"; }
                }
            }

            for (const auto& diskId: ids) {
                TDiskInfo diskInfo;
                State->GetDiskInfo(diskId, diskInfo);

                auto diskStat =
                    State->GetReplicaTable().CalculateDiskStats(diskId);

                TABLER() {
                    TABLED() { DumpDiskLink(out, TabletID(), diskId); }
                    TABLED() { out << diskInfo.Replicas.size() + 1; }
                    TABLED() {
                        for (ui32 i = 0; i < diskStat.CellsByState.size(); ++i)
                        {
                            if (i && !diskStat.CellsByState[i]) {
                                continue;
                            }

                            TStringBuf color =
                                i == 0 ? "green" :
                                i <= diskInfo.Replicas.size() ? "brown" :
                                "red";

                            if (i) {
                                out << " / ";
                            }

                            out << "<font color=" << color << ">";
                            if (i) {
                                out << "Minus " << i << ": ";
                            } else {
                                out << "Fine: ";
                            }
                            out << diskStat.CellsByState[i] << "</font>";
                        }
                    }
                    TABLED() {
                        out << "Ready: <font color=green>"
                            << diskStat.DeviceReadyCount << "</font>";
                        if (diskStat.DeviceReplacementCount) {
                            out << " / Fresh: <font color=blue>"
                                << diskStat.DeviceReplacementCount << "</font>";
                        }
                        if (diskStat.DeviceErrorCount) {
                            out << " / Error: <font color=red>"
                                << diskStat.DeviceErrorCount << "</font>";
                        }
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
            out << "Disks to reallocate";
            DumpSize(out, State->GetDisksToReallocate());
        }

        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Disk"; }
                    TABLEH() { out << "Notification scheduled"; }
                }
            }

            for (const auto& p: State->GetDisksToReallocate()) {
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

void TDiskRegistryActor::RenderUserNotifications(IOutputStream& out) const
{
    HTML(out) {
        const auto& notifications = State->GetUserNotifications();

        TAG(TH3) {
            out << "User notifications";
            DumpSize(out, notifications.Count);
        }

        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Disk"; }
                    TABLEH() { out << "Event"; }
                    // tablesorter's default parsers can't handle iso date+time
                    // 'sorter-usLongDate' works by luck,
                    // but looks more like a hack
                    TABLEH_CLASS("sorter-text") { out << "Timestamp"; }
                    TABLEH() { out << "SeqNo"; }
                    TABLEH() { out << "Notification scheduled"; }
                }
            }

            for (auto&& [id, data]: notifications.Storage) {
                for (const auto& notif: data.Notifications) {
                    TABLER() {
                        TABLED() { DumpDiskLink(out, TabletID(), id); }
                        TABLED() { out << GetEventName(notif); }
                        TABLED() { out
                            << TInstant::MicroSeconds(notif.GetTimestamp()); }
                        TABLED() { out << notif.GetSeqNo(); }

                        if (FindIfPtr(UserNotificationsBeingProcessed,
                            [&id = id, seqNo = notif.GetSeqNo()]
                            (const auto& n) {
                                return (n.GetSeqNo() == seqNo)
                                    && (GetEntityId(n) == id);
                            }))
                        {
                            TABLED() { out << UsersNotificationStartTs; }
                        } else {
                            TABLED() { out << "notification not started"; }
                        }
                    }
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleHttpInfo_RenderDisks(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    Y_UNUSED(params);

    TStringStream out;
    RenderDisksDetailed(out);
    SendHttpResponse(ctx, *requestInfo, std::move(out.Str()));
}

void TDiskRegistryActor::RenderDisks(IOutputStream& out) const
{
    DumpActionLink(
        out,
        TabletID(),
        "RenderDisks",
        "Disks",
        State->GetDiskCount());
}

void TDiskRegistryActor::RenderDisksDetailed(IOutputStream& out) const
{
    RenderDiskList(out);
    RenderMirroredDiskList(out);
    RenderMigrationList(out);
    RenderBrokenDiskList(out);
    RenderDisksToNotify(out);
    RenderUserNotifications(out);
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleHttpInfo_RenderRacks(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    Y_UNUSED(params);

    TStringStream out;
    RenderRacksDetailed(out);
    SendHttpResponse(ctx, *requestInfo, std::move(out.Str()));
}

void TDiskRegistryActor::RenderRacks(IOutputStream& out) const
{
    const ui32 rackCount = State->CalculateRackCount();
    DumpActionLink(out, TabletID(), "RenderRacks", "Racks", rackCount);
}

void TDiskRegistryActor::RenderRacksDetailed(IOutputStream& out) const
{
    for (const auto& poolName: State->GetPoolNames()) {
        RenderPoolRacks(out, poolName);
    }
}

void TDiskRegistryActor::RenderPoolRacks(
    IOutputStream& out,
    const TString& poolName) const
{
    static const char svg[] = "svg";
    static const char rect[] = "rect";
    static const char text[] = "text";

    auto racks = State->GatherRacksInfo(poolName);

    HTML(out) {
        TAG(TH3) {
            out << "Racks for pool " << (poolName ? poolName : "<default>");
            DumpSize(out, racks);
        }

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

        auto makeLegendBar = [&] (TString color) {
            TAG_ATTRS(
                    TTag<svg>,
                    {
                        {"width", "200"},
                        {"height", "30"}
                    })
            {
                makeRect(0, 0, 200, 30, color);
            }
        };

        TABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Meaning"; }
                    TABLEH() { out << "Color"; }
                }

                TABLER() {
                    TABLED() { out << "Free"; }
                    TABLED() { makeLegendBar(freeColor); }
                }

                TABLER() {
                    TABLED() { out << "Dirty"; }
                    TABLED() { makeLegendBar(dirtyColor); }
                }

                TABLER() {
                    TABLED() { out << "Occupied"; }
                    TABLED() { makeLegendBar(occupiedColor); }
                }

                TABLER() {
                    TABLED() { out << "Warning"; }
                    TABLED() { makeLegendBar(warningColor); }
                }

                TABLER() {
                    TABLED() { out << "Unavailable"; }
                    TABLED() { makeLegendBar(unavailableColor); }
                }

                TABLER() {
                    TABLED() { out << "Broken"; }
                    TABLED() { makeLegendBar(brokenColor); }
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleHttpInfo_RenderPlacementGroupList(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    Y_UNUSED(params);

    TStringStream out;
    RenderPlacementGroupListDetailed(out);
    SendHttpResponse(ctx, *requestInfo, std::move(out.Str()));
}

void TDiskRegistryActor::RenderPlacementGroupList(IOutputStream& out) const
{
    DumpActionLink(
        out,
        TabletID(),
        "RenderPlacementGroupList",
        "Placement groups",
        State->GetPlacementGroups().size());
}

void TDiskRegistryActor::RenderPlacementGroupListDetailed(
    IOutputStream& out) const
{
    HTML(out)
    {
        TAG(TH3)
        {
            out << "PlacementGroups";
            DumpSize(out, State->GetPlacementGroups());
        }

        OnShowRecentPGCheckboxClickedActionsJS(out);
        DIV()
        {
            TAG_ATTRS(
                TInput,
                {{"type", "checkbox"},
                 {"id", "showRecentPGCheckbox"},
                 {"onclick", "onShowRecentPGCheckboxClicked()"}})
            {}
            LABEL()
            {
                auto period = Config->GetPlacementGroupAlertPeriod();
                out << "Treat only recently broken partitions as broken"
                    << " (period=" << period.ToString() << ")";
            }
        }
        TAG_ATTRS(TDiv, {{"id", "TotalPGTable"}})
        {
            RenderPlacementGroupTable(out, false);
        }
        TAG_ATTRS(TDiv, {{"id", "RecentPGTable"}, {"hidden", "true"}})
        {
            RenderPlacementGroupTable(out, true);
        }
    }
}

void TDiskRegistryActor::RenderPlacementGroupTable(
    IOutputStream& out,
    bool showRecent) const
{
    auto brokenGroups = State->GatherBrokenGroupsInfo(
        Now(),
        Config->GetPlacementGroupAlertPeriod());

    HTML(out) {
        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "GroupId"; }
                    TABLEH() { out << "Strategy"; }
                    TABLEH() { out << "State"; }
                    TABLEH() { out << "Stats"; }
                    TABLEH() { out << "Disks"; }
                }
            }

            for (const auto& [groupId, groupInfo]: State->GetPlacementGroups()) {
                TABLER() {
                    const auto strategy = groupInfo.Config.GetPlacementStrategy();
                    const bool isPartitionGroup =
                        strategy == NProto::PLACEMENT_STRATEGY_PARTITION;

                    auto brokenGroupInfo = brokenGroups.FindPtr(groupId);
                    size_t brokenPartitionsCount = 0;
                    if (brokenGroupInfo) {
                        brokenPartitionsCount = showRecent
                            ? brokenGroupInfo->Recently.GetBrokenPartitionCount()
                            : brokenGroupInfo->Total.GetBrokenPartitionCount();
                    }

                    TABLED() {
                        const auto* color = brokenPartitionsCount == 0
                                ? "green"
                                : (brokenPartitionsCount == 1 ? "orange" : "red");
                        DumpSquare(out, color);
                        out << groupId;
                    }
                    TABLED() {
                        TStringBuf name = EPlacementStrategy_Name(strategy);
                        name.AfterPrefix("PLACEMENT_STRATEGY_", name);
                        out << name;
                    }
                    TABLED() {
                        size_t totalPartitionsCount = isPartitionGroup
                            ? groupInfo.Config.GetPlacementPartitionCount()
                            : groupInfo.Config.GetDisks().size();

                        out << Sprintf("%s: <font color=green>Fine: %zu</font>, ",
                            isPartitionGroup ? "Partitions" : "Disks",
                            totalPartitionsCount - brokenPartitionsCount);

                        if (brokenPartitionsCount > 0) {
                            out << Sprintf("<font color=%s>Broken: %zu</font>, ",
                            brokenPartitionsCount == 1 ? "orange" : "red",
                            brokenPartitionsCount);
                        }

                        out << Sprintf("Total: %zu<br>", totalPartitionsCount);
                    }
                    TABLED() {
                        if (groupInfo.Full) {
                            out << "<font color=red>GROUP IS FULL</font><br>";
                        }

                        out << "BiggestDisk: " << groupInfo.BiggestDiskId
                            << " ("
                            << FormatByteSize(groupInfo.BiggestDiskSize) << ")";
                    }
                    TABLED() {
                        TABLE_SORTABLE_CLASS("table table-bordered") {
                            TABLER() {
                                TABLED() { out << "ConfigVersion"; }
                                TABLED()
                                {
                                    out << groupInfo.Config.GetConfigVersion();
                                }
                            }
                            TABLER() {
                                TABLED() { out << "Disk count"; }
                                TABLED()
                                {
                                    out << groupInfo.Config.GetDisks().size();
                                }
                            }
                            TABLER() {
                                TABLED_ATTRS({{"colspan", "2"}}) {
                                    TABLE_SORTABLE_CLASS("table table-bordered") {
                                        TABLEHEAD() {
                                            TABLER() {
                                                TABLEH() { out << "DiskId"; }
                                                TABLEH() { out << "Racks"; }
                                                if (isPartitionGroup) {
                                                    TABLEH()
                                                    {
                                                        out << "Partition";
                                                    }
                                                }
                                            }
                                        }

                                        const auto brokenDisks = brokenGroupInfo
                                            ? brokenGroupInfo->Total.GetBrokenDisks()
                                            : THashSet<TString>();

                                        const auto sortedDisks =
                                            GetSortedDisksView(
                                                groupInfo.Config.GetDisks(),
                                                brokenDisks);

                                        for (const auto& d: sortedDisks) {
                                            TABLER() {
                                                TABLED() {
                                                    const bool isBroken =
                                                        brokenDisks.contains(
                                                            d->GetDiskId());
                                                    DumpSquare(
                                                        out,
                                                        isBroken ? "red"
                                                                 : "green");
                                                    DumpDiskLink(
                                                        out,
                                                        TabletID(),
                                                        d->GetDiskId());
                                                }
                                                TABLED() {
                                                    out << JoinSeq(
                                                        ", ",
                                                        d->GetDeviceRacks());
                                                }
                                                if (isPartitionGroup) {
                                                    TABLED() {
                                                        out << d->GetPlacementPartitionIndex();
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

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleHttpInfo_RenderAgentList(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    Y_UNUSED(params);

    TStringStream out;
    RenderAgentListDetailed(ctx.Now(), out);
    SendHttpResponse(ctx, *requestInfo, std::move(out.Str()));
}

void TDiskRegistryActor::RenderAgentList(IOutputStream& out) const
{
    DumpActionLink(
        out,
        TabletID(),
        "RenderAgentList",
        "Agents",
        State->GetAgents().size());
}

void TDiskRegistryActor::RenderAgentListDetailed(
    TInstant now,
    IOutputStream& out) const
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
                    TABLEH() { out << "Device states"; }
                }

                for (const auto& config: State->GetAgents()) {
                    ui32 onlineDevs = 0;
                    ui32 warningDevs = 0;
                    ui32 errorDevs = 0;
                    for (const auto& device: config.GetDevices()) {
                        switch (device.GetState()) {
                            case NProto::DEVICE_STATE_ONLINE: {
                                ++onlineDevs;
                                break;
                            }
                            case NProto::DEVICE_STATE_WARNING: {
                                ++warningDevs;
                                break;
                            }
                            case NProto::DEVICE_STATE_ERROR: {
                                ++errorDevs;
                                break;
                            }
                            default: {}
                        }
                    }

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
                            auto it = AgentRegInfo.find(config.GetAgentId());
                            const bool connected = it != AgentRegInfo.end()
                                && it->second.Connected;
                            DumpAgentState(out, config.GetState(), connected);
                        }
                        TABLED() {
                            out << (config.GetDedicatedDiskAgent()
                                    ? "true" : "false");
                        }
                        TABLED() {
                            out << TInstant::MicroSeconds(config.GetStateTs());
                        }
                        TABLED() { out << config.GetStateMessage(); }

                        TABLED() {
                            DumpDeviceState(
                                out,
                                NProto::DEVICE_STATE_ONLINE,
                                EDeviceStateFlags::NONE,
                                TStringBuilder() << " " << onlineDevs);
                            if (warningDevs) {
                                out << " / ";
                                DumpDeviceState(
                                    out,
                                    NProto::DEVICE_STATE_WARNING,
                                    EDeviceStateFlags::NONE,
                                    TStringBuilder() << " " << warningDevs);
                            }
                            if (errorDevs) {
                                out << " / ";
                                DumpDeviceState(
                                    out,
                                    NProto::DEVICE_STATE_ERROR,
                                    EDeviceStateFlags::NONE,
                                    TStringBuilder() << " " << errorDevs);
                            }
                        }
                    }
                }
            }
        }

        TAG(TH3) { out << "AgentList Parameters"; }

        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Agent"; }
                    TABLEH() { out << "RejectAgentTimeout (" << now << ")"; }
                }

                TABLER() {
                    TABLED() {
                        out << "default";
                    }
                    TABLED() {
                        out << State->GetRejectAgentTimeout(now, "");
                    }
                }
                for (const auto& agentId: State->GetAgentIdsWithOverriddenListParams()) {
                    TABLER() {
                        TABLED() {
                            out << agentId << " (overridden)";
                        }
                        TABLED() {
                            out << State->GetRejectAgentTimeout(now, agentId);
                        }
                    }
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleHttpInfo_RenderConfig(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    Y_UNUSED(params);

    TStringStream out;
    RenderConfigDetailed(out);
    SendHttpResponse(ctx, *requestInfo, std::move(out.Str()));
}

void TDiskRegistryActor::RenderConfig(IOutputStream& out) const
{
    DumpActionLink(
        out,
        TabletID(),
        "RenderConfig",
        "Config",
        State->GetConfig().KnownAgentsSize());
}

void TDiskRegistryActor::RenderConfigDetailed(IOutputStream& out) const
{
    HTML(out) {
        TAG(TH3) { out << "Config"; }

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

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleHttpInfo_RenderDirtyDeviceList(
    const NActors::TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    Y_UNUSED(params);

    TStringStream out;
    RenderDirtyDeviceListDetailed(out);
    SendHttpResponse(ctx, *requestInfo, std::move(out.Str()));
}

void TDiskRegistryActor::RenderDirtyDeviceList(IOutputStream& out) const
{
    auto dirtyDevices = State->GetDirtyDevices();
    if (dirtyDevices.empty()) {
        return;
    }

    DumpActionLink(
        out,
        TabletID(),
        "RenderDirtyDeviceList",
        "Dirty devices",
        dirtyDevices.size());
}

void TDiskRegistryActor::RenderDirtyDeviceListDetailed(IOutputStream& out) const
{
    auto dirtyDevices = State->GetDirtyDevices();
    if (dirtyDevices.empty()) {
        return;
    }

    TVector<TAdditionalColumn> additionalColumns;
    additionalColumns.push_back(TAdditionalColumn{
        .TitleInserter = [](IOutputStream& out)
        { out << "Automatically replaced"; },
        .DataInserter =
            [&dirtyDevices, this](size_t index, IOutputStream& out)
        {
            if (dirtyDevices.size() <= index) {
                Y_DEBUG_ABORT_UNLESS(false);
                out << "null";
                return;
            }
            if (State->IsAutomaticallyReplaced(
                    dirtyDevices[index].GetDeviceUUID())) {
                out << "Yes";
            } else {
                out << "No";
            }
        }});

    RenderDevicesWithDetails(
        out,
        dirtyDevices,
        "Dirty Devices",
        additionalColumns);
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleHttpInfo_RenderSuspendedDeviceList(
    const NActors::TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    Y_UNUSED(params);

    TStringStream out;
    RenderSuspendedDeviceListDetailed(out);
    SendHttpResponse(ctx, *requestInfo, std::move(out.Str()));
}

void TDiskRegistryActor::RenderSuspendedDeviceList(IOutputStream& out) const
{
    const auto suspendedDevices = State->GetSuspendedDevices();
    if (suspendedDevices.empty()) {
        return;
    }

    DumpActionLink(
        out,
        TabletID(),
        "RenderSuspendedDeviceList",
        "Suspended devices",
        suspendedDevices.size());
}

void TDiskRegistryActor::RenderSuspendedDeviceListDetailed(
    IOutputStream& out) const
{
    const auto suspendedDevices = State->GetSuspendedDevices();
    if (suspendedDevices.empty()) {
        return;
    }

    TVector<NProto::TDeviceConfig> suspendedDeviceConfigs;
    suspendedDeviceConfigs.reserve(suspendedDevices.size());
    for (const auto& suspendedDevice: suspendedDevices) {
        suspendedDeviceConfigs.push_back(
            State->GetDevice(suspendedDevice.GetId()));
    }

    TVector<TAdditionalColumn> additionalColumns;
    additionalColumns.push_back(TAdditionalColumn{
        .TitleInserter = [](IOutputStream& out)
        { out << "Resume after erase"; },
        .DataInserter =
            [&suspendedDevices](size_t index, IOutputStream& out)
        {
            if (suspendedDevices.size() <= index) {
                Y_DEBUG_ABORT_UNLESS(false);
                out << "null";
                return;
            }
            if (suspendedDevices[index].GetResumeAfterErase()) {
                out << "Yes";
            } else {
                out << "No";
            }
        }});

    RenderDevicesWithDetails(
        out,
        suspendedDeviceConfigs,
        "Suspended Devices",
        additionalColumns);
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::RenderAutomaticallyReplacedDeviceList(
    IOutputStream& out) const
{
    const auto& deviceInfos = State->GetAutomaticallyReplacedDevices();
    if (deviceInfos.empty()) {
        return;
    }

    HTML(out) {
        TAG(TH3) { out << "Automatically replaced devices"; }

        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "DeviceId"; }
                    TABLEH() { out << "ReplacementTs"; }
                    TABLEH() { out << "Time to erase"; }
                }
                TDuration freezeDuration =
                    Config->GetAutomaticallyReplacedDevicesFreezePeriod();
                for (const auto& deviceInfo: deviceInfos) {
                    TABLER() {
                        TABLED() {
                            DumpDeviceLink(out, TabletID(), deviceInfo.DeviceId);
                        }
                        TABLED() {
                            out << deviceInfo.ReplacementTs;
                        }
                        TABLED() {
                            if (freezeDuration) {
                                auto timeToClean = (deviceInfo.ReplacementTs +
                                                    freezeDuration) -
                                                   TInstant::Now();
                                out << timeToClean;
                            } else {
                                out << "+inf. (AutomaticallyReplacedDevicesFreezePeriod not set)";
                            }
                        }
                    }
                }
            }
        }
    }
}

void TDiskRegistryActor::RenderHtmlInfo(IOutputStream& out) const
{
    using namespace NMonitoringUtils;

    DumpDefaultHeader(out, *Info(), SelfId().NodeId(), *DiagnosticsConfig);

    HTML(out) {
        if (State) {
            RenderState(out);

            RenderDisks(out);

            RenderPlacementGroupList(out);

            RenderRacks(out);

            RenderAgentList(out);

            RenderConfig(out);

            RenderDirtyDeviceList(out);

            RenderBrokenDeviceList(out);

            RenderSuspendedDeviceList(out);

            RenderAutomaticallyReplacedDeviceList(out);
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

    static const THttpHandlers postActions{{
        {"volumeRealloc", &TDiskRegistryActor::HandleHttpInfo_VolumeRealloc},
        {"replaceDevice", &TDiskRegistryActor::HandleHttpInfo_ReplaceDevice},
        {"changeDeviceState",
         &TDiskRegistryActor::HandleHttpInfo_ChangeDeviseState},
        {"changeAgentState",
         &TDiskRegistryActor::HandleHttpInfo_ChangeAgentState},
    }};

    static const THttpHandlers getActions {{
        {"dev",    &TDiskRegistryActor::HandleHttpInfo_RenderDeviceHtmlInfo  },
        {"agent",  &TDiskRegistryActor::HandleHttpInfo_RenderAgentHtmlInfo   },
        {"disk",   &TDiskRegistryActor::HandleHttpInfo_RenderDiskHtmlInfo    },

        {"RenderDisks", &TDiskRegistryActor::HandleHttpInfo_RenderDisks},
        {"RenderBrokenDeviceList",
         &TDiskRegistryActor::HandleHttpInfo_RenderBrokenDeviceList},
        {"RenderPlacementGroupList",
         &TDiskRegistryActor::HandleHttpInfo_RenderPlacementGroupList},
        {"RenderRacks", &TDiskRegistryActor::HandleHttpInfo_RenderRacks},
        {"RenderAgentList",
         &TDiskRegistryActor::HandleHttpInfo_RenderAgentList},
        {"RenderConfig", &TDiskRegistryActor::HandleHttpInfo_RenderConfig},
        {"RenderDirtyDeviceList",
         &TDiskRegistryActor::HandleHttpInfo_RenderDirtyDeviceList},
        {"RenderSuspendedDeviceList",
         &TDiskRegistryActor::HandleHttpInfo_RenderSuspendedDeviceList},
    }};

    auto* msg = ev->Get();

    LOG_DEBUG(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] HTTP request: %s",
        TabletID(),
        msg->Query.Quote().data());

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        MakeIntrusive<TCallContext>());

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
    RenderHtmlInfo(out);

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
