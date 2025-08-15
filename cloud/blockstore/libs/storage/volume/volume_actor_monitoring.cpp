#include "volume_actor.h"

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/config.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/media.h>
#include <cloud/storage/core/libs/throttling/tablet_throttler.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/stream/str.h>
#include <util/string/join.h>

#include <google/protobuf/text_format.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NMonitoringUtils;

namespace {

////////////////////////////////////////////////////////////////////////////////

IOutputStream& operator <<(
    IOutputStream& out,
    ECheckpointData checkpointData)
{
    switch (checkpointData) {
        case ECheckpointData::DataPresent:
            return out << "<font color=green>data present</font>";
        case ECheckpointData::DataDeleted:
            return out << "<font color=red>data deleted</font>";
        default:
            return out
                << "(Unknown value "
                << static_cast<int>(checkpointData)
                << ")";
    }
}

IOutputStream& operator<<(IOutputStream& out, EShadowDiskState state)
{
    switch (state) {
        case EShadowDiskState::None:
        case EShadowDiskState::New:
        case EShadowDiskState::Preparing:
            out << ToString(state);
            break;
        case EShadowDiskState::Ready:
            out << "<font color=green>" << ToString(state) << "</font>";
            break;
        case EShadowDiskState::Error:
            out << "<font color=red>" << ToString(state) << "</font>";
            break;
    }
    return out;
}

IOutputStream& operator <<(
    IOutputStream& out,
    NProto::EVolumeIOMode ioMode)
{
    switch (ioMode) {
        case NProto::VOLUME_IO_OK:
            return out << "<font color=green>ok</font>";
        case NProto::VOLUME_IO_ERROR_READ_ONLY:
            return out << "<font color=red>read only</font>";
        default:
            return out
                << "(Unknown EVolumeIOMode value "
                << static_cast<int>(ioMode)
                << ")";
    }
}

IOutputStream& operator <<(
    IOutputStream& out,
    NProto::EVolumeAccessMode accessMode)
{
    switch (accessMode) {
        case NProto::VOLUME_ACCESS_READ_WRITE:
            return out << "Read/Write";
        case NProto::VOLUME_ACCESS_READ_ONLY:
            return out << "Read only";
        case NProto::VOLUME_ACCESS_REPAIR:
            return out << "Repair";
        case NProto::VOLUME_ACCESS_USER_READ_ONLY:
            return out << "User read only";
        default:
            Y_DEBUG_ABORT_UNLESS(false, "Unknown EVolumeAccessMode: %d", accessMode);
            return out << "Undefined";
    }
}

IOutputStream& operator <<(
    IOutputStream& out,
    const NKikimrBlockStore::TEncryptionDesc& desc)
{
    const auto encryptionMode =
        static_cast<NProto::EEncryptionMode>(desc.GetMode());

    HTML(out) {
        DIV() { out << NProto::EEncryptionMode_Name(encryptionMode); }

        switch (encryptionMode) {
            case NProto::ENCRYPTION_AES_XTS: {
                DIV() {
                    const auto& keyHash = desc.GetKeyHash();
                    if (keyHash.empty()) {
                        out << "Binding to the encryption key has not yet "
                               "occurred.";
                    } else {
                        out << "Encryption key hash: " << keyHash;
                    }
                }
                break;
            }

            case NProto::ENCRYPTION_AT_REST: {
                const auto& key = desc.GetEncryptedDataKey();
                DIV() { out << "Kek Id: " << key.GetKekId().Quote(); }
                DIV() {
                    out << "Encrypted DEK has " << key.GetCiphertext().size()
                        << " bytes length";
                }
                break;
            }
            default:
                break;
        }
    }

    return out;
}

////////////////////////////////////////////////////////////////////////////////

void OutputProgress(
    ui64 blocks,
    ui64 totalBlocks,
    ui32 blockSize,
    std::optional<ui64> blocksToProcess,
    IOutputStream& out)
{
    TString processedSize = "?";
    TString sizeToProcess = "?";
    TString totalSize = FormatByteSize(totalBlocks * blockSize);
    TString readyPercent = "?";
    if (blocksToProcess) {
        processedSize =
            FormatByteSize((totalBlocks - *blocksToProcess) * blockSize);
        sizeToProcess = FormatByteSize(*blocksToProcess * blockSize);

        // Calculate the percentage with an accuracy of up to tenths of a
        // percent.
        if (totalBlocks) {
            readyPercent =
                (TStringBuilder()
                 << (1000 * (totalBlocks - *blocksToProcess) / totalBlocks) * 0.1);
        }
    }

    out << blocks << " (<font color=green>" << processedSize
        << "</font> + <font color=red>" << sizeToProcess
        << "</font> = " << totalSize << ", " << readyPercent << "%)";
}


void RenderCheckpointRequest(
    IOutputStream& out,
    const TCheckpointRequest& request)
{
    HTML(out)
    {
        if (request.ShadowDiskId) {
            DIV()
            {
                out << "Shadow disk: " << request.ShadowDiskId.Quote();
            }
        }

        DIV()
        {
            out << "State: " << request.ShadowDiskState;
            if (request.ShadowDiskState == EShadowDiskState::Preparing) {
                out << ", processed blocks: "
                    << request.ShadowDiskProcessedBlockCount;
            }
        }

        if (request.CheckpointError) {
            DIV()
            {
                out << "Error: " << request.CheckpointError;
            }
        }
    }
}

void RenderCheckpointInfo(
    ui64 blocksCount,
    ui32 blockSize,
    const TActiveCheckpointInfo& checkpointInfo,
    IOutputStream& out)
{
    HTML(out)
    {
        DIV()
        {
            switch (checkpointInfo.Type) {
                case ECheckpointType::Light: {
                    out << "no data (light)";
                } break;
                case ECheckpointType::Normal: {
                    out << "normal (" << checkpointInfo.Data << ")";
                } break;
            }
        }
        if (checkpointInfo.IsShadowDiskBased()) {
            DIV()
            {
                out << " Shadow disk: " << checkpointInfo.ShadowDiskId.Quote();
            }
            DIV()
            {
                out << " State: " << checkpointInfo.ShadowDiskState;
            }
            if (checkpointInfo.ShadowDiskState == EShadowDiskState::Preparing) {
                DIV()
                {
                    out << "Block: ";
                    OutputProgress(
                        checkpointInfo.ProcessedBlockCount,
                        blocksCount,
                        blockSize,
                        blocksCount - checkpointInfo.ProcessedBlockCount,
                        out);
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void RenderFollowers(IOutputStream& out, const TFollowerDisks& followers)
{
    HTML (out) {
        TAG(TH3) {
            out << "Followers:";
        }
        DIV_CLASS ("row") {
            TABLE_SORTABLE_CLASS ("table table-bordered") {
                TABLEHEAD () {
                    TABLER () {
                        TABLEH () {
                            out << "Time";
                        }
                        TABLEH () {
                            out << "UUID";
                        }
                        TABLEH () {
                            out << "Follower";
                        }
                        TABLEH () {
                            out << "State";
                        }
                        TABLEH () {
                            out << "Migrated blocks";
                        }
                    }
                }
                for (const auto& follower: followers) {
                    TABLER () {
                        TABLED () {
                            out << follower.CreatedAt;
                        }
                        TABLED () {
                            out << follower.Link.LinkUUID;
                        }
                        TABLED () {
                            out << follower.Link.FollowerDiskIdForPrint();
                            if (follower.MediaKind) {
                                out << "<br>(";
                                out << NProto::EStorageMediaKind_Name(
                                    follower.MediaKind);
                                out << ")";
                            }
                        }
                        TABLED () {
                            out << ToString(follower.State) << " "
                                << follower.ErrorMessage;
                        }
                        TABLED () {
                            if (follower.MigratedBytes) {
                                out << FormatByteSize(*follower.MigratedBytes);
                            }
                        }
                    }
                }
            }
        }
    }
}

void RenderLeaders(IOutputStream& out, const TLeaderDisks& leaders)
{
    HTML (out) {
        TAG(TH3) {
            out << "Leaders:";
        }

        DIV_CLASS ("row") {
            TABLE_SORTABLE_CLASS ("table table-bordered") {
                TABLEHEAD () {
                    TABLER () {
                        TABLEH () {
                            out << "Time";
                        }
                        TABLEH () {
                            out << "UUID";
                        }
                        TABLEH () {
                            out << "Leader";
                        }
                        TABLEH () {
                            out << "State";
                        }
                    }
                }
                for (const auto& leader: leaders) {
                    TABLER () {
                        TABLED () {
                            out << leader.CreatedAt;
                        }
                        TABLED () {
                            out << leader.Link.LinkUUID;
                        }
                        TABLED () {
                            out << leader.Link.LeaderDiskIdForPrint();
                        }
                        TABLED () {
                            out << ToString(leader.State) << " "
                                << leader.ErrorMessage;
                        }
                    }
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void BuildVolumeRemoveClientButton(
    IOutputStream& out,
    ui64 id,
    const TString& diskId,
    const TString& clientId,
    const ui64 tabletId)
{
    out << "<form method='POST' name='RemoveClient_" << clientId << "'>"
        << "<input type='hidden' name='TabletID' value='" << tabletId << "'/>"
        << "<input type='hidden' name='ClientId' value='" << clientId << "'/>"
        << "<input type='hidden' name='Volume' value='" << diskId << "'/>"
        << "<input type='hidden' name='action' value='removeclient'/>"
        << "<input class='btn btn-primary' type='button' value='Remove' "
        << "data-toggle='modal' data-target='#volume-remove-client-"
        << id << "'/>"
        << "</form>" << Endl;

    BuildConfirmActionDialog(
        out,
        TStringBuilder() << "volume-remove-client-" << id,
        "Remove client",
        "Are you sure you want to remove client from volume?",
        TStringBuilder() << "volumeRemoveClient(\"" << clientId << "\");");
}

void BuildVolumeResetMountSeqNumberButton(
    IOutputStream& out,
    const TString& clientId,
    const ui64 tabletId)
{
    out << "<form method='POST' name='ResetMountSeqNumber_" << clientId << "'>"
        << "<input type='hidden' name='TabletID' value='" << tabletId << "'/>"
        << "<input type='hidden' name='ClientId' value='" << clientId << "'/>"
        << "<input type='hidden' name='action' value='resetmountseqnumber'/>"
        << "<input class='btn btn-primary' type='button' value='Reset' "
        << "data-toggle='modal' data-target='#volume-reset-seqnumber-"
        << clientId << "'/>"
        << "</form>" << Endl;

    BuildConfirmActionDialog(
        out,
        TStringBuilder() << "volume-reset-seqnumber-" << clientId,
        "Reset Mount Seq Number",
        "Are you sure you want to volume mount seq number?",
        TStringBuilder() << "resetMountSeqNumber(\"" << clientId << "\");");
}

void BuildHistoryNextButton(
    IOutputStream& out,
    THistoryLogKey key,
    ui64 tabletId)
{
    if (key.SeqNo) {
        --key.SeqNo;
    } else {
        key.Timestamp -= TDuration::MicroSeconds(1);
        key.SeqNo = Max<ui64>();
    }
    out << "<form method='GET' name='NextHistoryPage' style='display:inline-block'>"
        << "<input type='hidden' name='timestamp' value='" << key.Timestamp.MicroSeconds() << "'/>"
        << "<input type='hidden' name='seqno' value='" << key.SeqNo << "'/>"
        << "<input type='hidden' name='next' value='true'/>"
        << "<input type='hidden' name='TabletID' value='" << tabletId << "'/>"
        << "<input class='btn btn-primary display:inline-block' type='submit' value='Next>>'/>"
        << "</form>" << Endl;
}

void BuildStartPartitionsButton(
    IOutputStream& out,
    const TString& diskId,
    const ui64 tabletId)
{
    out << "<form method='POST' name='StartPartitions'>"
        << "<input type='hidden' name='TabletID' value='" << tabletId << "'/>"
        << "<input type='hidden' name='Volume' value='" << diskId << "'/>"
        << "<input type='hidden' name='action' value='startpartitions'/>"
        << "<input class='btn btn-primary' type='button' value='Start Partitions' "
        << "data-toggle='modal' data-target='#start-partitions'/>"
        << "</form>" << Endl;

    BuildConfirmActionDialog(
        out,
        "start-partitions",
        "Start partitions",
        "Are you sure you want to start volume partitions?",
        TStringBuilder() << "startPartitions();");
}

void BuildChangeThrottlingPolicyButton(
    IOutputStream& out,
    const TString& diskId,
    const NProto::TVolumePerformanceProfile& pp,
    const ui64 tabletId)
{
    out << "<form method='POST' name='ChangeThrottlingPolicy'>"
        << "<input type='hidden' name='TabletID' value='" << tabletId << "'/>"
        << "<input type='hidden' name='Volume' value='" << diskId << "'/>"
        << "<input type='hidden' name='action' value='changethrottlingpolicy'/>"
        << "<label style='font-weight: normal'> Max read Iops "
        << "<input type='text' name='MaxReadIops' value='"
        << pp.GetMaxReadIops() << "' style='font-weight: normal'/> </label>"
        << "<label style='font-weight: normal'> Max write Iops "
        << "<input type='text' name='MaxWriteIops' value='"
        << pp.GetMaxWriteIops() << "' style='font-weight: normal'/> </label>"
        << "<label style='font-weight: normal'> Max read bandwidth "
        << "<input type='text' name='MaxReadBandwidth' value='"
        << pp.GetMaxReadBandwidth()
        << "' style='font-weight: normal'/> </label>"
        << "<label style='font-weight: normal'> Max write bandwith "
        << "<input type='text' name='MaxWriteBandwidth' value='"
        << pp.GetMaxWriteBandwidth()
        << "' style='font-weight: normal'/> </label>"
        << "<br> <input class='btn btn-primary' type='button'"
        << " value='Change Throttling Policy' "
        << "data-toggle='modal' data-target='#change-throttling-policy'/>"
        << "</form>" << Endl;

    BuildConfirmActionDialog(
        out,
        "change-throttling-policy",
        "Change throttling policy",
        "Are you sure you want to change throttling policy?",
        TStringBuilder() << "changeThrottlingPolicy();");
}

////////////////////////////////////////////////////////////////////////////////

void OutputClientInfo(
    IOutputStream& out,
    const TString& clientId,
    const TString& diskId,
    ui64 clientNo,
    ui64 tabletId,
    NProto::EVolumeMountMode mountMode,
    NProto::EVolumeAccessMode accessMode,
    ui64 disconnectTimestamp,
    ui64 lastActivityTimestamp,
    bool isStale,
    TVolumeClientState::TPipes pipes)
{
    HTML(out) {
        TABLER() {
            TABLED() { out << clientId; }
            TABLED() {
                out << accessMode;
            }
            TABLED() {
                if (mountMode == NProto::VOLUME_MOUNT_LOCAL) {
                    out << "Local";
                } else {
                    out << "Remote";
                }
            }
            TABLED() {
                auto time = disconnectTimestamp;
                if (time) {
                    out << TInstant::MicroSeconds(time).ToStringLocalUpToSeconds();
                }
            }
            TABLED() {
                auto time = lastActivityTimestamp;
                if (time) {
                    out << TInstant::MicroSeconds(time).ToStringLocalUpToSeconds();
                }
            }
            TABLED() {
                if (isStale) {
                    out << "Stale";
                } else {
                    out << "Actual";
                }
            }
            TABLED() {
                UL() {
                    for (const auto& p: pipes) {
                        if (p.second.State != TVolumeClientState::EPipeState::DEACTIVATED) {
                            if (p.second.State == TVolumeClientState::EPipeState::WAIT_START) {
                                LI() {
                                    out << p.first << "[Wait]";
                                }
                            } else if (p.second.State == TVolumeClientState::EPipeState::ACTIVE) {
                                LI() {
                                    out << p.first << "[Active]";
                                }
                            }
                        }
                    }
                }
            }
            TABLED() {
                BuildVolumeRemoveClientButton(
                    out,
                    clientNo,
                    diskId,
                    clientId,
                    tabletId);
            }
        }
    }
}

void RenderTextWithTooltip(
    IOutputStream& out,
    const TString& text,
    const TString& tooltip)
{
    HTML (out) {
        DIV_CLASS ("tooltip-latency") {
            out << text;
            if (tooltip) {
                SPAN_CLASS ("tooltiptext-latency") {
                    out << tooltip;
                }
            }
        }
    }
}

void RenderLatencyTable(IOutputStream& out, const TString& parentId)
{
    HTML (out) {
        TABLE_CLASS ("table-latency") {
            TABLEHEAD () {
                TABLER () {
                    TABLEH () {
                        out << "Time";
                    }
                    TABLEH () {
                        out << "Ok";
                    }
                    TABLEH () {
                        out << "Fail";
                    }
                    TABLEH () {
                        out << "Inflight";
                    }
                }
            }

            for (const auto& [key, descr, tooltip]:
                 TRequestsTimeTracker::GetTimeBuckets())
            {
                TABLER () {
                    TABLED () {
                        RenderTextWithTooltip(out, descr, tooltip);
                    }
                    TABLED_ATTRS ({{"id", parentId + "_ok_" + key}}) {
                    }
                    TABLED_ATTRS ({{"id", parentId + "_fail_" + key}}) {
                    }
                    TABLED_ATTRS ({{"id", parentId + "_inflight_" + key}}) {
                    }
                }
            }
        }
    }
}

void RenderPercentilesTable(IOutputStream& out, const TString& parentId)
{
    HTML (out) {
        TABLE_CLASS ("table-latency") {
            TABLEHEAD () {
                TABLER () {
                    TABLEH () {
                        RenderTextWithTooltip(out, "Perc", "Percentile");
                    }
                    TABLEH () {
                        RenderTextWithTooltip(out, "R", "Read");
                    }
                    TABLEH () {
                        RenderTextWithTooltip(out, "W", "Write");
                    }
                    TABLEH () {
                        RenderTextWithTooltip(out, "Z", "Zero");
                    }
                    TABLEH () {
                        RenderTextWithTooltip(out, "D", "Describe");
                    }
                }
            }

            for (const auto& [key, descr, tooltip]:
                 TRequestsTimeTracker::GetPercentileBuckets())
            {
                TABLER () {
                    TABLED () {
                        RenderTextWithTooltip(out, descr, tooltip);
                    }
                    TABLED_ATTRS ({{"id", "R_" + parentId + "_ok_" + key}}) {
                    }
                    TABLED_ATTRS ({{"id", "W_" + parentId + "_ok_" + key}}) {
                    }
                    TABLED_ATTRS ({{"id", "Z_" + parentId + "_ok_" + key}}) {
                    }
                    TABLED_ATTRS ({{"id", "D_" + parentId + "_ok_" + key}}) {
                    }
                }
            }
        }
    }
}

void RenderSizeTable(IOutputStream& out, ui32 blockSize)
{
    HTML (out) {
        TABLE_CLASS ("table table-bordered") {
            TABLEHEAD () {
                TABLER () {
                    TABLEH () {
                        out << "Size";
                    }
                    TABLEH () {
                        out << "Read";
                    }
                    TABLEH () {
                        out << "Write";
                    }
                    TABLEH () {
                        out << "Zero";
                    }
                    TABLEH () {
                        out << "Describe";
                    }
                }
            }
            for (const auto& [key, descr, tooltip]:
                 TRequestsTimeTracker::GetSizeBuckets(blockSize))
            {
                TABLER () {
                    TABLED () {
                        TAG (TH4) {
                            out << "Size: " << descr;
                        }
                        RenderPercentilesTable(out, key);
                    }
                    TABLED () {
                        RenderLatencyTable(out, "R_" + key);
                    }
                    TABLED_ATTRS () {
                        RenderLatencyTable(out, "W_" + key);
                    }
                    TABLED_ATTRS () {
                        RenderLatencyTable(out, "Z_" + key);
                    }
                    TABLED_ATTRS () {
                        RenderLatencyTable(out, "D_" + key);
                    }
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

bool IsSetDefaultThrottlingPolicy(const TVolumeState* state)
{
    const auto& defaultConfig = state->GetConfig().GetPerformanceProfile();
    const auto& volumeThrottlingPolicyConfig =
        state->GetThrottlingPolicy().GetConfig();

    return defaultConfig.GetMaxReadIops() ==
               volumeThrottlingPolicyConfig.GetMaxReadIops() &&
           defaultConfig.GetMaxWriteIops() ==
               volumeThrottlingPolicyConfig.GetMaxWriteIops() &&
           defaultConfig.GetMaxReadBandwidth() ==
               volumeThrottlingPolicyConfig.GetMaxReadBandwidth() &&
           defaultConfig.GetMaxWriteBandwidth() ==
               volumeThrottlingPolicyConfig.GetMaxWriteBandwidth();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool TVolumeActor::CanChangeThrottlingPolicy() const
{
    const auto& volumeConfig = GetNewestConfig();
    return Config->IsChangeThrottlingPolicyFeatureEnabled(
        volumeConfig.GetCloudId(),
        volumeConfig.GetFolderId(),
        volumeConfig.GetDiskId());
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleHttpInfo(
    const NMon::TEvRemoteHttpInfo::TPtr& ev,
    const TActorContext& ctx)
{
    using THttpHandler = void(TVolumeActor::*)(
        const NActors::TActorContext&,
        const TCgiParameters&,
        TRequestInfoPtr);

    using THttpHandlers = THashMap<TString, THttpHandler>;
    using TActor = TVolumeActor;

    const THttpHandlers postActions {{
        {"removeclient",           &TActor::HandleHttpInfo_RemoveClient          },
        {"resetmountseqnumber",    &TActor::HandleHttpInfo_ResetMountSeqNumber   },
        {"createCheckpoint",       &TActor::HandleHttpInfo_CreateCheckpoint      },
        {"deleteCheckpoint",       &TActor::HandleHttpInfo_DeleteCheckpoint      },
        {"startpartitions",        &TActor::HandleHttpInfo_StartPartitions       },
        {"changethrottlingpolicy", &TActor::HandleHttpInfo_ChangeThrottlingPolicy},
        {"resetTransactionsLatency", &TActor::HandleHttpInfo_ResetTransactionsLatency},
    }};

    const THttpHandlers getActions {{
        {"rendernpinfo", &TActor::HandleHttpInfo_RenderNonreplPartitionInfo },
        {"getRequestsLatency", &TActor::HandleHttpInfo_GetRequestsLatency },
        {"getTransactionsLatency", &TActor::HandleHttpInfo_GetTransactionsLatency },
    }};

    auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s HTTP request: %s",
        LogTitle.GetWithTime().c_str(),
        msg->Query.Quote().c_str());

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        MakeIntrusive<TCallContext>());

    LWTRACK(
        RequestReceived_Volume,
        requestInfo->CallContext->LWOrbit,
        "HttpInfo",
        requestInfo->CallContext->RequestId);

    if (State) {
        auto methodType = GetHttpMethodType(*msg);
        auto params = GatherHttpParameters(*msg);
        const auto& action = params.Get("action");

        if (const auto* handler = postActions.FindPtr(action)) {
            if (methodType != HTTP_METHOD_POST) {
                RejectHttpRequest(ctx, *requestInfo, "Wrong HTTP method");
                return;
            }

            std::invoke(*handler, this, ctx, params, std::move(requestInfo));
            return;
        }

        if (const auto* handler = getActions.FindPtr(action)) {
            std::invoke(*handler, this, ctx, params, std::move(requestInfo));
            return;
        }

        const auto& activeTab = params.Get("tab");

        HandleHttpInfo_Default(
            ctx,
            State->GetMountHistory().GetSlice(),
            State->GetMetaHistory(),
            activeTab,
            params,
            requestInfo);

        return;
    }

    TStringStream out;
    DumpTabletNotReady(out);

    SendHttpResponse(ctx, *requestInfo, std::move(out.Str()));
}

void TVolumeActor::HandleHttpInfo_Default(
    const NActors::TActorContext& ctx,
    const TVolumeMountHistorySlice& history,
    const TVector<TVolumeMetaHistoryItem>& metaHistory,
    const TStringBuf tabName,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    ui64 ts = 0;
    ui64 seqNo = 0;

    if (TryFromString(params.Get("timestamp"), ts) &&
        TryFromString(params.Get("seqno"), seqNo))
    {
        auto cancelRoutine =
        [] (const TActorContext& ctx, TRequestInfo& requestInfo)
        {
            TStringStream out;

            DumpTabletNotReady(out);
            NCloud::Reply(
                ctx,
                requestInfo,
                std::make_unique<NMon::TEvRemoteHttpInfoRes>(
                    out.Str()));
        };

        requestInfo->CancelRoutine = cancelRoutine;

        ProcessReadHistory(
            ctx,
            std::move(requestInfo),
            {TInstant::MicroSeconds(ts), seqNo},
            ctx.Now() - Config->GetVolumeHistoryDuration(),
            Config->GetVolumeHistoryCacheSize(),
            true);
        return;
    }

    const char* overviewTabName = "Overview";
    const char* historyTabName = "History";
    const char* checkpointsTabName = "Checkpoints";
    const char* linksTabName = "Links";
    const char* latencyTabName = "Latency";
    const char* transactionsTabName = "Transactions";
    const char* tracesTabName = "Traces";
    const char* storageConfigTabName = "StorageConfig";
    const char* rawVolumeConfigTabName = "RawVolumeConfig";

    const char* activeTab = "tab-pane active";
    const char* inactiveTab = "tab-pane";

    const char* overviewTab = inactiveTab;
    const char* historyTab = inactiveTab;
    const char* checkpointsTab = inactiveTab;
    const char* linksTab = inactiveTab;
    const char* latencyTab = inactiveTab;
    const char* transactionsTab = inactiveTab;
    const char* tracesTab = inactiveTab;
    const char* storageConfigTab = inactiveTab;
    const char* rawVolumeConfigTab = inactiveTab;

    if (tabName.empty() || tabName == overviewTabName) {
        overviewTab = activeTab;
    } else if (tabName == historyTabName) {
        historyTab = activeTab;
    } else if (tabName == checkpointsTabName) {
        checkpointsTab = activeTab;
    } else if (tabName == linksTabName) {
        linksTab = activeTab;
    } else if (tabName == latencyTabName) {
        latencyTab = activeTab;
    } else if (tabName == transactionsTabName) {
        transactionsTab = activeTab;
    } else if (tabName == tracesTabName) {
        tracesTab = activeTab;
    } else if (tabName == storageConfigTabName) {
        storageConfigTab = activeTab;
    } else if (tabName == rawVolumeConfigTabName) {
        rawVolumeConfigTab = activeTab;
    }

    TStringStream out;

    HTML(out) {
        AddLatencyCSS(out);

        DIV_CLASS_ID("container-fluid", "tabs") {
            BuildVolumeTabs(out);

            DIV_CLASS("tab-content") {
                DIV_CLASS_ID(overviewTab, overviewTabName) {
                    DumpDefaultHeader(out, *Info(), SelfId().NodeId(), *DiagnosticsConfig);
                    RenderHtmlInfo(out, ctx.Now());
                }

                DIV_CLASS_ID(historyTab, historyTabName) {
                    RenderHistory(history, metaHistory, out);
                }

                DIV_CLASS_ID(checkpointsTab, checkpointsTabName) {
                    RenderCheckpoints(out);
                }

                DIV_CLASS_ID(linksTab, linksTabName) {
                    RenderLinks(out);
                }

                DIV_CLASS_ID(latencyTab, latencyTabName) {
                    RenderLatency(out);
                }

                DIV_CLASS_ID(transactionsTab, transactionsTabName) {
                    RenderTransactions(out);
                }

                DIV_CLASS_ID(tracesTab, tracesTabName) {
                    RenderTraces(out);
                }

                DIV_CLASS_ID(storageConfigTab, storageConfigTabName) {
                    RenderStorageConfig(out);
                }

                DIV_CLASS_ID(rawVolumeConfigTab, rawVolumeConfigTabName) {
                    RenderRawVolumeConfig(out);
                }
            }
        }
    }

    SendHttpResponse(ctx, *requestInfo, std::move(out.Str()));
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::RenderHistory(
    const TVolumeMountHistorySlice& history,
    const TVector<TVolumeMetaHistoryItem>& metaHistory,
    IOutputStream& out) const
{
    using namespace NMonitoringUtils;

    HTML(out) {
        DIV_CLASS("row") {
            TAG(TH3) { out << "History"; }
            TABLE_SORTABLE_CLASS("table table-bordered") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "Time"; }
                        TABLEH() { out << "Host"; }
                        TABLEH() { out << "Operation type"; }
                        TABLEH() { out << "Result"; }
                        TABLEH() { out << "Operation"; }
                    }
                }
                for (const auto& h: history.Items) {
                    TABLER() {
                        TABLED() {
                            out << h.Key.Timestamp;
                        }
                        TABLED() {
                            out << h.Operation.GetTabletHost();
                        }
                        TABLED() {
                            TStringBuf type;
                            if (h.Operation.HasAdd()) {
                                type = "Add";
                            } else if (h.Operation.HasRemove()) {
                                type = "Remove";
                            } else {
                                type = "Update";
                            }
                            out << type;
                        }
                        TABLED() {
                            if (FAILED(h.Operation.GetError().GetCode())) {
                                SerializeToTextFormat(h.Operation.GetError(), out);
                            } else {
                                out << "OK";
                            }
                        }
                        TABLED() {
                            if (h.Operation.HasAdd()) {
                                SerializeToTextFormat(h.Operation.GetAdd(), out);
                            } else if (h.Operation.HasRemove()) {
                                SerializeToTextFormat(h.Operation.GetRemove(), out);
                            } else {
                                SerializeToTextFormat(h.Operation.GetUpdateVolumeMeta(), out);
                            }
                        }
                    }
                }
            }
            if (history.Items.size() && history.HasMoreItems()) {
                TABLE_CLASS("table") {
                    TABLER() {
                        TABLED() {
                            BuildHistoryNextButton(
                                out,
                                history.Items.back().Key,
                                TabletID());
                        }
                    }
                }
            }
        }

        DIV_CLASS("row") {
            TAG(TH3) { out << "MetaHistory"; }
            TABLE_SORTABLE_CLASS("table table-bordered") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "Time"; }
                        TABLEH() { out << "Meta"; }
                    }
                }
                auto it = metaHistory.rbegin();
                ui32 displayedCount = 0;
                const ui32 limit =
                    Config->GetVolumeMetaHistoryDisplayedRecordLimit();
                while (it != metaHistory.rend() && displayedCount < limit) {
                    const auto& item = *it;

                    TABLER() {
                        TABLED() {
                            out << item.Timestamp;
                        }
                        TABLED() {
                            item.Meta.PrintJSON(out);
                        }
                    }

                    ++it;
                    ++displayedCount;
                }
            }
        }
    }
}

void TVolumeActor::RenderCheckpoints(IOutputStream& out) const
{
    using namespace NMonitoringUtils;

    HTML(out) {
        DIV_CLASS("row") {
            TAG(TH3) {
                BuildMenuButton(out, "checkpoint-add");
                out << "Checkpoints";
            }
            out << "<div class='collapse form-group' id='checkpoint-add'>";
            BuildCreateCheckpointButton(out, TabletID());
            out << "</div>";

            TABLE_SORTABLE_CLASS("table table-bordered") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "CheckpointId"; }
                        TABLEH() { out << "Info"; }
                        TABLEH() { out << "Delete"; }
                    }
                }
                for (const auto& [r, checkpointInfo]:
                    State->GetCheckpointStore().GetActiveCheckpoints())
                {
                    TABLER() {
                        TABLED() { out << r; }
                        TABLED() {
                            RenderCheckpointInfo(
                                GetBlocksCount(),
                                State->GetMeta()
                                    .GetVolumeConfig()
                                    .GetBlockSize(),
                                checkpointInfo,
                                out);
                        }
                        TABLED() {
                            BuildDeleteCheckpointButton(
                                out,
                                TabletID(),
                                r);
                        }
                    }
                }
            }
        };
        DIV_CLASS("row") {
            const bool isDiskRegistryMediaKind =
                State->IsDiskRegistryMediaKind();
            TAG(TH3) { out << "CheckpointRequests"; }
            TABLE_SORTABLE_CLASS("table table-bordered") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "RequestId"; }
                        TABLEH() { out << "CheckpointId"; }
                        if (isDiskRegistryMediaKind) {
                            TABLEH() { out << "ShadowDisk"; }
                        }
                        TABLEH() { out << "Timestamp"; }
                        TABLEH() { out << "State"; }
                        TABLEH() { out << "Type"; }
                    }
                }
                for (const auto& r:
                     State->GetCheckpointStore().GetCheckpointRequests())
                {
                    TABLER() {
                        TABLED() { out << r.RequestId; }
                        TABLED() { out << r.CheckpointId; }
                        if (isDiskRegistryMediaKind) {
                            TABLED()
                            {
                                RenderCheckpointRequest(out, r);
                            }
                        }
                        TABLED() { out << r.Timestamp; }
                        TABLED() { out << r.State; }
                        TABLED() { out << r.ReqType; }
                    }
                }
            }
        }
        out << R"___(
            <script type='text/javascript'>
            function createCheckpoint() {
                document.forms['CreateCheckpoint'].submit();
            }
            function deleteCheckpoint(checkpointId) {
                document.forms['DeleteCheckpoint_' + checkpointId].submit();
            }
            </script>
        )___";
    }
}

void TVolumeActor::RenderLinks(IOutputStream& out) const
{
    RenderFollowers(out, State->GetAllFollowers());
    RenderLeaders(out, State->GetAllLeaders());
}

void TVolumeActor::RenderLatency(IOutputStream& out) const {
    using namespace NMonitoringUtils;

    if (!State) {
        return;
    }

 const TString style = R"(
        <style>
            .table-latency {
                width: 100%;
                border-collapse: collapse;
                padding: 0;
            }

            .table-latency th,
            .table-latency td {
                padding: 0 8px 0 8px;
                border: none;
            }

            .table-latency th {
                font-weight: bold;
                text-align: center;
            }

            .table-latency td {
                text-align: right;
            }

            .table-latency th:not(:last-child),
            .table-latency td:not(:last-child) {
                border-right: 1px solid black;
            }

            .tooltip-latency {
                position: relative;
                display: inline-block;
            }

            .tooltip-latency .tooltiptext-latency {
                visibility: hidden;
                width: 120px;
                background-color: black;
                color: #fff;
                text-align: center;
                border-radius: 6px;
                padding: 5px 0;
                position: absolute;
                z-index: 1;
            }

            .tooltip-latency:hover .tooltiptext-latency {
                visibility: visible;
            }

        </style>
        )";

    const TString containerId = "requests-latency-container";
    const TString toggleId = "requests-auto-refresh-toggle";

    HTML (out) {
        out << style;

        RenderAutoRefreshToggle(out, toggleId, "Auto update info", true);

        out << "<div id=\"" << containerId << "\">";
        DIV_CLASS ("row") {
            RenderSizeTable(out, State->GetBlockSize());
        }
        out << "</div>";

        out << R"(<script>
            function applyRequestsValues(stat) {
                for (let key in stat) {
                    const element = document.getElementById(key);
                    if (element) {
                        element.textContent = stat[key];
                    }
                }
            }
            function updateRequestsData(result, container) {
                applyRequestsValues(result.stat);
                applyRequestsValues(result.percentiles);
            }
        </script>)";

        RenderAutoRefreshScript(
            out,
            containerId,
            toggleId,
            "getRequestsLatency",
            TabletID(),
            1000,
            "updateRequestsData"
        );
    }
}

void TVolumeActor::RenderTransactions(IOutputStream& out) const
{
    HTML (out) {
        DumpLatency(
            out,
            TabletID(),
            TransactionTimeTracker,
            8   // columnCount
        );
    }
}

void TVolumeActor::RenderTraces(IOutputStream& out) const
{
    using namespace NMonitoringUtils;
    const auto& diskId = State->GetDiskId();
    HTML(out) {
        DIV_CLASS("row") {
            TAG(TH3) {
                out << "<a href=\"../tracelogs/slow?diskId=" << diskId << "\">"
                        "Slow logs for " << diskId << "</a><br>";
                out << "<a href=\"../tracelogs/random?diskId=" << diskId << "\">"
                       "Random samples for " << diskId << "</a>";
            }
        }
    }
}

void TVolumeActor::RenderStorageConfig(IOutputStream& out) const
{
    using namespace NMonitoringUtils;

    HTML(out) {
        DIV_CLASS("row") {
            TAG(TH3) {
                out << "StorageConfig";
            }
            TABLE_SORTABLE_CLASS("table table-bordered") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "Field name"; }
                        TABLEH() { out << "Value"; }
                    }
                }
                const auto& protoValues = Config->GetStorageConfigProto();
                constexpr i32 expectedNonRepeatedFieldIndex = -1;
                const auto* descriptor =
                    NProto::TStorageServiceConfig::GetDescriptor();
                if (!descriptor) {
                    return;
                }

                const auto* reflection =
                    NProto::TStorageServiceConfig::GetReflection();

                for (int i = 0; i < descriptor->field_count(); ++i) {
                    TStringBuilder value;
                    const auto* fieldDescriptor = descriptor->field(i);
                    if (fieldDescriptor->is_repeated()) {
                        const auto repeatedSize =
                            reflection->FieldSize(protoValues, fieldDescriptor);
                        if (!repeatedSize) {
                            continue;
                        }

                        for (int j = 0; j < repeatedSize; ++j) {
                            TString curValue;
                            google::protobuf::TextFormat::PrintFieldValueToString(
                                protoValues,
                                fieldDescriptor,
                                j,
                                &curValue);
                            value.append(curValue);
                            value.append("; ");
                        }
                    } else if (
                        reflection->HasField(protoValues, fieldDescriptor))
                    {
                        google::protobuf::TextFormat::PrintFieldValueToString(
                            protoValues,
                            fieldDescriptor,
                            expectedNonRepeatedFieldIndex,
                            &value);
                    } else {
                        continue;
                    }

                    TABLER() {
                        TABLED() {
                            out << fieldDescriptor->name();;
                        }
                        TABLED() {
                            out << value;
                        }
                    }
                }
            }
        }
    }
}

void TVolumeActor::RenderRawVolumeConfig(IOutputStream& out) const
{
    if (!State) {
        return;
    }

    const auto& volumeConfig = State->GetMeta().GetVolumeConfig();

    HTML(out) {
        DIV_CLASS("row") {
            TAG(TH3) {
                out << "RawVolumeConfig";
            }

            PRE() {
                SerializeToTextFormatPretty(volumeConfig, out);
            }
        }
    }
}

void TVolumeActor::RenderHtmlInfo(IOutputStream& out, TInstant now) const
{
    using namespace NMonitoringUtils;

    if (!State) {
        return;
    }

    const auto mediaKind = State->GetConfig().GetStorageMediaKind();

    HTML(out) {
        DIV_CLASS("row") {
            DIV_CLASS("col-md-6") {
                DumpMonitoringVolumeLink(
                    out,
                    *DiagnosticsConfig,
                    State->GetDiskId());
            }
        }

        DIV_CLASS("row") {
            DIV_CLASS("col-md-6") {
                RenderStatus(out);
            }
        }

        if (IsDiskRegistryMediaKind(mediaKind)) {
            DIV_CLASS("row") {
                DIV_CLASS("col-md-6") {
                    RenderMigrationStatus(out);
                }
            }
        }

        if (IsReliableDiskRegistryMediaKind(mediaKind)) {
            DIV_CLASS("row") {
                DIV_CLASS("col-md-6") {
                    RenderScrubbingStatus(out);
                }
            }

            DIV_CLASS("row") {
                DIV_CLASS("col-md-6") {
                    RenderResyncStatus(out);
                }
            }
        }

        if (LaggingDevicesAreAllowed()) {
            DIV_CLASS("row")
            {
                DIV_CLASS("col-md-6")
                {
                    RenderLaggingStatus(out);
                }
            }
        }

        DIV_CLASS("row") {
            DIV_CLASS("col-md-6") {
                RenderCommonButtons(out);
            }
        }

        DIV_CLASS("row") {
            DIV_CLASS("col-md-6") {
                RenderConfig(out);
            }
        }

        DIV_CLASS("row") {
            RenderTabletList(out);
        }

        DIV_CLASS("row") {
            RenderClientList(out, now);
        }

        DIV_CLASS("row") {
            RenderMountSeqNumber(out);
        }
    }
}

void TVolumeActor::RenderTabletList(IOutputStream& out) const
{
    HTML(out) {
        TAG(TH3) { out << "Partitions"; }
        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Partition"; }
                    TABLEH() { out << "Status"; }
                    TABLEH() { out << "Blocks Count"; }
                }
            }

            for (const auto& partition: State->GetPartitions()) {
                TABLER() {
                    TABLED() {
                        out << "<a href='../tablets?TabletID="
                            << partition.TabletId
                            << "'>"
                            << partition.TabletId
                            << "</a>";
                    }
                    TABLED() {
                        out << partition.GetStatus();
                    }
                    TABLED() {
                        out << partition.PartitionConfig.GetBlocksCount();
                    }
                }
            }

            const auto mediaKind = State->GetConfig().GetStorageMediaKind();
            if (IsDiskRegistryMediaKind(mediaKind)) {
                TABLER() {
                    TABLED() {
                        out << "<a href='?action=rendernpinfo"
                            << "&TabletID=" << TabletID()
                            << "'>"
                            << "nonreplicated partition"
                            << "</a>";
                    }
                    TABLED() {
                    }
                    TABLED() {
                        out << State->GetConfig().GetBlocksCount();
                    }
                }
            }
        }
    }
}

void TVolumeActor::RenderConfig(IOutputStream& out) const
{
    if (!State) {
        return;
    }

    const auto& volumeConfig = State->GetMeta().GetVolumeConfig();

    ui64 blocksCount = GetBlocksCount();
    ui32 blockSize = volumeConfig.GetBlockSize();
    const double blocksPerStripe = volumeConfig.GetBlocksPerStripe();
    const double stripes = blocksPerStripe
        ? double(blocksCount) / blocksPerStripe
        : 0;

    HTML(out) {
        TAG(TH3) { out << "Volume Config"; }
        TABLE_SORTABLE_CLASS("table table-condensed") {
            TABLEHEAD() {
                TABLER() {
                    TABLED() { out << "Disk Id"; }
                    TABLED() {
                        const auto mediaKind =
                            State->GetConfig().GetStorageMediaKind();
                        if (DiskRegistryTabletId &&
                            IsDiskRegistryMediaKind(mediaKind))
                        {
                            out << "<a href='?action=disk&TabletID="
                                << DiskRegistryTabletId
                                << "&DiskID=" << volumeConfig.GetDiskId()
                                << "'>" << volumeConfig.GetDiskId() << "</a>";
                        } else {
                            out << volumeConfig.GetDiskId();
                        }
                    }
                }
                TABLER() {
                    TABLED() { out << "Base Disk Id"; }
                    TABLED() { out << volumeConfig.GetBaseDiskId(); }
                }
                TABLER() {
                    TABLED() { out << "Base Disk Checkpoint Id"; }
                    TABLED() { out << volumeConfig.GetBaseDiskCheckpointId(); }
                }
                TABLER() {
                    TABLED() { out << "Folder Id"; }
                    TABLED() { out << volumeConfig.GetFolderId(); }
                }
                TABLER() {
                    TABLED() { out << "Cloud Id"; }
                    TABLED() { out << volumeConfig.GetCloudId(); }
                }
                TABLER() {
                    TABLED() { out << "Project Id"; }
                    TABLED() { out << volumeConfig.GetProjectId(); }
                }

                TABLER() {
                    TABLED() { out << "Block size"; }
                    TABLED() { out << blockSize; }
                }

                TABLER() {
                    TABLED() { out << "Blocks count"; }
                    TABLED() {
                        out << blocksCount;
                        out << " ("
                            << FormatByteSize(blocksCount * blockSize)
                            << ")";
                    }
                }

                if (State->GetTrackUsedBlocks()) {
                    const auto used = State->GetUsedBlockCount();

                    TABLER() {
                        TABLED() { out << "Used blocks count"; }
                        TABLED() {
                            out << used;
                            out << " ("
                                << FormatByteSize(used * blockSize)
                                << ")";
                        }
                    }
                }

                TABLER() {
                    TABLED() { out << "BlocksPerStripe"; }
                    TABLED() {
                        out << blocksPerStripe;
                        out << " ("
                            << FormatByteSize(blocksPerStripe * blockSize)
                            << ")";
                    }
                }

                TABLER() {
                    TABLED() { out << "Stripes"; }
                    TABLED() { out << stripes << Endl; }
                }

                TABLER() {
                    TABLED() { out << "StripesPerPartition"; }
                    TABLED() {
                        out << (stripes / volumeConfig.PartitionsSize()) << Endl;
                    }
                }

                TABLER() {
                    TABLED() { out << "Number of channels"; }
                    TABLED() { out << volumeConfig.ExplicitChannelProfilesSize(); }
                }

                TABLER() {
                    TABLED() { out << "Storage media kind"; }
                    TABLED() {
                        out << NCloud::NProto::EStorageMediaKind_Name(
                            (NCloud::NProto::EStorageMediaKind)volumeConfig.GetStorageMediaKind());
                    }
                }

                TABLER() {
                    TABLED() { out << "Encryption"; }
                    TABLED() { out << volumeConfig.GetEncryptionDesc(); }
                }

                TABLER() {
                    TABLED() { out << "Channel profile id"; }
                    TABLED() {
                        out << volumeConfig.GetChannelProfileId();
                    }
                }

                TABLER() {
                    TABLED() { out << "Partition tablet version"; }
                    TABLED() {
                        out << volumeConfig.GetTabletVersion();
                    }
                }

                TABLER() {
                    TABLED() { out << "Tags"; }
                    TABLED() {
                        out << volumeConfig.GetTagsStr();
                    }
                }

                TABLER() {
                    TABLED() { out << "UseRdma"; }
                    TABLED() {
                        out << State->GetUseRdma();
                    }
                }

                TABLER() {
                    TABLED() { out << "UseFastPath"; }
                    TABLED() {
                        out << State->GetUseFastPath();
                    }
                }

                TABLER() {
                    TABLED() { out << "Throttler"; }
                    TABLED() {
                        const auto& tp = State->GetThrottlingPolicy();
                        TABLE_CLASS("table table-condensed") {
                            TABLEBODY() {
                                if (Throttler) {
                                    TABLER() {
                                        TABLED() { out << "PostponedQueueSize"; }
                                        TABLED() { out << Throttler->GetPostponedRequestsCount(); }
                                    }
                                }
                                TABLER() {
                                    TABLED() { out << "WriteCostMultiplier"; }
                                    TABLED() { out << tp.GetWriteCostMultiplier(); }
                                }
                                TABLER() {
                                    TABLED() { out << "PostponedQueueWeight"; }
                                    TABLED() { out << tp.CalculatePostponedWeight(); }
                                }
                                TABLER() {
                                    TABLED() { out << "BackpressureFeatures"; }
                                    TABLED() {
                                        const auto& bp = tp.GetCurrentBackpressure();
                                        TABLE_CLASS("table table-condensed") {
                                            TABLEBODY() {
                                                TABLER() {
                                                    TABLED() { out << "FreshIndexScore"; }
                                                    TABLED() { out << bp.FreshIndexScore; }
                                                }
                                                TABLER() {
                                                    TABLED() { out << "CompactionScore"; }
                                                    TABLED() { out << bp.CompactionScore; }
                                                }
                                                TABLER() {
                                                    TABLED() { out << "DiskSpaceScore"; }
                                                    TABLED() { out << bp.DiskSpaceScore; }
                                                }
                                                TABLER() {
                                                    TABLED() { out << "CleanupScore"; }
                                                    TABLED() { out << bp.CleanupScore; }
                                                }
                                            }
                                        }
                                    }
                                }
                                TABLER() {
                                    TABLED() { out << "ThrottlerParams"; }
                                    TABLED() {
                                        TABLE_CLASS("table table-condensed") {
                                            TABLEBODY() {
                                                TABLER() {
                                                    TABLED() { out << "ReadC1"; }
                                                    TABLED() { out << tp.C1(TVolumeThrottlingPolicy::EOpType::Read); }
                                                }
                                                TABLER() {
                                                    TABLED() { out << "ReadC2"; }
                                                    TABLED() { out << FormatByteSize(tp.C2(TVolumeThrottlingPolicy::EOpType::Read)); }
                                                }
                                                TABLER() {
                                                    TABLED() { out << "WriteC1"; }
                                                    TABLED() { out << tp.C1(TVolumeThrottlingPolicy::EOpType::Write); }
                                                }
                                                TABLER() {
                                                    TABLED() { out << "WriteC2"; }
                                                    TABLED() { out << FormatByteSize(tp.C2(TVolumeThrottlingPolicy::EOpType::Write)); }
                                                }
                                                TABLER() {
                                                    TABLED() { out << "DescribeC1"; }
                                                    TABLED() { out << tp.C1(TVolumeThrottlingPolicy::EOpType::Describe); }
                                                }
                                                TABLER() {
                                                    TABLED() { out << "DescribeC2"; }
                                                    TABLED() { out << FormatByteSize(tp.C2(TVolumeThrottlingPolicy::EOpType::Describe)); }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                TABLER() {
                    TABLED() { out << "ExplicitChannelProfiles"; }
                    TABLED() {
                        if (volumeConfig.ExplicitChannelProfilesSize()) {
                            out << "Yes";
                        } else {
                            out << "No";
                        }
                    }
                }
            }
        }
    }
}

void TVolumeActor::RenderClientList(IOutputStream& out, TInstant now) const
{
    if (!State) {
        return;
    }

    const auto& clients = State->GetClients();
    if (clients.empty()) {
        return;
    }

    const auto& diskId = State->GetDiskId();

    HTML(out) {
        TAG(TH3) { out << "Volume Clients"; }
        TABLE_SORTABLE_CLASS("table table-condensed") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Client Id"; }
                    TABLEH() { out << "Access mode"; }
                    TABLEH() { out << "Mount mode"; }
                    TABLEH() { out << "Disconnect time"; }
                    TABLEH() { out << "Last activity time"; }
                    TABLEH() { out << "State"; }
                    TABLEH() { out << "Pipe server actor"; }
                    TABLEH() { out << "Remove"; }
                }
            }
            ui64 clientNo = 0;
            for (const auto& pair: clients) {
                const auto& clientId = pair.first;
                const auto& clientInfo = pair.second;
                const auto& volumeClientInfo = clientInfo.GetVolumeClientInfo();

                OutputClientInfo(
                    out,
                    clientId,
                    diskId,
                    clientNo++,
                    TabletID(),
                    volumeClientInfo.GetVolumeMountMode(),
                    volumeClientInfo.GetVolumeAccessMode(),
                    volumeClientInfo.GetDisconnectTimestamp(),
                    volumeClientInfo.GetLastActivityTimestamp(),
                    State->IsClientStale(clientInfo, now),
                    clientInfo.GetPipes());
            }
        }

        out << R"___(
                <script>
                function volumeRemoveClient(clientId) {
                    document.forms["RemoveClient_" + clientId].submit();
                }
                </script>
            )___";
    }
}

void TVolumeActor::RenderMountSeqNumber(IOutputStream& out) const
{
    if (!State) {
        return;
    }

    const auto& clients = State->GetClients();
    if (clients.empty()) {
        return;
    }

    HTML(out) {
        TAG(TH3) { out << "Volume Mount Sequence Number"; }
        TABLE_SORTABLE_CLASS("table table-condensed") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Client Id"; }
                    TABLEH() { out << "Mount Generation"; }
                    TABLEH() { out << "Reset"; }
                }
            }

            TABLER() {
                const auto& rwClient = State->GetReadWriteAccessClientId();
                TABLED() { out << rwClient; }
                TABLED() { out << State->GetMountSeqNumber(); }
                TABLED() {
                    BuildVolumeResetMountSeqNumberButton(
                        out,
                        rwClient,
                        TabletID());
                }
            }
        }

        out << R"___(
                <script>
                function resetMountSeqNumber(clientId) {
                    document.forms["ResetMountSeqNumber_" + clientId].submit();
                }
                </script>
            )___";
    }
}

void TVolumeActor::RenderStatus(IOutputStream& out) const
{
    HTML(out) {
        TAG(TH3) {
            TString statusText = "offline";
            TString cssClass = "label-danger";

            auto status = GetVolumeStatus();
            if (status == TVolumeActor::STATUS_INACTIVE) {
                statusText = GetVolumeStatusString(status);
                cssClass = "label-default";
            } else if (status != TVolumeActor::STATUS_OFFLINE) {
                statusText = GetVolumeStatusString(status);
                cssClass = "label-success";
            }

            out << "Status:";

            SPAN_CLASS_STYLE("label " + cssClass, "margin-left:10px") {
                out << statusText;
            }

            if (!CanExecuteWriteRequest()) {
                SPAN_CLASS_STYLE("label label-danger", "margin-left:10px") {
                    out << "Writes blocked by checkpoint";
                }
            }
        }
    }
}

void TVolumeActor::RenderMigrationStatus(IOutputStream& out) const
{
    HTML(out) {
        bool active = State->GetMeta().GetMigrations().size()
            || State->GetFilteredFreshDevices().size();
        TStringBuf label = State->GetFilteredFreshDevices().empty()
            ? "Migration" : "Replication";

        TAG(TH3) {
            TString statusText = "inactive";
            TString cssClass = "label-default";

            if (active) {
                statusText = "in progress";
                cssClass = "label-success";
            }

            out << label << "Status:";

            SPAN_CLASS_STYLE("label " + cssClass, "margin-left:10px") {
                out << statusText;
            }
        }

        if (!active) {
            return;
        }

        const auto totalBlocks = GetBlocksCount();
        const auto migrationIndex = State->GetMeta().GetMigrationIndex();
        const auto blockSize = State->GetBlockSize();

        TABLE_SORTABLE_CLASS("table table-condensed") {
            TABLEHEAD() {
                TABLER() {
                    TABLED() { out << label << " index: "; }
                    TABLED() {
                        OutputProgress(
                            migrationIndex,
                            totalBlocks,
                            blockSize,
                            State->GetBlockCountToMigrate(),
                            out);
                    }
                }
            }
        }
    }
}

void TVolumeActor::RenderScrubbingStatus(IOutputStream& out) const
{
    const auto& scrubbing = State->GetScrubbingInfo();
    const auto totalBlocks = GetBlocksCount();
    const auto blockSize = State->GetBlockSize();

    auto outputRanges = [&](const TBlockRangeSet64& ranges)
    {
        size_t count = 0;
        for (auto range: ranges) {
            out << range.Print();
            if (++count > 1000) {
                out << "...truncated";
                break;
            }
        }
    };

    HTML (out) {
        TAG (TH3) {
            TString statusText = "inactive";
            TString cssClass = "label-default";

            if (scrubbing.Running) {
                statusText = "in progress";
                cssClass = "label-success";
            }

            out << "ScrubbingStatus:";

            SPAN_CLASS_STYLE ("label " + cssClass, "margin-left:10px") {
                out << statusText;
            }
        }

        if (!scrubbing.Running) {
            return;
        }

        TABLE_SORTABLE_CLASS("table table-condensed") {
            TABLEHEAD() {
                TABLER() {
                    TABLED() { out << "Scrubbing progress: "; }
                    TABLED() {
                        OutputProgress(
                            scrubbing.CurrentRange.Start,
                            totalBlocks,
                            blockSize,
                            totalBlocks - scrubbing.CurrentRange.Start,
                            out);

                        out << "<br>Full scan count: " << scrubbing.FullScanCount;
                    }
                }
                TABLER() {
                    TABLED () { out << "Minors: "; }
                    TABLED () {
                        outputRanges(scrubbing.Minors);
                    }
                }
                TABLER() {
                    TABLED () { out << "Majors: "; }
                    TABLED () {
                        outputRanges(scrubbing.Majors);
                    }
                }
                TABLER() {
                    TABLED () { out << "Fixed: "; }
                    TABLED () {
                        outputRanges(scrubbing.Fixed);
                    }
                }
                TABLER() {
                    TABLED () { out << "Fixed partial: "; }
                    TABLED () {
                        outputRanges(scrubbing.FixedPartial);
                    }
                }
            }
        }

    }
}

void TVolumeActor::RenderResyncStatus(IOutputStream& out) const
{
    HTML(out) {
        TAG(TH3) {
            TString statusText = "inactive";
            TString cssClass = "label-default";

            if (State->IsMirrorResyncNeeded()) {
                statusText = "in progress";
                cssClass = "label-success";
            }

            out << "ResyncStatus:";

            SPAN_CLASS_STYLE("label " + cssClass, "margin-left:10px") {
                out << statusText;
            }
        }

        if (!State->IsMirrorResyncNeeded()) {
            return;
        }

        const auto totalBlocks = GetBlocksCount();
        const auto resyncIndex = State->GetMeta().GetResyncIndex();
        const auto blockSize = State->GetBlockSize();

        TABLE_SORTABLE_CLASS("table table-condensed") {
            TABLEHEAD() {
                TABLER() {
                    TABLED() { out << "Resync index: "; }
                    TABLED() {
                        OutputProgress(
                            resyncIndex,
                            totalBlocks,
                            blockSize,
                            totalBlocks - resyncIndex,
                            out);
                    }
                }
            }
        }
    }
}

void TVolumeActor::RenderLaggingStatus(IOutputStream& out) const
{
    HTML(out)
    {
        TAG(TH3)
        {
            TString statusText = "ok";
            TString cssClass = "label-success";

            if (State->HasLaggingAgents()) {
                statusText = "has lagging devices";
                cssClass = "label-info";
            }

            if (!State->GetNonreplicatedPartitionConfig()
                     ->GetOutdatedDeviceIds()
                     .empty())
            {
                statusText = "has outdated devices";
                cssClass = "label-danger";
            }

            out << "LaggingStatus:";

            SPAN_CLASS_STYLE("label " + cssClass, "margin-left:10px")
            {
                out << statusText;
            }
        }

        if (!State->HasLaggingAgents() ||
            State->GetLaggingAgentsMigrationInfo().empty())
        {
            return;
        }

        const auto& laggingInfos = State->GetLaggingAgentsMigrationInfo();
        const auto blockSize = State->GetBlockSize();

        ui64 cleanBlocks = 0;
        ui64 dirtyBlocks = 0;
        TStringBuilder laggingAgentIds;
        for (const auto& [laggingAgentId, laggingInfo] : laggingInfos) {
            if (!laggingAgentIds.empty()) {
                laggingAgentIds << ", ";
            }
            laggingAgentIds << laggingAgentId;
            cleanBlocks += laggingInfo.CleanBlocks;
            dirtyBlocks += laggingInfo.DirtyBlocks;
        }

        TABLE_SORTABLE_CLASS("table table-condensed")
        {
            TABLEHEAD()
            {
                TABLER()
                {
                    TABLED()
                    {
                        out << "Lagging agents " << laggingAgentIds
                            << " migration progress: ";
                    }
                    TABLED()
                    {
                        OutputProgress(
                            cleanBlocks,
                            cleanBlocks + dirtyBlocks,
                            blockSize,
                            dirtyBlocks,
                            out);
                    }
                }
            }
        }
    }
}

void TVolumeActor::RenderLaggingStateForDevice(
    IOutputStream& out,
    const NProto::TDeviceConfig& d)
{
    const char* stateMsg = "ok";
    const char* color = "green";
    auto laggingDevices = State->GetLaggingDeviceIds();

    if (laggingDevices.contains(d.GetDeviceUUID())) {
        stateMsg = "lagging";
        color = "blue";
    }

    const auto& laggingInfos = State->GetLaggingAgentsMigrationInfo();
    if (laggingInfos.contains(d.GetAgentId())) {
        stateMsg = "migrating";
        color = "blue";
    }

    if (State->GetNonreplicatedPartitionConfig()
            ->GetOutdatedDeviceIds()
            .contains(d.GetDeviceUUID()))
    {
        stateMsg = "outdated";
        color = "red";
    }

    out << "<font color=" << color << ">";
    out << stateMsg;
    out << "</font>";
}

void TVolumeActor::RenderCommonButtons(IOutputStream& out) const
{
    if (!State) {
        return;
    }

    HTML(out) {
        TAG(TH3) { out << "Controls"; }
        TABLE_SORTABLE_CLASS("table table-condensed") {
            TABLER() {
                TABLED() {
                    BuildStartPartitionsButton(
                        out,
                        State->GetDiskId(),
                        TabletID());
                }
            }
        }

        out << R"___(
                <script>
                function startPartitions() {
                    document.forms["StartPartitions"].submit();
                }
                </script>
            )___";

        if (CanChangeThrottlingPolicy()) {
            TABLE_SORTABLE_CLASS("table table-condensed") {
                TABLER() {
                    TABLED() {
                        TAG (TH3) {
                            const bool isDefaultPolicy =
                                IsSetDefaultThrottlingPolicy(State.get());

                            TString statusText =
                                isDefaultPolicy ? "Default" : "Custom";
                            TString cssClass = isDefaultPolicy ? "label-success"
                                                               : "label-danger";

                            out << "Throttling Policy status: ";
                            SPAN_CLASS ("label " + cssClass) {
                                out << statusText;
                            }
                        }
                        BuildChangeThrottlingPolicyButton(
                            out,
                            State->GetDiskId(),
                            State->GetConfig().GetPerformanceProfile(),
                            TabletID());
                    }
                }
            }

            out << R"___(
                    <script>
                    function changeThrottlingPolicy() {
                        document.forms["ChangeThrottlingPolicy"].submit();
                    }
                    </script>
                )___";
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleHttpInfo_StartPartitions(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    Y_UNUSED(params);

    StartPartitionsIfNeeded(ctx);

    SendHttpResponse(ctx, *requestInfo, "Start initiated", EAlertLevel::SUCCESS);
}

void TVolumeActor::HandleHttpInfo_ChangeThrottlingPolicy(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    if (!CanChangeThrottlingPolicy()) {
        SendHttpResponse(
            ctx,
            *requestInfo,
            "ThrottlingPolicy can't be changed for this volume",
            EAlertLevel::WARNING);

        return;
    }

    NProto::TVolumePerformanceProfile pp =
        State->GetConfig().GetPerformanceProfile();
    auto getParam = [&] (const TStringBuf name) {
        const auto& s = params.Get(name);
        return FromStringWithDefault(s, Max<ui32>());
    };
    auto getParam64 = [&] (const TStringBuf name) {
        const auto& s = params.Get(name);
        return FromStringWithDefault(s, static_cast<ui64>(Max<ui32>()));
    };
    const auto maxReadIops = getParam("MaxReadIops");
    const auto maxWriteIops = getParam("MaxWriteIops");
    const auto maxReadBandwidth = getParam64("MaxReadBandwidth");
    const auto maxWriteBandwidth = getParam64("MaxWriteBandwidth");

    pp.SetMaxReadIops(Min(pp.GetMaxReadIops(), maxReadIops));
    pp.SetMaxWriteIops(Min(pp.GetMaxWriteIops(), maxWriteIops));
    pp.SetMaxReadBandwidth(Min(pp.GetMaxReadBandwidth(), maxReadBandwidth));
    pp.SetMaxWriteBandwidth(Min(pp.GetMaxWriteBandwidth(), maxWriteBandwidth));

    State->ResetThrottlingPolicy(pp);

    SendHttpResponse(
        ctx,
        *requestInfo,
        "ThrottlingPolicy changed",
        EAlertLevel::SUCCESS);
}

void TVolumeActor::HandleHttpInfo_RenderNonreplPartitionInfo(
    const NActors::TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    Y_UNUSED(params);

    TStringStream out;

    auto meta = State->GetMeta();
    if (!State->GetNonreplicatedPartitionConfig()) {
        HTML(out) {
            out << "no config, allocation result: "
                << FormatError(StorageAllocationResult);
        }

        return;
    }

    const auto& config = *State->GetNonreplicatedPartitionConfig();

    HTML(out) {
        BODY() {
            TABLE_CLASS("table table-bordered") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "Property"; }
                        TABLEH() { out << "Value"; }
                    }
                }
                TABLER() {
                    TABLED() { out << "BlockSize"; }
                    TABLED() { out << config.GetBlockSize(); }
                }
                TABLER() {
                    TABLED() { out << "Blocks"; }
                    TABLED() { out << config.GetBlockCount(); }
                }
                TABLER() {
                    TABLED() { out << "IOMode"; }
                    TABLED() { out << meta.GetIOMode(); }
                }
                TABLER() {
                    TABLED() { out << "IOModeTs"; }
                    TABLED() { out << meta.GetIOModeTs(); }
                }
                TABLER() {
                    TABLED() { out << "MuteIOErrors"; }
                    TABLED() { out << meta.GetMuteIOErrors(); }
                }
                TABLER() {
                    TABLED() { out << "MaxTimedOutDeviceStateDuration"; }
                    TABLED() {
                        const bool overridden =
                            config.IsMaxTimedOutDeviceStateDurationOverridden();
                        if (config.GetMaxTimedOutDeviceStateDuration() ||
                            overridden)
                        {
                            out << config.GetMaxTimedOutDeviceStateDuration();
                        } else {
                            out << Config->GetMaxTimedOutDeviceStateDuration();
                        }
                        if (overridden) {
                            out << " (overridden)";
                        }
                    }
                }

                auto infos = State->GetPartitionStatInfos();
                if (!infos.empty() && infos.front().LastCounters) {
                    const auto& counters = infos.front().LastCounters;
                    TABLER() {
                        TABLED() { out << "HasBrokenDevice"; }
                        TABLED() {
                            out << counters->Simple.HasBrokenDevice.Value;
                        }
                    }
                    TABLER() {
                        TABLED() { out << "HasBrokenDeviceSilent"; }
                        TABLED() {
                            out << counters->Simple.HasBrokenDeviceSilent.Value;
                        }
                    }
                }

                auto findMigration =
                    [&] (const TString& deviceId) -> const NProto::TDeviceConfig* {
                        for (const auto& migration: meta.GetMigrations()) {
                            if (migration.GetSourceDeviceId() == deviceId) {
                                return &migration.GetTargetDevice();
                            }
                        }

                        return nullptr;
                    };

                bool renderLaggingState = IsReliableDiskRegistryMediaKind(
                    State->GetConfig().GetStorageMediaKind());
                auto outputDevices = [&] (const TDevices& devices) {
                    TABLED() {
                        TABLE_CLASS("table table-bordered") {
                            TABLEHEAD() {
                                TABLER() {
                                    TABLEH() { out << "DeviceNo"; }
                                    TABLEH() { out << "Offset"; }
                                    TABLEH() { out << "NodeId"; }
                                    TABLEH() { out << "TransportId"; }
                                    TABLEH() { out << "RdmaEndpoint"; }
                                    TABLEH() { out << "DeviceName"; }
                                    TABLEH() { out << "DeviceUUID"; }
                                    TABLEH() { out << "BlockSize"; }
                                    TABLEH() { out << "Blocks"; }
                                    TABLEH() { out << "BlockRange"; }
                                    if (renderLaggingState) {
                                        TABLEH()
                                        {
                                            out << "LaggingState";
                                        }
                                    }
                                }
                            }

                            ui64 currentOffset = 0;
                            ui64 currentBlockCount = 0;
                            auto outputDevice = [&] (const NProto::TDeviceConfig& d) {
                                TABLED() { out << d.GetNodeId(); }
                                TABLED() { out << d.GetTransportId(); }
                                TABLED() {
                                    const auto& e = d.GetRdmaEndpoint();
                                    out << e.GetHost() << ":" << e.GetPort();
                                }
                                TABLED() { out << d.GetDeviceName(); }
                                TABLED() {
                                    const bool isFresh = Find(
                                        meta.GetFreshDeviceIds().begin(),
                                        meta.GetFreshDeviceIds().end(),
                                        d.GetDeviceUUID()
                                    ) != meta.GetFreshDeviceIds().end();

                                    if (isFresh) {
                                        out << "<font color=blue>";
                                    }
                                    out << d.GetDeviceUUID();
                                    if (isFresh) {
                                        out << "</font>";
                                    }
                                }
                                TABLED() { out << d.GetBlockSize(); }
                                TABLED() { out << d.GetBlocksCount(); }
                                TABLED() {
                                    const auto currentRange =
                                        TBlockRange64::WithLength(
                                            currentBlockCount,
                                            d.GetBlocksCount());
                                    out << DescribeRange(currentRange);
                                }
                                if (renderLaggingState) {
                                    TABLED()
                                    {
                                        RenderLaggingStateForDevice(out, d);
                                    }
                                }
                            };

                            for (int i = 0; i < devices.size(); ++i) {
                                const auto& device = devices[i];

                                TABLER() {
                                    TABLED() { out << i; }
                                    TABLED() {
                                        out << FormatByteSize(currentOffset);
                                    }
                                    outputDevice(device);
                                }

                                auto* m = findMigration(device.GetDeviceUUID());
                                if (m) {
                                    TABLER() {
                                        TABLED() { out << i << " migration"; }
                                        TABLED() {
                                            out << FormatByteSize(currentOffset);
                                        }
                                        outputDevice(*m);
                                    }
                                }

                                currentBlockCount += device.GetBlocksCount();
                                currentOffset +=
                                    device.GetBlocksCount() * device.GetBlockSize();
                            }
                        }
                    }
                };

                TABLER() {
                    TABLED() { out << "Devices"; }
                    outputDevices(config.GetDevices());
                }

                for (ui32 i = 0; i < meta.ReplicasSize(); ++i) {
                    TABLER() {
                        TABLED() {
                            out << "Devices (Replica " << (i + 1) << ")";
                        }
                        outputDevices(meta.GetReplicas(i).GetDevices());
                    }
                }
            }
        }
    }

    SendHttpResponse(ctx, *requestInfo, std::move(out.Str()));
}

void TVolumeActor::HandleHttpInfo_GetRequestsLatency(
    const NActors::TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    Y_UNUSED(params);

    if (!State) {
        return;
    }

    NCloud::Reply(
        ctx,
        *requestInfo,
        std::make_unique<NMon::TEvRemoteJsonInfoRes>(
            RequestTimeTracker.GetStatJson(
                GetCycleCount(),
                State->GetBlockSize())));
}

void TVolumeActor::HandleHttpInfo_GetTransactionsLatency(
    const NActors::TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    Y_UNUSED(params);

    NCloud::Reply(
        ctx,
        *requestInfo,
        std::make_unique<NMon::TEvRemoteJsonInfoRes>(
            TransactionTimeTracker.GetStatJson(GetCycleCount())));
}

void TVolumeActor::HandleHttpInfo_ResetTransactionsLatency(
    const NActors::TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    Y_UNUSED(params);
    TransactionTimeTracker.ResetStats();
    SendHttpResponse(ctx, *requestInfo, "reset successfully");
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::RejectHttpRequest(
    const TActorContext& ctx,
    TRequestInfo& requestInfo,
    TString message)
{
    LOG_ERROR(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s %s",
        LogTitle.GetWithTime().c_str(),
        message.c_str());

    SendHttpResponse(ctx, requestInfo, std::move(message), EAlertLevel::DANGER);
}

void TVolumeActor::SendHttpResponse(
    const TActorContext& ctx,
    TRequestInfo& requestInfo,
    TString message,
    EAlertLevel alertLevel)
{
    TStringStream out;
    BuildTabletNotifyPageWithRedirect(out, message, TabletID(), alertLevel);

    SendHttpResponse(ctx, requestInfo, std::move(out.Str()));
}

void TVolumeActor::SendHttpResponse(
    const TActorContext& ctx,
    TRequestInfo& requestInfo,
    TString message)
{
    LWTRACK(
        ResponseSent_Volume,
        requestInfo.CallContext->LWOrbit,
        "HttpInfo",
        requestInfo.CallContext->RequestId);

    NCloud::Reply(
        ctx,
        requestInfo,
        std::make_unique<NMon::TEvRemoteHttpInfoRes>(std::move(message)));
}

}   // namespace NCloud::NBlockStore::NStorage
