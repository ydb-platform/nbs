#include "volume_actor.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/disk_counters.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/config.h>

#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NPartition;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

void FillCheckpoints(
    TVector<TString> checkpoints,
    NProto::TStatVolumeResponse& response)
{
    for (auto& cp: checkpoints) {
        *response.MutableCheckpoints()->Add() = std::move(cp);
    }
}

////////////////////////////////////////////////////////////////////////////////

#define MERGE_FIELD(name)                                                      \
    target.Set##name(target.Get##name() + source.Get##name());                 \
// MERGE_FIELD

#define MERGE_FIELD_MAX(name)                                                  \
    target.Set##name(Max(target.Get##name(), source.Get##name()));             \
// MERGE_FIELD_MAX

void MergeIOCounters(const NProto::TIOCounters& source, NProto::TIOCounters& target)
{
    MERGE_FIELD(RequestsCount);
    MERGE_FIELD(BlocksCount);
    MERGE_FIELD(ExecTime);
    MERGE_FIELD(WaitTime);
    MERGE_FIELD(BatchCount);
}

void Merge(const NProto::TVolumeStats& source, NProto::TVolumeStats& target)
{
    MergeIOCounters(
        source.GetUserReadCounters(),
        *target.MutableUserReadCounters()
    );
    MergeIOCounters(
        source.GetUserWriteCounters(),
        *target.MutableUserWriteCounters()
    );
    MergeIOCounters(
        source.GetSysReadCounters(),
        *target.MutableSysReadCounters()
    );
    MergeIOCounters(
        source.GetSysWriteCounters(),
        *target.MutableSysWriteCounters()
    );
    MergeIOCounters(
        source.GetRealSysReadCounters(),
        *target.MutableRealSysReadCounters()
    );
    MergeIOCounters(
        source.GetRealSysWriteCounters(),
        *target.MutableRealSysWriteCounters()
    );

    MERGE_FIELD(MixedBlobsCount);
    MERGE_FIELD(MergedBlobsCount);

    MERGE_FIELD(FreshBlocksCount);
    MERGE_FIELD(MixedBlocksCount);
    MERGE_FIELD(MergedBlocksCount);
    MERGE_FIELD(UsedBlocksCount);
    MERGE_FIELD(LogicalUsedBlocksCount);
    MERGE_FIELD(GarbageBlocksCount);

    MERGE_FIELD(NonEmptyRangeCount);
    MERGE_FIELD(CheckpointBlocksCount);

    MERGE_FIELD_MAX(CompactionGarbageScore);

    MERGE_FIELD(GarbageQueueSize);

    MERGE_FIELD_MAX(CleanupDelay);
    MERGE_FIELD_MAX(CompactionDelay);

    MERGE_FIELD(CleanupQueueBytes);

    MERGE_FIELD(UnconfirmedBlobCount);
    MERGE_FIELD(ConfirmedBlobCount);
}

////////////////////////////////////////////////////////////////////////////////

class TStatPartitionActor final
    : public TActorBootstrapped<TStatPartitionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TVector<TActorId> PartActorIds;
    NProto::TStatVolumeResponse Record;
    TVector<TString> Checkpoints;
    ui32 Responses = 0;

public:
    TStatPartitionActor(
        TRequestInfoPtr requestInfo,
        TVector<TActorId> partActorIds,
        NProto::TStatVolumeResponse record,
        TVector<TString> checkpoints);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleStatPartitionResponse(
        const TEvPartition::TEvStatPartitionResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TStatPartitionActor::TStatPartitionActor(
        TRequestInfoPtr requestInfo,
        TVector<TActorId> partActorIds,
        NProto::TStatVolumeResponse record,
        TVector<TString> checkpoints)
    : RequestInfo(std::move(requestInfo))
    , PartActorIds(std::move(partActorIds))
    , Record(std::move(record))
    , Checkpoints(std::move(checkpoints))
{}

void TStatPartitionActor::Bootstrap(const TActorContext& ctx)
{
    for (const auto& partActorId: PartActorIds) {
        // TODO: handle undelivery
        NCloud::Send(
            ctx,
            partActorId,
            std::make_unique<TEvPartition::TEvStatPartitionRequest>()
        );
    }

    Become(&TThis::StateWork);
}

////////////////////////////////////////////////////////////////////////////////

void TStatPartitionActor::HandleStatPartitionResponse(
    const TEvPartition::TEvStatPartitionResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (FAILED(msg->GetStatus())) {
        Record.MutableError()->CopyFrom(msg->GetError());
    } else {
        auto& target = *Record.MutableStats();
        const auto& source = msg->Record.GetStats();
        Merge(source, target);
    }

    if (++Responses == PartActorIds.size() || FAILED(msg->GetStatus())) {
        auto response = std::make_unique<TEvService::TEvStatVolumeResponse>(
            Record
        );
        FillCheckpoints(std::move(Checkpoints), response->Record);

        LWTRACK(
            ResponseSent_Volume,
            RequestInfo->CallContext->LWOrbit,
            "StatVolume",
            RequestInfo->CallContext->RequestId);

        NCloud::Reply(ctx, *RequestInfo, std::move(response));

        Die(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TStatPartitionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvPartition::TEvStatPartitionResponse, HandleStatPartitionResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::VOLUME,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleStatVolume(
    const TEvService::TEvStatVolumeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_VOLUME_COUNTER(StatVolume);

    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo<TEvService::TStatVolumeMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    LWTRACK(
        RequestReceived_Volume,
        requestInfo->CallContext->LWOrbit,
        "StatVolume",
        requestInfo->CallContext->RequestId);

    const bool noPartition = msg->Record.GetNoPartition();

    if (!noPartition && State->GetPartitionsState() != TPartitionInfo::READY) {
        StartPartitionsIfNeeded(ctx);

        if (!State->Ready()) {
            LOG_DEBUG(
                ctx,
                TBlockStoreComponents::VOLUME,
                "%s StatVolume request delayed until volume and partitions are "
                "ready",
                LogTitle.GetWithTime().c_str());

            PendingRequests.emplace_back(NActors::IEventHandlePtr(ev.Release()), requestInfo);
            return;
        }
    }

    NProto::TStatVolumeResponse record;
    auto& protoFieldsToValues = *record.MutableStorageConfigFieldsToValues();

    for (const auto& field: msg->Record.GetStorageConfigFields()) {
        const auto configValue = Config->GetValueByName(field);
        switch (configValue.Status) {
            using TStatus = TStorageConfig::TValueByName::ENameStatus;
            case TStatus::FoundInDefaults:
                protoFieldsToValues[field] = "Default";
                break;
            case TStatus::FoundInProto:
                protoFieldsToValues[field] = configValue.Value;
                break;
            case TStatus::NotFound:
                protoFieldsToValues[field] = "Not found";
                break;
        }
    }

    auto* volume = record.MutableVolume();
    record.SetMountSeqNumber(
        State->GetMountSeqNumber());
    VolumeConfigToVolume(State->GetMeta().GetVolumeConfig(), *volume);

    auto* stats = record.MutableStats();
    stats->SetWriteAndZeroRequestsInFlight(WriteAndZeroRequestsInFlight.Size());
    stats->SetBoostBudget(
        State->GetThrottlingPolicy().GetCurrentBoostBudget().MilliSeconds());
    stats->SetVolumeUsedBlocksCount(State->GetUsedBlockCount());

    auto* clients = record.MutableClients();
    for (const auto& x: State->GetClients()) {
        auto* client = clients->Add();
        client->SetClientId(x.second.GetVolumeClientInfo().GetClientId());
        client->SetInstanceId(x.second.GetVolumeClientInfo().GetInstanceId());
        client->SetDisconnectTimestamp(
            x.second.GetVolumeClientInfo().GetDisconnectTimestamp());
    }
    SortBy(clients->begin(), clients->end(), [] (const auto& x) {
        return x.GetClientId();
    });

    record.SetTabletHost(FQDNHostName());

    record.SetVolumeGeneration(Executor()->Generation());

    if (const auto& partConfig = State->GetNonreplicatedPartitionConfig()) {
        stats->SetMaxTimedOutDeviceStateDuration(
            partConfig->GetMaxTimedOutDeviceStateDuration().MilliSeconds());
    }

    auto* throttlingInfo = record.MutableVolatileThrottlingInfo();
    throttlingInfo->SetVersion(State->GetThrottlingPolicy().GetVolatileThrottlingVersion());
    *throttlingInfo->MutableActualPerformanceProfile() = State->GetThrottlingPolicy().GetCurrentPerformanceProfile();
    *throttlingInfo->MutableAppliedRule() = State->GetThrottlingPolicy().GetVolatileThrottlingRule();

    TActiveCheckpointsMap activeCheckpoints = State->GetCheckpointStore().GetActiveCheckpoints();
    TVector<TString> checkpoints(Reserve(activeCheckpoints.size()));
    for (const auto& [checkpoint, _] : activeCheckpoints) {
        checkpoints.push_back(checkpoint);
    }

    TStringBuilder debugString;
    for (const auto& statInfo: State->GetPartitionStatInfos()) {
        if (debugString) {
            debugString << "\t";
        }
        debugString << statInfo.CachedCountersProto.DebugString();
    }
    record.SetDebugString(std::move(debugString));

    if (!noPartition && State->GetPartitions()) {
        TVector<TActorId> partActorIds;
        for (const auto& partition: State->GetPartitions()) {
            partActorIds.push_back(partition.GetTopActorId());
        }

        NCloud::Register<TStatPartitionActor>(
            ctx,
            std::move(requestInfo),
            std::move(partActorIds),
            std::move(record),
            std::move(checkpoints));
    } else {
        auto response = std::make_unique<TEvService::TEvStatVolumeResponse>();
        response->Record.CopyFrom(record);
        auto* volume = response->Record.MutableVolume();
        State->FillDeviceInfo(*volume);
        volume->SetResyncInProgress(State->IsMirrorResyncNeeded());
        FillCheckpoints(std::move(checkpoints), response->Record);

        LWTRACK(
            ResponseSent_Volume,
            requestInfo->CallContext->LWOrbit,
            "StatVolume",
            requestInfo->CallContext->RequestId);

        NCloud::Reply(ctx, *requestInfo, std::move(response));
    }
}

void TVolumeActor::HandleGetVolumeInfo(
    const TEvVolume::TEvGetVolumeInfoRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_VOLUME_COUNTER(GetVolumeInfo);

    auto response = std::make_unique<TEvVolume::TEvGetVolumeInfoResponse>();

    auto* volume = response->Record.MutableVolume();
    State->FillDeviceInfo(*volume);
    VolumeConfigToVolume(State->GetMeta().GetVolumeConfig(), *volume);
    volume->SetResyncInProgress(State->IsMirrorResyncNeeded());

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        ev->Get()->CallContext);

    NCloud::Reply(ctx, *requestInfo, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
