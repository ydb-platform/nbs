#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/volume_model.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/private/api/protos/disk.pb.h>

#include <contrib/ydb/core/blobstorage/base/blobstorage_events.h>
#include <contrib/ydb/core/sys_view/common/events.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <library/cpp/json/json_writer.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

NPrivateProto::TClusterCapacityInfo ToResponse(
    const NProto::TClusterCapacityInfo& capacityInfo)
{
    NPrivateProto::TClusterCapacityInfo info;
    info.SetFreeBytes(capacityInfo.GetFreeBytes());
    info.SetTotalBytes(capacityInfo.GetTotalBytes());
    info.SetStorageMediaKind(capacityInfo.GetStorageMediaKind());

    return info;
}

////////////////////////////////////////////////////////////////////////////////

class TGetClusterCapacityActor final
    : public TActorBootstrapped<TGetClusterCapacityActor>
{
private:
    struct TStoragePoolStats
    {
        ui64 TotalBytes = 0;
        ui64 FreeBytes = 0;
    };

    struct TPoolId
    {
        ui64 BoxId = 0;
        ui64 StoragePoolId = 0;

        bool operator == (const TPoolId& other) const = default;
    };

    struct TPoolIdIdHash
    {
        ui64 operator ()(const TPoolId& poolId) const
        {
            return MultiHash(poolId.BoxId, poolId.StoragePoolId);
        }
    };

    THashSet<TString> UsedPools;
    THashMap<NProto::EStorageMediaKind, TStoragePoolStats> Stats;
    THashMap<TPoolId, NProto::EStorageMediaKind, TPoolIdIdHash> StorageIdToKind;
    THashMap<ui32, NProto::EStorageMediaKind> GroupIdToKind;

    const TRequestInfoPtr RequestInfo;
    TVector<NPrivateProto::TClusterCapacityInfo> Capacities;

    const TStorageConfigPtr Config;
    TActorId BSControllerPipeClient;

public:
    explicit TGetClusterCapacityActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config);

    void Bootstrap(const TActorContext& ctx);

private:
    void CreateBSControllerPipeClient(const TActorContext& ctx);
    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvExecuteActionResponse> response);

    void HandleSuccess(const TActorContext& ctx, const TString& output);
    void HandleEmptyClusterCapacity(
        const TActorContext& ctx,
        const TString& component);

private:
    STFUNC(StateGetDiskRegistryBasedCapacity);
    STFUNC(StateGetYDBStoragePools);
    STFUNC(StateGetYDBGroups);
    STFUNC(StateGetYDBVSlots);

    void HandleConnect(
        TEvTabletPipe::TEvClientConnected::TPtr& ev,
        const TActorContext& ctx);

    void HandleDisconnect(
        TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
        const TActorContext& ctx);

    void HandleGetStoragePoolsResponse(
        const NSysView::TEvSysView::TEvGetStoragePoolsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleGetGroupsResponse(
        const NSysView::TEvSysView::TEvGetGroupsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleGetVSlotsResponse(
        const NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleGetDiskRegistryCapacityResponse(
        const TEvDiskRegistry::TEvGetClusterCapacityResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TGetClusterCapacityActor::TGetClusterCapacityActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config)
    : RequestInfo(std::move(requestInfo))
    , Config(config)
{
    constexpr NProto::EStorageMediaKind kinds[] {
        NProto::STORAGE_MEDIA_HDD,
        NProto::STORAGE_MEDIA_SSD,
        NProto::STORAGE_MEDIA_HYBRID
    };

    auto prefix = Config->GetSchemeShardDir() + ':';

    for (auto kind: kinds) {
        const auto pools = GetPoolKinds(*Config, kind, "", "", "");
        UsedPools.emplace(prefix + pools.Fresh);
        UsedPools.emplace(prefix + pools.Index);
        UsedPools.emplace(prefix + pools.Log);
        UsedPools.emplace(prefix + pools.Merged);
        UsedPools.emplace(prefix + pools.Mixed);
        UsedPools.emplace(prefix + pools.System);
    }
}

void TGetClusterCapacityActor::Bootstrap(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvDiskRegistry::TEvGetClusterCapacityRequest>();

    Become(&TThis::StateGetDiskRegistryBasedCapacity);

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Sending get cluster capacity request");

    NCloud::Send(ctx, MakeDiskRegistryProxyServiceId(), std::move(request));
}

void TGetClusterCapacityActor::CreateBSControllerPipeClient(
    const TActorContext& ctx)
{
    NTabletPipe::TClientConfig clientConfig;
    clientConfig.RetryPolicy = {
        .RetryLimitCount = Config->GetPipeClientRetryCount(),
        .MinRetryTime = Config->GetPipeClientMinRetryTime(),
        .MaxRetryTime = Config->GetPipeClientMaxRetryTime()};

    BSControllerPipeClient = ctx.Register(NTabletPipe::CreateClient(
        ctx.SelfID,
        MakeBSControllerID(0),
        clientConfig));

    LOG_INFO(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Tablet client: %lu (remote: %s)",
        MakeBSControllerID(0),
        ToString(BSControllerPipeClient).data());
}

void TGetClusterCapacityActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvExecuteActionResponse> response)
{
    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_GetClusterCapacity",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TGetClusterCapacityActor::HandleSuccess(
    const TActorContext& ctx,
    const TString& output)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();
    response->Record.SetOutput(output);
    ReplyAndDie(ctx, std::move(response));
}

void TGetClusterCapacityActor::HandleEmptyClusterCapacity(
    const TActorContext& ctx,
    const TString& component)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(
        MakeError(
            E_REJECTED,
            TStringBuilder() <<
                "Got empty capacity response from " <<
                component));
    ReplyAndDie(ctx, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TGetClusterCapacityActor::HandleGetDiskRegistryCapacityResponse(
    const TEvDiskRegistry::TEvGetClusterCapacityResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    const auto& error = msg->GetError();

    if (HasError(error)) {
        LOG_ERROR_S(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Getting DiskRegistry capacity failed: " << FormatError(error));
    }

    if (msg->Record.GetCapacity().empty()) {
        HandleEmptyClusterCapacity(ctx, "DiskRegistry");
        return;
    }

    for (const NProto::TClusterCapacityInfo& capacityInfo:
         msg->Record.GetCapacity())
    {
        NPrivateProto::TClusterCapacityInfo capacity = ToResponse(capacityInfo);
        Capacities.push_back(std::move(capacity));
    }

    Become(&TThis::StateGetYDBStoragePools);

    CreateBSControllerPipeClient(ctx);

    auto request =
        std::make_unique<NSysView::TEvSysView::TEvGetStoragePoolsRequest>();

    NTabletPipe::SendData(ctx, BSControllerPipeClient, request.release());
}

////////////////////////////////////////////////////////////////////////////////

void TGetClusterCapacityActor::HandleGetStoragePoolsResponse(
    const NSysView::TEvSysView::TEvGetStoragePoolsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& record = ev->Get()->Record;

    LOG_DEBUG_S(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Got BSController storage pools response " << record.ShortDebugString());

    for (const auto& storagePool: record.GetEntries()) {
        const auto& poolKey = storagePool.GetKey();
        const auto& poolInfo = storagePool.GetInfo();

        LOG_INFO_S(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Observed pool " <<
            poolInfo.GetName() <<
            " and PDisks " <<
            poolInfo.GetPDiskFilter());

        if (!UsedPools.contains(poolInfo.GetName())) {
            continue;
        }

        const auto YdbPoolToStorageMediaKind = Config->GetPoolKindToMediaKindMapping();
        auto it = YdbPoolToStorageMediaKind.find(poolInfo.GetKind());
        if (it != YdbPoolToStorageMediaKind.end()) {
            const auto key = TPoolId{
                poolKey.GetBoxId(),
                poolKey.GetStoragePoolId()};
            StorageIdToKind[key] = it->second;
        }
    }

    Become(&TThis::StateGetYDBGroups);

    auto request =
        std::make_unique<NSysView::TEvSysView::TEvGetGroupsRequest>();

    NTabletPipe::SendData(ctx, BSControllerPipeClient, request.release());
}

void TGetClusterCapacityActor::HandleGetGroupsResponse(
    const NSysView::TEvSysView::TEvGetGroupsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& record = ev->Get()->Record;

    LOG_DEBUG_S(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Got BSController groups response " << record.ShortDebugString());

    for (const auto& groupInfo: record.GetEntries()) {
        const auto& group = groupInfo.GetInfo();
        const auto key = TPoolId{
            group.GetBoxId(),
            group.GetStoragePoolId()};
        auto it = StorageIdToKind.find(key);
        if (it == StorageIdToKind.end()) {
            continue;
        }

        GroupIdToKind[groupInfo.GetKey().GetGroupId()] = it->second;
    }

    Become(&TThis::StateGetYDBVSlots);

    auto request =
        std::make_unique<NSysView::TEvSysView::TEvGetVSlotsRequest>();

    NTabletPipe::SendData(ctx, BSControllerPipeClient, request.release());
}

void TGetClusterCapacityActor::HandleGetVSlotsResponse(
    const NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& record = ev->Get()->Record;

    LOG_DEBUG_S(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Got BSController vslot response " + record.ShortDebugString());

    for (const auto& slotInfo: record.GetEntries()) {
        const auto& slot = slotInfo.GetInfo();
        auto it = GroupIdToKind.find(slot.GetGroupId());
        if (it == GroupIdToKind.end()) {
            continue;
        }

        Stats[it->second].TotalBytes +=
            slot.GetAllocatedSize() + slot.GetAvailableSize();

        Stats[it->second].FreeBytes += slot.GetAvailableSize();
    }

    for (const auto& [kind, stat]: Stats) {
        auto& capacity = Capacities.emplace_back();
        capacity.SetStorageMediaKind(kind);
        capacity.SetFreeBytes(stat.FreeBytes);
        capacity.SetTotalBytes(stat.TotalBytes);

        LOG_INFO_S(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Pool " <<
            NProto::EStorageMediaKind_Name(kind) <<
            " has " <<
            capacity.GetFreeBytes() <<
            " free bytes and " <<
            capacity.GetTotalBytes() <<
            " bytes totaly");
    }

    NPrivateProto::TGetClusterCapacityResponse result;
    for (auto& capacity: Capacities) {
        *result.AddCapacity() = std::move(capacity);
    }

    TString output;
    google::protobuf::util::MessageToJsonString(result, &output);
    HandleSuccess(ctx, std::move(output));
}

void TGetClusterCapacityActor::HandleConnect(
    TEvTabletPipe::TEvClientConnected::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (msg->Status != NKikimrProto::OK) {
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
            "Failed to connect to BSC %lu: %s",
            msg->TabletId,
            NKikimrProto::EReplyStatus_Name(msg->Status).data());

        auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(
            MakeError(E_REJECTED, "Pipe to BSC is broken"));
        ReplyAndDie(ctx, std::move(response));

        return;
    }

    LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
        "Pipe connected to BSC %lu",
        msg->TabletId);
}

void TGetClusterCapacityActor::HandleDisconnect(
    TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
    const TActorContext& ctx)
{
    if (ev->Sender != BSControllerPipeClient) {
        return;
    }

    LOG_ERROR(ctx, TBlockStoreComponents::SERVICE, "Connection to BSC is broken");

    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(
        MakeError(E_REJECTED, "Pipe to BSC is broken"));
    ReplyAndDie(ctx, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TGetClusterCapacityActor::StateGetDiskRegistryBasedCapacity)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistry::TEvGetClusterCapacityResponse,
            HandleGetDiskRegistryCapacityResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TGetClusterCapacityActor::StateGetYDBStoragePools)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            NSysView::TEvSysView::TEvGetStoragePoolsResponse,
            HandleGetStoragePoolsResponse);

        HFunc(TEvTabletPipe::TEvClientConnected, HandleConnect);
        HFunc(TEvTabletPipe::TEvClientDestroyed, HandleDisconnect);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TGetClusterCapacityActor::StateGetYDBGroups)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            NSysView::TEvSysView::TEvGetGroupsResponse,
            HandleGetGroupsResponse);

        HFunc(TEvTabletPipe::TEvClientConnected, HandleConnect);
        HFunc(TEvTabletPipe::TEvClientDestroyed, HandleDisconnect);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TGetClusterCapacityActor::StateGetYDBVSlots)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            NSysView::TEvSysView::TEvGetVSlotsResponse,
            HandleGetVSlotsResponse);

        HFunc(TEvTabletPipe::TEvClientConnected, HandleConnect);
        HFunc(TEvTabletPipe::TEvClientDestroyed, HandleDisconnect);

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

TResultOrError<IActorPtr> TServiceActor::CreateGetClusterCapacityActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    Y_UNUSED(input);
    return {std::make_unique<TGetClusterCapacityActor>(
        std::move(requestInfo),
        Config)};
}

}   // namespace NCloud::NBlockStore::NStorage
