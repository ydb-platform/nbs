#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
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
        NProto::EStorageMediaKind MediaKind;
        ui64 TotalBytes = 0;
        ui64 FreeBytes = 0;
    };

    TVector<std::pair<TString, TStoragePoolStats>> NbsYdbStoragePools
    {
        {"NBS:rot", {.MediaKind = NProto::STORAGE_MEDIA_HDD}},
        {"NBS:ssd", {.MediaKind = NProto::STORAGE_MEDIA_SSD}},
    };

    const TRequestInfoPtr RequestInfo;
    TVector<NPrivateProto::TClusterCapacityInfo> Capacities;

    const TStorageConfigPtr Config;
    TActorId BSControllerPipeClient;

    THashMap<std::pair<ui64, ui64>, std::size_t> StorageIdToPool;

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
    STFUNC(StateGetYDBBasedCapacity);
    STFUNC(StateGetYDBBasedStoragePools);

    void HandleGetStoragePoolsResponse(
        const NSysView::TEvSysView::TEvGetStoragePoolsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleGetDiskRegistryCapacityResponse(
        const TEvDiskRegistry::TEvGetClusterCapacityResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleGetYDBCapacityResponse(
        const NSysView::TEvSysView::TEvGetGroupsResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TGetClusterCapacityActor::TGetClusterCapacityActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config)
    : RequestInfo(std::move(requestInfo))
    , Config(config)
{}

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
        TBlockStoreComponents::DISK_REGISTRY_PROXY,
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
    NProto::TError error;
    error.SetMessage("Got empty capacity response from " + component);
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(
        std::move(error));
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

    Become(&TThis::StateGetYDBBasedStoragePools);

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
        "Got BSController storage pools response " + record.ShortDebugString());

    for (const auto& storagePool: record.GetEntries()) {
        const auto& poolKey = storagePool.GetKey();
        const auto& poolInfo = storagePool.GetInfo();
        auto it = FindIf(NbsYdbStoragePools, [&] (const auto& pool) {
            return pool.first == poolInfo.GetName();
        });
        if (it != NbsYdbStoragePools.end()) {
            StorageIdToPool[std::make_pair(poolKey.GetBoxId(), poolKey.GetStoragePoolId())] =
                std::distance(NbsYdbStoragePools.begin(), it);
        }

        LOG_INFO_S(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Observed BSController storage pool " + poolInfo.GetName());
    }

    Become(&TThis::StateGetYDBBasedCapacity);

    auto request =
        std::make_unique<NSysView::TEvSysView::TEvGetGroupsRequest>();

    NTabletPipe::SendData(ctx, BSControllerPipeClient, request.release());
}

void TGetClusterCapacityActor::HandleGetYDBCapacityResponse(
    const NSysView::TEvSysView::TEvGetGroupsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& record = ev->Get()->Record;

    LOG_DEBUG_S(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Got BSController groups response " + record.ShortDebugString());

    for (const auto& groupInfo: record.GetEntries()) {
        const auto& group = groupInfo.GetInfo();
        auto it = StorageIdToPool.find(std::make_pair(group.GetBoxId(), group.GetStoragePoolId()));
        if (it == StorageIdToPool.end()) {
            continue;
        }

        auto& info = NbsYdbStoragePools[it->second].second;
        info.TotalBytes += group.GetAllocatedSize() + group.GetAvailableSize();
        info.FreeBytes += group.GetAvailableSize();
    }

    for (const auto& pool: NbsYdbStoragePools) {
        auto& capacity = Capacities.emplace_back();
        capacity.SetStorageMediaKind(pool.second.MediaKind);
        capacity.SetFreeBytes(pool.second.FreeBytes);
        capacity.SetTotalBytes(pool.second.TotalBytes);
    }

    NPrivateProto::TGetClusterCapacityResponse result;
    for (auto& capacity: Capacities) {
        *result.AddCapacity() = std::move(capacity);
    }

    TString output;
    google::protobuf::util::MessageToJsonString(result, &output);
    HandleSuccess(ctx, std::move(output));
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

STFUNC(TGetClusterCapacityActor::StateGetYDBBasedStoragePools)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            NSysView::TEvSysView::TEvGetStoragePoolsResponse,
            HandleGetStoragePoolsResponse);

        IgnoreFunc(TEvTabletPipe::TEvClientConnected);
        IgnoreFunc(TEvTabletPipe::TEvClientDestroyed);
        IgnoreFunc(TEvTabletPipe::TEvServerConnected);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TGetClusterCapacityActor::StateGetYDBBasedCapacity)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            NSysView::TEvSysView::TEvGetGroupsResponse,
            HandleGetYDBCapacityResponse);

        IgnoreFunc(NKikimr::TEvTabletPipe::TEvClientConnected);
        IgnoreFunc(NKikimr::TEvTabletPipe::TEvClientDestroyed);
        IgnoreFunc(NKikimr::TEvTabletPipe::TEvServerConnected);

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
