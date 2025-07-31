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
    STFUNC(StateGetYDBBasedCapacity);

    void HandleGetDiskRegistryCapacityResponse(
        const TEvDiskRegistry::TEvGetClusterCapacityResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleGetYDBCapacityResponse(
        const TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev,
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

    Become(&TThis::StateGetYDBBasedCapacity);

    CreateBSControllerPipeClient(ctx);

    auto request =
        std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();

    request->Record.MutableRequest()->AddCommand()->MutableQueryBaseConfig();
    auto& read = *request->Record.MutableRequest()
                      ->AddCommand()
                      ->MutableReadStoragePool();
    read.SetBoxId(UINT64_MAX);

    NTabletPipe::SendData(ctx, BSControllerPipeClient, request.release());
}

////////////////////////////////////////////////////////////////////////////////

void TGetClusterCapacityActor::HandleGetYDBCapacityResponse(
    const TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& record = ev->Get()->Record.GetResponse();

    LOG_DEBUG_S(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Got BSController capacity response " + record.ShortDebugString());

    if (!record.GetSuccess() || !record.StatusSize() ||
        !record.GetStatus(0).GetSuccess() || !record.GetStatus(1).GetSuccess())
    {
        LOG_ERROR_S(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Getting BSController capacity failed");
        HandleEmptyClusterCapacity(ctx, "BSController");
    }

    std::map<std::pair<ui64, ui64>, NKikimrBlobStorage::TDefineStoragePool>
        pools;
    for (const auto& pool: record.GetStatus(1).GetStoragePool()) {
        pools[{pool.GetBoxId(), pool.GetStoragePoolId()}] = pool;
    }

    std::map<ui64, TString> poolNameByGroupID;
    for (const auto& group: record.GetStatus(0).GetBaseConfig().GetGroup()) {
        LOG_DEBUG_S(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Got group " << group.ShortDebugString());

        poolNameByGroupID[group.GetGroupId()] =
            pools[{group.GetBoxId(), group.GetStoragePoolId()}].GetName();
    }

    const TString hddPoolName = "NBS:rot";
    const TString ssdPoolName = "NBS:ssd";

    ui64 totalBytesSSD = 0;
    ui64 freeBytesSSD = 0;
    ui64 totalBytesHDD = 0;
    ui64 freeBytesHDD = 0;

    for (const auto& vslot: record.GetStatus(0).GetBaseConfig().GetVSlot()) {
        LOG_DEBUG_S(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Got vslot " << vslot.ShortDebugString());

        if (poolNameByGroupID[vslot.GetGroupId()].find(ssdPoolName) !=
            TString::npos)
        {
            totalBytesSSD += vslot.GetVDiskMetrics().GetAllocatedSize() +
                             vslot.GetVDiskMetrics().GetAvailableSize();
            freeBytesSSD += vslot.GetVDiskMetrics().GetAvailableSize();
        } else if (
            poolNameByGroupID[vslot.GetGroupId()].find(hddPoolName) !=
            TString::npos)
        {
            totalBytesHDD += vslot.GetVDiskMetrics().GetAllocatedSize() +
                             vslot.GetVDiskMetrics().GetAvailableSize();
            freeBytesHDD += vslot.GetVDiskMetrics().GetAvailableSize();
        }
    }

    auto& ssd_capacity = Capacities.emplace_back();
    ssd_capacity.SetStorageMediaKind(
        NProto::EStorageMediaKind::STORAGE_MEDIA_SSD);
    ssd_capacity.SetFreeBytes(freeBytesSSD);
    ssd_capacity.SetTotalBytes(totalBytesSSD);

    auto& hdd_capacity = Capacities.emplace_back();
    hdd_capacity.SetStorageMediaKind(
        NProto::EStorageMediaKind::STORAGE_MEDIA_HDD);
    hdd_capacity.SetFreeBytes(freeBytesHDD);
    hdd_capacity.SetTotalBytes(totalBytesHDD);

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

STFUNC(TGetClusterCapacityActor::StateGetYDBBasedCapacity)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvBlobStorage::TEvControllerConfigResponse,
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
