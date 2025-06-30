#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/private/api/protos/disk.pb.h>

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
    info.SetFree(capacityInfo.GetFreeBytes());
    info.SetTotal(capacityInfo.GetTotalBytes());
    info.SetKind(capacityInfo.GetStorageMediaKind());

    return info;
}

////////////////////////////////////////////////////////////////////////////////

class TGetCapacityActor final: public TActorBootstrapped<TGetCapacityActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    TVector<NPrivateProto::TClusterCapacityInfo> Capacities;

public:
    explicit TGetCapacityActor(TRequestInfoPtr requestInfo);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvExecuteActionResponse> response);

    void HandleSuccess(const TActorContext& ctx, const TString& output);
    void HandleEmptyClusterCapacity(const TActorContext& ctx, const TString& component);

private:
    STFUNC(StateGetDiskRegistryBasedCapacity);
    STFUNC(StateGetYDBBasedCapacity);

    void HandleGetDiskRegistyCapacity(
        const TEvDiskRegistry::TEvGetClusterCapacityResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleGetYDBCapacity(
        const NSysView::TEvSysView::TEvGetStorageStatsResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TGetCapacityActor::TGetCapacityActor(TRequestInfoPtr requestInfo)
    : RequestInfo(std::move(requestInfo))
{}

void TGetCapacityActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateGetDiskRegistryBasedCapacity);

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Sending get nameservice config request");

    NCloud::Send(
        ctx,
        MakeDiskRegistryProxyServiceId(),
        std::make_unique<TEvDiskRegistry::TEvGetClusterCapacityRequest>());
}

void TGetCapacityActor::ReplyAndDie(
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

void TGetCapacityActor::HandleSuccess(
    const TActorContext& ctx,
    const TString& output)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();
    response->Record.SetOutput(output);
    ReplyAndDie(ctx, std::move(response));
}

void TGetCapacityActor::HandleEmptyClusterCapacity(
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

void TGetCapacityActor::HandleGetDiskRegistyCapacity(
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
    NCloud::Send(
        ctx,
        MakeBlobStorageProxyID(0),
        std::make_unique<NSysView::TEvSysView::TEvGetStorageStatsRequest>());
}

////////////////////////////////////////////////////////////////////////////////

void TGetCapacityActor::HandleGetYDBCapacity(
    const NSysView::TEvSysView::TEvGetStorageStatsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (FAILED(msg->GetStatus())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    if (msg->Record.GetEntries().empty()) {
        HandleEmptyClusterCapacity(ctx, "BSController");
        return;
    }

    ui64 totalBytesSSD = 0;
    ui64 freeBytesSSD = 0;
    ui64 totalBytesHDD = 0;
    ui64 freeBytesHDD = 0;

    for (auto& entry: msg->Record.GetEntries()) {
        if (!entry.HasPDiskFilter()) {
            continue;
        }

        if (entry.GetPDiskFilter().find("ssd") != TString::npos) {
            freeBytesSSD += entry.GetCurrentAvailableSize();
            totalBytesSSD += entry.GetCurrentAllocatedSize();
        } else if (entry.GetPDiskFilter().find("hdd") != TString::npos) {
            freeBytesHDD += entry.GetCurrentAvailableSize();
            totalBytesHDD += entry.GetCurrentAllocatedSize();
        } else {
            LOG_WARN_S(
                ctx,
                TBlockStoreComponents::SERVICE,
                "Unknown PDiskFilter for YDB group entry");
        }
    }

    auto& ssd_capacity = Capacities.emplace_back();
    ssd_capacity.SetStorageMediaKind(NProto::EStorageMediaKind::STORAGE_MEDIA_SSD);
    ssd_capacity.SetFreeBytes(freeBytesSSD);
    ssd_capacity.SetTotalBytes(totalBytesSSD);

    auto& hdd_capacity = Capacities.emplace_back();
    hdd_capacity.SetStorageMediaKind(NProto::EStorageMediaKind::STORAGE_MEDIA_HDD);
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

STFUNC(TGetCapacityActor::StateGetDiskRegistryBasedCapacity)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistry::TEvGetClusterCapacityResponse,
            HandleGetDiskRegistyCapacity);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TGetCapacityActor::StateGetYDBBasedCapacity)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            NSysView::TEvSysView::TEvGetStorageStatsResponse,
            HandleGetYDBCapacity);

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

TResultOrError<IActorPtr> TServiceActor::CreateGetCapacityActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    Y_UNUSED(input);
    return {std::make_unique<TGetCapacityActor>(std::move(requestInfo))};
}

}   // namespace NCloud::NBlockStore::NStorage
