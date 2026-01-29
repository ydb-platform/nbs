#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/disk_agent/disk_agent_private.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <library/cpp/json/json_reader.h>

#include <util/string/printf.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]] TResultOrError<TString> ParseSerialNumber(const TString& jsonStr)
{
    NJson::TJsonValue value;

    if (!NJson::ReadJsonTree(jsonStr, &value)) {
        return MakeError(E_ARGUMENT, "Failed to parse input");
    }

    if (!value.Has("SerialNumber")) {
        return MakeError(E_ARGUMENT, "SerialNumber should be defined");
    }

    TString sn = value["SerialNumber"].GetString();

    if (!sn) {
        return MakeError(E_ARGUMENT, "Empty serial number");
    }

    return sn;
}

////////////////////////////////////////////////////////////////////////////////

class TLocalNVMeDeviceOpsActor
    : public TActorBootstrapped<TLocalNVMeDeviceOpsActor>
{
protected:
    const TRequestInfoPtr RequestInfo;
    TString Input;

public:
    TLocalNVMeDeviceOpsActor(TRequestInfoPtr requestInfo, TString input);

    void Bootstrap(const TActorContext& ctx)
    {
        Become(&TThis::StateWork);

        DoBootstrap(ctx);
    }

protected:
    virtual void DoBootstrap(const TActorContext& ctx) = 0;

private:
    STFUNC(StateWork);

    void HandleListNVMeDevicesResponse(
        const TEvDiskAgentPrivate::TEvListNVMeDevicesResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleAcquireNVMeDeviceResponse(
        const TEvDiskAgentPrivate::TEvAcquireNVMeDeviceResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleReleaseNVMeDeviceResponse(
        const TEvDiskAgentPrivate::TEvReleaseNVMeDeviceResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleListNVMeDevicesUndelivery(
        const TEvDiskAgentPrivate::TEvListNVMeDevicesRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleAcquireNVMeDeviceUndelivery(
        const TEvDiskAgentPrivate::TEvAcquireNVMeDeviceRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleReleaseNVMeDeviceUndelivery(
        const TEvDiskAgentPrivate::TEvReleaseNVMeDeviceRequest::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TLocalNVMeDeviceOpsActor::TLocalNVMeDeviceOpsActor(
    TRequestInfoPtr requestInfo,
    TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TLocalNVMeDeviceOpsActor::HandleListNVMeDevicesResponse(
    const TEvDiskAgentPrivate::TEvListNVMeDevicesResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto response =
        std::make_unique<TEvService::TEvExecuteActionResponse>(msg->GetError());

    if (!HasError(msg->GetError())) {
        NProto::TNVMeDeviceList proto;
        proto.MutableDevices()->Assign(
            std::make_move_iterator(msg->NVMeDevices.begin()),
            std::make_move_iterator(msg->NVMeDevices.end()));

        google::protobuf::util::MessageToJsonString(
            proto,
            response->Record.MutableOutput());
    }

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

void TLocalNVMeDeviceOpsActor::HandleAcquireNVMeDeviceResponse(
    const TEvDiskAgentPrivate::TEvAcquireNVMeDeviceResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto response =
        std::make_unique<TEvService::TEvExecuteActionResponse>(msg->GetError());
    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

void TLocalNVMeDeviceOpsActor::HandleReleaseNVMeDeviceResponse(
    const TEvDiskAgentPrivate::TEvReleaseNVMeDeviceResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto response =
        std::make_unique<TEvService::TEvExecuteActionResponse>(msg->GetError());
    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

void TLocalNVMeDeviceOpsActor::HandleListNVMeDevicesUndelivery(
    const TEvDiskAgentPrivate::TEvListNVMeDevicesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_WARN(
        ctx,
        TBlockStoreComponents::SERVICE,
        "List NVMe disks request undelivered");

    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(
        MakeError(E_PRECONDITION_FAILED, "Local Disk Agent not found"));
    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

void TLocalNVMeDeviceOpsActor::HandleAcquireNVMeDeviceUndelivery(
    const TEvDiskAgentPrivate::TEvAcquireNVMeDeviceRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_WARN(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Acquire NVMe device request undelivered");

    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(
        MakeError(E_PRECONDITION_FAILED, "Local Disk Agent not found"));
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TLocalNVMeDeviceOpsActor::HandleReleaseNVMeDeviceUndelivery(
    const TEvDiskAgentPrivate::TEvReleaseNVMeDeviceRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_WARN(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Release NVMe device request undelivered");

    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(
        MakeError(E_PRECONDITION_FAILED, "Local Disk Agent not found"));
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TLocalNVMeDeviceOpsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskAgentPrivate::TEvListNVMeDevicesResponse,
            HandleListNVMeDevicesResponse);

        HFunc(
            TEvDiskAgentPrivate::TEvAcquireNVMeDeviceResponse,
            HandleAcquireNVMeDeviceResponse);

        HFunc(
            TEvDiskAgentPrivate::TEvReleaseNVMeDeviceResponse,
            HandleReleaseNVMeDeviceResponse);

        HFunc(
            TEvDiskAgentPrivate::TEvListNVMeDevicesRequest,
            HandleListNVMeDevicesUndelivery);

        HFunc(
            TEvDiskAgentPrivate::TEvAcquireNVMeDeviceRequest,
            HandleAcquireNVMeDeviceUndelivery);

        HFunc(
            TEvDiskAgentPrivate::TEvReleaseNVMeDeviceRequest,
            HandleReleaseNVMeDeviceUndelivery);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TListNVMeDevicesActor final
    : public TLocalNVMeDeviceOpsActor
{
public:
    using TLocalNVMeDeviceOpsActor::TLocalNVMeDeviceOpsActor;

    void DoBootstrap(const TActorContext& ctx) final
    {
        LOG_INFO(ctx, TBlockStoreComponents::SERVICE, "List NVMe disks");

        auto request =
            std::make_unique<TEvDiskAgentPrivate::TEvListNVMeDevicesRequest>();

        NCloud::SendWithUndeliveryTracking(
            ctx,
            MakeDiskAgentServiceId(ctx.SelfID.NodeId()),
            std::move(request));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TAcquireNVMeDeviceActor final
    : public TLocalNVMeDeviceOpsActor
{
public:
    using TLocalNVMeDeviceOpsActor::TLocalNVMeDeviceOpsActor;

    void DoBootstrap(const TActorContext& ctx) final
    {
        auto [sn, error] = ParseSerialNumber(Input);

        if (HasError(error)) {
            auto response =
                std::make_unique<TEvService::TEvExecuteActionResponse>(error);
            NCloud::Reply(ctx, *RequestInfo, std::move(response));
            Die(ctx);
            return;
        }

        LOG_INFO(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Acquire NVMe disk %s",
            sn.Quote().c_str());

        auto request = std::make_unique<
            TEvDiskAgentPrivate::TEvAcquireNVMeDeviceRequest>();
        request->SerialNumber = sn;

        NCloud::SendWithUndeliveryTracking(
            ctx,
            MakeDiskAgentServiceId(ctx.SelfID.NodeId()),
            std::move(request));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TReleaseNVMeDeviceActor final
    : public TLocalNVMeDeviceOpsActor
{
public:
    using TLocalNVMeDeviceOpsActor::TLocalNVMeDeviceOpsActor;

    void DoBootstrap(const TActorContext& ctx) final
    {
        auto [sn, error] = ParseSerialNumber(Input);

        if (HasError(error)) {
            auto response =
                std::make_unique<TEvService::TEvExecuteActionResponse>(error);
            NCloud::Reply(ctx, *RequestInfo, std::move(response));
            Die(ctx);
            return;
        }

        LOG_INFO(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Release NVMe disk %s",
            sn.Quote().c_str());

        auto request = std::make_unique<
            TEvDiskAgentPrivate::TEvReleaseNVMeDeviceRequest>();
        request->SerialNumber = sn;

        NCloud::SendWithUndeliveryTracking(
            ctx,
            MakeDiskAgentServiceId(ctx.SelfID.NodeId()),
            std::move(request));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IActorPtr> TServiceActor::CreateListNVMeDevicesActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TListNVMeDevicesActor>(
        std::move(requestInfo),
        std::move(input))};
}

TResultOrError<IActorPtr> TServiceActor::CreateAcquireNVMeDeviceActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TAcquireNVMeDeviceActor>(
        std::move(requestInfo),
        std::move(input))};
}

TResultOrError<IActorPtr> TServiceActor::CreateReleaseNVMeDeviceActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TReleaseNVMeDeviceActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
