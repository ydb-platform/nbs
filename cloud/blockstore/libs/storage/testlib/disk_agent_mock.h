#pragma once

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/disk_agent/actors/multi_agent_write_blocks_actor.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>

#include <util/generic/hash.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

using TCreateDirectCopyActorFunc = std::function<void(
    const TEvDiskAgent::TEvDirectCopyBlocksRequest::TPtr& ev,
    const NActors::TActorContext& ctx,
    NActors::TActorId owner)>;

struct TDiskAgentState
{
    TDuration ResponseDelay;
    NProto::TError Error;
    TCreateDirectCopyActorFunc CreateDirectCopyActorFunc;
};

using TDiskAgentStatePtr = std::shared_ptr<TDiskAgentState>;

////////////////////////////////////////////////////////////////////////////////

class TDiskAgentMock final
    : public NActors::TActor<TDiskAgentMock>
{
    struct TDeviceState
    {
        NProto::TDeviceConfig Config;
        THashMap<ui64, TString> Content;
    };

private:
    THashMap<TString, TDeviceState> Devices;
    TDiskAgentStatePtr State;

public:
    TDiskAgentMock(
            google::protobuf::RepeatedPtrField<NProto::TDeviceConfig> devices,
            TDiskAgentStatePtr state = {})
        : TActor(&TThis::StateWork)
        , State(std::move(state))
    {
        Devices.reserve(devices.size());

        for (auto& device: devices) {
            auto& dst = Devices[device.GetDeviceUUID()];
            dst.Config = std::move(device);
        }

        if (!State) {
            State = std::make_shared<TDiskAgentState>();
        }
    }

private:
    template <typename T>
    void Reply(
        const NActors::TActorContext& ctx,
        T& request,
        NActors::IEventBasePtr response)
    {
        if (State->ResponseDelay == TDuration::Max()) {
            return;
        }

        if (State->ResponseDelay) {
            auto event = std::make_unique<NActors::IEventHandle>(
                request.Sender,
                ctx.SelfID,
                response.release(),
                0,   // flags
                request.Cookie);

            ctx.Schedule(State->ResponseDelay, std::move(event));

            return;
        }

        NCloud::Reply(ctx, request, std::move(response));
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(NActors::TEvents::TEvPoisonPill, HandlePoisonPill);

            HFunc(TEvDiskAgent::TEvReadDeviceBlocksRequest, HandleReadDeviceBlocks);
            HFunc(TEvDiskAgent::TEvWriteDeviceBlocksRequest, HandleWriteDeviceBlocks);
            HFunc(TEvDiskAgent::TEvZeroDeviceBlocksRequest, HandleZeroDeviceBlocks);
            HFunc(TEvDiskAgent::TEvChecksumDeviceBlocksRequest, HandleChecksumDeviceBlocks);
            HFunc(TEvDiskAgent::TEvDirectCopyBlocksRequest, HandleDirectCopyBlocks);
            HFunc(TEvDiskAgent::TEvAcquireDevicesRequest, HandleAcquireDevicesRequest);
            HFunc(TEvDiskAgent::TEvReleaseDevicesRequest, HandleReleaseDevicesRequest);

            default:
                Y_ABORT("Unexpected event %x", ev->GetTypeRewrite());
        }
    }

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        Y_UNUSED(ev);

        Die(ctx);
    }

    void HandleReadDeviceBlocks(
        const TEvDiskAgent::TEvReadDeviceBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        const auto& request = ev->Get()->Record;

        auto response =
            std::make_unique<TEvDiskAgent::TEvReadDeviceBlocksResponse>();

        const auto& device = Devices.at(request.GetDeviceUUID());
        const auto& [config, content] = device;

        auto error = State->Error;

        if (!config.GetDeviceUUID()) {
            error = MakeError(E_ARGUMENT, "invalid device");
        }

        if (HasError(error)) {
            response->Record.MutableError()->CopyFrom(error);
            Reply(ctx, *ev, std::move(response));
            return;
        }

        const auto k = request.GetBlockSize() / config.GetBlockSize();

        auto startIndex = request.GetStartIndex() * k;
        const auto endIndex = startIndex + request.GetBlocksCount() * k;

        Y_ABORT_UNLESS(endIndex <= config.GetBlocksCount());

        for (ui32 i = 0; i < request.GetBlocksCount(); ++i) {
            auto& buf = *response->Record.MutableBlocks()->AddBuffers();

            bool allZeroes = true;
            for (ui32 m = 0; m != k; ++m) {
                auto it = content.find(startIndex++);
                if (it != content.end()) {
                    buf += it->second;
                    allZeroes = false;
                } else {
                    buf += TString(config.GetBlockSize(), 0);
                }
            }
            if (allZeroes &&
                request.GetHeaders().GetOptimizeNetworkTransfer() ==
                    NProto::EOptimizeNetworkTransfer::SKIP_VOID_BLOCKS)
            {
                buf.clear();
            }
        }

        Reply(ctx, *ev, std::move(response));
    }

    void HandleWriteDeviceBlocks(
        const TEvDiskAgent::TEvWriteDeviceBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        auto* msg = ev->Get();
        const auto& request = msg->Record;

        if (!request.GetReplicationTargets().empty()) {
            NCloud::Register<TMultiAgentWriteBlocksActor>(
                ctx,
                SelfId(),
                CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
                std::move(msg->Record));
            return;
        }

        auto response =
            std::make_unique<TEvDiskAgent::TEvWriteDeviceBlocksResponse>();

        auto& device = Devices.at(request.GetDeviceUUID());
        const auto& config = device.Config;

        auto error = State->Error;

        if (!config.GetDeviceUUID()) {
            error = MakeError(E_ARGUMENT, "invalid device");
        }

        if (HasError(error)) {
            response->Record.MutableError()->CopyFrom(error);
            Reply(ctx, *ev, std::move(response));
            return;
        }

        auto& content = device.Content;

        const auto k = request.GetBlockSize() / config.GetBlockSize();

        auto i = request.GetStartIndex() * k;

        for (TStringBuf buf: request.GetBlocks().GetBuffers()) {
            Y_ABORT_UNLESS(i < config.GetBlocksCount());

            while (buf) {
                content[i++] = buf.SubStr(0, config.GetBlockSize());
                buf.Skip(config.GetBlockSize());
            }
        }

        Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvWriteDeviceBlocksResponse>());
    }

    void HandleZeroDeviceBlocks(
        const TEvDiskAgent::TEvZeroDeviceBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        const auto& request = ev->Get()->Record;

        auto response =
            std::make_unique<TEvDiskAgent::TEvWriteDeviceBlocksResponse>();

        auto& device = Devices.at(request.GetDeviceUUID());
        const auto& config = device.Config;

        auto error = State->Error;

        if (!config.GetDeviceUUID()) {
            error = MakeError(E_ARGUMENT, "invalid device");
        }

        if (HasError(error)) {
            response->Record.MutableError()->CopyFrom(error);
            Reply(ctx, *ev, std::move(response));
            return;
        }

        const auto k = request.GetBlockSize() / config.GetBlockSize();

        const auto startIndex = request.GetStartIndex() * k;
        const auto endIndex = Min(
            startIndex + request.GetBlocksCount() * k,
            config.GetBlocksCount());

        for (ui32 i = startIndex; i != endIndex; ++i) {
            device.Content[i] = TString(config.GetBlockSize(), 0);
        }

        Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvZeroDeviceBlocksResponse>());
    }

    void HandleChecksumDeviceBlocks(
        const TEvDiskAgent::TEvChecksumDeviceBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        const auto& request = ev->Get()->Record;

        auto response =
            std::make_unique<TEvDiskAgent::TEvChecksumDeviceBlocksResponse>();

        const auto& device = Devices.at(request.GetDeviceUUID());
        const auto& [config, content] = device;

        auto error = State->Error;

        if (!config.GetDeviceUUID()) {
            error = MakeError(E_ARGUMENT, "invalid device");
        }

        if (HasError(error)) {
            response->Record.MutableError()->CopyFrom(error);
            Reply(ctx, *ev, std::move(response));
            return;
        }

        const auto k = request.GetBlockSize() / config.GetBlockSize();

        auto startIndex = request.GetStartIndex() * k;
        const auto endIndex = startIndex + request.GetBlocksCount() * k;

        Y_ABORT_UNLESS(endIndex <= config.GetBlocksCount());

        TString zeroBlock(config.GetBlockSize(), 0);
        TBlockChecksum checksum;

        for (ui32 i = 0; i < request.GetBlocksCount(); ++i) {
            for (ui32 m = 0; m != k; ++m) {
                const TString* buf;

                auto it = content.find(startIndex++);
                if (it != content.end()) {
                    buf = &it->second;
                } else {
                    buf = &zeroBlock;
                }
                checksum.Extend(buf->data(), buf->size());
            }
        }

        response->Record.SetChecksum(checksum.GetValue());

        Reply(ctx, *ev, std::move(response));
    }

    void HandleDirectCopyBlocks(
        const TEvDiskAgent::TEvDirectCopyBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        State->CreateDirectCopyActorFunc(ev, ctx, SelfId());
    }

    void HandleAcquireDevicesRequest(
        TEvDiskAgent::TEvAcquireDevicesRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        auto response =
            std::make_unique<TEvDiskAgent::TEvAcquireDevicesResponse>();

        Reply(ctx, *ev, std::move(response));
    }

    void HandleReleaseDevicesRequest(
        TEvDiskAgent::TEvReleaseDevicesRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        auto response =
            std::make_unique<TEvDiskAgent::TEvReleaseDevicesResponse>();

        Reply(ctx, *ev, std::move(response));
    }
};

}   // namespace NCloud::NBlockStore::NStorage
