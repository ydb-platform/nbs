#include "disk_agent_actor.h"

#include <cloud/blockstore/libs/storage/disk_agent/model/device_client.h>

#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/journalled_device_tcp_server/server.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NJournalled;
using namespace NKikimr;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr NProto::EVolumeAccessMode DefaultAccessMode =
    NProto::VOLUME_ACCESS_READ_WRITE;
constexpr ui64 DefaultMountSeqNumber = 0;
constexpr ui64 DefaultVolumeGeneration = 0;

////////////////////////////////////////////////////////////////////////////////

void CopyHeaders(
    NProto::THeaders& dst,
    const NCloud::NProto::TDeviceRequestHeaders& src)
{
    dst.SetClientId(src.GetClientId());
    dst.SetRequestTimeout(src.GetRequestTimeout());
}

auto CreateWriteBlocksRequest(
    NCloud::NProto::TDevicePageGroup&& group,
    ui32 blockSize) -> std::shared_ptr<NProto::TWriteBlocksRequest>
{
    auto request = std::make_shared<NProto::TWriteBlocksRequest>();

    request->SetStartIndex(group.GetFirstPageNo());
    request->SetBlockSize(blockSize);

    NProto::TIOVector& blocks = *request->MutableBlocks();
    *blocks.MutableBuffers() = std::move(*group.MutableContent());

    return request;
}

auto CreateReadBlocksRequest(const NCloud::NProto::TDevicePageGroupRef& group)
    -> std::shared_ptr<NProto::TReadBlocksRequest>
{
    auto request = std::make_shared<NProto::TReadBlocksRequest>();

    request->SetStartIndex(group.GetFirstPageNo());
    request->SetBlocksCount(group.GetPageCount());
    request->SetBlockSize(group.GetPageSize());

    return request;
}

auto ValidateWriteLogRecordRequest(
    const NCloud::NProto::TWriteLogRecordRequest& request)
    -> TResultOrError<ui32>
{
    ui32 blockSize = 0;

    if (request.GetDeviceUUID().empty()) {
        return MakeError(E_ARGUMENT, "empty device UUID");
    }

    if (request.PageGroupsSize() == 0) {
        return MakeError(E_ARGUMENT, "nothing to write");
    }

    for (const auto& group: request.GetPageGroups()) {
        if (group.ContentSize() == 0) {
            return MakeError(E_ARGUMENT, "empty page group");
        }

        for (TStringBuf block: group.GetContent()) {
            if (block.empty()) {
                return MakeError(
                    E_ARGUMENT,
                    "invalid page data: block must not be empty");
            }

            if (blockSize == 0) {
                blockSize = block.size();
                continue;
            }

            if (blockSize != block.size()) {
                return MakeError(
                    E_ARGUMENT,
                    TStringBuilder() << "invalid page data: block size "
                                        "mismatch: expected "
                                     << blockSize << ", got " << block.size());
            }
        }
    }

    return blockSize;
}

auto ValidateReadPagesRequest(const NCloud::NProto::TReadPagesRequest& request)
    -> NProto::TError
{
    if (request.GetDeviceUUID().empty()) {
        return MakeError(E_ARGUMENT, "empty device UUID");
    }

    if (request.PageGroupRefsSize() == 0) {
        return MakeError(E_ARGUMENT, "nothing to read");
    }

    for (const auto& group: request.GetPageGroupRefs()) {
        if (group.GetPageCount() == 0) {
            return MakeError(
                E_ARGUMENT,
                "page group ref must contain at least one page");
        }

        if (group.GetPageSize() == 0) {
            return MakeError(E_ARGUMENT, "page size must be greater than zero");
        }
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

template <typename TOrigResponse, typename TEvResponse>
class TPromiseActor final
    : public TActor<TPromiseActor<TOrigResponse, TEvResponse>>
{
    using TThis = TPromiseActor<TOrigResponse, TEvResponse>;
    using TBase = TActor<TThis>;

private:
    TPromise<TOrigResponse> Promise;

public:
    explicit TPromiseActor(TPromise<TOrigResponse> promise)
        : TActor<TThis>(&TThis::StateWork)
        , Promise(std::move(promise))
    {}

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
            HFunc(TEvResponse, HandleResponse);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::DISK_AGENT_WORKER,
                    __PRETTY_FUNCTION__);
                break;
        }
    }

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        Promise.SetValue(ErrorResponse<TOrigResponse>(E_REJECTED, "Stopping"));

        TThis::Die(ctx);
    }

    void HandleResponse(
        const TEventHandle<TEvResponse>::TPtr& ev,
        const TActorContext& ctx)
    {
        TOrigResponse response;
        response.MutableError()->CopyFrom(ev->Get()->Record.GetError());

        Promise.SetValue(std::move(response));

        TThis::Die(ctx);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TJournalledDeviceHandler final: public IServerBackend
{
private:
    TActorSystem* ActorSystem = nullptr;
    const TActorId DiskAgentActorId;
    const TDeviceClientPtr DeviceClient;

public:
    TJournalledDeviceHandler(
        TActorSystem* actorSystem,
        const TActorId& diskAgentActorId,
        TDeviceClientPtr deviceClient)
        : ActorSystem(actorSystem)
        , DiskAgentActorId(diskAgentActorId)
        , DeviceClient(std::move(deviceClient))
    {}

    // IServerBackend

    [[nodiscard]] auto AcquireDevices(
        TInstant now,
        NCloud::NProto::TAcquireDevicesRequest request)
        -> TFuture<NCloud::NProto::TAcquireDevicesResponse> final
    {
        Y_UNUSED(now);

        auto promise = NewPromise<NCloud::NProto::TAcquireDevicesResponse>();

        auto ev = std::make_unique<TEvDiskAgent::TEvAcquireDevicesRequest>();

        CopyHeaders(*ev->Record.MutableHeaders(), request.GetHeaders());
        ev->Record.MutableDeviceUUIDs()->Assign(
            request.GetDeviceUUIDs().begin(),
            request.GetDeviceUUIDs().end());
        ev->Record.SetAccessMode(DefaultAccessMode);
        ev->Record.SetMountSeqNumber(DefaultMountSeqNumber);
        ev->Record.SetDiskId(request.GetHeaders().GetClientId());
        ev->Record.SetVolumeGeneration(DefaultVolumeGeneration);

        const TActorId actorId = ActorSystem->Register(
            new TPromiseActor<
                NCloud::NProto::TAcquireDevicesResponse,
                TEvDiskAgent::TEvAcquireDevicesResponse>(promise));

        const bool ok = ActorSystem->Send(new IEventHandle(
            DiskAgentActorId,
            actorId,
            ev.release(),
            0,   // flags
            0    // cookie
            ));
        Y_DEBUG_ABORT_UNLESS(ok);

        return promise;
    }

    [[nodiscard]] auto ReleaseDevices(
        TInstant now,
        NCloud::NProto::TReleaseDevicesRequest request)
        -> TFuture<NCloud::NProto::TReleaseDevicesResponse> final
    {
        Y_UNUSED(now);

        auto promise = NewPromise<NCloud::NProto::TReleaseDevicesResponse>();

        auto ev = std::make_unique<TEvDiskAgent::TEvReleaseDevicesRequest>();

        CopyHeaders(*ev->Record.MutableHeaders(), request.GetHeaders());
        ev->Record.MutableDeviceUUIDs()->Assign(
            request.GetDeviceUUIDs().begin(),
            request.GetDeviceUUIDs().end());

        const TActorId actorId = ActorSystem->Register(
            new TPromiseActor<
                NCloud::NProto::TReleaseDevicesResponse,
                TEvDiskAgent::TEvReleaseDevicesResponse>(promise));

        const bool ok = ActorSystem->Send(new IEventHandle(
            DiskAgentActorId,
            actorId,
            ev.release(),
            0,   // flags
            0    // cookie
            ));
        Y_DEBUG_ABORT_UNLESS(ok);

        return promise;
    }

    [[nodiscard]] auto ReadPages(
        TInstant now,
        NCloud::NProto::TReadPagesRequest request)
        -> TFuture<NCloud::NProto::TReadPagesResponse> final
    {
        if (auto error = ValidateReadPagesRequest(request); HasError(error)) {
            return MakeFuture<NCloud::NProto::TReadPagesResponse>(
                TErrorResponse(error));
        }

        auto [storageAdapter, error] = DeviceClient->AccessDevice(
            request.GetDeviceUUID(),
            request.GetHeaders().GetClientId(),
            DefaultAccessMode);

        if (HasError(error)) {
            return MakeFuture<NCloud::NProto::TReadPagesResponse>(
                TErrorResponse(error));
        }

        auto response = std::make_shared<NCloud::NProto::TReadPagesResponse>();

        TVector<TFuture<NProto::TReadBlocksResponse>> futures;
        futures.reserve(request.PageGroupRefsSize());

        for (const auto& group: request.GetPageGroupRefs()) {
            futures.push_back(storageAdapter->ReadBlocks(
                now,
                CreateCallContext(),
                CreateReadBlocksRequest(group),
                group.GetPageSize(),
                TStringBuf()   // dataBuffer
                ));
        }

        auto all = WaitAll(futures);

        return all.Apply(
            [futures,
             request = std::move(request)](const TFuture<void>& future) mutable
                -> NCloud::NProto::TReadPagesResponse
            {
                if (future.HasException()) {
                    return TErrorResponse(ResultOrError(future).GetError());
                }

                NCloud::NProto::TReadPagesResponse response;
                auto& groups = *response.MutablePageGroups();
                groups.Reserve(futures.size());

                for (size_t i = 0; i != futures.size(); ++i) {
                    NProto::TReadBlocksResponse sub = futures[i].ExtractValue();
                    if (HasError(sub)) {
                        return TErrorResponse(sub.GetError());
                    }

                    auto& group = *groups.Add();

                    group.SetFirstPageNo(
                        request.GetPageGroupRefs(i).GetFirstPageNo());

                    *group.MutableContent() =
                        std::move(*sub.MutableBlocks()->MutableBuffers());
                }

                return response;
            });
    }

    [[nodiscard]] auto WriteLogRecord(
        TInstant now,
        NCloud::NProto::TWriteLogRecordRequest request)
        -> TFuture<NCloud::NProto::TWriteLogRecordResponse> final
    {
        // TODO(sharpeye): check LogSequenceNumber

        ui32 requestBlockSize = 0;
        if (auto [bs, error] = ValidateWriteLogRecordRequest(request);
            HasError(error))
        {
            return MakeFuture<NCloud::NProto::TWriteLogRecordResponse>(
                TErrorResponse(error));
        } else {
            requestBlockSize = bs;
        }

        auto [storageAdapter, error] = DeviceClient->AccessDevice(
            request.GetDeviceUUID(),
            request.GetHeaders().GetClientId(),
            DefaultAccessMode);

        if (HasError(error)) {
            return MakeFuture<NCloud::NProto::TWriteLogRecordResponse>(
                TErrorResponse(error));
        }

        TVector<TFuture<NProto::TWriteBlocksResponse>> futures;
        futures.reserve(request.PageGroupsSize());

        for (auto& group: *request.MutablePageGroups()) {
            futures.push_back(storageAdapter->WriteBlocks(
                now,
                CreateCallContext(),
                CreateWriteBlocksRequest(std::move(group), requestBlockSize),
                requestBlockSize,
                TStringBuf()   // dataBuffer
                ));
        }

        auto all = WaitAll(futures);

        return all.Apply(
            [futures](const TFuture<void>& future) mutable
                -> NCloud::NProto::TWriteLogRecordResponse
            {
                if (future.HasException()) {
                    return TErrorResponse(ResultOrError(future).GetError());
                }

                for (const auto& future: futures) {
                    const auto& sub = future.GetValue();
                    if (HasError(sub)) {
                        return TErrorResponse(sub.GetError());
                    }
                }

                return {};
            });
    }
};

////////////////////////////////////////////////////////////////////////////////

TNetworkAddress CreateNetworkAddress(TStringBuf s)
{
    TStringBuf hostRef;
    TStringBuf portRef;
    s.RSplit(':', hostRef, portRef);

    return {
        hostRef ? TString(hostRef).c_str() : nullptr,
        FromString<ui16>(portRef)};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentActor::StartJournalledDeviceTcpServer(
    const NActors::TActorContext& ctx)
{
    if (!State) {
        return;
    }

    if (AgentConfig->GetJournalledDeviceTcpServerListenAddress().empty()) {
        return;
    }

    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::DISK_AGENT,
        "Starting journaled device TCP server on "
            << AgentConfig->GetJournalledDeviceTcpServerListenAddress().Quote()
            << "...");

    try {
        const TNetworkAddress listenAddress = CreateNetworkAddress(
            AgentConfig->GetJournalledDeviceTcpServerListenAddress());

        Executor = TExecutor::Create("JD");
        Executor->Start();

        JournalledDeviceTcpServer = NJournalled::CreateServer(
            listenAddress,
            Logging,
            Executor,
            std::make_shared<TJournalledDeviceHandler>(
                TActivationContext::ActorSystem(),
                ctx.SelfID,
                State->GetDeviceClient()));

        JournalledDeviceTcpServer->Start();

        LOG_INFO_S(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Journalled device TCP server started on "
                << AgentConfig->GetJournalledDeviceTcpServerListenAddress()
                       .Quote());

    } catch (...) {
        LOG_ERROR_S(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Could not start journalled device TCP server"
                << ": " << CurrentExceptionMessage());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
