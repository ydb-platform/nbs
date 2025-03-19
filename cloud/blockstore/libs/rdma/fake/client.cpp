#include "client.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/rdma/iface/client.h>
#include <cloud/blockstore/libs/rdma/iface/protobuf.h>
#include <cloud/blockstore/libs/rdma/iface/protocol.h>
#include <cloud/blockstore/libs/service_local/rdma_protocol.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <chrono>
#include <utility>

namespace NCloud::NBlockStore {

using namespace NActors;
using namespace NStorage;
using namespace NThreading;
using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration RequestTimeout = 5s;

////////////////////////////////////////////////////////////////////////////////

struct TEvFakeRdmaClient
{
    struct TStartEndpoint
    {
        TString AgentId;
        TPromise<NRdma::IClientEndpointPtr> Promise;
    };

    struct TStopEndpoint
    {
        TString AgentId;
        TPromise<void> Promise;
    };

    struct TSendRequest
    {
        ui64 ClientReqId;
        TString AgentId;
        NRdma::TClientRequestPtr Request;
        TCallContextPtr CallContext;
    };

    struct TUpdateNodeId
    {
        TString AgentId;
        ui32 NodeId = 0;
    };

    struct TCancelRequest
    {
        TString AgentId;
        ui64 ClientReqId;
    };

    struct TOperationCompleted
    {
        TString AgentId;
    };

    enum EEvents
    {
        EvBegin = EventSpaceBegin(TEvents::ES_USERSPACE),

        EvStartEndpoint,
        EvStopEndpoint,
        EvSendRequest,
        EvUpdateNodeId,
        EvCancelRequest,
        EvOperationCompleted,

        EvEnd
    };

    using TEvStartEndpoint = TRequestEvent<TStartEndpoint, EvStartEndpoint>;
    using TEvStopEndpoint = TRequestEvent<TStopEndpoint, EvStopEndpoint>;
    using TEvSendRequest = TRequestEvent<TSendRequest, EvSendRequest>;
    using TEvUpdateNodeId = TResponseEvent<TUpdateNodeId, EvUpdateNodeId>;
    using TEvCancelRequest = TRequestEvent<TCancelRequest, EvCancelRequest>;
    using TEvOperationCompleted =
        TRequestEvent<TOperationCompleted, EvOperationCompleted>;
};

////////////////////////////////////////////////////////////////////////////////

struct TClientRequest: public NRdma::TClientRequest
{
    TActorId RdmaActorId;

    std::unique_ptr<char[]> RequestStorage;
    std::unique_ptr<char[]> ResponseStorage;

public:
    TClientRequest(
            const TActorId& rdmaActorId,
            NRdma::IClientHandlerPtr handler,
            std::unique_ptr<NRdma::TNullContext> context,
            ui32 requestSize,
            ui32 responseSize)
        : NRdma::TClientRequest(std::move(handler), std::move(context))
        , RdmaActorId(rdmaActorId)
        , RequestStorage(std::make_unique<char []>(requestSize))
        , ResponseStorage(std::make_unique<char []>(responseSize))
    {
        RequestBuffer = {RequestStorage.get(), requestSize};
        ResponseBuffer = {ResponseStorage.get(), responseSize};
    }
};

////////////////////////////////////////////////////////////////////////////////

void AbortRequest(
    NRdma::TClientRequestPtr request,
    ui32 error,
    TStringBuf message)
{
    const size_t len =
        NRdma::SerializeError(error, message, request->ResponseBuffer);

    auto* handler = request->Handler.get();
    handler->HandleResponse(std::move(request), NRdma::RDMA_PROTO_FAIL, len);
}

////////////////////////////////////////////////////////////////////////////////

class TClientEndpoint
    : public NRdma::IClientEndpoint
{
private:
    const IActorSystemPtr ActorSystem;
    const TActorId RdmaActorId;
    const TString AgentId;

    std::atomic_uint64_t ReqIdPool{0};

public:
    TClientEndpoint(
        IActorSystemPtr actorSystem,
        const TActorId& rdmaActorId,
        TString agentId);

    auto AllocateRequest(
        NRdma::IClientHandlerPtr handler,
        std::unique_ptr<NRdma::TNullContext> context,
        size_t requestBytes,
        size_t responseBytes)
        -> TResultOrError<NRdma::TClientRequestPtr> override;

    ui64 SendRequest(
        NRdma::TClientRequestPtr req,
        TCallContextPtr callContext) override;

    void CancelRequest(ui64 reqId) override;

    TFuture<void> Stop() override;

    void TryForceReconnect() override;

    ui64 TakeNewReqId() {
        return ReqIdPool.fetch_add(1);
    }
};

////////////////////////////////////////////////////////////////////////////////

TClientEndpoint::TClientEndpoint(
        IActorSystemPtr actorSystem,
        const TActorId& rdmaActorId,
        TString agentId)
    : ActorSystem(std::move(actorSystem))
    , RdmaActorId(rdmaActorId)
    , AgentId(std::move(agentId))
{}

auto TClientEndpoint::AllocateRequest(
    NRdma::IClientHandlerPtr handler,
    std::unique_ptr<NRdma::TNullContext> context,
    size_t requestBytes,
    size_t responseBytes) -> TResultOrError<NRdma::TClientRequestPtr>
{
    auto req = std::make_unique<TClientRequest>(
        RdmaActorId,
        std::move(handler),
        std::move(context),
        requestBytes,
        responseBytes);

    return NRdma::TClientRequestPtr(std::move(req));
}

ui64 TClientEndpoint::SendRequest(
    NRdma::TClientRequestPtr req,
    TCallContextPtr callContext)
{
    auto request = std::make_unique<TEvFakeRdmaClient::TEvSendRequest>();

    auto clientReqId = TakeNewReqId();

    request->ClientReqId = clientReqId;
    request->AgentId = AgentId;
    request->Request = std::move(req);
    request->CallContext = std::move(callContext);

    ActorSystem->Send(RdmaActorId, std::move(request));
    return clientReqId;
}

void TClientEndpoint::CancelRequest(ui64 reqId)
{
    auto request = std::make_unique<TEvFakeRdmaClient::TEvCancelRequest>();
    request->ClientReqId = reqId;
    request->AgentId = AgentId;

    ActorSystem->Send(RdmaActorId, std::move(request));
}

TFuture<void> TClientEndpoint::Stop()
{
    auto request = std::make_unique<TEvFakeRdmaClient::TEvStopEndpoint>();
    request->AgentId = AgentId;
    request->Promise = NewPromise();

    auto future = request->Promise.GetFuture();

    ActorSystem->Send(RdmaActorId, std::move(request));

    return future;
}

void TClientEndpoint::TryForceReconnect() {}

////////////////////////////////////////////////////////////////////////////////

class TExecuteRequestActor final
    : public TActorBootstrapped<TExecuteRequestActor>
{
private:
    const TActorId Parent;
    const ui32 NodeId;
    const TString AgentId;
    NRdma::TClientRequestPtr Request;
    TCallContextPtr CallContext;

public:
    TExecuteRequestActor(
            TActorId parent,
            ui32 nodeId,
            TString agentId,
            NRdma::TClientRequestPtr request,
            TCallContextPtr callContext)
        : Parent(parent)
        , NodeId(nodeId)
        , AgentId(std::move(agentId))
        , Request(std::move(request))
        , CallContext(std::move(callContext))
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        Become(&TThis::StateWork);

        const auto error = ExecuteRequest(ctx);
        if (HasError(error)) {
            AbortRequest(
                std::move(Request),
                error.GetCode(),
                error.GetMessage());
            return;
        }

        NCloud::Schedule<TEvents::TEvWakeup>(ctx, RequestTimeout);
    }

private:
    NProto::TError ExecuteRequest(const TActorContext& ctx)
    {
        auto [proto, error] =
            TBlockStoreProtocol::Serializer()->Parse(Request->RequestBuffer);
        if (HasError(error)) {
            return error;
        }

        switch (proto.MsgId) {
            case TBlockStoreProtocol::ReadDeviceBlocksRequest:
                Y_ABORT_IF(proto.Data);
                return SendReadBlocksRequest(
                    ctx,
                    static_cast<NProto::TReadDeviceBlocksRequest&>(
                        *proto.Proto));

            case TBlockStoreProtocol::WriteDeviceBlocksRequest:
                return SendWriteBlocksRequest(
                    ctx,
                    static_cast<NProto::TWriteDeviceBlocksRequest&>(
                        *proto.Proto),
                    proto.Data);

            case TBlockStoreProtocol::ZeroDeviceBlocksRequest:
                Y_ABORT_IF(proto.Data);
                return SendZeroBlocksRequest(
                    ctx,
                    static_cast<NProto::TZeroDeviceBlocksRequest&>(
                        *proto.Proto));

            case TBlockStoreProtocol::ChecksumDeviceBlocksRequest:
                Y_ABORT_IF(proto.Data);
                return SendChecksumBlocksRequest(
                    ctx,
                    static_cast<NProto::TChecksumDeviceBlocksRequest&>(
                        *proto.Proto));

            default:
                return MakeError(
                    E_NOT_IMPLEMENTED,
                    TStringBuilder() << "MsgId: " << proto.MsgId);
        }
    }

    NProto::TError SendReadBlocksRequest(
        const TActorContext& ctx,
        NProto::TReadDeviceBlocksRequest& proto)
    {
        LOG_DEBUG_S(
            ctx,
            TBlockStoreComponents::RDMA,
            "Send ReadDeviceBlocks to #"
                << NodeId << " " << proto.GetStartIndex() << ":"
                << FormatByteSize(
                       static_cast<ui64>(proto.GetBlocksCount()) *
                       proto.GetBlockSize()));

        auto request =
            std::make_unique<TEvDiskAgent::TEvReadDeviceBlocksRequest>(
                CallContext,
                std::move(proto));

        auto event = std::make_unique<IEventHandle>(
            MakeDiskAgentServiceId(NodeId),
            ctx.SelfID,
            request.release(),
            IEventHandle::FlagForwardOnNondelivery,
            0,            // cookie
            &ctx.SelfID   // forwardOnNondelivery
        );

        ctx.Send(std::move(event));

        return {};
    }

    NProto::TError SendWriteBlocksRequest(
        const TActorContext& ctx,
        NProto::TWriteDeviceBlocksRequest& proto,
        TStringBuf requestData)
    {
        Y_ABORT_IF(requestData.empty());

        LOG_DEBUG_S(
            ctx,
            TBlockStoreComponents::RDMA,
            "Send WriteDeviceBlocks to #"
                << NodeId << " " << proto.GetStartIndex() << ":"
                << FormatByteSize(requestData.size()));

        auto request =
            std::make_unique<TEvDiskAgent::TEvWriteDeviceBlocksRequest>(
                CallContext,
                std::move(proto));
        request->Record.MutableBlocks()->AddBuffers(
            requestData.data(),
            requestData.length());

        auto event = std::make_unique<IEventHandle>(
            MakeDiskAgentServiceId(NodeId),
            ctx.SelfID,
            request.release(),
            IEventHandle::FlagForwardOnNondelivery,
            0,            // cookie
            &ctx.SelfID   // forwardOnNondelivery
        );

        ctx.Send(std::move(event));

        return {};
    }

    NProto::TError SendZeroBlocksRequest(
        const TActorContext& ctx,
        NProto::TZeroDeviceBlocksRequest& proto)
    {
        LOG_DEBUG_S(
            ctx,
            TBlockStoreComponents::RDMA,
            "Send ZeroDeviceBlocks to #"
                << NodeId << " " << proto.GetStartIndex() << ":"
                << FormatByteSize(
                       static_cast<ui64>(proto.GetBlocksCount()) *
                       proto.GetBlocksCount()));

        auto request =
            std::make_unique<TEvDiskAgent::TEvZeroDeviceBlocksRequest>(
                CallContext,
                std::move(proto));

        auto event = std::make_unique<IEventHandle>(
            MakeDiskAgentServiceId(NodeId),
            ctx.SelfID,
            request.release(),
            IEventHandle::FlagForwardOnNondelivery,
            0,            // cookie
            &ctx.SelfID   // forwardOnNondelivery
        );

        ctx.Send(std::move(event));

        return {};
    }

    NProto::TError SendChecksumBlocksRequest(
        const TActorContext& ctx,
        NProto::TChecksumDeviceBlocksRequest& proto)
    {
        LOG_DEBUG_S(
            ctx,
            TBlockStoreComponents::RDMA,
            "Send ChecksumDeviceBlocks to #"
                << NodeId << " " << proto.GetStartIndex() << ":"
                << FormatByteSize(
                       static_cast<ui64>(proto.GetBlocksCount()) *
                       proto.GetBlocksCount()));

        auto request =
            std::make_unique<TEvDiskAgent::TEvChecksumDeviceBlocksRequest>(
                CallContext,
                std::move(proto));

        auto event = std::make_unique<IEventHandle>(
            MakeDiskAgentServiceId(NodeId),
            ctx.SelfID,
            request.release(),
            IEventHandle::FlagForwardOnNondelivery,
            0,            // cookie
            &ctx.SelfID   // forwardOnNondelivery
        );

        ctx.Send(std::move(event));

        return {};
    }

    void ReplyAndDie(const TActorContext& ctx)
    {
        auto completion =
            std::make_unique<TEvFakeRdmaClient::TEvOperationCompleted>();
        completion->AgentId = AgentId;
        NCloud::Send(ctx, Parent, std::move(completion));
        Die(ctx);
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvWakeup, HandleWakeup);

            HFunc(
                TEvDiskAgent::TEvReadDeviceBlocksResponse,
                HandleReadDeviceBlocksResponse);

            HFunc(
                TEvDiskAgent::TEvWriteDeviceBlocksResponse,
                HandleWriteDeviceBlocksResponse);

            HFunc(
                TEvDiskAgent::TEvZeroDeviceBlocksResponse,
                HandleZeroDeviceBlocksResponse);

            HFunc(
                TEvDiskAgent::TEvChecksumDeviceBlocksResponse,
                HandleChecksumDeviceBlocksResponse);

            HFunc(
                TEvDiskAgent::TEvReadDeviceBlocksRequest,
                HandleReadUndelivery);
            HFunc(
                TEvDiskAgent::TEvWriteDeviceBlocksRequest,
                HandleWriteUndelivery);
            HFunc(
                TEvDiskAgent::TEvZeroDeviceBlocksRequest,
                HandleZeroUndelivery);
            HFunc(
                TEvDiskAgent::TEvChecksumDeviceBlocksRequest,
                HandleChecksumUndelivery);
            HFunc(TEvFakeRdmaClient::TEvCancelRequest, HandleCancelRequest);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::RDMA,
                    __PRETTY_FUNCTION__);
                break;
        }
    }

    void HandleCancelRequest(
        const TEvFakeRdmaClient::TEvCancelRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);
        AbortRequest(std::move(Request), E_CANCELLED, "request cancelled");
        ReplyAndDie(ctx);
    }

    void HandleReadUndelivery(
        const TEvDiskAgent::TEvReadDeviceBlocksRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        AbortRequest(
            std::move(Request),
            E_REJECTED,
            "ReadDeviceBlocks request undelivered");

        ReplyAndDie(ctx);
    }

    void HandleWriteUndelivery(
        const TEvDiskAgent::TEvWriteDeviceBlocksRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        AbortRequest(
            std::move(Request),
            E_REJECTED,
            "WriteDeviceBlocks request undelivered");

        ReplyAndDie(ctx);
    }

    void HandleZeroUndelivery(
        const TEvDiskAgent::TEvZeroDeviceBlocksRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        AbortRequest(
            std::move(Request),
            E_REJECTED,
            "ZeroDeviceBlocks request undelivered");

        ReplyAndDie(ctx);
    }

    void HandleChecksumUndelivery(
        const TEvDiskAgent::TEvChecksumDeviceBlocksRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        AbortRequest(
            std::move(Request),
            E_REJECTED,
            "ChecksumDeviceBlocks request undelivered");

        ReplyAndDie(ctx);
    }

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        LOG_ERROR_S(ctx, TBlockStoreComponents::RDMA, "Request timedout");
        AbortRequest(std::move(Request), E_REJECTED, "timeout");

        ReplyAndDie(ctx);
    }

    template <typename TResponse>
    void HandleResponse(
        const TActorContext& ctx,
        const TResponse& response,
        ui32 msgId)
    {
        size_t len = NRdma::TProtoMessageSerializer::Serialize(
            Request->ResponseBuffer,
            msgId,
            0,   // flags
            response);

        auto* handler = Request->Handler.get();
        handler->HandleResponse(std::move(Request), NRdma::RDMA_PROTO_OK, len);

        ReplyAndDie(ctx);
    }

    void HandleReadDeviceBlocksResponse(
        const TEvDiskAgent::TEvReadDeviceBlocksResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        NProto::TReadDeviceBlocksResponse& proto = ev->Get()->Record;

        NProto::TIOVector blocks;
        blocks.Swap(proto.MutableBlocks());

        const auto& buffers = blocks.GetBuffers();
        ui64 size = 0;
        for (const auto& buf: buffers) {
            size += buf.size();
        }

        LOG_DEBUG_S(
            ctx,
            TBlockStoreComponents::RDMA,
            "Got ReadDeviceBlocks response from #"
                << NodeId << ": " << FormatError(proto.GetError()) << ", "
                << FormatByteSize(size));

        TStackVec<TBlockDataRef> parts;
        parts.reserve(blocks.BuffersSize());

        for (const auto& buffer: blocks.GetBuffers()) {
            parts.emplace_back(TBlockDataRef(buffer.data(), buffer.size()));
        }

        size_t len = NRdma::TProtoMessageSerializer::SerializeWithData(
            Request->ResponseBuffer,
            TBlockStoreProtocol::ReadDeviceBlocksResponse,
            0,   // flags
            proto,
            parts);

        auto* handler = Request->Handler.get();
        handler->HandleResponse(std::move(Request), NRdma::RDMA_PROTO_OK, len);

        ReplyAndDie(ctx);
    }

    void HandleWriteDeviceBlocksResponse(
        const TEvDiskAgent::TEvWriteDeviceBlocksResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();

        LOG_DEBUG_S(
            ctx,
            TBlockStoreComponents::RDMA,
            "Got WriteDeviceBlocks response from #"
                << NodeId << ": " << FormatError(msg->GetError()));

        HandleResponse(
            ctx,
            msg->Record,
            TBlockStoreProtocol::WriteDeviceBlocksResponse);
    }

    void HandleZeroDeviceBlocksResponse(
        const TEvDiskAgent::TEvZeroDeviceBlocksResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();

        LOG_DEBUG_S(
            ctx,
            TBlockStoreComponents::RDMA,
            "Got WriteDeviceBlocks response from #"
                << NodeId << ": " << FormatError(msg->GetError()));

        HandleResponse(
            ctx,
            msg->Record,
            TBlockStoreProtocol::ZeroDeviceBlocksResponse);
    }

    void HandleChecksumDeviceBlocksResponse(
        const TEvDiskAgent::TEvChecksumDeviceBlocksResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();

        LOG_DEBUG_S(
            ctx,
            TBlockStoreComponents::RDMA,
            "Got ChecksumDeviceBlocks response from #"
                << NodeId << ": " << FormatError(msg->GetError()));

        HandleResponse(
            ctx,
            msg->Record,
            TBlockStoreProtocol::ChecksumDeviceBlocksResponse);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TUpdateNodeIdActor final
    : public TActorBootstrapped<TUpdateNodeIdActor>
{
private:
    const TActorId Owner;
    const TString AgentId;

public:
    TUpdateNodeIdActor(const TActorId& owner, TString agentId)
        : Owner(owner)
        , AgentId(std::move(agentId))
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        Become(&TThis::StateWork);

        LOG_INFO_S(
            ctx,
            TBlockStoreComponents::RDMA,
            "Update node id for " << AgentId.Quote());

        auto request =
            std::make_unique<TEvDiskRegistry::TEvGetAgentNodeIdRequest>();
        request->Record.SetAgentId(AgentId);

        ctx.Send(MakeDiskRegistryProxyServiceId(), request.release());

        NCloud::Schedule<TEvents::TEvWakeup>(ctx, RequestTimeout);
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(
                TEvDiskRegistry::TEvGetAgentNodeIdResponse,
                HandleGetAgentNodeIdResponse);

            HFunc(TEvents::TEvWakeup, HandleWakeup);
            default:
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::RDMA,
                    __PRETTY_FUNCTION__);
                break;
        }
    }

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        LOG_ERROR_S(
            ctx,
            TBlockStoreComponents::RDMA,
            "Node id update timedout");

        auto request = std::make_unique<TEvFakeRdmaClient::TEvUpdateNodeId>(
            MakeError(E_REJECTED, "timeout"));
        request->AgentId = AgentId;

        NCloud::Send(ctx, Owner, std::move(request));

        Die(ctx);
    }

    void HandleGetAgentNodeIdResponse(
        const TEvDiskRegistry::TEvGetAgentNodeIdResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();

        auto request = std::make_unique<TEvFakeRdmaClient::TEvUpdateNodeId>(
            msg->GetError());
        request->AgentId = AgentId;
        request->NodeId = msg->Record.GetNodeId();

        NCloud::Send(ctx, Owner, std::move(request));

        Die(ctx);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFakeRdmaClientActor
    : public TActor<TFakeRdmaClientActor>
{

    struct TCancellationInfo {
        THashMap<ui64, TActorId> ClientReqIdToActorId;
        THashMap<TActorId, ui64> ActorIdToClientReqId;
    };

    struct TEndpoint
    {
        ui32 NodeId = 0;
        ui32 Refs = 0;
        std::shared_ptr<TClientEndpoint> Endpoint;
        TCancellationInfo CancellationInfo;
    };

private:
    IActorSystemPtr ActorSystem;
    THashMap<TString, TEndpoint> Endpoints;

public:
    explicit TFakeRdmaClientActor(IActorSystemPtr actorSystem)
        : TActor(&TThis::StateWork)
        , ActorSystem(std::move(actorSystem))
    {}

private:
    void UpdateNodeId(const TActorContext& ctx, const TString& agentId) const
    {
        NCloud::Register<TUpdateNodeIdActor>(ctx, SelfId(), agentId);
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

            HFunc(TEvFakeRdmaClient::TEvStartEndpoint, HandleStartEndpoint);
            HFunc(TEvFakeRdmaClient::TEvStopEndpoint, HandleStopEndpoint);
            HFunc(TEvFakeRdmaClient::TEvSendRequest, HandleSendRequest);
            HFunc(TEvFakeRdmaClient::TEvUpdateNodeId, HandleUpdateNodeId);
            HFunc(TEvFakeRdmaClient::TEvCancelRequest, HandleCancelRequest);
            HFunc(
                TEvFakeRdmaClient::TEvOperationCompleted,
                HandleOperationCompleted);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::RDMA,
                    __PRETTY_FUNCTION__);
                break;
        }
    }

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        Die(ctx);
    }

    void HandleStartEndpoint(
        const TEvFakeRdmaClient::TEvStartEndpoint::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();

        LOG_INFO_S(
            ctx,
            TBlockStoreComponents::RDMA,
            "Start endpoint for " << msg->AgentId.Quote());

        TEndpoint& ep = Endpoints[msg->AgentId];
        ++ep.Refs;

        if (ep.Refs == 1) {
            LOG_INFO_S(
                ctx,
                TBlockStoreComponents::RDMA,
                "Endpoint for " << msg->AgentId.Quote() << " is started");

            UpdateNodeId(ctx, msg->AgentId);
            ep.Endpoint = std::make_shared<TClientEndpoint>(
                ActorSystem,
                SelfId(),
                msg->AgentId);
        }

        msg->Promise.SetValue(ep.Endpoint);
    }

    void HandleStopEndpoint(
        const TEvFakeRdmaClient::TEvStopEndpoint::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();

        LOG_INFO_S(
            ctx,
            TBlockStoreComponents::RDMA,
            "Stop endpoint for " << msg->AgentId.Quote());

        auto it = Endpoints.find(msg->AgentId);
        if (it != Endpoints.end()) {
            TEndpoint& ep = it->second;
            Y_ABORT_UNLESS(ep.Refs > 0);

            if (--ep.Refs == 0) {
                LOG_INFO_S(
                    ctx,
                    TBlockStoreComponents::RDMA,
                    "Endpoint for " << msg->AgentId.Quote() << " is stopped");

                Endpoints.erase(it);
            }
        }

        msg->Promise.SetValue();
    }

    void HandleSendRequest(
        const TEvFakeRdmaClient::TEvSendRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();

        TEndpoint* ep = Endpoints.FindPtr(msg->AgentId);
        if (!ep) {
            AbortRequest(
                std::move(msg->Request),
                E_RDMA_UNAVAILABLE,
                TStringBuilder() << "endpoint for " << msg->AgentId.Quote()
                                 << " is not started");

            return;
        }

        if (!ep->NodeId) {
            AbortRequest(
                std::move(msg->Request),
                E_REJECTED,
                TStringBuilder() << "node id for " << msg->AgentId.Quote()
                                 << " is not resolved yet");

            return;
        }

        auto& info = ep->CancellationInfo;

        LOG_INFO_S(
            ctx,
            TBlockStoreComponents::RDMA,
            "Send request agentId" << msg->AgentId.Quote() << ", clientReqId "
                                   << msg->ClientReqId);

        auto [it, inserted] =
            info.ClientReqIdToActorId.try_emplace(msg->ClientReqId);
        Y_ABORT_UNLESS(inserted);

        it->second = NCloud::Register<TExecuteRequestActor>(
            ctx,
            SelfId(),
            ep->NodeId,
            msg->AgentId,
            std::move(msg->Request),
            std::move(msg->CallContext));
        info.ActorIdToClientReqId[it->second] = it->first;
    }

    void HandleUpdateNodeId(
        const TEvFakeRdmaClient::TEvUpdateNodeId::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();

        if (HasError(msg->GetError())) {
            LOG_ERROR_S(
                ctx,
                TBlockStoreComponents::RDMA,
                "Can't update node id for " << msg->AgentId.Quote() << ": "
                                            << FormatError(msg->GetError()));
            return;
        }

        TEndpoint* ep = Endpoints.FindPtr(msg->AgentId);
        if (ep) {
            LOG_INFO_S(
                ctx,
                TBlockStoreComponents::RDMA,
                "Update node id for " << msg->AgentId.Quote() << ": #"
                                      << msg->NodeId);

            ep->NodeId = msg->NodeId;
        }
    }

    void HandleCancelRequest(
        const TEvFakeRdmaClient::TEvCancelRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();
        auto* ep = Endpoints.FindPtr(msg->AgentId);
        if (!ep) {
            return;
        }
        auto& info = ep->CancellationInfo;

        auto it = info.ClientReqIdToActorId.find(msg->ClientReqId);
        if (it == info.ClientReqIdToActorId.end()) {
            return;
        }

        auto actorId = it->second;
        ForwardMessageToActor(ev, ctx, actorId);
    }

    void HandleOperationCompleted(
        const TEvFakeRdmaClient::TEvOperationCompleted::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ctx);
        auto* msg = ev->Get();
        auto* ep = Endpoints.FindPtr(msg->AgentId);
        if (!ep) {
            return;
        }
        auto& info = ep->CancellationInfo;

        auto actorId = ev->Sender;
        auto it = info.ActorIdToClientReqId.find(actorId);
        auto clientReqId = it->second;

        LOG_INFO_S(
            ctx,
            TBlockStoreComponents::RDMA,
            "op completed agentId" << msg->AgentId.Quote() << ", clientReqId "
                                   << clientReqId);

        info.ClientReqIdToActorId.erase(clientReqId);

        info.ActorIdToClientReqId.erase(it);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFakeRdmaClient final
    : public NRdma::IClient
{
private:
    IActorSystemPtr ActorSystem;

    TLog Log;
    TActorId RdmaActorId;

public:
    explicit TFakeRdmaClient(IActorSystemPtr actorSystem);

    // IStartable

    void Start() override;
    void Stop() override;

    // NRdma::IClient

    auto StartEndpoint(TString host, ui32 port)
        -> TFuture<NRdma::IClientEndpointPtr> override;

    void DumpHtml(IOutputStream& out) const override;

    [[nodiscard]] bool IsAlignedDataEnabled() const override;
};

////////////////////////////////////////////////////////////////////////////////

TFakeRdmaClient::TFakeRdmaClient(IActorSystemPtr actorSystem)
    : ActorSystem(std::move(actorSystem))
{}

void TFakeRdmaClient::Start()
{
    Log = ActorSystem->CreateLog("BLOCKSTORE_RDMA");
    RdmaActorId = ActorSystem->Register(
        std::make_unique<TFakeRdmaClientActor>(ActorSystem));
}

void TFakeRdmaClient::Stop()
{
    ActorSystem->Send(
        RdmaActorId,
        std::make_unique<TEvents::TEvPoisonPill>());
}

auto TFakeRdmaClient::StartEndpoint(TString host, ui32 port)
    -> TFuture<NRdma::IClientEndpointPtr>
{
    STORAGE_INFO("Start endpoint " << host << ":" << port);

    auto request = std::make_unique<TEvFakeRdmaClient::TEvStartEndpoint>();
    request->AgentId = std::move(host);
    request->Promise = NewPromise<NRdma::IClientEndpointPtr>();

    auto future = request->Promise.GetFuture();

    ActorSystem->Send(RdmaActorId, std::move(request));

    return future;
}

void TFakeRdmaClient::DumpHtml(IOutputStream& out) const
{
    out << "Fake RDMA client";
}

bool TFakeRdmaClient::IsAlignedDataEnabled() const
{
    return false;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NRdma::IClientPtr CreateFakeRdmaClient(IActorSystemPtr actorSystem)
{
    return std::make_shared<TFakeRdmaClient>(std::move(actorSystem));
}

}   // namespace NCloud::NBlockStore
