#include "remote_partition_actor.h"

#include <cloud/blockstore/libs/rdma/iface/client.h>
#include <cloud/blockstore/libs/rdma/iface/protobuf.h>
#include <cloud/blockstore/libs/rdma/iface/protocol.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service_rdma/rdma_protocol.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/storage/core/libs/common/sglist_block_range.h>

namespace NCloud::NBlockStore::NStorage {
using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TReadRequestsHandler: public NRdma::IClientHandler
{
    TActorSystem* ActorSystem;
    const TRequestInfoPtr RequestInfo;
    const size_t BlockSize;
    const std::optional<TGuardedSgList> SglistToPutData;

public:
    TReadRequestsHandler(
            TActorSystem* actorSystem,
            TRequestInfoPtr reqInfo,
            size_t blockSize,
            std::optional<TGuardedSgList> sglistToPutData)
        : ActorSystem(actorSystem)
        , RequestInfo(std::move(reqInfo))
        , BlockSize(blockSize)
        , SglistToPutData(std::move(sglistToPutData))
    {}

    NProto::TError PutDataToOutputSglist(TStringBuf data, bool fillWithZeros)
    {
        const auto& guardedSglist = *SglistToPutData;

        auto guard = guardedSglist.Acquire();
        if (!guard) {
            return MakeError(E_CANCELLED, "cant acquire sglist");
        }

        const auto& dstSglist = guard.Get();

        if (fillWithZeros) {
            for (auto bufRef: dstSglist) {
                memset(const_cast<char*>(bufRef.Data()), 0, bufRef.Size());
            }

            return {};
        }

        auto size = SgListCopy({data.Data(), data.Size()}, dstSglist);
        Y_DEBUG_ABORT_UNLESS(size == data.Size());
        Y_DEBUG_ABORT_UNLESS(size == SgListGetSize(dstSglist));

        return {};
    }

    void PutDataToProto(
        NProto::TReadBlocksResponse& proto,
        const TStringBuf data,
        bool fillWithZeros) const
    {
        auto blocksCount = proto.GetBlocks().BuffersSize();

        proto.ClearBlocks();
        auto buffer = data;
        for (size_t i = 0; i < blocksCount; ++i) {
            auto block = fillWithZeros ? TString(BlockSize, '\0')
                                       : TString(buffer.Head(BlockSize));
            proto.MutableBlocks()->AddBuffers(std::move(block));
            buffer.Skip(BlockSize);
        }
    }

    void ReplyWithError(const NProto::TError& err)
    {
        std::unique_ptr<IEventBase> resp;
        if (SglistToPutData) {
            resp =
                std::make_unique<TEvService::TEvReadBlocksLocalResponse>(err);
        } else {
            resp = std::make_unique<TEvService::TEvReadBlocksResponse>(err);
        }

        auto event = std::make_unique<IEventHandle>(
            RequestInfo->Sender,
            TActorId(),
            resp.release(),
            0,
            RequestInfo->Cookie);
        ActorSystem->Send(event.release());
    }

    void ReplyWithResult(NProto::TReadBlocksResponse proto)
    {
        std::unique_ptr<IEventBase> resp;
        if (SglistToPutData) {
            resp = std::make_unique<TEvService::TEvReadBlocksLocalResponse>(
                std::move(proto));
        } else {
            resp = std::make_unique<TEvService::TEvReadBlocksResponse>(
                std::move(proto));
        }

        auto event = std::make_unique<IEventHandle>(
            RequestInfo->Sender,
            TActorId(),
            resp.release(),
            0,
            RequestInfo->Cookie);
        ActorSystem->Send(event.release());
    }

    void HandleResponse(
        NRdma::TClientRequestPtr req,
        ui32 status,
        size_t responseBytes) override
    {
        auto buffer = req->ResponseBuffer.Head(responseBytes);
        if (status != NRdma::RDMA_PROTO_OK) {
            ReplyWithError(NRdma::ParseError(buffer));
            return;
        }

        auto* serializer = NCloud::NBlockStore::NStorage::
            TBlockStoreServerProtocol::Serializer();
        auto [result, err] = serializer->Parse(buffer);
        if (HasError(err)) {
            ReplyWithError(MakeError(
                E_ARGUMENT,
                TStringBuilder() << "received invalid response from remote "
                                    "host, parsing error: "
                                 << err.GetMessage()));
            return;
        }

        Y_ABORT_UNLESS(
            result.MsgId == TBlockStoreServerProtocol::EvReadBlocksResponse);

        NProto::TReadBlocksResponse proto =
            *static_cast<NProto::TReadBlocksResponse*>(result.Proto.get());
        auto fillWithZeros = result.Data.Size() == 0;
        if (SglistToPutData) {
            auto err = PutDataToOutputSglist(result.Data, fillWithZeros);
            if (HasError(err)) {
                ReplyWithError(err);
            }
        } else {
            PutDataToProto(proto, result.Data, fillWithZeros);
        }

        ReplyWithResult(std::move(proto));
    }
};

template <typename TMethod>
class TGeneralRequestsHandler: public NRdma::IClientHandler
{
    TActorSystem* ActorSystem;
    const TRequestInfoPtr RequestInfo;
    const ui32 ExpectedMsgId;

public:
    TGeneralRequestsHandler(
            TActorSystem* actorSystem,
            TRequestInfoPtr reqInfo,
            ui32 expectedMsgId)
        : ActorSystem(actorSystem)
        , RequestInfo(std::move(reqInfo))
        , ExpectedMsgId(expectedMsgId)
    {}

    void ReplyWithError(const NProto::TError& err)
    {
        auto resp = std::make_unique<typename TMethod::TResponse>(err);

        auto event = std::make_unique<IEventHandle>(
            RequestInfo->Sender,
            TActorId(),
            resp.release(),
            0,
            RequestInfo->Cookie);
        ActorSystem->Send(event.release());
    }

    void HandleResponse(
        NRdma::TClientRequestPtr req,
        ui32 status,
        size_t responseBytes) override
    {
        auto buffer = req->ResponseBuffer.Head(responseBytes);
        if (status != NRdma::RDMA_PROTO_OK) {
            ReplyWithError(NRdma::ParseError(buffer));
            return;
        }

        auto* serializer = NCloud::NBlockStore::NStorage::
            TBlockStoreServerProtocol::Serializer();
        auto [result, err] = serializer->Parse(buffer);
        if (HasError(err)) {
            ReplyWithError(MakeError(
                E_ARGUMENT,
                TStringBuilder() << "received invalid response from remote "
                                    "host, parsing error: "
                                 << err.GetMessage()));
            return;
        }

        Y_ABORT_UNLESS(result.MsgId == ExpectedMsgId);

        auto proto =
            std::unique_ptr<typename TMethod::TResponse::ProtoRecordType>(
                static_cast<TMethod::TResponse::ProtoRecordType*>(
                    result.Proto.release()));

        auto resp =
            std::make_unique<typename TMethod::TResponse>(std::move(*proto));

        auto event = std::make_unique<IEventHandle>(
            RequestInfo->Sender,
            TActorId(),
            resp.release(),
            0,
            RequestInfo->Cookie);
        ActorSystem->Send(event.release());
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TRemotePartitionActor::TRemotePartitionActor(
        TString remoteHost,
        NRdma::TRdmaConfigPtr config,
        NRdma::IClientPtr rdmaClient,
        size_t blockSize)
    : Host(std::move(remoteHost))
    , Config(std::move(config))
    , BlockSize(blockSize)
    , RdmaClient(std::move(rdmaClient))
{}

void TRemotePartitionActor::Bootstrap(const TActorContext& ctx)
{
    Y_UNUSED(ctx);
    FutureEndpoint = RdmaClient->StartEndpoint(
        Host,
        Config->GetBlockstoreServerTarget().GetEndpoint().GetPort());

    Become(&TThis::StateWork);
}

template <typename TMethod>
TResultOrError<NRdma::TClientRequestPtr> TRemotePartitionActor::InitRequest(
    TMethod::TRequest::ProtoRecordType& proto,
    const TSgList& data,
    size_t additionalSpaceForResponseData,
    int msgId,
    NRdma::IClientHandlerPtr handler)
{
    auto ep = GetEndpoint();
    if (!ep) {
        return MakeError(E_REJECTED, "connection not established");
    }

    auto dataSize = SgListGetSize(data);

    auto [req, err] = ep->AllocateRequest(
        std::move(handler),
        nullptr,
        NRdma::TProtoMessageSerializer::MessageByteSize(proto, dataSize),
        4_KB + additionalSpaceForResponseData);
    if (HasError(err)) {
        return err;
    }

    auto msgType =
        NCloud::NBlockStore::NStorage::TBlockStoreServerProtocol::EMessageType(
            msgId);

    if (data) {
        NRdma::TProtoMessageSerializer::SerializeWithData(
            req->RequestBuffer,
            msgType,
            0,
            proto,
            data);
    } else {
        NRdma::TProtoMessageSerializer::Serialize(
            req->RequestBuffer,
            msgType,
            0,
            proto);
    }

    return std::move(req);
}

NRdma::IClientEndpointPtr TRemotePartitionActor::GetEndpoint()
{
    if (Endpoint) {
        return Endpoint;
    }

    if (!FutureEndpoint.HasValue()) {
        return {};
    }

    Endpoint = std::move(FutureEndpoint.ExtractValue());

    return Endpoint;
}

STFUNC(TRemotePartitionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvService::TEvReadBlocksRequest, HandleReadBlocks);
        HFunc(TEvService::TEvReadBlocksLocalRequest, HandleReadBlocksLocal);

        HFunc(TEvService::TEvWriteBlocksRequest, HandleWriteBlocks);
        HFunc(TEvService::TEvWriteBlocksLocalRequest, HandleWriteBlocksLocal);

        HFunc(TEvService::TEvZeroBlocksRequest, HandleZeroBlocks);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION);
            break;
    }
}

void TRemotePartitionActor::HandlePoisonPill(
    const NActors::TEvents::TEvPoisonPill::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);
    Die(ctx);
}

template <typename TMethod>
void TRemotePartitionActor::HandleReadMethod(
    const TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto& record = ev->Get()->Record;
    auto additionalSpace = record.GetBlocksCount() * BlockSize;

    std::optional<TGuardedSgList> sglist;
    if constexpr (std::is_same_v<TMethod, TEvService::TReadBlocksLocalMethod>) {
        sglist = std::move(record.Sglist);
    }

    auto handlerPtr = std::make_shared<TReadRequestsHandler>(
        NActors::TActorContext::ActorSystem(),
        CreateRequestInfo(ev->Sender, ev->Cookie, ev->Get()->CallContext),
        BlockSize,
        std::move(sglist));

    auto [req, err] = InitRequest<TMethod>(
        record,
        TSgList{},
        additionalSpace,
        TBlockStoreServerProtocol::EvReadBlocksRequest,
        std::move(handlerPtr));
    if (HasError(err)) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<typename TMethod::TResponse>(std::move(err)));
        return;
    }
    GetEndpoint()->SendRequest(std::move(req), ev->Get()->CallContext);
}

void TRemotePartitionActor::HandleReadBlocks(
    const TEvService::TEvReadBlocksRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    HandleReadMethod<TEvService::TReadBlocksMethod>(ev, ctx);
}

void TRemotePartitionActor::HandleReadBlocksLocal(
    const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    HandleReadMethod<TEvService::TReadBlocksLocalMethod>(ev, ctx);
}

void TRemotePartitionActor::HandleWriteBlocks(
    const TEvService::TEvWriteBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto& record = ev->Get()->Record;

    auto handlerPtr = std::make_shared<
        TGeneralRequestsHandler<TEvService::TWriteBlocksMethod>>(
        NActors::TActorContext::ActorSystem(),
        CreateRequestInfo(ev->Sender, ev->Cookie, ev->Get()->CallContext),
        TBlockStoreServerProtocol::EvWriteBlocksResponse);

    auto blocksCount = record.GetBlocks().BuffersSize();
    auto blocks = std::move(*record.MutableBlocks()->MutableBuffers());
    record.MutableBlocks()->ClearBuffers();
    TSgList data;
    data.reserve(blocksCount);
    for (const auto& buf: blocks) {
        data.emplace_back(buf.Data(), buf.Size());
        // Add empty blocks to pass blocksCount.
        record.MutableBlocks()->AddBuffers();
    }

    auto [req, err] = InitRequest<TEvService::TWriteBlocksMethod>(
        record,
        data,
        0,   // additionalSpaceForResponseData
        TBlockStoreServerProtocol::EvWriteBlocksRequest,
        std::move(handlerPtr));
    if (HasError(err)) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvService::TEvWriteBlocksResponse>(
                std::move(err)));
        return;
    }
    GetEndpoint()->SendRequest(std::move(req), ev->Get()->CallContext);
}

void TRemotePartitionActor::HandleWriteBlocksLocal(
    const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto& record = ev->Get()->Record;

    auto handlerPtr = std::make_shared<
        TGeneralRequestsHandler<TEvService::TWriteBlocksLocalMethod>>(
        NActors::TActorContext::ActorSystem(),
        CreateRequestInfo(ev->Sender, ev->Cookie, ev->Get()->CallContext),
        TBlockStoreServerProtocol::EvWriteBlocksResponse);

    auto guard = record.Sglist.Acquire();

    if (!guard) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(
                MakeError(E_CANCELLED, "can't acquire sglist")));
        return;
    }

    auto blocksCount = record.BlocksCount;
    if (record.GetBlocks().BuffersSize() != blocksCount) {
        record.MutableBlocks()->ClearBuffers();
        for (ui32 i = 0; i < blocksCount; ++i) {
            // Add empty blocks to pass blocksCount.
            record.MutableBlocks()->AddBuffers();
        }
    }
    const TSgList& data = guard.Get();

    auto [req, err] = InitRequest<TEvService::TWriteBlocksMethod>(
        record,
        data,
        0,   // additionalSpaceForResponseData
        TBlockStoreServerProtocol::EvWriteBlocksRequest,
        std::move(handlerPtr));
    if (HasError(err)) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(
                std::move(err)));
        return;
    }

    GetEndpoint()->SendRequest(std::move(req), ev->Get()->CallContext);
}

void TRemotePartitionActor::HandleZeroBlocks(
    const TEvService::TEvZeroBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto& record = ev->Get()->Record;

    auto handlerPtr = std::make_shared<
        TGeneralRequestsHandler<TEvService::TZeroBlocksMethod>>(
        NActors::TActorContext::ActorSystem(),
        CreateRequestInfo(ev->Sender, ev->Cookie, ev->Get()->CallContext),
        TBlockStoreServerProtocol::EvZeroBlocksResponse);

    const TSgList emptyData;
    auto [req, err] = InitRequest<TEvService::TZeroBlocksMethod>(
        record,
        emptyData,
        0,   // additionalSpaceForResponseData
        TBlockStoreServerProtocol::EvZeroBlocksRequest,
        std::move(handlerPtr));
    if (HasError(err)) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvService::TEvZeroBlocksResponse>(
                std::move(err)));
        return;
    }

    GetEndpoint()->SendRequest(std::move(req), ev->Get()->CallContext);
}

}   // namespace NCloud::NBlockStore::NStorage
