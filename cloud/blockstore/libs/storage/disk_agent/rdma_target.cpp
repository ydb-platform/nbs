#include "rdma_target.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/rdma/iface/protobuf.h>
#include <cloud/blockstore/libs/rdma/iface/server.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service_local/rdma_protocol.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_client.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/generic/hash.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration LOG_THROTTLER_PERIOD = TDuration::MilliSeconds(500);

////////////////////////////////////////////////////////////////////////////////

struct TRequestLogDetails
{
    TString DeviceUUID;
    TString ClientId;

    template <typename T>
    TRequestLogDetails(T& request)
        : DeviceUUID(std::move(*request.MutableDeviceUUID()))
        , ClientId(std::move(*request.MutableHeaders()->MutableClientId()))
    {
    }

    TRequestLogDetails(TRequestLogDetails&&) = default;

    // XXX not actually used, but required by the compiler
    // TODO remove it after the C++23 upgrade
    // see https://en.cppreference.com/w/cpp/utility/functional/move_only_function
    TRequestLogDetails(const TRequestLogDetails&) = default;
};

////////////////////////////////////////////////////////////////////////////////

// Thread-safe. After Init() public method HandleRequest() can be called from
// any thread.
class TRequestHandler final
    : public NRdma::IServerHandler
    , public std::enable_shared_from_this<TRequestHandler>
{
private:
    const THashMap<TString, TStorageAdapterPtr> Devices;
    const ITaskQueuePtr TaskQueue;

    mutable TLogThrottler LogThrottler{LOG_THROTTLER_PERIOD};
    TLog Log;

    const TDeviceClientPtr DeviceClient;

    std::weak_ptr<NRdma::IServerEndpoint> Endpoint;
    const NRdma::TProtoMessageSerializer* Serializer =
        TBlockStoreProtocol::Serializer();

public:
    TRequestHandler(
            THashMap<TString, TStorageAdapterPtr> devices,
            ITaskQueuePtr taskQueue,
            TDeviceClientPtr deviceClient)
        : Devices(std::move(devices))
        , TaskQueue(std::move(taskQueue))
        , DeviceClient(std::move(deviceClient))
    {}

    void Init(NRdma::IServerEndpointPtr endpoint, TLog log)
    {
        Endpoint = std::move(endpoint);
        Log = std::move(log);
    }

    void HandleRequest(
        void* context,
        TCallContextPtr callContext,
        TStringBuf in,
        TStringBuf out) override
    {
        auto doHandleRequest = [self = shared_from_this(),
                                context = context,
                                callContext = std::move(callContext),
                                in = std::move(in),
                                out = std::move(out)]() -> NProto::TError
        {
            return self->DoHandleRequest(
                context,
                std::move(callContext),
                std::move(in),
                std::move(out));
        };

        auto safeHandleRequest =
            [endpoint = Endpoint,
             context = context,
             doHandleRequest = std::move(doHandleRequest)]()
        {
            auto error =
                SafeExecute<NProto::TError>(std::move(doHandleRequest));

            if (HasError(error)) {
                if (auto ep = endpoint.lock()) {
                    ep->SendError(context, error.GetCode(), error.GetMessage());
                }
            }
        };

        TaskQueue->ExecuteSimple(std::move(safeHandleRequest));
    }

private:
    NProto::TError DoHandleRequest(
        void* context,
        TCallContextPtr callContext,
        TStringBuf in,
        TStringBuf out) const
    {
        auto resultOrError = Serializer->Parse(in);

        if (HasError(resultOrError)) {
            return resultOrError.GetError();
        }

        const auto& request = resultOrError.GetResult();

        switch (request.MsgId) {
            case TBlockStoreProtocol::ReadDeviceBlocksRequest:
                return HandleReadBlocksRequest(
                    context,
                    callContext,
                    static_cast<NProto::TReadDeviceBlocksRequest&>(*request.Proto),
                    request.Data,
                    out);

            case TBlockStoreProtocol::WriteDeviceBlocksRequest:
                return HandleWriteBlocksRequest(
                    context,
                    callContext,
                    static_cast<NProto::TWriteDeviceBlocksRequest&>(*request.Proto),
                    request.Data,
                    out);

            case TBlockStoreProtocol::ZeroDeviceBlocksRequest:
                return HandleZeroBlocksRequest(
                    context,
                    callContext,
                    static_cast<NProto::TZeroDeviceBlocksRequest&>(*request.Proto),
                    request.Data,
                    out);

            case TBlockStoreProtocol::ChecksumDeviceBlocksRequest:
                return HandleChecksumBlocksRequest(
                    context,
                    callContext,
                    static_cast<NProto::TChecksumDeviceBlocksRequest&>(*request.Proto),
                    request.Data,
                    out);

            default:
                return MakeError(E_NOT_IMPLEMENTED);
        }
    }

    TStorageAdapterPtr GetDevice(
        const TString& uuid,
        const TString& clientId,
        NProto::EVolumeAccessMode accessMode) const
    {
        if (DeviceClient->IsDeviceDisabled(uuid)) {
            STORAGE_ERROR_T(
                LogThrottler,
                "[" << uuid << "/" << clientId << "] Device disabled. Drop request.");

            ythrow TServiceError(E_ARGUMENT);
        }

        NProto::TError error =
            DeviceClient->AccessDevice(uuid, clientId, accessMode);

        if (HasError(error)) {
            ythrow TServiceError(error);
        }

        auto it = Devices.find(uuid);

        if (it == Devices.cend()) {
            ythrow TServiceError(E_NOT_FOUND);
        }

        return it->second;
    }

    template <typename Future, typename TResponseMethod>
    void SubscribeForResponse(
        Future& future,
        void* context,
        TStringBuf out,
        TRequestLogDetails rld,
        TResponseMethod method) const
    {
        auto handleResponse = [self = shared_from_this(),
                               context = context,
                               out = std::move(out),
                               logDetails = std::move(rld),
                               method = method](Future future)
        {
            self->TaskQueue->ExecuteSimple(
                [self = self,
                 context = context,
                 out = std::move(out),
                 logDetails = std::move(logDetails),
                 future = std::move(future),
                 method = method]()
                {
                    const TRequestHandler* obj = self.get();
                    (obj->*method)(
                              context,
                              std::move(out),
                              std::move(logDetails),
                              std::move(future));
                });
        };
        future.Subscribe(std::move(handleResponse));
    }

    NProto::TError HandleReadBlocksRequest(
        void* context,
        TCallContextPtr callContext,
        NProto::TReadDeviceBlocksRequest& request,
        TStringBuf requestData,
        TStringBuf out) const
    {
        if (Y_UNLIKELY(requestData.length() != 0)) {
            return MakeError(E_ARGUMENT);
        }

        auto device = GetDevice(
            request.GetDeviceUUID(),
            request.GetHeaders().GetClientId(),
            NProto::VOLUME_ACCESS_READ_ONLY);

        auto req = std::make_shared<NProto::TReadBlocksRequest>();

        req->SetStartIndex(request.GetStartIndex());
        req->SetBlocksCount(request.GetBlocksCount());

        auto future = device->ReadBlocks(
            std::move(callContext),
            std::move(req),
            request.GetBlockSize());

        SubscribeForResponse(
            future,
            context,
            std::move(out),
            TRequestLogDetails(request),
            &TRequestHandler::HandleReadBlocksResponse);

        return {};
    }

    void HandleReadBlocksResponse(
        void* context,
        TStringBuf out,
        TRequestLogDetails rld,
        TFuture<NProto::TReadBlocksResponse> future) const
    {
        auto& response = future.GetValue();
        auto& blocks = response.GetBlocks();
        auto& error = response.GetError();

        NProto::TReadDeviceBlocksResponse proto;

        if (HasError(error)) {
            STORAGE_ERROR_T(LogThrottler, "[" << rld.DeviceUUID
                << "/" << rld.ClientId << "] read error: "
                << error.GetMessage() << " (" << error.GetCode() << ")");

            *proto.MutableError() = error;
        }

        TStackVec<IOutputStream::TPart> parts;
        parts.reserve(blocks.BuffersSize());

        for (auto buffer: blocks.GetBuffers()) {
            parts.emplace_back(TStringBuf(buffer));
        }

        size_t bytes = Serializer->Serialize(
            out,
            TBlockStoreProtocol::ReadDeviceBlocksResponse,
            proto,
            TContIOVector(parts.data(), parts.size()));

        if (auto ep = Endpoint.lock()) {
            ep->SendResponse(context, bytes);
        }
    }

    NProto::TError HandleWriteBlocksRequest(
        void* context,
        TCallContextPtr callContext,
        NProto::TWriteDeviceBlocksRequest& request,
        TStringBuf requestData,
        TStringBuf out) const
    {
        if (Y_UNLIKELY(requestData.length() == 0)) {
            return MakeError(E_ARGUMENT);
        }

        auto device = GetDevice(
            request.GetDeviceUUID(),
            request.GetHeaders().GetClientId(),
            NProto::VOLUME_ACCESS_READ_WRITE);

        auto req = std::make_shared<NProto::TWriteBlocksRequest>();

        req->SetStartIndex(request.GetStartIndex());
        req->MutableBlocks()->AddBuffers(
            requestData.data(),
            requestData.length());

        auto future = device->WriteBlocks(
            std::move(callContext),
            std::move(req),
            request.GetBlockSize());

        SubscribeForResponse(
            future,
            context,
            std::move(out),
            TRequestLogDetails(request),
            &TRequestHandler::HandleWriteBlocksResponse);

        return {};
    }

    void HandleWriteBlocksResponse(
        void* context,
        TStringBuf out,
        TRequestLogDetails rld,
        TFuture<NProto::TWriteBlocksResponse> future) const
    {
        auto& response = future.GetValue();
        auto& error = response.GetError();

        NProto::TWriteDeviceBlocksResponse proto;

        if (HasError(error)) {
            STORAGE_ERROR_T(LogThrottler, "[" << rld.DeviceUUID
                << "/" << rld.ClientId << "] write error: "
                << error.GetMessage() << " (" << error.GetCode() << ")");

            *proto.MutableError() = error;
        }

        size_t bytes = Serializer->Serialize(
            out,
            TBlockStoreProtocol::WriteDeviceBlocksResponse,
            proto,
            TContIOVector(nullptr, 0));

        if (auto ep = Endpoint.lock()) {
            ep->SendResponse(context, bytes);
        }
    }

    NProto::TError HandleZeroBlocksRequest(
        void* context,
        TCallContextPtr callContext,
        NProto::TZeroDeviceBlocksRequest& request,
        TStringBuf requestData,
        TStringBuf out) const
    {
        if (Y_UNLIKELY(requestData.length() != 0)) {
            return MakeError(E_ARGUMENT);
        }

        auto device = GetDevice(
            request.GetDeviceUUID(),
            request.GetHeaders().GetClientId(),
            NProto::VOLUME_ACCESS_READ_WRITE);

        auto req = std::make_shared<NProto::TZeroBlocksRequest>();
        req->SetStartIndex(request.GetStartIndex());
        req->SetBlocksCount(request.GetBlocksCount());

        auto future = device->ZeroBlocks(
            std::move(callContext),
            std::move(req),
            request.GetBlockSize());

        SubscribeForResponse(
            future,
            context,
            std::move(out),
            TRequestLogDetails(request),
            &TRequestHandler::HandleZeroBlocksResponse);

        return {};
    }

    void HandleZeroBlocksResponse(
        void* context,
        TStringBuf out,
        TRequestLogDetails rld,
        TFuture<NProto::TZeroBlocksResponse> future) const
    {
        auto& response = future.GetValue();
        auto& error = response.GetError();

        NProto::TZeroDeviceBlocksResponse proto;

        if (HasError(error)) {
            STORAGE_ERROR_T(LogThrottler, "[" << rld.DeviceUUID
                << "/" << rld.ClientId << "] zero error: "
                << error.GetMessage() << " (" << error.GetCode() << ")");

            *proto.MutableError() = error;
        }

        size_t bytes = Serializer->Serialize(
            out,
            TBlockStoreProtocol::ZeroDeviceBlocksResponse,
            proto,
            TContIOVector(nullptr, 0));

        if (auto ep = Endpoint.lock()) {
            ep->SendResponse(context, bytes);
        }
    }

    NProto::TError HandleChecksumBlocksRequest(
        void* context,
        TCallContextPtr callContext,
        NProto::TChecksumDeviceBlocksRequest& request,
        TStringBuf requestData,
        TStringBuf out) const
    {
        if (Y_UNLIKELY(requestData.length() != 0)) {
            return MakeError(E_ARGUMENT);
        }

        auto device = GetDevice(
            request.GetDeviceUUID(),
            request.GetHeaders().GetClientId(),
            NProto::VOLUME_ACCESS_READ_ONLY);

        auto req = std::make_shared<NProto::TReadBlocksRequest>();

        req->SetStartIndex(request.GetStartIndex());
        req->SetBlocksCount(request.GetBlocksCount());

        auto future = device->ReadBlocks(
            std::move(callContext),
            std::move(req),
            request.GetBlockSize());

        SubscribeForResponse(
            future,
            context,
            std::move(out),
            TRequestLogDetails(request),
            &TRequestHandler::HandleChecksumBlocksResponse);

        return {};
    }

    void HandleChecksumBlocksResponse(
        void* context,
        TStringBuf out,
        TRequestLogDetails rld,
        TFuture<NProto::TReadBlocksResponse> future) const
    {
        auto& response = future.GetValue();
        auto& blocks = response.GetBlocks();
        auto& error = response.GetError();

        NProto::TChecksumDeviceBlocksResponse proto;

        if (HasError(error)) {
            STORAGE_ERROR_T(LogThrottler, "[" << rld.DeviceUUID
                << "/" << rld.ClientId << "] checksum(read) error: "
                << error.GetMessage() << " (" << error.GetCode() << ")");

            *proto.MutableError() = error;
        }

        TBlockChecksum checksum;
        for (const auto& buffer: blocks.GetBuffers()) {
            checksum.Extend(buffer.Data(), buffer.Size());
        }
        proto.SetChecksum(checksum.GetValue());

        size_t bytes = Serializer->Serialize(
            out,
            TBlockStoreProtocol::ChecksumDeviceBlocksResponse,
            proto,
            TContIOVector(nullptr, 0));

        if (auto ep = Endpoint.lock()) {
            ep->SendResponse(context, bytes);
        }
    }
};

///////////////////////////////////////////////////////////////////////////////

class TRdmaTarget final
    : public IRdmaTarget
{
private:
    const NProto::TRdmaEndpoint Config;

    std::shared_ptr<TRequestHandler> Handler;
    ILoggingServicePtr Logging;
    NRdma::IServerPtr Server;

    TLog Log;

public:
    TRdmaTarget(
            NProto::TRdmaEndpoint config,
            ILoggingServicePtr logging,
            NRdma::IServerPtr server,
            TDeviceClientPtr deviceClient,
            THashMap<TString, TStorageAdapterPtr> devices,
            ITaskQueuePtr taskQueue)
        : Config(std::move(config))
        , Logging(std::move(logging))
        , Server(std::move(server))
    {
        Handler = std::make_shared<TRequestHandler>(
            std::move(devices),
            std::move(taskQueue),
            std::move(deviceClient));
    }

    void Start() override
    {
        Log = Logging->CreateLog("BLOCKSTORE_DISK_AGENT");

        auto endpoint = Server->StartEndpoint(
            Config.GetHost(),
            Config.GetPort(),
            Handler);

        if (endpoint == nullptr) {
            STORAGE_ERROR("unable to set up RDMA endpoint");
            return;
        }

        Handler->Init(std::move(endpoint), std::move(Log));
    }

    void Stop() override
    {}
};

}   // namespace

IRdmaTargetPtr CreateRdmaTarget(
    NProto::TRdmaEndpoint config,
    ILoggingServicePtr logging,
    NRdma::IServerPtr server,
    TDeviceClientPtr deviceClient,
    THashMap<TString, TStorageAdapterPtr> devices)
{
    // TODO
    auto threadPool = CreateThreadPool("RDMA", 1);
    threadPool->Start();

    return std::make_shared<TRdmaTarget>(
        std::move(config),
        std::move(logging),
        std::move(server),
        std::move(deviceClient),
        std::move(devices),
        std::move(threadPool));
}

}   // namespace NCloud::NBlockStore::NStorage
