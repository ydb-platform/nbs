#include "side_channel.h"

#include <cloud/filestore/libs/storage/fastshard/server/protos/fastshard.pb.h>
#include <cloud/filestore/libs/storage/model/utils.h>

#include <cloud/filestore/public/api/protos/data.pb.h>
#include <cloud/filestore/public/api/protos/headers.pb.h>

#include <util/system/spinlock.h>

namespace NCloud::NFileStore {

using namespace NStorage::NFastShard;
using namespace NStorage::NFastShard::NProtoSrv;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

void MoveIovecsToBuffer(NProto::TWriteDataRequest& request)
{
    ui64 totalLength = 0;
    for (const auto& iovec: request.GetIovecs()) {
        totalLength += iovec.GetLength();
    }

    TString buffer;
    buffer.ReserveAndResize(totalLength);
    char* dst = buffer.begin();
    for (const auto& iovec: request.GetIovecs()) {
        memcpy(
            dst,
            reinterpret_cast<const char*>(iovec.GetBase()),
            iovec.GetLength());
        dst += iovec.GetLength();
    }

    request.SetBuffer(std::move(buffer));
    request.SetBufferOffset(0);
    request.MutableIovecs()->Clear();
}

void MoveBufferToIovecs(
    NProto::TReadDataResponse& response,
    google::protobuf::RepeatedPtrField<NProto::TIovec>& iovecs)
{
    if (iovecs.empty() || response.GetBuffer().empty()) {
        return;
    }

    ui32 currentOffset = response.GetBufferOffset();
    const ui64 bufferSize = response.GetBuffer().size();
    const char* src = response.GetBuffer().data();
    for (const auto& iovec: iovecs) {
        if (currentOffset >= bufferSize) {
            break;
        }
        const ui64 bytesToCopy =
            Min<ui64>(iovec.GetLength(), bufferSize - currentOffset);
        memcpy(
            reinterpret_cast<char*>(iovec.GetBase()),
            src + currentOffset,
            bytesToCopy);
        currentOffset += bytesToCopy;
    }

    response.SetLength(bufferSize - response.GetBufferOffset());
    response.ClearBuffer();
    response.SetBufferOffset(0);
}

////////////////////////////////////////////////////////////////////////////////

class TEndpointPool: public TThrRefBase
{
private:
    TLog Log;
    TAdaptiveLock Lock;
    TDeque<IAsyncEndpointPtr> Endpoints;
    ui32 Generation = 0;

public:
    explicit TEndpointPool(TLog log)
        : Log(std::move(log))
    {}

public:
    IAsyncEndpointPtr Pop(ui32* generation)
    {
        IAsyncEndpointPtr e;

        with_lock (Lock) {
            *generation = Generation;

            if (!Endpoints) {
                return nullptr;
            }

            e = Endpoints.front();
            Endpoints.pop_front();
        }

        STORAGE_DEBUG("endpoint popped from pool, generation=" << *generation);
        return e;
    }

    void Push(IAsyncEndpointPtr e, ui32 generation)
    {
        if (!e) {
            return;
        }

        with_lock (Lock) {
            if (generation < Generation) {
                return;
            }

            Endpoints.push_back(std::move(e));
        }

        STORAGE_DEBUG("endpoint added to pool, generation=" << generation);
    }

    ui32 AddressChanged()
    {
        ui32 generation = 0;
        with_lock (Lock) {
            Endpoints.clear();
            generation = ++Generation;
        }

        STORAGE_DEBUG("address changed, generation=" << generation);
        return generation;
    }
};

using TEndpointPoolPtr = TIntrusivePtr<TEndpointPool>;

////////////////////////////////////////////////////////////////////////////////

class TFileSystemTCPSideChannel: public TThrRefBase
{
private:
    TAdaptiveLock Lock;

    TString Host;
    ui16 Port = 0;

    const TString ActualFileSystemId;
    TLog Log;
    std::shared_ptr<IAsyncClient> Client;
    TEndpointPoolPtr EndpointPool;

public:
    TFileSystemTCPSideChannel(
            TString actualFileSystemId,
            TLog log,
            std::shared_ptr<IAsyncClient> client)
        : ActualFileSystemId(std::move(actualFileSystemId))
        , Log(std::move(log))
        , Client(std::move(client))
        , EndpointPool(MakeIntrusive<TEndpointPool>(Log))
    {}

public:
    void Update(const NProto::TBackendInfo& backendInfo)
    {
        const ui32 port = backendInfo.GetFastShardPort();
        const TString& host = backendInfo.GetFastShardHost();

        ui32 generation = 0;
        with_lock (Lock) {
            if (Host != host || Port != port) {
                generation = EndpointPool->AddressChanged();
                STORAGE_INFO("updated side channel connection params"
                    << ": host=" << host
                    << ", port=" << port
                    << ", fileSystemId=" << backendInfo.GetActualFileSystemId()
                    << ", generation=" << generation);
            }

            Host = host;
            Port = port;
        }

        auto connection = TryConnect();
        if (!connection.Initialized()) {
            return;
        }

        connection.Subscribe(
            [
                ep = EndpointPool,
                generation
            ] (const TFuture<IAsyncEndpointPtr>& f) {
                ep->Push(UnsafeExtractValue(f), generation);
            });
    }

    template <typename TReq,
              typename TResp,
              typename TFillBody,
              typename TExtractBody>
    bool Dispatch(
        const TReq& request,
        TPromise<TResp> response,
        TFillBody fillBody,
        TExtractBody extractBody)
    {
        STORAGE_TRACE("dispatching request for"
            << ": address=" << GetAddressDebugString()
            << ", fileSystemId=" << ActualFileSystemId);

        ui32 generation = 0;
        IAsyncEndpointPtr e = EndpointPool->Pop(&generation);

        auto complete = [
            response = std::move(response),
            ep = EndpointPool,
            extractBody
        ](IAsyncEndpointPtr e, ui32 generation, TResponse r) mutable
        {
            TResp resp;
            bool shardMoved = false;
            if (r.HasError()) {
                if (r.GetError().GetCode() == E_NOT_FOUND) {
                    shardMoved = true;
                    *resp.MutableError() = MakeError(
                        E_REJECTED,
                        TStringBuilder() << "shard moved: "
                            << FormatError(r.GetError()));
                } else {
                    *resp.MutableError() = r.GetError();
                }
            } else {
                extractBody(resp, r);
            }
            response.SetValue(std::move(resp));

            if (!shardMoved) {
                ep->Push(std::move(e), generation);
            }
        };

        TRequest req;
        req.SetFileSystemId(ActualFileSystemId);
        fillBody(req, request);

        if (e) {
            auto result = e->Send(std::move(req));
            result.Subscribe([
                complete = std::move(complete),
                ep = EndpointPool,
                e = std::move(e),
                generation
            ] (const TFuture<TResponse>& f) mutable {
                complete(std::move(e), generation, UnsafeExtractValue(f));
            });

            STORAGE_TRACE("sent request for"
                << ": address=" << GetAddressDebugString()
                << ", fileSystemId=" << ActualFileSystemId);

            return true;
        }

        auto connection = TryConnect();
        if (!connection.Initialized()) {
            STORAGE_TRACE("conn not initialized for"
                << ": address=" << GetAddressDebugString()
                << ", fileSystemId=" << ActualFileSystemId);
            return false;
        }

        connection.Subscribe([
            req = std::move(req),
            complete = std::move(complete),
            ep = EndpointPool,
            generation
        ] (const TFuture<IAsyncEndpointPtr>& f) mutable {
            auto e = UnsafeExtractValue(f);

            if (!e) {
                TResponse errorResponse;
                errorResponse.MutableError()->SetCode(E_UNAVAILABLE);
                complete(nullptr, 0, std::move(errorResponse));
                return;
            }

            auto result = e->Send(std::move(req));
            result.Subscribe([
                complete = std::move(complete),
                ep = std::move(ep),
                e = std::move(e),
                generation
            ] (const TFuture<TResponse>& f) mutable {
                complete(std::move(e), generation, UnsafeExtractValue(f));
            });
        });

        STORAGE_TRACE("connecting:"
            << ": address=" << GetAddressDebugString()
            << ", fileSystemId=" << ActualFileSystemId);
        return true;
    }

private:
    TFuture<IAsyncEndpointPtr> TryConnect()
    {
        TString host;
        ui16 port = 0;
        with_lock (Lock) {
            if (!Host || !Port) {
                return {};
            }

            host = Host;
            port = Port;
        }

        auto connection = Client->Connect(host, port);
        connection.Subscribe([
            Log = Log,
            host = std::move(host),
            port = port
        ] (const TFuture<IAsyncEndpointPtr>& f) {
            if (f.GetValue()) {
                STORAGE_INFO("connected to host=" << host << ", port=" << port);
            } else {
                STORAGE_WARN("failed to connect to host=" << host
                    << ", port=" << port);
            }
        });

        return connection;
    }

    TString GetAddressDebugString() const
    {
        TString host;
        ui16 port = 0;
        with_lock(Lock) {
            host = Host;
            port = Port;
        }

        return TStringBuilder() << "[" << host << "]:" << port;
    }
};

using TFileSystemTCPSideChannelPtr = TIntrusivePtr<TFileSystemTCPSideChannel>;

////////////////////////////////////////////////////////////////////////////////

class TTCPSideChannel: public ISideChannel
{
private:
    TAdaptiveLock Lock;

    THashMap<TString, TFileSystemTCPSideChannelPtr> Id2Channel;

    TLog Log;
    std::shared_ptr<IAsyncClient> Client;

public:
    TTCPSideChannel(
            ILoggingService& logging,
            std::shared_ptr<IAsyncClient> client)
        : Log(logging.CreateLog("TCP_SIDE_CHANNEL"))
        , Client(std::move(client))
    {}

public:
    bool ExecuteRequest(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request,
        TPromise<NProto::TReadDataResponse> response) override
    {
        Y_UNUSED(callContext);

        // Iovec base addresses are pointers in this process and cannot be
        // sent over the wire. Strip them from the request and copy data
        // from the response Buffer back into the original iovecs.
        google::protobuf::RepeatedPtrField<NProto::TIovec> iovecs;
        if (request->IovecsSize() > 0) {
            iovecs.Swap(request->MutableIovecs());
        }

        auto channel = AccessFileSystemChannel(*request);

        return channel->Dispatch(
            *request,
            std::move(response),
            [](TRequest& req, const NProto::TReadDataRequest& body) {
                *req.MutableReadData() = body;
            },
            [iovecs = std::move(iovecs)](
                NProto::TReadDataResponse& resp,
                TResponse& r) mutable
            {
                resp = std::move(*r.MutableReadData());
                MoveBufferToIovecs(resp, iovecs);
            });
    }

    bool ExecuteRequest(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataRequest> request,
        TPromise<NProto::TWriteDataResponse> response) override
    {
        Y_UNUSED(callContext);

        const ui64 iovecsSize = request->IovecsSize();
        const ui64 bufferOffset = request->GetBufferOffset();
        if (iovecsSize > 0 && bufferOffset > 0) {
            NProto::TWriteDataResponse errorResponse;
            *errorResponse.MutableError() = MakeError(
                E_ARGUMENT,
                TStringBuilder() << "BufferOffset=" << bufferOffset
                    << ", iovecsSize=" << iovecsSize << " are both nonzero");
            response.SetValue(std::move(errorResponse));
            return true;
        }

        auto channel = AccessFileSystemChannel(*request);

        return channel->Dispatch(
            *request,
            std::move(response),
            [](TRequest& req, const NProto::TWriteDataRequest& body) {
                auto& writeData = *req.MutableWriteData();
                writeData = body;

                // Iovec base addresses are pointers in this process and cannot
                // be sent over the wire. Materialize them into Buffer before
                // dispatching.
                if (writeData.IovecsSize() > 0) {
                    MoveIovecsToBuffer(writeData);
                }
            },
            [](NProto::TWriteDataResponse& resp, TResponse& r) {
                resp = std::move(*r.MutableWriteData());
            });
    }

    void Update(const NProto::TBackendInfo& backendInfo) override
    {
        auto channel = AccessFileSystemChannel(
            backendInfo.GetActualFileSystemId());

        channel->Update(backendInfo);
    }

private:
    TString MakeFileSystemId(const TString& mainFileSystemId, ui32 shardNo)
        const
    {
        // TODO(#5894): Properly obtain shardNo -> shardFileSystemId mapping.
        //  This code is a temporary hack intended to be used in the prototype.
        if (!shardNo) {
            return mainFileSystemId;
        }

        constexpr TStringBuf ShardNumPrefix = "_s";
        return TStringBuilder() << mainFileSystemId
            << ShardNumPrefix << shardNo;
    }

    TFileSystemTCPSideChannelPtr AccessFileSystemChannel(
        const TString& actualFileSystemId)
    {
        with_lock (Lock) {
            auto& channel = Id2Channel[actualFileSystemId];
            if (!channel) {
                STORAGE_INFO("initializing channel for fileSystemId="
                    << actualFileSystemId);
                channel = MakeIntrusive<TFileSystemTCPSideChannel>(
                    actualFileSystemId,
                    Log,
                    Client);
            }

            return channel;
        }
    }

    template <typename T>
    TFileSystemTCPSideChannelPtr AccessFileSystemChannel(const T& request)
    {
        auto fileSystemId = MakeFileSystemId(
            request.GetFileSystemId(),
            NStorage::ExtractShardNo(request.GetHandle()));

        return AccessFileSystemChannel(fileSystemId);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ISideChannelPtr CreateTCPSideChannel(
    ILoggingService& logging,
    std::shared_ptr<IAsyncClient> client)
{
    return std::make_shared<TTCPSideChannel>(
        logging,
        std::move(client));
}

}   // namespace NCloud::NFileStore
