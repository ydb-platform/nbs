#include "side_channel.h"

#include <cloud/filestore/libs/storage/fastshard/server/protos/fastshard.pb.h>

#include <cloud/filestore/public/api/protos/data.pb.h>
#include <cloud/filestore/public/api/protos/headers.pb.h>

#include <util/system/spinlock.h>

namespace NCloud::NFileStore {

using namespace NStorage::NFastShard;
using namespace NStorage::NFastShard::NProtoSrv;
using namespace NThreading;

namespace {

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
            if (!Endpoints) {
                return nullptr;
            }

            e = Endpoints.front();
            Endpoints.pop_front();
            *generation = Generation;
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

class TTCPSideChannel: public ISideChannel
{
private:
    TAdaptiveLock Lock;

    TString Host;
    ui16 Port = 0;

    TLog Log;
    std::shared_ptr<IAsyncClient> Client;
    TEndpointPoolPtr EndpointPool;

public:
    TTCPSideChannel(
            ILoggingService& logging,
            std::shared_ptr<IAsyncClient> client)
        : Log(logging.CreateLog("TCP_SIDE_CHANNEL"))
        , Client(std::move(client))
        , EndpointPool(MakeIntrusive<TEndpointPool>(Log))
    {}

public:
    bool ExecuteRequest(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request,
        TPromise<NProto::TReadDataResponse> response) override
    {
        Y_UNUSED(callContext);

        return Dispatch(
            *request,
            std::move(response),
            [](TRequest& req, const NProto::TReadDataRequest& body) {
                *req.MutableReadData() = body;
            },
            [](NProto::TReadDataResponse& resp, TResponse& r) {
                resp = std::move(*r.MutableReadData());
            });
    }

    bool ExecuteRequest(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataRequest> request,
        TPromise<NProto::TWriteDataResponse> response) override
    {
        Y_UNUSED(callContext);

        return Dispatch(
            *request,
            std::move(response),
            [](TRequest& req, const NProto::TWriteDataRequest& body) {
                *req.MutableWriteData() = body;
            },
            [](NProto::TWriteDataResponse& resp, TResponse& r) {
                resp = std::move(*r.MutableWriteData());
            });
    }

    void Update(const NProto::TBackendInfo& backendInfo) override
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
                    << ", generation=" << generation);
            }

            Host = host;
            Port = port;
        }

        TryConnect().Subscribe(
            [
                ep = EndpointPool,
                generation
            ] (const TFuture<IAsyncEndpointPtr>& f) {
                ep->Push(UnsafeExtractValue(f), generation);
            });
    }

private:
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
        ui32 generation = 0;
        IAsyncEndpointPtr e = EndpointPool->Pop(&generation);

        auto send = [response = std::move(response), extractBody](
            TFuture<TResponse> f) mutable
        {
            auto r = f.ExtractValue();
            TResp resp;
            if (r.HasError()) {
                *resp.MutableError() = r.GetError();
            } else {
                extractBody(resp, r);
            }
            response.SetValue(std::move(resp));
        };

        TRequest req;
        req.SetFileSystemId(request.GetFileSystemId());
        fillBody(req, request);

        if (e) {
            auto result = e->Send(std::move(req));
            result.Subscribe([
                ep = EndpointPool,
                e = std::move(e),
                generation
            ] (const TFuture<TResponse>&) mutable {
                ep->Push(std::move(e), generation);
            });
            result.Subscribe(std::move(send));

            return true;
        }

        auto connection = TryConnect();
        if (!connection.Initialized()) {
            return false;
        }

        connection.Subscribe([
            req = std::move(req),
            send = std::move(send),
            ep = EndpointPool,
            generation
        ] (const TFuture<IAsyncEndpointPtr>& f) mutable {
            auto e = UnsafeExtractValue(f);

            if (!e) {
                TResponse errorResponse;
                errorResponse.MutableError()->SetCode(E_UNAVAILABLE);
                send(MakeFuture(std::move(errorResponse)));
                return;
            }

            auto result = e->Send(std::move(req));
            result.Subscribe([
                ep = std::move(ep),
                e = std::move(e),
                generation
            ] (const TFuture<TResponse>&) mutable {
                ep->Push(std::move(e), generation);
            });
            result.Subscribe(std::move(send));
        });

        return true;
    }

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

        auto connection = Client->Connect(Host, Port);
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
