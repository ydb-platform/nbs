#include "side_channel.h"

#include <cloud/filestore/libs/storage/fastshard/client/async_client.h>
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

class TTCPSideChannel: public ISideChannel
{
private:
    TAdaptiveLock Lock;

    bool IsOverloaded = false;
    // Set once on the first Update() with a valid port; never reset.
    bool ConnectStarted = false;
    TFuture<IAsyncEndpointPtr> ConnectFuture;
    // Promoted from ConnectFuture once HasValue() is true.
    // TODO: endpoint pool
    std::shared_ptr<IAsyncEndpoint> ReadyEndpoint;

    TAsyncClient Client;

    // Returns the ready endpoint under lock, promoting ConnectFuture if done.
    // Returns nullptr if not connected yet.
    std::shared_ptr<IAsyncEndpoint> GetEndpointLocked()
    {
        if (ReadyEndpoint) {
            return ReadyEndpoint;
        }
        if (!ConnectFuture.Initialized() || !ConnectFuture.HasValue()) {
            return nullptr;
        }
        auto ep = ConnectFuture.ExtractValue();
        if (ep) {
            ReadyEndpoint = std::move(ep);
        }
        ConnectFuture = {};
        return ReadyEndpoint;
    }

    template <typename TReq, typename TResp, typename FillBody, typename ExtractBody>
    bool Dispatch(
        std::shared_ptr<TReq> request,
        TPromise<TResp> response,
        FillBody fillBody,
        ExtractBody extractBody)
    {
        std::shared_ptr<IAsyncEndpoint> ep;
        {
            auto guard = Guard(Lock);
            if (IsOverloaded) {
                return false;
            }
            ep = GetEndpointLocked();
        }
        if (!ep) {
            return false;
        }

        TRequest req;
        req.SetFileSystemId(request->GetFileSystemId());
        fillBody(req, *request);

        ep->Send(std::move(req)).Subscribe(
            [response = std::move(response), extractBody](
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
            });

        return true;
    }

public:
    bool ExecuteRequest(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request,
        TPromise<NProto::TReadDataResponse> response) override
    {
        Y_UNUSED(callContext);

        return Dispatch(
            std::move(request),
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
            std::move(request),
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
        bool shouldConnect = false;

        with_lock (Lock) {
            IsOverloaded = backendInfo.GetIsOverloaded();
            shouldConnect = host && port != 0 && !ConnectStarted;
            if (shouldConnect) {
                ConnectStarted = true;
            }
        }

        if (!shouldConnect) {
            return;
        }

        with_lock (Lock) {
            ConnectFuture = Client.Connect(host, port);
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ISideChannelPtr CreateTCPSideChannel()
{
    return std::make_shared<TTCPSideChannel>();
}

}   // namespace NCloud::NFileStore
