#include "service_su.h"

#include "remote_storage_provider.h"

#include <cloud/blockstore/config/server.pb.h>

#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/server/config.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>

#include <cloud/blockstore/libs/rdma/iface/client.h>

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/durable.h>
#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/client/throttling.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/encryption/encryption_client.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NServer {

using namespace NActors;
using namespace NThreading;

using namespace NCloud::NBlockStore::NStorage;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////
struct TRemoteStorage
    : public IStorage
    , std::enable_shared_from_this<TRemoteStorage>
{
    const IBlockStorePtr Endpoint;

    TRemoteStorage(
            IBlockStorePtr endpoint)
        : Endpoint(std::move(endpoint))
    {

    }

    NThreading::TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        return Endpoint->ZeroBlocks(std::move(callContext), std::move(request));
    }

    NThreading::TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        return Endpoint->ReadBlocksLocal(std::move(callContext), std::move(request));
    }

    NThreading::TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        return Endpoint->WriteBlocksLocal(std::move(callContext), std::move(request));
    }

    NThreading::TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override
    {
        Y_UNUSED(method);
        return MakeFuture(MakeError(E_NOT_IMPLEMENTED));
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

   void ReportIOError() override
   {
   }
};


struct TDescribeResult
    : public std::enable_shared_from_this<TDescribeResult>
{
    TDescribeResult(THashMap<TString, TFuture<NProto::TDescribeVolumeResponse>> futures)
        : Futures(std::move(futures))
        , Promise(NewPromise<NProto::TDescribeVolumeResponse>())
    {
    }

    void Setup()
    {
        auto self = shared_from_this();
        for (auto& f: Futures) {
            f.second.Subscribe([=, suId = f.first] (const auto& future) {
                if (self->Promise.HasValue()) {
                    return;
                }
                auto response = future.GetValue();
                if (!HasError(response)) {
                    response.SetShardId(suId);
                    self->Promise.SetValue(response);
                    return;
                }
                auto old = self->Counter.fetch_add(1, std::memory_order_relaxed);
                if (old == self->Futures.size() - 1) {
                    self->Promise.SetValue(response);
                }
            });
        }
    }

    std::atomic<ui64> Counter{0};
    THashMap<TString, TFuture<NProto::TDescribeVolumeResponse>> Futures;
    TPromise<NProto::TDescribeVolumeResponse> Promise;
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStoragePtr CreateRemoteEndpoint(IBlockStorePtr endpoint)
{
    return std::make_shared<TRemoteStorage>(std::move(endpoint));
}

NThreading::TFuture<NProto::TDescribeVolumeResponse> DescribeRemoteVolume(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TDescribeVolumeRequest> request,
    const IBlockStorePtr localService,
    const IRemoteStorageProviderPtr suProvider,
    const ILoggingServicePtr logging,
    NClient::TClientAppConfigPtr clientConfig)
{
        auto Log = logging->CreateLog("BLOCKSTORE_DESCRIBE");
        Y_UNUSED(callContext);

        auto endpoints = suProvider->GetRemoteEndpoints(std::move(clientConfig));

        THashMap<TString, TFuture<NProto::TDescribeVolumeResponse>> futures;

        for (auto& endp: endpoints) {
            auto callContext = MakeIntrusive<TCallContext>();

            STORAGE_ERROR(
                TStringBuilder()
                    << "Send remote Describe Request for volume " << request->GetDiskId());

            auto req = std::make_shared<NProto::TDescribeVolumeRequest>();
            req->CopyFrom(*request);
            req->MutableHeaders()->ClearInternal();

            auto future = endp.second.Service->DescribeVolume(
                callContext,
                std::move(req));

            futures.emplace(endp.first, std::move(future));
        }

        {
            auto callContext = MakeIntrusive<TCallContext>();

            STORAGE_ERROR(
                TStringBuilder()
                    << "Send local Describe Request for volume " << request->GetDiskId());

            auto req = std::make_shared<NProto::TDescribeVolumeRequest>();
            req->CopyFrom(*request);
            req->MutableHeaders()->ClearInternal();

            auto future = localService->DescribeVolume(
                callContext,
                std::move(req));

            futures.emplace("", std::move(future));
        }

        auto waitResult = std::make_shared<TDescribeResult>(std::move(futures));
        waitResult->Setup();

        return waitResult->Promise.GetFuture();
}

}   // namespace NCloud::NBlockStore::NServer
