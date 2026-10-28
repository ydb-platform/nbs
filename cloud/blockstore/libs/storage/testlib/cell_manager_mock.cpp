#include "cell_manager_mock.h"

#include <cloud/blockstore/libs/cells/iface/cell_manager.h>
#include <cloud/blockstore/libs/service/service_method.h>
#include <cloud/blockstore/libs/storage/api/service.h>

#include <cloud/storage/core/libs/actors/helpers.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NThreading;

namespace {

struct TDescribeVolumeMethod: TBlockStoreDescribeVolumeMethod
{
    using TRequestEvent = TEvService::TEvDescribeVolumeRequest;
    using TResponseEvent = TEvService::TEvDescribeVolumeResponse;
};

struct TExecuteActionMethod: TBlockStoreExecuteActionMethod
{
    using TRequestEvent = TEvService::TEvExecuteActionRequest;
    using TResponseEvent = TEvService::TEvExecuteActionResponse;
};

template <typename TMethod>
class TServiceRequestActor final
    : public TActorBootstrapped<TServiceRequestActor<TMethod>>
{
private:
    using TBase = TActorBootstrapped<TServiceRequestActor<TMethod>>;
    using TThis = TServiceRequestActor<TMethod>;
    using TRequestEvent = typename TMethod::TRequestEvent;
    using TResponseEvent = typename TMethod::TResponseEvent;
    using TBase::Become;
    using TBase::Die;

    const TCallContextPtr CallContext;
    const std::shared_ptr<typename TMethod::TRequest> Request;
    TPromise<typename TMethod::TResponse> Promise;

public:
    TServiceRequestActor(TCallContextPtr callContext,
                         std::shared_ptr<typename TMethod::TRequest> request,
                         TPromise<typename TMethod::TResponse> promise)
        : CallContext(std::move(callContext))
        , Request(std::move(request))
        , Promise(std::move(promise))
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        Become(&TThis::StateWork);
        NCloud::SendWithUndeliveryTracking(
            ctx, MakeStorageServiceId(),
            std::make_unique<TRequestEvent>(CallContext, *Request));
        ctx.Schedule(
            TDuration::MilliSeconds(Request->GetHeaders().GetRequestTimeout()),
            new TEvents::TEvWakeup());
    }

private:
    void HandleResponse(const typename TResponseEvent::TPtr& ev,
                        const TActorContext& ctx)
    {
        Promise.SetValue(std::move(ev->Get()->Record));
        Die(ctx);
    }

    void Fail(const TActorContext& ctx, ui32 code)
    {
        Promise.SetValue(ErrorResponse<typename TMethod::TResponse>(
            code, "Test cell service did not answer the request"));
        Die(ctx);
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TResponseEvent, HandleResponse);
            case TRequestEvent::EventType:
            case TEvents::TEvPoisonPill::EventType:
                Fail(TActivationContext::AsActorContext(), E_REJECTED);
                break;
            case TEvents::TEvWakeup::EventType:
                Fail(TActivationContext::AsActorContext(), E_TIMEOUT);
                break;
        }
    }
};

class TCellService final: public TBlockStoreImpl<TCellService, IBlockStore>
{
private:
    TTestActorRuntimeBase& Runtime;
    const ui32 NodeIdx;

public:
    TCellService(TTestActorRuntimeBase& runtime, ui32 nodeIdx)
        : Runtime(runtime)
        , NodeIdx(nodeIdx)
    {}

    void Start() override
    {}

    void Stop() override
    {}

    TStorageBuffer AllocateBuffer(size_t) override
    {
        return {};
    }

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        if constexpr (std::is_same_v<TMethod, TBlockStoreDescribeVolumeMethod>)
        {
            return SendRequest<TDescribeVolumeMethod>(std::move(callContext),
                                                      std::move(request));
        } else if constexpr (
            std::is_same_v<TMethod, TBlockStoreExecuteActionMethod>)
        {
            return SendRequest<TExecuteActionMethod>(std::move(callContext),
                                                     std::move(request));
        } else {
            return MakeFuture(ErrorResponse<typename TMethod::TResponse>(
                E_NOT_IMPLEMENTED,
                "Test cell transport supports only DescribeVolume and "
                "ExecuteAction"));
        }
    }

private:
    template <typename TMethod>
    TFuture<typename TMethod::TResponse> SendRequest(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        auto promise = NewPromise<typename TMethod::TResponse>();
        const auto actor = Runtime.Register(
            new TServiceRequestActor<TMethod>(
                std::move(callContext), std::move(request), promise), NodeIdx);
        Runtime.EnableScheduleForActor(actor);
        return promise.GetFuture();
    }
};

struct TConnection final: NCells::ICellConnection
{
    IBlockStorePtr Service;

    explicit TConnection(IBlockStorePtr service)
        : Service(std::move(service))
    {}

    TString GetHost() const override
    {
        return "test-cell-host";
    }

    IBlockStorePtr GetService() override
    {
        return Service;
    }

    IStoragePtr GetStorage() override
    {
        return {};
    }
};

struct TCellManagerMock final: NCells::ICellManager
{
    TTestActorRuntimeBase& Runtime;
    const THashMap<TString, ui32> CellNodes;

    TCellManagerMock(TTestActorRuntimeBase& runtime,
                     THashMap<TString, ui32> cellNodes)
        : ICellManager(nullptr)
        , Runtime(runtime)
        , CellNodes(std::move(cellNodes))
    {}

    NCells::TCellConnectionFuture CreateConnection(
        const TString& cellId, const TString&,
        const NClient::TClientAppConfigPtr&,
        NCells::ICellConnectionObserverPtr) override
    {
        const auto* node = CellNodes.FindPtr(cellId);
        if (!node) {
            return MakeFuture(
                TResultOrError<NCells::ICellConnectionPtr>(MakeError(
                    E_NOT_FOUND, "Test cell is not configured: " + cellId)));
        }
        return MakeFuture(TResultOrError<NCells::ICellConnectionPtr>(
            std::make_shared<TConnection>(
                std::make_shared<TCellService>(Runtime, *node))));
    }

    NCells::TDescribeVolumeFuture DescribeVolume(
        TCallContextPtr, const TString&, const NProto::THeaders&,
        const NProto::TClientConfig&) override
    {
        return MakeFuture(ErrorResponse<NProto::TDescribeVolumeResponse>(
            E_NOT_IMPLEMENTED,
            "Test cell manager does not search other cells"));
    }

    std::shared_ptr<NCells::TCellInboundActivity> GetInboundActivity() override
    {
        return {};
    }

    NCells::TCellsSnapshot GetSnapshot() override
    {
        return {};
    }

    TFuture<TVector<NCells::TCellDescribeResult>> SearchVolume(
        TString, TDuration) override
    {
        return MakeFuture(TVector<NCells::TCellDescribeResult>());
    }

    void Start() override
    {}

    void Stop() override
    {}
};

}   // namespace

NCells::ICellManagerPtr CreateCellManagerMock(TTestActorRuntimeBase& runtime,
                                              THashMap<TString, ui32> cellNodes)
{
    return std::make_shared<TCellManagerMock>(runtime, std::move(cellNodes));
}

}   // namespace NCloud::NBlockStore::NStorage
