#include "cell_manager_mock.h"

#include <cloud/blockstore/libs/cells/iface/cell_manager.h>
#include <cloud/blockstore/libs/service/service_method.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/common/constants.h>
#include <cloud/blockstore/libs/storage/api/service.h>

#include <cloud/storage/core/libs/actors/helpers.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TDescribeVolumeMethod: TBlockStoreDescribeVolumeMethod
{
    using TRequestEvent = TEvService::TEvDescribeVolumeRequest;
    using TResponseEvent = TEvService::TEvDescribeVolumeResponse;
};

////////////////////////////////////////////////////////////////////////////////

struct TExecuteActionMethod: TBlockStoreExecuteActionMethod
{
    using TRequestEvent = TEvService::TEvExecuteActionRequest;
    using TResponseEvent = TEvService::TEvExecuteActionResponse;
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteBlocksLocalMethod: TBlockStoreWriteBlocksLocalMethod
{
    using TRequestEvent = TEvService::TEvWriteBlocksLocalRequest;
    using TResponseEvent = TEvService::TEvWriteBlocksLocalResponse;
};

struct TZeroBlocksMethod: TBlockStoreZeroBlocksMethod
{
    using TRequestEvent = TEvService::TEvZeroBlocksRequest;
    using TResponseEvent = TEvService::TEvZeroBlocksResponse;
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
    TServiceRequestActor(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request,
        TPromise<typename TMethod::TResponse> promise)
        : CallContext(std::move(callContext))
        , Request(std::move(request))
        , Promise(std::move(promise))
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        Become(&TThis::StateWork);
        const auto timeout =
            TDuration::MilliSeconds(Request->GetHeaders().GetRequestTimeout());
        NCloud::SendWithUndeliveryTracking(
            ctx,
            MakeStorageServiceId(),
            std::make_unique<TRequestEvent>(CallContext, std::move(*Request)));
        ctx.Schedule(timeout, new TEvents::TEvWakeup());
    }

private:
    void HandleResponse(
        const typename TResponseEvent::TPtr& ev,
        const TActorContext& ctx)
    {
        Promise.SetValue(std::move(ev->Get()->Record));
        Die(ctx);
    }

    void Fail(const TActorContext& ctx, ui32 code)
    {
        Promise.SetValue(
            ErrorResponse<typename TMethod::TResponse>(
                code,
                "Test cell service did not answer the request"));
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

////////////////////////////////////////////////////////////////////////////////

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
            return SendRequest<TDescribeVolumeMethod>(
                std::move(callContext),
                std::move(request));
        }

        if constexpr (std::is_same_v<TMethod, TBlockStoreExecuteActionMethod>) {
            return SendRequest<TExecuteActionMethod>(
                std::move(callContext),
                std::move(request));
        }

        if constexpr (std::is_same_v<TMethod, TBlockStoreWriteBlocksLocalMethod>) {
            return SendRequest<TWriteBlocksLocalMethod>(
                std::move(callContext), std::move(request));
        }
        if constexpr (std::is_same_v<TMethod, TBlockStoreZeroBlocksMethod>) {
            return SendRequest<TZeroBlocksMethod>(
                std::move(callContext), std::move(request));
        }

        return MakeFuture(
            ErrorResponse<typename TMethod::TResponse>(
                E_NOT_IMPLEMENTED,
                "Test cell transport supports only DescribeVolume and "
                "ExecuteAction"));
    }

private:
    template <typename TMethod>
    TFuture<typename TMethod::TResponse> SendRequest(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        auto* headers = request->MutableHeaders();
        if (!headers->GetRequestTimeout()) {
            headers->SetRequestTimeout(30000);
        }
        // This mock replaces the trusted internal transport boundary only.
        headers->MutableInternal()->SetIsCellCopyRequest(
            headers->GetCopyLinkUUID() && headers->GetCellId() &&
            headers->GetExactDiskIdMatch() &&
            headers->GetClientId() == CopyVolumeClientId);
        auto promise = NewPromise<typename TMethod::TResponse>();
        const auto actor = Runtime.Register(
            new TServiceRequestActor<TMethod>(
                std::move(callContext),
                std::move(request),
                promise),
            NodeIdx);
        Runtime.EnableScheduleForActor(actor);
        return promise.GetFuture();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCellStorage final: public IStorage
{
    const IBlockStorePtr Service;

public:
    explicit TCellStorage(IBlockStorePtr service)
        : Service(std::move(service))
    {}

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr context,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        return Service->WriteBlocksLocal(std::move(context), std::move(request));
    }
    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr context,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        return Service->ZeroBlocks(std::move(context), std::move(request));
    }
    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr context,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        return Service->ReadBlocksLocal(std::move(context), std::move(request));
    }
    TFuture<NProto::TError> EraseDevice(NProto::EDeviceEraseMethod) override
    {
        return MakeFuture(MakeError(E_NOT_IMPLEMENTED));
    }
    TStorageBuffer AllocateBuffer(size_t) override
    {
        return {};
    }
    void ReportIOError() override
    {}
};

struct TConnection final: NCells::ICellConnection
{
    IBlockStorePtr Service;

    const bool EnableCopyData;

    explicit TConnection(IBlockStorePtr service, bool enableCopyData)
        : Service(std::move(service))
        , EnableCopyData(enableCopyData)
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
        return EnableCopyData ? std::make_shared<TCellStorage>(Service) : nullptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TCellManagerMock final: NCells::ICellManager
{
    TTestActorRuntimeBase& Runtime;
    const THashMap<TString, ui32> CellNodes;
    const bool EnableCopyData;

    TCellManagerMock(
        TTestActorRuntimeBase& runtime,
        THashMap<TString, ui32> cellNodes,
        bool enableCopyData)
        : ICellManager(nullptr)
        , Runtime(runtime)
        , CellNodes(std::move(cellNodes))
        , EnableCopyData(enableCopyData)
    {}

    NCells::TCellConnectionFuture CreateConnection(
        const TString& cellId,
        const TString&,
        const NClient::TClientAppConfigPtr&,
        NCells::ICellConnectionObserverPtr) override
    {
        const auto* node = CellNodes.FindPtr(cellId);
        if (!node) {
            return MakeFuture(
                TResultOrError<NCells::ICellConnectionPtr>(MakeError(
                    E_INVALID_STATE,
                    "Test cell is not configured: " + cellId)));
        }

        return MakeFuture(
            TResultOrError<NCells::ICellConnectionPtr>(
                std::make_shared<TConnection>(
                    std::make_shared<TCellService>(Runtime, *node), EnableCopyData)));
    }

    NCells::TCellControlServiceFuture GetControlService(
        const TString& cellId,
        const NClient::TClientAppConfigPtr&) override
    {
        const auto* node = CellNodes.FindPtr(cellId);
        if (!node) {
            return MakeFuture(TResultOrError<IBlockStorePtr>(MakeError(
                E_INVALID_STATE,
                "Test cell is not configured: " + cellId)));
        }
        return MakeFuture(TResultOrError<IBlockStorePtr>(
            std::make_shared<TCellService>(Runtime, *node)));
    }

    NCells::TDescribeVolumeFuture DescribeVolume(
        TCallContextPtr,
        const TString&,
        const NProto::THeaders&,
        const NProto::TClientConfig&) override
    {
        return MakeFuture(
            ErrorResponse<NProto::TDescribeVolumeResponse>(
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
        TString,
        TDuration) override
    {
        return MakeFuture(TVector<NCells::TCellDescribeResult>());
    }

    TFuture<NProto::TError> SwitchDataTransport(
        TString,
        NProto::ECellDataTransport) override
    {
        return MakeFuture(MakeError(E_NOT_IMPLEMENTED));
    }

    void Start() override
    {}

    void Stop() override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NCells::ICellManagerPtr CreateCellManagerMock(
    TTestActorRuntimeBase& runtime,
    THashMap<TString, ui32> cellNodes,
    bool enableCopyData)
{
    return std::make_shared<TCellManagerMock>(
        runtime, std::move(cellNodes), enableCopyData);
}

}   // namespace NCloud::NBlockStore::NStorage
