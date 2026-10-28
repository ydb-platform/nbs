#include "cell_proxy.h"

#include <cloud/blockstore/libs/cells/iface/cell_manager.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/storage/api/cell_proxy.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <cloud/storage/core/libs/actors/helpers.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/algorithm.h>
#include <util/string/builder.h>
#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TEvPrivate
{
    enum EEvents
    {
        EvConnectionCreated = TBlockStorePrivateEvents::CELLS_START + 10,
        EvRequestCompleted
    };

    struct TEvConnectionCreated
        : TEventLocal<TEvConnectionCreated, EvConnectionCreated>
    {
        TResultOrError<NCells::ICellConnectionPtr> Result;

        explicit TEvConnectionCreated(
            TResultOrError<NCells::ICellConnectionPtr> result)
            : Result(std::move(result))
        {}
    };

    template <typename TResponse>
    struct TEvRequestCompleted
        : TEventLocal<TEvRequestCompleted<TResponse>, EvRequestCompleted>
    {
        TResponse Response;

        explicit TEvRequestCompleted(TResponse response)
            : Response(std::move(response))
        {}
    };
};

////////////////////////////////////////////////////////////////////////////////

struct TDescribeVolumeMethod
{
    using TRequest = TEvCellProxy::TDescribeVolumeRequest;
    using TResponse = NProto::TDescribeVolumeResponse;
    using TResponseEvent = TEvCellProxy::TEvDescribeVolumeResponse;

    static constexpr TStringBuf Name = "DescribeVolume";

    static auto Execute(IBlockStore& service, TCallContextPtr callContext,
                        const TRequest& request,
                        const NProto::THeaders& headers)
    {
        auto record = std::make_shared<NProto::TDescribeVolumeRequest>();
        record->SetDiskId(request.DiskId);
        *record->MutableHeaders() = headers;
        return service.DescribeVolume(std::move(callContext),
                                      std::move(record));
    }

    static auto MakeResponse(TResponse response)
    {
        return std::make_unique<TResponseEvent>(
            response.GetError(), std::move(*response.MutableVolume()));
    }
};

struct TExecuteActionMethod
{
    using TRequest = TEvCellProxy::TExecuteActionRequest;
    using TResponse = NProto::TExecuteActionResponse;
    using TResponseEvent = TEvCellProxy::TEvExecuteActionResponse;

    static constexpr TStringBuf Name = "ExecuteAction";

    static auto Execute(IBlockStore& service, TCallContextPtr callContext,
                        const TRequest& request,
                        const NProto::THeaders& headers)
    {
        auto record =
            std::make_shared<NProto::TExecuteActionRequest>(request.Record);
        *record->MutableHeaders() = headers;
        return service.ExecuteAction(std::move(callContext), std::move(record));
    }

    static auto MakeResponse(TResponse response)
    {
        return std::make_unique<TResponseEvent>(
            response.GetError(), std::move(*response.MutableOutput()));
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
class TCellRequestActor final
    : public TActorBootstrapped<TCellRequestActor<TMethod>>
{
private:
    const NCells::ICellManagerPtr CellManager;
    const NClient::TClientAppConfigPtr ClientConfig;
    const TRequestInfoPtr RequestInfo;
    using TBase = TActorBootstrapped<TCellRequestActor<TMethod>>;
    using TThis = TCellRequestActor<TMethod>;
    using TResponseEvent =
        TEvPrivate::TEvRequestCompleted<typename TMethod::TResponse>;
    using TBase::Become;
    using TBase::Die;
    using TBase::SelfId;

    const typename TMethod::TRequest Request;

    NCells::ICellConnectionPtr Connection;
    TInstant Deadline;

public:
    TCellRequestActor(NCells::ICellManagerPtr cellManager,
                      NClient::TClientAppConfigPtr clientConfig,
                      TRequestInfoPtr requestInfo,
                      typename TMethod::TRequest request)
        : CellManager(std::move(cellManager))
        , ClientConfig(std::move(clientConfig))
        , RequestInfo(std::move(requestInfo))
        , Request(std::move(request))
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        Become(&TThis::StateWork);
        Deadline = ctx.Now() + Request.Timeout;
        ctx.Schedule(Request.Timeout, new TEvents::TEvWakeup());

        const auto error = SafeExecute<NProto::TError>(
            [&]
            {
                CellManager
                    ->CreateConnection(Request.CellId, {}, ClientConfig, {})
                    .Subscribe(
                        [actorSystem = ctx.ActorSystem(),
                         self = SelfId()](const auto& future)
                        {
                            actorSystem->Send(
                                self,
                                new TEvPrivate::TEvConnectionCreated(
                                    ExtractResponse(future)));
                        });
                return MakeError(S_OK);
            });
        if (HasError(error)) {
            ReplyAndDie(ctx, error);
        }
    }

private:
    void ReplyAndDie(const TActorContext& ctx, const NProto::TError& error)
    {
        NCloud::Reply(
            ctx, *RequestInfo,
            std::make_unique<typename TMethod::TResponseEvent>(error));
        Die(ctx);
    }

    bool CheckDeadline(const TActorContext& ctx)
    {
        if (ctx.Now() < Deadline) {
            return true;
        }
        ReplyAndDie(
            ctx,
            MakeError(
                E_TIMEOUT,
                TStringBuilder() << "Cell " << TMethod::Name << " timed out"));
        return false;
    }

    void HandleConnectionCreated(
        const TEvPrivate::TEvConnectionCreated::TPtr& ev,
        const TActorContext& ctx)
    {
        if (!CheckDeadline(ctx)) {
            return;
        }
        auto& result = ev->Get()->Result;
        if (HasError(result)) {
            ReplyAndDie(ctx, result.GetError());
            return;
        }
        Connection = result.ExtractResult();
        if (!Connection) {
            ReplyAndDie(ctx, MakeError(E_FAIL, "Cell connection is empty"));
            return;
        }

        const auto error = SafeExecute<NProto::TError>(
            [&]
            {
                auto service = Connection->GetService();
                if (!service) {
                    return MakeError(E_FAIL, "Cell service is empty");
                }
                NProto::THeaders headers;
                headers.SetClientId(FQDNHostName());
                headers.SetCellId(Request.CellId);
                headers.SetExactDiskIdMatch(true);
                headers.SetRequestTimeout(
                    Max<ui64>(1, (Deadline - ctx.Now()).MilliSeconds()));
                TMethod::Execute(*service, RequestInfo->CallContext, Request,
                                 headers)
                    .Subscribe(
                        [actorSystem = ctx.ActorSystem(),
                         self = SelfId()](const auto& future) {
                            actorSystem->Send(
                                self,
                                new TResponseEvent(ExtractResponse(future)));
                        });
                return MakeError(S_OK);
            });
        if (HasError(error)) {
            ReplyAndDie(ctx, error);
        }
    }

    void HandleResponse(const typename TResponseEvent::TPtr& ev,
                        const TActorContext& ctx)
    {
        if (!CheckDeadline(ctx)) {
            return;
        }
        NCloud::Reply(ctx, *RequestInfo,
                      TMethod::MakeResponse(std::move(ev->Get()->Response)));
        Die(ctx);
    }

    void HandleTimeout(const TActorContext& ctx)
    {
        ReplyAndDie(
            ctx,
            MakeError(
                E_TIMEOUT,
                TStringBuilder() << "Cell " << TMethod::Name << " timed out"));
    }

    void HandlePoisonPill(const TActorContext& ctx)
    {
        ReplyAndDie(
            ctx,
            MakeError(
                E_REJECTED,
                TStringBuilder() << "Cell " << TMethod::Name << " cancelled"));
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvConnectionCreated, HandleConnectionCreated);
            HFunc(TResponseEvent, HandleResponse);
            CFunc(TEvents::TEvWakeup::EventType, HandleTimeout);
            CFunc(TEvents::TEvPoisonPill::EventType, HandlePoisonPill);
            default:
                HandleUnexpectedEvent(ev, TBlockStoreComponents::CELLS,
                                      __PRETTY_FUNCTION__);
                break;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCellProxyActor final: public TActorBootstrapped<TCellProxyActor>
{
private:
    NCells::ICellManagerPtr CellManager;
    NClient::TClientAppConfigPtr ClientConfig;

public:
    void Bootstrap(const TActorContext& ctx)
    {
        Y_UNUSED(ctx);
        Become(&TThis::StateWork);
    }

private:
    void HandleInitialize(const TEvCellProxy::TEvInitializeRequest::TPtr& ev,
                          const TActorContext& ctx)
    {
        auto* msg = ev->Get();
        NProto::TError error;
        if (CellManager) {
            error =
                MakeError(E_INVALID_STATE, "CellProxy is already initialized");
        } else if (!msg->CellManager || !msg->ClientConfig) {
            error = MakeError(E_ARGUMENT,
                              "Cell manager and client config are required");
        } else {
            CellManager = std::move(msg->CellManager);
            ClientConfig = std::move(msg->ClientConfig);
        }
        if (ev->Sender) {
            NCloud::Reply(
                ctx, *ev,
                std::make_unique<TEvCellProxy::TEvInitializeResponse>(error));
        }
    }

    void HandleDescribeVolume(
        const TEvCellProxy::TEvDescribeVolumeRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();
        NProto::TError error;
        if (!CellManager) {
            error = MakeError(E_REJECTED, "CellProxy is not initialized");
        } else if (
            !msg->CellId || !msg->DiskId || msg->Timeout <= TDuration::Zero())
        {
            error =
                MakeError(E_ARGUMENT,
                          "CellId, DiskId and a positive timeout are required");
        }
        if (HasError(error)) {
            NCloud::Reply(
                ctx,
                *ev,
                std::make_unique<TEvCellProxy::TEvDescribeVolumeResponse>(
                    error));
            return;
        }
        NCloud::Register<TCellRequestActor<TDescribeVolumeMethod>>(
            ctx, CellManager, ClientConfig,
            CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
            TEvCellProxy::TDescribeVolumeRequest(msg->CellId, msg->DiskId,
                                                 msg->Timeout));
    }

    void HandleExecuteAction(
        const TEvCellProxy::TEvExecuteActionRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();
        NProto::TError error;
        if (!CellManager) {
            error = MakeError(E_REJECTED, "CellProxy is not initialized");
        } else if (
            !msg->CellId || !msg->Record.GetAction() ||
            msg->Timeout <= TDuration::Zero())
        {
            error =
                MakeError(E_ARGUMENT,
                          "CellId, Action and a positive timeout are required");
        }
        if (HasError(error)) {
            NCloud::Reply(
                ctx,
                *ev,
                std::make_unique<TEvCellProxy::TEvExecuteActionResponse>(
                    error));
            return;
        }
        NCloud::Register<TCellRequestActor<TExecuteActionMethod>>(
            ctx, CellManager, ClientConfig,
            CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
            TEvCellProxy::TExecuteActionRequest(msg->CellId,
                                                std::move(msg->Record),
                                                msg->Timeout));
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvCellProxy::TEvInitializeRequest, HandleInitialize);
            HFunc(TEvCellProxy::TEvDescribeVolumeRequest, HandleDescribeVolume);
            HFunc(TEvCellProxy::TEvExecuteActionRequest, HandleExecuteAction);
            CFunc(TEvents::TEvPoisonPill::EventType, Die);
            default:
                HandleUnexpectedEvent(ev, TBlockStoreComponents::CELLS,
                                      __PRETTY_FUNCTION__);
                break;
        }
    }
};

}   // namespace

IActorPtr CreateCellProxy()
{
    return std::make_unique<TCellProxyActor>();
}

}   // namespace NCloud::NBlockStore::NStorage
