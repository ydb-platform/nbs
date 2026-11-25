#include "cell_proxy.h"

#include <cloud/blockstore/libs/cells/iface/cell_manager.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/api/cell_proxy.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/common/format.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/generic/utility.h>
#include <util/string/builder.h>
#include <util/system/hostname.h>

#include <limits>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

const auto MaxRequestTimeout =
    TDuration::MilliSeconds(std::numeric_limits<ui32>::max());

////////////////////////////////////////////////////////////////////////////////

struct TEvPrivate
{
    enum EEvents
    {
        EvConnectionCreated = TBlockStorePrivateEvents::CELLS_PROXY_START,
        EvDescribeVolumeCompleted,
        EvExecuteActionCompleted,
        EvEnd
    };

    static_assert(
        EvEnd <= static_cast<int>(
            TBlockStorePrivateEvents::CELLS_PROXY_END));

    struct TEvConnectionCreated
        : TEventLocal<TEvConnectionCreated, EvConnectionCreated>
    {
        TResultOrError<NCells::ICellConnectionPtr> Result;

        explicit TEvConnectionCreated(
            TResultOrError<NCells::ICellConnectionPtr> result)
            : Result(std::move(result))
        {}
    };

    template <typename TResponse, ui32 EventId>
    struct TEvRequestCompleted
        : TEventLocal<TEvRequestCompleted<TResponse, EventId>, EventId>
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
    using TCompletionEvent = TEvPrivate::TEvRequestCompleted<
        TResponse,
        TEvPrivate::EvDescribeVolumeCompleted>;

    static constexpr TStringBuf Name = "DescribeVolume";

    static auto Execute(
        IBlockStore& service,
        TCallContextPtr callContext,
        const TRequest& request,
        const NProto::THeaders& headers)
    {
        auto record = std::make_shared<NProto::TDescribeVolumeRequest>();
        record->SetDiskId(request.DiskId);
        *record->MutableHeaders() = headers;
        record->MutableHeaders()->SetRequestId(callContext->RequestId);
        return service.DescribeVolume(
            std::move(callContext),
            std::move(record));
    }

    static auto MakeResponse(TResponse response)
    {
        return std::make_unique<TResponseEvent>(
            response.GetError(),
            std::move(*response.MutableVolume()));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TExecuteActionMethod
{
    using TRequest = TEvCellProxy::TExecuteActionRequest;
    using TResponse = NProto::TExecuteActionResponse;
    using TResponseEvent = TEvCellProxy::TEvExecuteActionResponse;
    using TCompletionEvent = TEvPrivate::TEvRequestCompleted<
        TResponse,
        TEvPrivate::EvExecuteActionCompleted>;

    static constexpr TStringBuf Name = "ExecuteAction";

    static auto Execute(
        IBlockStore& service,
        TCallContextPtr callContext,
        TRequest& request,
        const NProto::THeaders& headers)
    {
        auto record = std::make_shared<NProto::TExecuteActionRequest>(
            std::move(request.Record));
        auto* forwardedHeaders = record->MutableHeaders();
        forwardedHeaders->ClearInternal();
        forwardedHeaders->MergeFrom(headers);
        if (!forwardedHeaders->GetRequestId()) {
            forwardedHeaders->SetRequestId(callContext->RequestId);
        }
        return service.ExecuteAction(std::move(callContext), std::move(record));
    }

    static auto MakeResponse(TResponse response)
    {
        return std::make_unique<TResponseEvent>(
            response.GetError(),
            std::move(*response.MutableOutput()));
    }
};

////////////////////////////////////////////////////////////////////////////////

// CellManager clients must drain future callbacks before ActorSystem shutdown:
// the continuations below capture its raw pointer.
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
    using TRpcCompletedEvent = typename TMethod::TCompletionEvent;
    using TBase::Become;
    using TBase::Die;
    using TBase::SelfId;

    typename TMethod::TRequest Request;

    // Keep the connection alive after the local service handle is released.
    NCells::ICellConnectionPtr Connection;
    TMonotonic Deadline;

public:
    TCellRequestActor(
        NCells::ICellManagerPtr cellManager,
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
        Deadline = ctx.Monotonic() + Request.Timeout;
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
            ctx,
            *RequestInfo,
            std::make_unique<typename TMethod::TResponseEvent>(error));
        Die(ctx);
    }

    bool CheckDeadline(const TActorContext& ctx)
    {
        if (ctx.Monotonic() < Deadline) {
            return true;
        }

        ReplyTimeout(ctx);
        return false;
    }

    void HandleConnectionCreated(
        const TEvPrivate::TEvConnectionCreated::TPtr& ev,
        const TActorContext& ctx)
    {
        auto& result = ev->Get()->Result;
        if (HasError(result)) {
            ReplyAndDie(ctx, result.GetError());
            return;
        }

        if (!CheckDeadline(ctx)) {
            return;
        }

        Connection = result.ExtractResult();
        if (!Connection) {
            ReportCellProxyEmptyConnection(
                TStringBuilder() << "CellId=" << Request.CellId
                                 << ", method=" << TMethod::Name);
            ReplyAndDie(ctx, MakeError(E_FAIL, "Cell connection is empty"));
            return;
        }

        const auto error = SafeExecute<NProto::TError>(
            [&]
            {
                auto service = Connection->GetService();
                if (!service) {
                    ReportCellProxyEmptyService(
                        TStringBuilder() << "CellId=" << Request.CellId
                                         << ", method=" << TMethod::Name);
                    return MakeError(E_FAIL, "Cell service is empty");
                }
                NProto::THeaders headers;
                headers.SetClientId(FQDNHostName());
                headers.SetCellId(Request.CellId);
                headers.SetExactDiskIdMatch(true);
                headers.SetRequestTimeout(
                    Max<ui64>(
                        1,
                        (Deadline - ctx.Monotonic()).MilliSeconds()));
                TMethod::Execute(
                    *service,
                    RequestInfo->CallContext,
                    Request,
                    headers)
                    .Subscribe(
                        [actorSystem = ctx.ActorSystem(),
                         self = SelfId()](const auto& future)
                        {
                            actorSystem->Send(
                                self,
                                new TRpcCompletedEvent(
                                    ExtractResponse(future)));
                        });
                return MakeError(S_OK);
            });
        if (HasError(error)) {
            ReplyAndDie(ctx, error);
        }
    }

    void HandleResponse(
        const typename TRpcCompletedEvent::TPtr& ev,
        const TActorContext& ctx)
    {
        // Prefer the known RPC result while the timeout event is still pending.
        NCloud::Reply(
            ctx,
            *RequestInfo,
            TMethod::MakeResponse(std::move(ev->Get()->Response)));
        Die(ctx);
    }

    void ReplyTimeout(const TActorContext& ctx)
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
            HFunc(TRpcCompletedEvent, HandleResponse);
            CFunc(TEvents::TEvWakeup::EventType, ReplyTimeout);
            CFunc(TEvents::TEvPoisonPill::EventType, HandlePoisonPill);
            default:
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::CELLS,
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
    void HandleInitialize(
        const TEvCellProxy::TEvInitializeRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();
        NProto::TError error;
        if (CellManager) {
            error =
                MakeError(E_INVALID_STATE, "CellProxy is already initialized");
        } else if (!msg->CellManager || !msg->ClientConfig) {
            error = MakeError(
                E_ARGUMENT,
                "Cell manager and client config are required");
        } else {
            CellManager = std::move(msg->CellManager);
            ClientConfig = std::move(msg->ClientConfig);
            LOG_INFO(
                ctx,
                TBlockStoreComponents::CELLS,
                "CellProxy initialized");
        }
        if (HasError(error)) {
            LOG_ERROR(
                ctx,
                TBlockStoreComponents::CELLS,
                "CellProxy initialization failed: %s",
                FormatError(error).c_str());
        }
        if (ev->Sender) {
            NCloud::Reply(
                ctx,
                *ev,
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
            error = MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "CellId, DiskId and a positive timeout are required: "
                    << "CellId=\"" << msg->CellId << "\", DiskId=\""
                    << msg->DiskId << "\", Timeout="
                    << FormatDuration(msg->Timeout));
        } else if (msg->Timeout > MaxRequestTimeout) {
            error = MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "Timeout " << FormatDuration(msg->Timeout)
                    << " exceeds the uint32 millisecond limit");
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
            ctx,
            CellManager,
            ClientConfig,
            CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
            TEvCellProxy::TDescribeVolumeRequest(
                std::move(msg->CellId),
                std::move(msg->DiskId),
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
            error = MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "CellId, Action and a positive timeout are required: "
                    << "CellId=\"" << msg->CellId << "\", Action=\""
                    << msg->Record.GetAction()
                    << "\", Timeout=" << FormatDuration(msg->Timeout));
        } else if (msg->Timeout > MaxRequestTimeout) {
            error = MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "Timeout " << FormatDuration(msg->Timeout)
                    << " exceeds the uint32 millisecond limit");
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
            ctx,
            CellManager,
            ClientConfig,
            CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
            TEvCellProxy::TExecuteActionRequest(
                std::move(msg->CellId),
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
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::CELLS,
                    __PRETTY_FUNCTION__);
                break;
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateCellProxy()
{
    return std::make_unique<TCellProxyActor>();
}

}   // namespace NCloud::NBlockStore::NStorage
