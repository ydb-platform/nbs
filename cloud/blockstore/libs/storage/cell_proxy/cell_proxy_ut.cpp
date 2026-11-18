#include "cell_proxy.h"

#include <cloud/blockstore/libs/cells/iface/cell_manager.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/service/service_test.h>
#include <cloud/blockstore/libs/storage/api/cell_proxy.h>

#include <contrib/ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>
#include <util/system/hostname.h>

#include <limits>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NThreading;

namespace {

struct TConnection final: NCells::ICellConnection
{
    IBlockStorePtr Service;

    explicit TConnection(IBlockStorePtr service)
        : Service(std::move(service))
    {}

    TString GetHost() const override
    {
        return "cell-host";
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

struct TCellManager final: NCells::ICellManager
{
    std::function<NCells::TCellConnectionFuture()> Handler;
    TString LastCellId;
    NClient::TClientAppConfigPtr LastClientConfig;
    ui32 Requests = 0;

    TCellManager()
        : ICellManager(nullptr)
    {}

    NCells::TCellConnectionFuture CreateConnection(
        const TString& cellId,
        const TString& fqdn,
        const NClient::TClientAppConfigPtr& config,
        NCells::ICellConnectionObserverPtr observer) override
    {
        UNIT_ASSERT(fqdn.empty());
        UNIT_ASSERT(!observer);
        LastCellId = cellId;
        LastClientConfig = config;
        ++Requests;
        return Handler();
    }

    NCells::TDescribeVolumeFuture DescribeVolume(
        TCallContextPtr,
        const TString&,
        const NProto::THeaders&,
        const NProto::TClientConfig&) override
    {
        UNIT_FAIL("CellProxy must not search other cells");
        return MakeFuture(NProto::TDescribeVolumeResponse());
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
        UNIT_FAIL("CellProxy must not search other cells");
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

struct TService final: TTestService
{
    TCallContextPtr LastContext;

    TFuture<NProto::TDescribeVolumeResponse> DescribeVolume(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TDescribeVolumeRequest> request) override
    {
        LastContext = callContext;
        return TTestService::DescribeVolume(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TExecuteActionResponse> ExecuteAction(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TExecuteActionRequest> request) override
    {
        LastContext = callContext;
        return TTestService::ExecuteAction(
            std::move(callContext),
            std::move(request));
    }
};

void CheckForwardedHeaders(
    const NProto::THeaders& headers,
    const TString& cellId)
{
    UNIT_ASSERT_VALUES_EQUAL(FQDNHostName(), headers.GetClientId());
    UNIT_ASSERT_VALUES_EQUAL(cellId, headers.GetCellId());
    UNIT_ASSERT(headers.GetExactDiskIdMatch());
    UNIT_ASSERT(headers.GetRequestTimeout() > 0);
    UNIT_ASSERT(!headers.HasInternal());
}

struct TEnv: TTestActorRuntimeBase
{
    std::shared_ptr<TService> Service = std::make_shared<TService>();
    std::shared_ptr<TCellManager> Manager = std::make_shared<TCellManager>();
    NClient::TClientAppConfigPtr ClientConfig =
        std::make_shared<NClient::TClientAppConfig>();
    TActorId Sender;

    TEnv()
    {
        InitNodes();
        AppendToLogSettings(
            TBlockStoreComponents::START,
            TBlockStoreComponents::END,
            GetComponentName);
        SetDispatchTimeout(TDuration::Seconds(5));
        SetScheduledEventFilter([](auto&, auto&, TDuration, TInstant&)
                                { return false; });
        SetScheduledEventsSelectorFunc(CollapsedTimeScheduledEventsSelector);
        Sender = AllocateEdgeActor();
        RegisterService(
            MakeCellProxyServiceId(),
            Register(CreateCellProxy().release()));
        Manager->Handler = [&]
        {
            return MakeFuture(
                TResultOrError<NCells::ICellConnectionPtr>(
                    std::make_shared<TConnection>(Service)));
        };
        Service->DescribeVolumeHandler = [](auto request)
        {
            NProto::TDescribeVolumeResponse response;
            auto* volume = response.MutableVolume();
            volume->SetDiskId(request->GetDiskId());
            volume->SetBlockSize(4096);
            volume->SetBlocksCount(1024);
            volume->SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD);
            return MakeFuture(std::move(response));
        };
    }

    void Initialize()
    {
        Send(new IEventHandle(
            MakeCellProxyServiceId(),
            Sender,
            new TEvCellProxy::TEvInitializeRequest(Manager, ClientConfig)));
        const auto response =
            GrabEdgeEventRethrow<TEvCellProxy::TEvInitializeResponse>(Sender);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->Get()->GetStatus());
    }

    void Describe(
        ui64 cookie = 1,
        TString cellId = "cell-b",
        TString diskId = "follower",
        TDuration timeout = TDuration::Seconds(1))
    {
        Send(new IEventHandle(
            MakeCellProxyServiceId(),
            Sender,
            new TEvCellProxy::TEvDescribeVolumeRequest(
                std::move(cellId),
                std::move(diskId),
                timeout),
            0,
            cookie));
    }

    void ExecuteAction(
        TString cellId = "cell-b",
        TString action = "updatelinkonfollower",
        TDuration timeout = TDuration::Seconds(1),
        ui64 cookie = 1)
    {
        NProto::TExecuteActionRequest record;
        record.SetAction(std::move(action));
        record.SetInput("{}");
        Send(new IEventHandle(
            MakeCellProxyServiceId(),
            Sender,
            new TEvCellProxy::TEvExecuteActionRequest(
                std::move(cellId),
                std::move(record),
                timeout),
            0,
            cookie));
    }

    void CheckNoLateResponses(
        ui32 responseType,
        const std::function<void()>& complete)
    {
        ui32 lateResponses = 0;
        auto previousObserver = SetObserverFunc(
            [&](auto& event)
            {
                if (event->GetTypeRewrite() == responseType) {
                    ++lateResponses;
                    return TTestActorRuntimeBase::EEventAction::DROP;
                }
                return TTestActorRuntimeBase::DefaultObserverFunc(event);
            });
        Y_DEFER
        {
            SetObserverFunc(std::move(previousObserver));
        };

        complete();
        Schedule(
            new IEventHandle(Sender, Sender, new TEvents::TEvWakeup()),
            TDuration::MilliSeconds(1));
        UNIT_ASSERT(GrabEdgeEventRethrow<TEvents::TEvWakeup>(Sender));
        UNIT_ASSERT_VALUES_EQUAL(0, lateResponses);
    }

    void DisableTimeoutDelivery()
    {
        SetScheduledEventFilter(
            [](auto&, auto& event, TDuration, TInstant&)
            {
                return event->GetTypeRewrite() ==
                       TEvents::TEvWakeup::EventType;
            });
    }

    auto RecvAction()
    {
        return GrabEdgeEventRethrow<TEvCellProxy::TEvExecuteActionResponse>(
            Sender);
    }

    auto Recv()
    {
        return GrabEdgeEventRethrow<TEvCellProxy::TEvDescribeVolumeResponse>(
            Sender);
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TCellProxyTest)
{
    Y_UNIT_TEST(ShouldExecuteActionInTheRequestedCell)
    {
        TEnv env;
        env.Initialize();
        auto context = MakeIntrusive<TCallContext>(ui64{123});
        NProto::TExecuteActionRequest record;
        record.SetAction("updatelinkonfollower");
        record.SetInput(R"({"DiskId":"follower"})");
        auto* headers = record.MutableHeaders();
        headers->SetRequestId(456);
        headers->SetTraceId("caller-trace");
        headers->SetIdempotenceId("caller-idempotence");
        headers->SetClientId("caller-client");
        headers->SetCellId("wrong-cell");
        headers->SetRequestTimeout(std::numeric_limits<ui32>::max());
        headers->MutableInternal()->SetPeer("caller-peer");
        env.Service->ExecuteActionHandler = [record](auto request)
        {
            UNIT_ASSERT_VALUES_EQUAL(record.GetAction(), request->GetAction());
            UNIT_ASSERT_VALUES_EQUAL(record.GetInput(), request->GetInput());
            const auto& headers = request->GetHeaders();
            CheckForwardedHeaders(headers, "cell-b");
            UNIT_ASSERT_VALUES_EQUAL(
                record.GetHeaders().GetRequestId(),
                headers.GetRequestId());
            UNIT_ASSERT_VALUES_EQUAL(
                record.GetHeaders().GetTraceId(),
                headers.GetTraceId());
            UNIT_ASSERT_VALUES_EQUAL(
                record.GetHeaders().GetIdempotenceId(),
                headers.GetIdempotenceId());
            UNIT_ASSERT(
                headers.GetRequestTimeout() <=
                TDuration::Seconds(30).MilliSeconds());
            NProto::TExecuteActionResponse response;
            response.SetOutput("action-result");
            return MakeFuture(std::move(response));
        };
        env.Send(new IEventHandle(
            MakeCellProxyServiceId(),
            env.Sender,
            new TEvCellProxy::TEvExecuteActionRequest(
                context,
                "cell-b",
                std::move(record)),
            0,
            42));
        const auto response = env.RecvAction();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->Get()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL("action-result", response->Get()->Output);
        UNIT_ASSERT_VALUES_EQUAL(42, response->Cookie);
        UNIT_ASSERT_VALUES_EQUAL("cell-b", env.Manager->LastCellId);
        UNIT_ASSERT(env.Service->LastContext == context);
    }

    Y_UNIT_TEST(ShouldValidateExecuteActionRequests)
    {
        TEnv env;
        env.ExecuteAction();
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            env.RecvAction()->Get()->GetStatus());
        env.Initialize();
        env.ExecuteAction("");
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            env.RecvAction()->Get()->GetStatus());
        env.ExecuteAction("cell-b", "");
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            env.RecvAction()->Get()->GetStatus());
        env.ExecuteAction("cell-b", "action", TDuration::Zero());
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            env.RecvAction()->Get()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(0, env.Manager->Requests);
    }

    Y_UNIT_TEST(ShouldPreserveActionErrorsAndOutput)
    {
        TEnv env;
        env.Initialize();
        for (const auto code: {S_ALREADY, S_FALSE, E_NOT_FOUND, E_REJECTED}) {
            auto error = MakeError(code, "action response");
            error.SetFlags(123);
            env.Service->ExecuteActionHandler = [&](auto)
            {
                NProto::TExecuteActionResponse response;
                *response.MutableError() = error;
                response.SetOutput("error-details");
                return MakeFuture(std::move(response));
            };
            env.ExecuteAction();
            const auto response = env.RecvAction();
            UNIT_ASSERT_VALUES_EQUAL(
                error.SerializeAsString(),
                response->Get()->GetError().SerializeAsString());
            UNIT_ASSERT_VALUES_EQUAL("error-details", response->Get()->Output);
        }
    }

    Y_UNIT_TEST(ShouldNotExecuteActionAfterConnectionFailure)
    {
        TEnv env;
        env.Initialize();
        env.Manager->Handler = []
        {
            return MakeFuture(
                TResultOrError<NCells::ICellConnectionPtr>(
                    MakeError(E_INVALID_STATE, "unknown cell")));
        };
        env.Service->ExecuteActionHandler =
            [](auto) -> TFuture<NProto::TExecuteActionResponse>
        {
            UNIT_FAIL("Action must not execute after connection failure");
            return {};
        };
        env.ExecuteAction();
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            env.RecvAction()->Get()->GetStatus());
    }

    Y_UNIT_TEST(ShouldHandleExecuteActionExceptions)
    {
        TEnv env;
        env.Initialize();
        env.Service->ExecuteActionHandler =
            [](auto) -> TFuture<NProto::TExecuteActionResponse>
        {
            throw TServiceError(MakeError(E_ARGUMENT, "action exception"));
        };
        env.ExecuteAction();
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            env.RecvAction()->Get()->GetStatus());
        auto promise = NewPromise<NProto::TExecuteActionResponse>();
        env.Service->ExecuteActionHandler = [&](auto)
        {
            return promise.GetFuture();
        };
        env.ExecuteAction();
        promise.SetException(
            std::make_exception_ptr(
                TServiceError(MakeError(E_REJECTED, "future exception"))));
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            env.RecvAction()->Get()->GetStatus());
    }

    Y_UNIT_TEST(ShouldTimeOutExecuteActionAndIgnoreLateResponse)
    {
        TEnv env;
        env.Initialize();
        auto promise = NewPromise<NProto::TExecuteActionResponse>();
        env.Service->ExecuteActionHandler = [&](auto)
        {
            return promise.GetFuture();
        };
        env.ExecuteAction("cell-b", "action", TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(
            E_TIMEOUT,
            env.RecvAction()->Get()->GetStatus());
        env.CheckNoLateResponses(
            TEvCellProxy::EvExecuteActionResponse,
            [&] { promise.SetValue(NProto::TExecuteActionResponse()); });
    }

    Y_UNIT_TEST(ShouldRejectRequestsUntilInitialized)
    {
        TEnv env;
        env.Describe();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, env.Recv()->Get()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(0, env.Manager->Requests);
        env.Initialize();
        env.Describe();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, env.Recv()->Get()->GetStatus());
    }

    Y_UNIT_TEST(ShouldValidateInitialization)
    {
        TEnv env;
        env.Send(new IEventHandle(
            MakeCellProxyServiceId(),
            env.Sender,
            new TEvCellProxy::TEvInitializeRequest(nullptr, env.ClientConfig)));
        auto response =
            env.GrabEdgeEventRethrow<TEvCellProxy::TEvInitializeResponse>(
                env.Sender);
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->Get()->GetStatus());
        env.Initialize();
        auto replacementManager = std::make_shared<TCellManager>();
        replacementManager->Handler = env.Manager->Handler;
        env.Send(new IEventHandle(
            MakeCellProxyServiceId(),
            env.Sender,
            new TEvCellProxy::TEvInitializeRequest(
                replacementManager,
                env.ClientConfig)));
        response =
            env.GrabEdgeEventRethrow<TEvCellProxy::TEvInitializeResponse>(
                env.Sender);
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, response->Get()->GetStatus());

        env.Describe();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, env.Recv()->Get()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(1, env.Manager->Requests);
        UNIT_ASSERT_VALUES_EQUAL(0, replacementManager->Requests);
    }

    Y_UNIT_TEST(ShouldDescribeOnlyTheRequestedCellAndPreserveContext)
    {
        TEnv env;
        env.Initialize();
        const auto context = MakeIntrusive<TCallContext>(ui64{123});
        env.Service->DescribeVolumeHandler = [&](auto request)
        {
            UNIT_ASSERT_VALUES_EQUAL("follower", request->GetDiskId());
            CheckForwardedHeaders(request->GetHeaders(), "cell-b");
            UNIT_ASSERT_VALUES_EQUAL(
                context->RequestId,
                request->GetHeaders().GetRequestId());
            NProto::TDescribeVolumeResponse response;
            response.MutableVolume()->SetDiskId("follower");
            response.MutableVolume()->SetBlocksCount(123);
            response.MutableVolume()->SetTokenVersion(17);
            return MakeFuture(std::move(response));
        };
        env.Send(new IEventHandle(
            MakeCellProxyServiceId(),
            env.Sender,
            new TEvCellProxy::TEvDescribeVolumeRequest(
                context,
                "cell-b",
                "follower"),
            0,
            42));
        const auto response = env.Recv();
        UNIT_ASSERT(env.Service->LastContext == context);
        UNIT_ASSERT_VALUES_EQUAL(42, response->Cookie);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->Get()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            "follower",
            response->Get()->Volume.GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(123, response->Get()->Volume.GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(17, response->Get()->Volume.GetTokenVersion());
        UNIT_ASSERT_VALUES_EQUAL("cell-b", env.Manager->LastCellId);
        UNIT_ASSERT(env.Manager->LastClientConfig == env.ClientConfig);
    }

    Y_UNIT_TEST(ShouldPreserveConnectionErrorsWithoutFallback)
    {
        TEnv env;
        env.Initialize();
        auto error = MakeError(E_INVALID_STATE, "cell is not configured");
        error.SetFlags(123);
        env.Manager->Handler = [&]
        {
            return MakeFuture(
                TResultOrError<NCells::ICellConnectionPtr>(error));
        };
        env.Service->DescribeVolumeHandler =
            [](auto) -> TFuture<NProto::TDescribeVolumeResponse>
        {
            UNIT_FAIL("Describe must not run after connection failure");
            return {};
        };
        env.Describe();
        UNIT_ASSERT_VALUES_EQUAL(
            error.SerializeAsString(),
            env.Recv()->Get()->GetError().SerializeAsString());
    }

    Y_UNIT_TEST(ShouldPreserveRemoteErrorsAndStatuses)
    {
        TEnv env;
        env.Initialize();
        for (const auto code: {S_ALREADY, S_FALSE, E_NOT_FOUND, E_REJECTED}) {
            auto error = MakeError(code, "remote response");
            error.SetFlags(123);
            env.Service->DescribeVolumeHandler = [&](auto)
            {
                NProto::TDescribeVolumeResponse response;
                *response.MutableError() = error;
                return MakeFuture(std::move(response));
            };
            env.Describe();
            UNIT_ASSERT_VALUES_EQUAL(
                error.SerializeAsString(),
                env.Recv()->Get()->GetError().SerializeAsString());
        }
    }

    Y_UNIT_TEST(ShouldValidateDescribeArguments)
    {
        TEnv env;
        env.Initialize();
        env.Describe(1, "", "follower");
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, env.Recv()->Get()->GetStatus());
        env.Describe(1, "cell-b", "");
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, env.Recv()->Get()->GetStatus());
        env.Describe(1, "cell-b", "follower", TDuration::Zero());
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, env.Recv()->Get()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(0, env.Manager->Requests);
    }

    Y_UNIT_TEST(ShouldHandleSynchronousExceptions)
    {
        TEnv env;
        env.Initialize();
        const auto handler = env.Manager->Handler;
        env.Manager->Handler = []() -> NCells::TCellConnectionFuture
        {
            throw TServiceError(MakeError(E_REJECTED, "connection exception"));
        };
        env.Describe();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, env.Recv()->Get()->GetStatus());
        env.Manager->Handler = handler;
        env.Service->DescribeVolumeHandler =
            [](auto) -> TFuture<NProto::TDescribeVolumeResponse>
        {
            throw TServiceError(MakeError(E_ARGUMENT, "describe exception"));
        };
        env.Describe();
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, env.Recv()->Get()->GetStatus());
    }

    Y_UNIT_TEST(ShouldHandleExceptionalFutures)
    {
        TEnv env;
        env.Initialize();
        auto promise = NewPromise<TResultOrError<NCells::ICellConnectionPtr>>();
        env.Manager->Handler = [&]
        {
            return promise.GetFuture();
        };
        env.Describe();
        promise.SetException(
            std::make_exception_ptr(
                TServiceError(MakeError(E_REJECTED, "future exception"))));
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, env.Recv()->Get()->GetStatus());
    }

    Y_UNIT_TEST(ShouldUseOneDeadlineAndIgnoreLateResponses)
    {
        TEnv env;
        env.Initialize();
        auto connectionPromise =
            NewPromise<TResultOrError<NCells::ICellConnectionPtr>>();
        auto describePromise = NewPromise<NProto::TDescribeVolumeResponse>();
        env.Manager->Handler = [&]
        {
            return connectionPromise.GetFuture();
        };
        bool described = false;
        env.Service->DescribeVolumeHandler = [&](auto request)
        {
            described = true;
            UNIT_ASSERT(request->GetHeaders().GetRequestTimeout() <= 5);
            return describePromise.GetFuture();
        };
        env.Describe(1, "cell-b", "follower", TDuration::MilliSeconds(10));
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return env.Manager->Requests == 1;
        };
        env.DispatchEvents(options);
        env.UpdateCurrentTime(
            env.GetCurrentTime() + TDuration::MilliSeconds(5));
        connectionPromise.SetValue(
            TResultOrError<NCells::ICellConnectionPtr>(
                std::make_shared<TConnection>(env.Service)));
        UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, env.Recv()->Get()->GetStatus());
        UNIT_ASSERT(described);
        env.CheckNoLateResponses(
            TEvCellProxy::EvDescribeVolumeResponse,
            [&]
            { describePromise.SetValue(NProto::TDescribeVolumeResponse()); });
    }

    Y_UNIT_TEST(ShouldCorrelateConcurrentRequests)
    {
        TEnv env;
        env.Initialize();
        auto first = NewPromise<NProto::TDescribeVolumeResponse>();
        auto second = NewPromise<NProto::TDescribeVolumeResponse>();
        ui32 requests = 0;
        env.Service->DescribeVolumeHandler = [&](auto request)
        {
            ++requests;
            return request->GetDiskId() == "first" ? first.GetFuture()
                                                   : second.GetFuture();
        };
        env.Describe(1, "cell-b", "first");
        env.Describe(2, "cell-c", "second");
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return requests == 2;
        };
        env.DispatchEvents(options);
        NProto::TDescribeVolumeResponse reply;
        reply.MutableVolume()->SetDiskId("second");
        second.SetValue(reply);
        const auto secondResponse = env.Recv();
        UNIT_ASSERT_VALUES_EQUAL(2, secondResponse->Cookie);
        UNIT_ASSERT_VALUES_EQUAL(
            "second",
            secondResponse->Get()->Volume.GetDiskId());
        reply.MutableVolume()->SetDiskId("first");
        first.SetValue(reply);
        const auto firstResponse = env.Recv();
        UNIT_ASSERT_VALUES_EQUAL(1, firstResponse->Cookie);
        UNIT_ASSERT_VALUES_EQUAL(
            "first",
            firstResponse->Get()->Volume.GetDiskId());
    }

    Y_UNIT_TEST(ShouldNotExecuteAfterConnectionTimeout)
    {
        TEnv env;
        env.Initialize();
        auto promise = NewPromise<TResultOrError<NCells::ICellConnectionPtr>>();
        env.Manager->Handler = [&]
        {
            return promise.GetFuture();
        };
        bool described = false;
        env.Service->DescribeVolumeHandler = [&](auto)
        {
            described = true;
            return MakeFuture(NProto::TDescribeVolumeResponse());
        };

        env.Describe(1, "cell-b", "follower", TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, env.Recv()->Get()->GetStatus());
        env.CheckNoLateResponses(
            TEvCellProxy::EvDescribeVolumeResponse,
            [&]
            {
                promise.SetValue(
                    TResultOrError<NCells::ICellConnectionPtr>(
                        std::make_shared<TConnection>(env.Service)));
            });
        UNIT_ASSERT(!described);
    }

    Y_UNIT_TEST(ShouldUseCallContextRequestIdForActionWithoutHeaderId)
    {
        TEnv env;
        env.Initialize();
        const auto context = MakeIntrusive<TCallContext>(ui64{123});
        env.Service->ExecuteActionHandler = [&](auto request)
        {
            UNIT_ASSERT_VALUES_EQUAL(
                context->RequestId,
                request->GetHeaders().GetRequestId());
            return MakeFuture(NProto::TExecuteActionResponse());
        };
        NProto::TExecuteActionRequest record;
        record.SetAction("getstorageconfig");
        record.SetInput("{}");
        env.Send(new IEventHandle(
            MakeCellProxyServiceId(),
            env.Sender,
            new TEvCellProxy::TEvExecuteActionRequest(
                context,
                "cell-b",
                std::move(record))));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, env.RecvAction()->Get()->GetStatus());
    }

    Y_UNIT_TEST(ShouldValidateTimeoutUpperBound)
    {
        TEnv env;
        env.Initialize();
        const auto maxTimeoutMs = std::numeric_limits<ui32>::max();
        const auto maxTimeout = TDuration::MilliSeconds(maxTimeoutMs);
        const auto excessiveTimeout = maxTimeout + TDuration::MicroSeconds(1);

        env.Describe(1, "cell-b", "follower", excessiveTimeout);
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, env.Recv()->Get()->GetStatus());
        env.ExecuteAction("cell-b", "action", excessiveTimeout);
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            env.RecvAction()->Get()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(0, env.Manager->Requests);

        env.Service->DescribeVolumeHandler = [&](auto request)
        {
            UNIT_ASSERT_VALUES_EQUAL(
                maxTimeoutMs,
                request->GetHeaders().GetRequestTimeout());
            return MakeFuture(NProto::TDescribeVolumeResponse());
        };
        env.Service->ExecuteActionHandler = [&](auto request)
        {
            UNIT_ASSERT_VALUES_EQUAL(
                maxTimeoutMs,
                request->GetHeaders().GetRequestTimeout());
            return MakeFuture(NProto::TExecuteActionResponse());
        };
        env.Describe(1, "cell-b", "follower", maxTimeout);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, env.Recv()->Get()->GetStatus());
        env.ExecuteAction("cell-b", "action", maxTimeout);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, env.RecvAction()->Get()->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(2, env.Manager->Requests);
    }

    Y_UNIT_TEST(ShouldPreserveActionResultBeforeTimeoutDelivery)
    {
        for (const auto code: {S_OK, E_ARGUMENT}) {
            TEnv env;
            env.Initialize();
            env.DisableTimeoutDelivery();
            auto promise = NewPromise<NProto::TExecuteActionResponse>();
            bool executed = false;
            env.Service->ExecuteActionHandler = [&](auto)
            {
                executed = true;
                return promise.GetFuture();
            };
            const auto timeout = TDuration::MilliSeconds(10);
            env.ExecuteAction("cell-b", "action", timeout, 42);
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return executed;
            };
            env.DispatchEvents(options);
            env.UpdateCurrentTime(env.GetCurrentTime() + timeout);

            const auto error = MakeError(code, "known remote result");
            NProto::TExecuteActionResponse reply;
            *reply.MutableError() = error;
            reply.SetOutput("action-result");
            promise.SetValue(std::move(reply));
            const auto response = env.RecvAction();
            UNIT_ASSERT_VALUES_EQUAL(
                error.SerializeAsString(),
                response->Get()->GetError().SerializeAsString());
            UNIT_ASSERT_VALUES_EQUAL("action-result", response->Get()->Output);
            UNIT_ASSERT_VALUES_EQUAL(42, response->Cookie);
        }
    }

    Y_UNIT_TEST(ShouldCheckDeadlineOnlyForSuccessfulConnection)
    {
        for (const bool connected: {false, true}) {
            TEnv env;
            env.Initialize();
            env.DisableTimeoutDelivery();
            auto promise =
                NewPromise<TResultOrError<NCells::ICellConnectionPtr>>();
            env.Manager->Handler = [&]
            {
                return promise.GetFuture();
            };
            env.Service->DescribeVolumeHandler =
                [](auto) -> TFuture<NProto::TDescribeVolumeResponse>
            {
                UNIT_FAIL("RPC must not start after the connection deadline");
                return {};
            };
            const auto timeout = TDuration::MilliSeconds(10);
            env.Describe(1, "cell-b", "follower", timeout);
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return env.Manager->Requests == 1;
            };
            env.DispatchEvents(options);
            env.UpdateCurrentTime(env.GetCurrentTime() + timeout);

            if (connected) {
                promise.SetValue(
                    TResultOrError<NCells::ICellConnectionPtr>(
                        std::make_shared<TConnection>(env.Service)));
            } else {
                promise.SetValue(
                    TResultOrError<NCells::ICellConnectionPtr>(
                        MakeError(E_INVALID_STATE, "cell is not configured")));
            }
            UNIT_ASSERT_VALUES_EQUAL(
                connected ? E_TIMEOUT : E_INVALID_STATE,
                env.Recv()->Get()->GetStatus());
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
