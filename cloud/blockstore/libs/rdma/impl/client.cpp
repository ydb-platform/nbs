#include "client.h"

#include "adaptive_wait.h"
#include "buffer.h"
#include "event.h"
#include "poll.h"
#include "rcu.h"
#include "log.h"
#include "list.h"
#include "utils.h"
#include "verbs.h"
#include "work_queue.h"

#include <cloud/blockstore/libs/rdma/iface/probes.h>
#include <cloud/blockstore/libs/rdma/iface/protobuf.h>
#include <cloud/blockstore/libs/rdma/iface/protocol.h>

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/service/context.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/history.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/random/random.h>
#include <util/stream/format.h>
#include <util/system/datetime.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>

namespace NCloud::NBlockStore::NRdma {

using namespace NMonitoring;
using namespace NThreading;

LWTRACE_USING(BLOCKSTORE_RDMA_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration POLL_TIMEOUT = TDuration::Seconds(1);
constexpr TDuration RESOLVE_TIMEOUT = TDuration::Seconds(10);
constexpr TDuration MIN_CONNECT_TIMEOUT = TDuration::Seconds(1);
constexpr TDuration FLUSH_TIMEOUT = TDuration::Seconds(10);
constexpr TDuration LOG_THROTTLER_PERIOD = TDuration::Seconds(60);
constexpr TDuration MIN_RECONNECT_DELAY = TDuration::MilliSeconds(10);

constexpr size_t REQUEST_HISTORY_SIZE = 1024;

////////////////////////////////////////////////////////////////////////////////

struct TRequest;
using TRequestPtr = std::unique_ptr<TRequest>;

struct TEndpointCounters;
using TEndpointCountersPtr = std::shared_ptr<TEndpointCounters>;

class TClientEndpoint;
using TClientEndpointPtr = std::shared_ptr<TClientEndpoint>;

class TConnectionPoller;
using TConnectionPollerPtr = std::unique_ptr<TConnectionPoller>;

class TCompletionPoller;
using TCompletionPollerPtr = std::unique_ptr<TCompletionPoller>;

struct TRequestId;
using TRequestIdPtr = std::unique_ptr<TRequestId>;

class TRequestHandle;
using TRequestHandlePtr = std::unique_ptr<TRequestHandle>;

////////////////////////////////////////////////////////////////////////////////

struct TRequest
    : TClientRequest
    , TListNode<TRequest>
{
    const ui64 StartedCycles;

    std::weak_ptr<TClientEndpoint> Endpoint;

    TCallContextPtr CallContext;
    ui32 ReqId = 0;
    NThreading::TPromise<ui32> ReqIdForRequestHandle;

    TPooledBuffer InBuffer{};
    TPooledBuffer OutBuffer{};

    TRequest(
            std::weak_ptr<TClientEndpoint> endpoint,
            IClientHandlerPtr handler,
            std::unique_ptr<TNullContext> context)
        : TClientRequest(std::move(handler), std::move(context))
        , StartedCycles(GetCycleCount())
        , Endpoint(std::move(endpoint))
    {}

    ~TRequest() override;
};

////////////////////////////////////////////////////////////////////////////////

class TActiveRequests
{
private:
    THashMap<ui32, TRequestPtr> Requests;
    THistory<ui32> CompletedRequests = THistory<ui32>(REQUEST_HISTORY_SIZE);
    THistory<ui32> TimedOutRequests = THistory<ui32>(REQUEST_HISTORY_SIZE);
    THistory<ui32> CancelledRequests = THistory<ui32>(REQUEST_HISTORY_SIZE);

public:
    ui32 CreateId()
    {
        for (;;) {
            // must be unique through all in-flight requests
            ui32 reqId = RandomNumber<ui32>(RDMA_MAX_REQID);
            if (reqId && Requests.find(reqId) == Requests.end()) {
                return reqId;
            }
        }
    }

    void Push(TRequestPtr req)
    {
        Y_ABORT_UNLESS(Requests.emplace(req->ReqId, std::move(req)).second);
    }

    TRequestPtr Pop(ui32 reqId)
    {
        auto it = Requests.find(reqId);
        if (it != Requests.end()) {
            TRequestPtr req = std::move(it->second);
            CompletedRequests.Put(it->first);
            Requests.erase(it);
            return req;
        }
        return nullptr;
    }

    TRequestPtr Pop()
    {
        if (Requests.empty()) {
            return nullptr;
        }
        auto it = std::begin(Requests);
        TRequestPtr req = std::move(it->second);
        CompletedRequests.Put(it->first);
        Requests.erase(it);
        return req;
    }

    TRequest* Get(ui32 reqId)
    {
        auto it = Requests.find(reqId);
        if (it != Requests.end()) {
            return it->second.get();
        }
        return nullptr;
    }

    bool TimedOut(ui32 reqId) const
    {
        return TimedOutRequests.Contains(reqId);
    }

    bool Completed(ui32 reqId) const
    {
        return CompletedRequests.Contains(reqId);
    }

    [[nodiscard]] bool Cancelled(ui32 reqId) const
    {
        return CancelledRequests.Contains(reqId);
    }

    TRequestPtr PopCancelledRequest(ui64 reqId)
    {
        auto it = Requests.find(reqId);
        if (it != Requests.end()) {
            TRequestPtr req = std::move(it->second);
            CancelledRequests.Put(it->first);
            Requests.erase(it);
            return req;
        }
        return nullptr;
    }

    TVector<TRequestPtr> PopTimedOutRequests(ui64 timeoutCycles)
    {
        TVector<TRequestPtr> requests;
        for (auto& x: Requests) {
            auto started = x.second->StartedCycles;
            auto now = GetCycleCount();

            if (started && started + timeoutCycles < now) {
                requests.push_back(std::move(x.second));
            }
        }

        for (const auto& x: requests) {
            TimedOutRequests.Put(x->ReqId);
            Requests.erase(x->ReqId);
        }

        return requests;
    }
};

////////////////////////////////////////////////////////////////////////////////

enum class EEndpointState
{
    Disconnecting,
    Disconnected,
    ResolvingAddress,
    ResolvingRoute,
    Connecting,
    Connected,
};

////////////////////////////////////////////////////////////////////////////////

const char* GetEndpointStateName(EEndpointState state)
{
    static const char* names[] = {
        "Disconnecting",
        "Disconnected",
        "ResolvingAddress",
        "ResolvingRoute",
        "Connecting",
        "Connected",
    };

    if ((size_t)state < Y_ARRAY_SIZE(names)) {
        return names[(size_t)state];
    }
    return "Undefined";
}

////////////////////////////////////////////////////////////////////////////////

struct TEndpointCounters
{
    TDynamicCounters::TCounterPtr QueuedRequests;
    TDynamicCounters::TCounterPtr ActiveRequests;
    TDynamicCounters::TCounterPtr AbortedRequests;
    TDynamicCounters::TCounterPtr CompletedRequests;
    TDynamicCounters::TCounterPtr UnknownRequests;

    TDynamicCounters::TCounterPtr ActiveSend;
    TDynamicCounters::TCounterPtr ActiveRecv;

    TDynamicCounters::TCounterPtr SendErrors;
    TDynamicCounters::TCounterPtr RecvErrors;

    TDynamicCounters::TCounterPtr UnexpectedCompletions;

    void Register(TDynamicCounters& counters)
    {
        QueuedRequests = counters.GetCounter("QueuedRequests");
        ActiveRequests = counters.GetCounter("ActiveRequests");
        CompletedRequests = counters.GetCounter("CompletedRequests", true);
        AbortedRequests = counters.GetCounter("AbortedRequests");
        UnknownRequests = counters.GetCounter("UnknownRequests");

        ActiveSend = counters.GetCounter("ActiveSend");
        ActiveRecv = counters.GetCounter("ActiveRecv");

        SendErrors = counters.GetCounter("SendErrors");
        RecvErrors = counters.GetCounter("RecvErrors");

        UnexpectedCompletions = counters.GetCounter("UnexpectedCompletions");
    }

    void RequestEnqueued()
    {
        QueuedRequests->Inc();
    }

    void RequestDequeued()
    {
        QueuedRequests->Dec();
    }

    void SendRequestStarted()
    {
        ActiveRequests->Inc();
        ActiveSend->Inc();
    }

    void RecvResponseStarted()
    {
        ActiveRecv->Inc();
    }

    void SendRequestCompleted()
    {
        ActiveSend->Dec();
    }

    void SendRequestError()
    {
        ActiveRequests->Dec();
        SendErrors->Inc();
    }

    void RecvResponseCompleted()
    {
        ActiveRecv->Dec();
        ActiveRequests->Dec();
        CompletedRequests->Inc();
    }

    void RecvResponseError()
    {
        ActiveRecv->Dec();
        ActiveRequests->Dec();
        RecvErrors->Inc();
    }

    void RequestAborted()
    {
        ActiveRequests->Dec();
        AbortedRequests->Inc();
    }

    void UnknownRequest()
    {
        UnknownRequests->Inc();
    }

    void UnexpectedCompletion()
    {
        UnexpectedCompletions->Inc();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TReconnect
{
    const TDuration MaxDelay;

    TDuration Delay;
    TTimerHandle Timer;
    TAdaptiveLock Lock;

    TReconnect(TDuration maxDelay)
        : MaxDelay(maxDelay)
    {}

    void Cancel()
    {
        auto guard = Guard(Lock);

        Delay = TDuration::Zero();
        Timer.Clear();
    }

    void Schedule(TDuration minDelay = MIN_RECONNECT_DELAY)
    {
        auto guard = Guard(Lock);

        Delay = Min(Delay ? Delay * 2 : minDelay, MaxDelay);
        Timer.Set(Delay);
    }

    bool Hanging() const
    {
        auto guard = Guard(Lock);

        return Delay >= MaxDelay / 2;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TRequestId: TListNode<TRequestId>
{
    ui32 ReqId = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TClientEndpoint final
    : public IClientEndpoint
    , public NVerbs::ICompletionHandler
    , public std::enable_shared_from_this<TClientEndpoint>
{
    // TODO
    friend class TClient;
    friend class TCompletionPoller;
    friend struct TRequest;

private:
    NVerbs::IVerbsPtr Verbs;

    NVerbs::TConnectionPtr Connection;
    TString Host;
    ui32 Port;
    IClientHandlerPtr Handler;
    TEndpointCountersPtr Counters;
    TLog Log;
    TReconnect Reconnect;

    struct {
        TLogThrottler Unexpected = TLogThrottler(LOG_THROTTLER_PERIOD);
    } LogThrottler;

    // config might be adjusted during initial handshake
    TClientConfigPtr OriginalConfig;
    TClientConfig Config;
    const EWaitMode WaitMode;
    bool ResetConfig = false;

    TCompletionPoller* Poller = nullptr;

    std::atomic<EEndpointState> State = EEndpointState::Disconnected;
    std::atomic<ui32> Status = S_OK;
    std::atomic_flag StopFlag = ATOMIC_FLAG_INIT;

    NVerbs::TCompletionChannelPtr CompletionChannel = NVerbs::NullPtr;
    NVerbs::TCompletionQueuePtr CompletionQueue = NVerbs::NullPtr;

    TPromise<IClientEndpointPtr> StartResult = NewPromise<IClientEndpointPtr>();
    TPromise<void> StopResult = NewPromise<void>();

    ui64 FlushStartCycles = 0;

    TBufferPool SendBuffers;
    TBufferPool RecvBuffers;
    TMutex AllocationLock;

    TPooledBuffer SendBuffer {};
    TPooledBuffer RecvBuffer {};

    TVector<TSendWr> SendWrs;
    TVector<TRecvWr> RecvWrs;

    TWorkQueue<TSendWr> SendQueue;
    TWorkQueue<TRecvWr> RecvQueue;

    union {
        const ui64 Id = RandomNumber(Max<ui64>());
        struct {
            const ui32 RecvMagic;
            const ui32 SendMagic;
        };
    };
    ui16 Generation = Max<ui16>();

    TLockFreeList<TRequest> InputRequests;
    TLockFreeList<TRequestId> CancelRequests;
    TEventHandle CancelRequestEvent;
    TEventHandle RequestEvent;
    TEventHandle DisconnectEvent;

    TSimpleList<TRequest> QueuedRequests;
    TActiveRequests ActiveRequests;

public:
    static TClientEndpoint* FromEvent(rdma_cm_event* event)
    {
        Y_ABORT_UNLESS(event->id && event->id->context);
        return static_cast<TClientEndpoint*>(event->id->context);
    }

    TClientEndpoint(
        NVerbs::IVerbsPtr Verbs,
        NVerbs::TConnectionPtr connection,
        TString host,
        ui32 port,
        TClientConfigPtr config,
        TEndpointCountersPtr stats,
        TLog log);
    ~TClientEndpoint() override;

    // called from CM and CQ threads
    bool CheckState(EEndpointState expectedState) const;
    void ChangeState(EEndpointState expectedState, EEndpointState newState);
    void ChangeState(EEndpointState newState) noexcept;
    void Disconnect() noexcept;
    void FlushQueues() noexcept;

    // called from CM thread
    void CreateQP();
    void DestroyQP() noexcept;
    void StartReceive();
    void SetConnection(NVerbs::TConnectionPtr connection) noexcept;
    int ReconnectTimerHandle() const;
    bool ShouldStop() const;

    // called from external thread
    TResultOrError<TClientRequestPtr> AllocateRequest(
        IClientHandlerPtr handler,
        std::unique_ptr<TNullContext> context,
        size_t requestBytes,
        size_t responseBytes) noexcept override;
    IRequestHandlePtr SendRequest(
        TClientRequestPtr creq,
        TCallContextPtr callContext) noexcept override;
    TFuture<void> Stop() noexcept override;

    // called from external thread
    void CancelRequest(TRequestIdPtr reqId) noexcept;

    // called from CQ thread
    void HandleCompletionEvent(ibv_wc* wc) override;
    bool HandleInputRequests();
    bool HandleCancelRequests();
    bool HandleCompletionEvents();
    bool AbortRequests() noexcept;
    bool Flushed() const;
    bool FlushHanging() const;

private:
    // called from CQ thread
    void HandleQueuedRequests();
    bool IsWorkRequestValid(const TWorkRequestId& id) const;
    void HandleFlush(const TWorkRequestId& id) noexcept;
    void SendRequest(TRequestPtr req, TSendWr* send);
    void SendRequestCompleted(TSendWr* send, ibv_wc_status status) noexcept;
    void RecvResponse(TRecvWr* recv);
    void RecvResponseCompleted(TRecvWr* recv, ibv_wc_status status);
    void AbortRequest(TRequestPtr req, ui32 err, const TString& msg) noexcept;
    void FreeRequest(TRequest* creq) noexcept;
};
////////////////////////////////////////////////////////////////////////////////

class TRequestHandle: public IRequestHandle
{
    std::shared_ptr<TClientEndpoint> Endpoint;
    NThreading::TFuture<ui32> ReqIdFuture;

public:
    TRequestHandle(
            std::shared_ptr<TClientEndpoint> endpoint,
            NThreading::TFuture<ui32> reqIdFuture)
        : Endpoint(std::move(endpoint))
        , ReqIdFuture(std::move(reqIdFuture))
    {}

    ~TRequestHandle() override = default;

    void CancelRequest() override
    {
        ReqIdFuture.Subscribe(
            [endpoint = Endpoint](const NThreading::TFuture<ui32>& reqIdFuture)
            {
                auto reqId = std::make_unique<TRequestId>();
                reqId->ReqId = reqIdFuture.GetValue();
                endpoint->CancelRequest(std::move(reqId));
            });
    }
};

////////////////////////////////////////////////////////////////////////////////

TRequest::~TRequest()
{
    auto clientEndpoint = Endpoint.lock();
    if (clientEndpoint) {
        clientEndpoint->FreeRequest(this);
    }
}

////////////////////////////////////////////////////////////////////////////////

TClientEndpoint::TClientEndpoint(
        NVerbs::IVerbsPtr verbs,
        NVerbs::TConnectionPtr connection,
        TString host,
        ui32 port,
        TClientConfigPtr config,
        TEndpointCountersPtr stats,
        TLog log)
    : Verbs(std::move(verbs))
    , Connection(std::move(connection))
    , Host(std::move(host))
    , Port(port)
    , Counters(std::move(stats))
    , Log(log)
    , Reconnect(config->MaxReconnectDelay)
    , OriginalConfig(std::move(config))
    , Config(*OriginalConfig)
    , WaitMode(Config.WaitMode)
{
    // user data attached to connection events
    Connection->context = this;

    Log.SetFormatter([=](ELogPriority p, TStringBuf msg) {
        Y_UNUSED(p);
        return TStringBuilder() << "[" << Id << "] " << msg;
    });

    RDMA_INFO("start endpoint " << Host
        << " [send_magic=" << Hex(SendMagic, HF_FULL)
        << " recv_magic=" << Hex(RecvMagic, HF_FULL) << "]");
}

TClientEndpoint::~TClientEndpoint()
{
    // Free resources if endpoint wasn't properly stopped. We don't need to
    // detach it, because if we got there, poller is also getting destroyed
    if (!StopResult.HasValue()) {
        DestroyQP();
        Connection.reset();
    }

    RDMA_INFO("stop endpoint");
}

bool TClientEndpoint::CheckState(EEndpointState expectedState) const
{
    return State == expectedState;
}

void TClientEndpoint::ChangeState(
    EEndpointState expectedState,
    EEndpointState newState)
{
    auto actualState = State.exchange(newState);

    Y_ABORT_UNLESS(actualState == expectedState,
        "invalid state transition (new: %s, expected: %s, actual: %s)",
        GetEndpointStateName(newState),
        GetEndpointStateName(expectedState),
        GetEndpointStateName(actualState));

    RDMA_DEBUG(GetEndpointStateName(expectedState)
        << " -> " << GetEndpointStateName(newState));
}

void TClientEndpoint::ChangeState(EEndpointState newState) noexcept
{
    auto currentState = State.exchange(newState);

    RDMA_DEBUG(GetEndpointStateName(currentState)
        << " -> " << GetEndpointStateName(newState));
}

void TClientEndpoint::CreateQP()
{
    CompletionChannel = Verbs->CreateCompletionChannel(Connection->verbs);
    SetNonBlock(CompletionChannel->fd, true);

    if (ResetConfig) {
        Config = *OriginalConfig;
        ResetConfig = false;
    }

    CompletionQueue = Verbs->CreateCompletionQueue(
        Connection->verbs,
        2 * Config.QueueSize,     // send + recv
        this,
        CompletionChannel.get(),
        0);                       // comp_vector

    ibv_qp_init_attr qp_attrs = {
        .qp_context = nullptr,
        .send_cq = CompletionQueue.get(),
        .recv_cq = CompletionQueue.get(),
        .cap = {
            .max_send_wr = Config.QueueSize,
            .max_recv_wr = Config.QueueSize,
            .max_send_sge = RDMA_MAX_SEND_SGE,
            .max_recv_sge = RDMA_MAX_RECV_SGE,
            .max_inline_data = 16,
        },
        .qp_type = IBV_QPT_RC,
        .sq_sig_all = 1,
    };

    Verbs->CreateQP(Connection.get(), &qp_attrs);

    SendBuffers.Init(
        Verbs,
        Connection->pd,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ);

    RecvBuffers.Init(
        Verbs,
        Connection->pd,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);

    SendBuffer = SendBuffers.AcquireBuffer(
        Config.QueueSize * sizeof(TRequestMessage), true);

    RecvBuffer = RecvBuffers.AcquireBuffer(
        Config.QueueSize * sizeof(TResponseMessage), true);

    SendWrs.resize(Config.QueueSize);
    RecvWrs.resize(Config.QueueSize);

    Generation++;

    if (Generation > 1) {
        RDMA_DEBUG("new generation " << Generation);
    }

    ui32 i = 0;
    ui64 requestMsg = SendBuffer.Address;
    for (auto& wr: SendWrs) {
        wr.wr.opcode = IBV_WR_SEND;

        wr.wr.wr_id = TWorkRequestId(SendMagic, Generation, i++).Id;
        wr.wr.sg_list = wr.sg_list;
        wr.wr.num_sge = 1;

        wr.sg_list[0].lkey = SendBuffer.Key;
        wr.sg_list[0].addr = requestMsg;
        wr.sg_list[0].length = sizeof(TRequestMessage);

        SendQueue.Push(&wr);
        requestMsg += sizeof(TRequestMessage);
    }

    ui32 j = 0;
    ui64 responseMsg = RecvBuffer.Address;
    for (auto& wr: RecvWrs) {
        wr.wr.wr_id = TWorkRequestId(RecvMagic, Generation, j++).Id;
        wr.wr.sg_list = wr.sg_list;
        wr.wr.num_sge = 1;

        wr.sg_list[0].lkey = RecvBuffer.Key;
        wr.sg_list[0].addr = responseMsg;
        wr.sg_list[0].length = sizeof(TResponseMessage);

        RecvQueue.Push(&wr);
        responseMsg += sizeof(TResponseMessage);
    }
}

void TClientEndpoint::DestroyQP() noexcept
{
    Verbs->DestroyQP(Connection.get());

    CompletionQueue.reset();
    CompletionChannel.reset();

    with_lock (AllocationLock) {
        SendBuffers.ReleaseBuffer(SendBuffer);
        RecvBuffers.ReleaseBuffer(RecvBuffer);
    }

    SendQueue.Clear();
    RecvQueue.Clear();

    FlushStartCycles = 0;
}

void TClientEndpoint::StartReceive()
{
    while (auto* recv = RecvQueue.Pop()) {
        RecvResponse(recv);
    }
}

// implements IClientEndpoint
TResultOrError<TClientRequestPtr> TClientEndpoint::AllocateRequest(
    IClientHandlerPtr handler,
    std::unique_ptr<TNullContext> context,
    size_t requestBytes,
    size_t responseBytes) noexcept
{
    if (!CheckState(EEndpointState::Connected)) {
        return MakeError(Status, "unable to allocate request");
    }

    if (requestBytes > Config.MaxBufferSize) {
        return MakeError(E_FAIL, TStringBuilder()
            << "request exceeds maximum supported size " << requestBytes
            << " > " << Config.MaxBufferSize);
    }

    if (responseBytes > Config.MaxBufferSize) {
        return MakeError(E_FAIL, TStringBuilder()
            << "response exceeds maximum supported size " << responseBytes
            << " > " << Config.MaxBufferSize);
    }

    auto req = std::make_unique<TRequest>(
        shared_from_this(),
        std::move(handler),
        std::move(context));

    with_lock (AllocationLock) {
        if (requestBytes) {
            req->InBuffer = SendBuffers.AcquireBuffer(requestBytes);
        }
        if (responseBytes) {
            req->OutBuffer = RecvBuffers.AcquireBuffer(responseBytes);
        }
    }

    req->RequestBuffer = TStringBuf {
        reinterpret_cast<char*>(req->InBuffer.Address),
        req->InBuffer.Length,
    };
    req->ResponseBuffer = TStringBuf {
        reinterpret_cast<char*>(req->OutBuffer.Address),
        req->OutBuffer.Length,
    };

    return TClientRequestPtr(std::move(req));
}

// implements IClientEndpoint
IRequestHandlePtr TClientEndpoint::SendRequest(
    TClientRequestPtr creq,
    TCallContextPtr callContext) noexcept
{
    TRequestPtr req(static_cast<TRequest*>(creq.release()));
    req->CallContext = std::move(callContext);

    LWTRACK(
        RequestEnqueued,
        req->CallContext->LWOrbit,
        req->CallContext->RequestId);

    if (!CheckState(EEndpointState::Connected)) {
        AbortRequest(
            std::move(req),
            Status,
            "endpoint is unavailable");
        return {};
    }
    auto self = shared_from_this();
    auto reqIdPromise = NewPromise<ui32>();
    auto reqHandle =
        std::make_unique<TRequestHandle>(self, reqIdPromise.GetFuture());

    req->ReqIdForRequestHandle = std::move(reqIdPromise);

    Counters->RequestEnqueued();
    InputRequests.Enqueue(std::move(req));

    if (WaitMode == EWaitMode::Poll) {
        RequestEvent.Set();
    }
    return reqHandle;
}

bool TClientEndpoint::HandleInputRequests()
{
    if (WaitMode == EWaitMode::Poll) {
        RequestEvent.Clear();
    }

    auto requests = InputRequests.DequeueAll();
    if (!requests) {
        return false;
    }

    QueuedRequests.Append(std::move(requests));
    HandleQueuedRequests();
    return true;
}

void TClientEndpoint::HandleQueuedRequests()
{
    while (QueuedRequests) {
        auto* send = SendQueue.Pop();
        if (!send) {
            // no more WRs available
            break;
        }

        auto req = QueuedRequests.Dequeue();
        Y_ABORT_UNLESS(req);

        Counters->RequestDequeued();
        SendRequest(std::move(req), send);
    }
}

bool TClientEndpoint::HandleCancelRequests()
{
    if (WaitMode == EWaitMode::Poll) {
        CancelRequestEvent.Clear();
    }

    auto requests = CancelRequests.DequeueAll();
    if (!requests) {
        return false;
    }

    bool ret = false;
    while (auto reqId = requests.Dequeue()) {
        auto cancelledRequest =
            ActiveRequests.PopCancelledRequest(reqId->ReqId);
        if (cancelledRequest) {
            auto len = SerializeError(
                E_CANCELLED,
                TStringBuf("request is cancelled"),
                static_cast<TStringBuf>(cancelledRequest->OutBuffer));

            auto* handler = cancelledRequest->Handler.get();
            handler->HandleResponse(
                std::move(cancelledRequest),
                RDMA_PROTO_CANCELED,
                len);
            ret = true;
        }
    }

    return ret;
}

bool TClientEndpoint::AbortRequests() noexcept
{
    bool ret = false;

    if (WaitMode == EWaitMode::Poll) {
        DisconnectEvent.Clear();
    }

    auto requests = InputRequests.DequeueAll();
    if (requests) {
        QueuedRequests.Append(std::move(requests));
        ret = true;
    }

    while (QueuedRequests) {
        auto req = QueuedRequests.Dequeue();
        Y_ABORT_UNLESS(req);

        Counters->RequestDequeued();
        AbortRequest(std::move(req), Status, "endpoint is unavailable");
    }

    while (auto req = ActiveRequests.Pop()) {
        Counters->RequestAborted();
        AbortRequest(std::move(req), Status, "endpoint is unavailable");
        ret = true;
    }

    return ret;
}

void TClientEndpoint::AbortRequest(
    TRequestPtr req,
    ui32 err, const
    TString& msg) noexcept
{
    auto len = SerializeError(
        err,
        msg,
        static_cast<TStringBuf>(req->OutBuffer));

    auto* handler = req->Handler.get();
    handler->HandleResponse(std::move(req), RDMA_PROTO_FAIL, len);
}

bool TClientEndpoint::HandleCompletionEvents()
{
    ibv_cq* cq = CompletionQueue.get();

    if (WaitMode == EWaitMode::Poll) {
        Verbs->GetCompletionEvent(cq);
        Verbs->AckCompletionEvents(cq, 1);
        Verbs->RequestCompletionEvent(cq, 0);
    }

    if (Verbs->PollCompletionQueue(cq, this)) {
        HandleQueuedRequests();
        return true;
    }

    return false;
}

bool TClientEndpoint::IsWorkRequestValid(const TWorkRequestId& id) const
{
    if (id.Magic == SendMagic && id.Index < SendWrs.size()) {
        return true;
    }
    if (id.Magic == RecvMagic && id.Index < RecvWrs.size()) {
        return true;
    }
    return false;
}

void TClientEndpoint::HandleFlush(const TWorkRequestId& id) noexcept
{
    // flush WRs have opcode=0
    if (id.Magic == SendMagic && id.Index < SendWrs.size()) {
        SendQueue.Push(&SendWrs[id.Index]);
        return;
    }
    if (id.Magic == RecvMagic && id.Index < RecvWrs.size()) {
        RecvQueue.Push(&RecvWrs[id.Index]);
        return;
    }
}

// implements NVerbs::ICompletionHandler
void TClientEndpoint::HandleCompletionEvent(ibv_wc* wc)
{
    auto id = TWorkRequestId(wc->wr_id);

    RDMA_TRACE(NVerbs::GetOpcodeName(wc->opcode) << " " << id
        << " completed with " << NVerbs::GetStatusString(wc->status));

    if (!IsWorkRequestValid(id)) {
        RDMA_ERROR(LogThrottler.Unexpected, Log,
            "unexpected completion " << NVerbs::PrintCompletion(wc));

        Counters->UnexpectedCompletion();
        return;
    }

    if (wc->status == IBV_WC_WR_FLUSH_ERR) {
        HandleFlush(id);
        return;
    }

    switch (wc->opcode) {
        case IBV_WC_SEND:
            SendRequestCompleted(&SendWrs[id.Index], wc->status);
            break;

        case IBV_WC_RECV:
            RecvResponseCompleted(&RecvWrs[id.Index], wc->status);
            break;

        default:
            RDMA_ERROR(LogThrottler.Unexpected, Log,
                "unexpected completion " << NVerbs::PrintCompletion(wc));

            Counters->UnexpectedCompletion();
    }
}

void TClientEndpoint::SendRequest(TRequestPtr req, TSendWr* send)
{
    auto reqId = ActiveRequests.CreateId();
    req->ReqId = reqId;

    auto* requestMsg = send->Message<TRequestMessage>();
    Zero(*requestMsg);

    InitMessageHeader(requestMsg, RDMA_PROTO_VERSION);

    requestMsg->ReqId = req->ReqId;
    requestMsg->In = req->InBuffer;
    requestMsg->Out = req->OutBuffer;

    RDMA_TRACE("SEND " << TWorkRequestId(send->wr.wr_id));

    try {
        Verbs->PostSend(Connection->qp, &send->wr);
    } catch (const TServiceError& e) {
        RDMA_ERROR(
            "SEND " << TWorkRequestId(send->wr.wr_id) << ": " << e.what());
        SendQueue.Push(send);
        ReportRdmaError();
        Disconnect();

        Counters->RequestEnqueued();
        QueuedRequests.Enqueue(std::move(req));

        return;
    }

    LWTRACK(
        SendRequestStarted,
        req->CallContext->LWOrbit,
        req->CallContext->RequestId);

    Counters->SendRequestStarted();

    send->context = reinterpret_cast<void*>(static_cast<uintptr_t>(req->ReqId));
    auto reqIdPromise = std::move(req->ReqIdForRequestHandle);
    ActiveRequests.Push(std::move(req));

    reqIdPromise.SetValue(reqId);
}

void TClientEndpoint::SendRequestCompleted(
    TSendWr* send,
    ibv_wc_status status) noexcept
{
    Y_DEFER {
        Counters->SendRequestCompleted();
        SendQueue.Push(send);
    };

    if (status != IBV_WC_SUCCESS) {
        RDMA_ERROR("SEND " << TWorkRequestId(send->wr.wr_id)
            << ": " << NVerbs::GetStatusString(status));

        Counters->SendRequestError();
        ReportRdmaError();
        Disconnect();
        return;
    }

    auto reqId = SafeCast<ui32>(reinterpret_cast<uintptr_t>(send->context));

    if (auto* req = ActiveRequests.Get(reqId)) {
        LWTRACK(
            SendRequestCompleted,
            req->CallContext->LWOrbit,
            req->CallContext->RequestId);

    } else if (ActiveRequests.TimedOut(reqId)) {
        RDMA_INFO("SEND " << TWorkRequestId(send->wr.wr_id)
            << ": request has timed out before receiving send wc");

    } else if (ActiveRequests.Completed(reqId)) {
        RDMA_INFO("SEND " << TWorkRequestId(send->wr.wr_id)
            << ": request has been completed before receiving send wc");

    } else if (ActiveRequests.Cancelled(reqId)) {
        RDMA_INFO(
            "SEND " << TWorkRequestId(send->wr.wr_id)
                    << ": request was cancelled before receiving send wc");

    } else {
        RDMA_ERROR("SEND " << TWorkRequestId(send->wr.wr_id)
            << ": request not found")
        Counters->UnknownRequest();
    }
}

void TClientEndpoint::RecvResponse(TRecvWr* recv)
{
    auto* responseMsg = recv->Message<TResponseMessage>();
    Zero(*responseMsg);

    RDMA_TRACE("RECV " << TWorkRequestId(recv->wr.wr_id));

    try {
        Verbs->PostRecv(Connection->qp, &recv->wr);
    } catch (const TServiceError& e) {
        RDMA_ERROR(
            "RECV " << TWorkRequestId(recv->wr.wr_id) << ": " << e.what());
        RecvQueue.Push(recv);
        ReportRdmaError();
        Disconnect();
        return;
    }

    Counters->RecvResponseStarted();
}

void TClientEndpoint::RecvResponseCompleted(
    TRecvWr* recv,
    ibv_wc_status wc_status)
{
    if (wc_status != IBV_WC_SUCCESS) {
        RDMA_ERROR("RECV " << TWorkRequestId(recv->wr.wr_id)
            << ": " << NVerbs::GetStatusString(wc_status));

        Counters->RecvResponseError();
        RecvQueue.Push(recv);
        ReportRdmaError();
        Disconnect();
        return;
    }

    auto* msg = recv->Message<TResponseMessage>();
    int version = ParseMessageHeader(msg);
    if (version != RDMA_PROTO_VERSION) {
        RDMA_ERROR("RECV " << TWorkRequestId(recv->wr.wr_id)
            << ": incompatible protocol version "
            << version << ", expected " << int(RDMA_PROTO_VERSION));

        Counters->RecvResponseError();
        RecvResponse(recv);
        return;
    }
    const ui32 reqId = msg->ReqId;
    const ui32 status = msg->Status;
    const ui32 responseBytes = msg->ResponseBytes;

    Counters->RecvResponseCompleted();
    RecvResponse(recv);

    auto req = ActiveRequests.Pop(reqId);
    if (!req) {
        RDMA_ERROR("RECV " << TWorkRequestId(recv->wr.wr_id)
            << ": request not found");

        Counters->UnknownRequest();
        return;
    }

    LWTRACK(
        RecvResponseCompleted,
        req->CallContext->LWOrbit,
        req->CallContext->RequestId);

    auto* handler = req->Handler.get();
    handler->HandleResponse(
        std::move(req),
        status,
        responseBytes);
}

TFuture<void> TClientEndpoint::Stop() noexcept
{
    if (!StopFlag.test_and_set()) {
        Disconnect();
    }
    return StopResult.GetFuture();
}

bool TClientEndpoint::ShouldStop() const
{
    return StopFlag.test();
}

void TClientEndpoint::CancelRequest(TRequestIdPtr reqId) noexcept
{
    Counters->RequestEnqueued();
    CancelRequests.Enqueue(std::move(reqId));

    if (WaitMode == EWaitMode::Poll) {
        CancelRequestEvent.Set();
    }
}

void TClientEndpoint::SetConnection(NVerbs::TConnectionPtr connection) noexcept
{
    connection->context = this;
    Connection = std::move(connection);
}

int TClientEndpoint::ReconnectTimerHandle() const
{
    return Reconnect.Timer.Handle();
}

void TClientEndpoint::FlushQueues() noexcept
{
    RDMA_DEBUG("flush queues");

    try {
        ibv_qp_attr attr = {.qp_state = IBV_QPS_ERR};
        Verbs->ModifyQP(Connection->qp, &attr, IBV_QP_STATE);
        FlushStartCycles = GetCycleCount();

    } catch (const TServiceError& e) {
        RDMA_ERROR("flush error: " << e.what());
    }
}

void TClientEndpoint::Disconnect() noexcept
{
    Status = E_RDMA_UNAVAILABLE;

    switch (State) {
        // queues are empty, reconnect is scheduled, nothing to do
        case EEndpointState::Disconnecting:
        case EEndpointState::Disconnected:
        case EEndpointState::Connecting:
            return;

        // schedule reconnect
        case EEndpointState::ResolvingAddress:
        case EEndpointState::ResolvingRoute:
            break;

        // flush queues, signal the poller and schedule reconnect
        case EEndpointState::Connected:
            RDMA_INFO("disconnect");

            ChangeState(
                EEndpointState::Connected,
                EEndpointState::Disconnecting);

            FlushQueues();

            if (WaitMode == EWaitMode::Poll) {
                DisconnectEvent.Set();
            }
    }

    Reconnect.Schedule();
}

bool TClientEndpoint::Flushed() const
{
    return SendQueue.Size() == Config.QueueSize
        && RecvQueue.Size() == Config.QueueSize;
}

bool TClientEndpoint::FlushHanging() const
{
    return FlushStartCycles && CyclesToDurationSafe(GetCycleCount() -
        FlushStartCycles) >= FLUSH_TIMEOUT;
}

void TClientEndpoint::FreeRequest(TRequest* req) noexcept
{
    with_lock (AllocationLock) {
        SendBuffers.ReleaseBuffer(req->InBuffer);
        RecvBuffers.ReleaseBuffer(req->OutBuffer);
    }
}

////////////////////////////////////////////////////////////////////////////////

struct IConnectionEventHandler
{
    virtual ~IConnectionEventHandler() = default;

    virtual void HandleConnectionEvent(NVerbs::TConnectionEventPtr event) = 0;
    virtual void Reconnect(TClientEndpoint* endpoint) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TConnectionPoller final
    : public IStartable
    , private ISimpleThread
{
private:
    enum EPollEvent
    {
        ConnectionEvent = 0,
        ReconnectTimer = 1,
    };

private:
    NVerbs::IVerbsPtr Verbs;

    IConnectionEventHandler* EventHandler;
    TLog Log;

    NVerbs::TEventChannelPtr EventChannel = NVerbs::NullPtr;
    TPollHandle PollHandle;

    TAtomic StopFlag = 0;
    TEventHandle StopEvent;

public:
    TConnectionPoller(
            NVerbs::IVerbsPtr verbs,
            IConnectionEventHandler* eventHandler,
            TLog log)
        : Verbs(std::move(verbs))
        , EventHandler(eventHandler)
        , Log(log)
    {
        PollHandle.Attach(StopEvent.Handle(), EPOLLIN);

        EventChannel = Verbs->CreateEventChannel();
        SetNonBlock(EventChannel->fd, true);
        PollHandle.Attach(
            EventChannel->fd,
            EPOLLIN,
            PtrEventTag(EventChannel.get(), EPollEvent::ConnectionEvent));
    }

    void Start() override
    {
        ISimpleThread::Start();
    }

    void Stop() override
    {
        AtomicSet(StopFlag, 1);
        StopEvent.Set();

        Join();
    }

    NVerbs::TConnectionPtr CreateConnection()
    {
        return Verbs->CreateConnection(
            EventChannel.get(),
            nullptr,    // context
            RDMA_PS_TCP);
    }

    void Attach(TClientEndpoint* endpoint)
    {
        PollHandle.Attach(
            endpoint->ReconnectTimerHandle(),
            EPOLLIN | EPOLLET,
            PtrEventTag(endpoint, EPollEvent::ReconnectTimer));
    }

    void Detach(TClientEndpoint* endpoint)
    {
        PollHandle.Detach(endpoint->ReconnectTimerHandle());
    }

private:
    bool ShouldStop() const
    {
        return AtomicGet(StopFlag) != 0;
    }

    void* ThreadProc() override
    {
        NCloud::SetCurrentThreadName("RDMA.CM");

        while (!ShouldStop()) {
            size_t signaled = PollHandle.Wait(POLL_TIMEOUT);

            for (size_t i = 0; i < signaled; ++i) {
                const auto& event = PollHandle.GetEvent(i);

                if (!event.events || !event.data.ptr) {
                    continue;
                }

                switch (EventFromTag(event.data.ptr)) {
                    case EPollEvent::ConnectionEvent:
                        HandleConnectionEvents();
                        break;

                    case EPollEvent::ReconnectTimer:
                        EventHandler->Reconnect(
                            PtrFromTag<TClientEndpoint>(event.data.ptr));
                        break;
                }
            }
        }

        return nullptr;
    }

    NVerbs::TConnectionEventPtr GetConnectionEvent()
    {
        try {
            return Verbs->GetConnectionEvent(EventChannel.get());

        } catch (const TServiceError &e) {
            RDMA_ERROR(e.what());
            return NVerbs::NullPtr;
        }
    }

    void HandleConnectionEvents()
    {
        while (auto event = GetConnectionEvent()) {
            EventHandler->HandleConnectionEvent(std::move(event));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCompletionPoller final
    : public IStartable
    , private ISimpleThread
{
private:
    enum EPollEvent
    {
        Completion = 0,
        Request = 1,
        Disconnect = 2,
        CancelRequest = 3,
    };

private:
    NVerbs::IVerbsPtr Verbs;

    TClientConfigPtr Config;
    TLog Log;

    TRCUList<TClientEndpointPtr> Endpoints;
    TPollHandle PollHandle;

    TAtomic StopFlag = 0;
    TEventHandle StopEvent;

public:
    TCompletionPoller(
            NVerbs::IVerbsPtr verbs,
            TClientConfigPtr config,
            TLog log)
        : Verbs(std::move(verbs))
        , Config(std::move(config))
        , Log(log)
    {
        if (Config->WaitMode == EWaitMode::Poll) {
            PollHandle.Attach(StopEvent.Handle(), EPOLLIN);
        }
    }

    void Start() override
    {
        ISimpleThread::Start();
    }

    void Stop() override
    {
        AtomicSet(StopFlag, 1);

        if (Config->WaitMode == EWaitMode::Poll) {
            StopEvent.Set();
        }

        Join();
    }

    void Acquire(TClientEndpointPtr endpoint)
    {
        endpoint->Poller = this;
        Endpoints.Add(std::move(endpoint));
    }

    void Release(TClientEndpoint* endpoint)
    {
        endpoint->Poller = nullptr;
        Endpoints.Delete([=](auto x) {
            return endpoint == x.get();
        });
    }

    void Attach(TClientEndpoint* endpoint)
    {
        if (Config->WaitMode == EWaitMode::Poll) {
            PollHandle.Attach(
                endpoint->CompletionChannel->fd,
                EPOLLIN,
                PtrEventTag(endpoint, EPollEvent::Completion));

            PollHandle.Attach(
                endpoint->RequestEvent.Handle(),
                EPOLLIN,
                PtrEventTag(endpoint, EPollEvent::Request));

            PollHandle.Attach(
                endpoint->CancelRequestEvent.Handle(),
                EPOLLIN,
                PtrEventTag(endpoint, EPollEvent::CancelRequest));

            PollHandle.Attach(
                endpoint->DisconnectEvent.Handle(),
                EPOLLIN,
                PtrEventTag(endpoint, EPollEvent::Disconnect));

            Verbs->RequestCompletionEvent(endpoint->CompletionQueue.get(), 0);
        }
    }

    void Detach(TClientEndpoint* endpoint)
    {
        if (Config->WaitMode == EWaitMode::Poll) {
            PollHandle.Detach(endpoint->CompletionChannel->fd);
            PollHandle.Detach(endpoint->RequestEvent.Handle());
            PollHandle.Detach(endpoint->CancelRequestEvent.Handle());
            PollHandle.Detach(endpoint->DisconnectEvent.Handle());
        }
    }

    auto GetEndpoints()
    {
        return Endpoints.Get();
    }

private:
    bool ShouldStop() const
    {
        return AtomicGet(StopFlag) != 0;
    }

    void* ThreadProc() override
    {
        SetHighestThreadPriority();
        NCloud::SetCurrentThreadName("RDMA.CQ");

        switch (Config->WaitMode) {
            case EWaitMode::Poll:
                Execute<EWaitMode::Poll>();
                break;

            case EWaitMode::BusyWait:
                Execute<EWaitMode::BusyWait>();
                break;

            case EWaitMode::AdaptiveWait:
                Execute<EWaitMode::AdaptiveWait>();
                break;
        }

        return nullptr;
    }

    void HandlePollEvent(const epoll_event& event)
    {
        auto* endpoint = PtrFromTag<TClientEndpoint>(event.data.ptr);

        try {
            switch (EventFromTag(event.data.ptr)) {
                case EPollEvent::Completion:
                    endpoint->HandleCompletionEvents();
                    break;

                case EPollEvent::Request:
                    endpoint->HandleInputRequests();
                    break;

                case EPollEvent::CancelRequest:
                    endpoint->HandleCancelRequests();
                    break;

                case EPollEvent::Disconnect:
                    endpoint->AbortRequests();
                    break;
            }
        } catch (const TServiceError& e) {
            RDMA_ERROR(endpoint->Log, e.what());
            endpoint->Disconnect();
        }
    }

    void HandlePollEvents()
    {
        // wait for completion events
        size_t signaled = PollHandle.Wait(POLL_TIMEOUT);

        for (size_t i = 0; i < signaled; ++i) {
            const auto& event = PollHandle.GetEvent(i);

            if (event.events && event.data.ptr) {
                HandlePollEvent(event);
            }
        }
    }

    bool HandleEvents()
    {
        auto endpoints = Endpoints.Get();
        auto hasWork = false;

        for (const auto& endpoint: *endpoints) {
            try {
                if (endpoint->CheckState(EEndpointState::Connected)) {
                    hasWork |= endpoint->HandleInputRequests();
                    hasWork |= endpoint->HandleCancelRequests();
                    hasWork |= endpoint->HandleCompletionEvents();
                }
                if (endpoint->CheckState(EEndpointState::Disconnecting)) {
                    hasWork |= endpoint->HandleCompletionEvents();
                    hasWork |= endpoint->AbortRequests();
                }
            } catch (const TServiceError& e) {
                RDMA_ERROR(endpoint->Log, e.what());
                endpoint->Disconnect();
            }
        }

        return hasWork;
    }

    void DropTimedOutRequests()
    {
        auto endpoints = Endpoints.Get();

        for (const auto& endpoint: *endpoints) {
            if (!endpoint->CheckState(EEndpointState::Connected)) {
                continue;
            }

            auto requests = endpoint->ActiveRequests.PopTimedOutRequests(
                DurationToCyclesSafe(Config->MaxResponseDelay));

            for (auto& request: requests) {
                endpoint->AbortRequest(
                    std::move(request),
                    E_TIMEOUT,
                    "request timeout");
            }
        }
    }

    void DisconnectFlushed()
    {
        auto endpoints = Endpoints.Get();

        for (const auto& endpoint: *endpoints) {
            if (!endpoint->CheckState(EEndpointState::Disconnecting)) {
                continue;
            }

            if (!endpoint->Flushed()) {
                if (!endpoint->FlushHanging()) {
                    continue;
                }
                RDMA_ERROR(endpoint->Log, "flush timeout "
                    << "[send_queue.size=" << endpoint->SendQueue.Size()
                    << " recv_queue.size=" << endpoint->RecvQueue.Size() << "]");
            }

            endpoint->ChangeState(
                EEndpointState::Disconnecting,
                EEndpointState::Disconnected);
        }
    }

    template <EWaitMode WaitMode>
    void Execute()
    {
        TAdaptiveWait aw(
            Config->AdaptiveWaitSleepDuration,
            Config->AdaptiveWaitSleepDelay);

        while (!ShouldStop()) {
            switch (WaitMode) {
                case EWaitMode::Poll:
                    HandlePollEvents();
                    break;

                case EWaitMode::BusyWait:
                    HandleEvents();
                    break;

                case EWaitMode::AdaptiveWait:
                    if (HandleEvents()) {
                        aw.Reset();
                    } else {
                        aw.Sleep();
                    }
            }

            DropTimedOutRequests();
            DisconnectFlushed();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TClient final
    : public IClient
    , public IConnectionEventHandler
{
private:
    NVerbs::IVerbsPtr Verbs;

    ILoggingServicePtr Logging;
    IMonitoringServicePtr Monitoring;

    TClientConfigPtr Config;
    TEndpointCountersPtr Counters;
    TLog Log;

    TConnectionPollerPtr ConnectionPoller;
    TVector<TCompletionPollerPtr> CompletionPollers;

public:
    TClient(
        NVerbs::IVerbsPtr verbs,
        ILoggingServicePtr logging,
        IMonitoringServicePtr monitoring,
        TClientConfigPtr config);

    // called from external thread
    void Start() noexcept override;
    void Stop() noexcept override;
    TFuture<IClientEndpointPtr> StartEndpoint(
        TString host,
        ui32 port) noexcept override;
    void DumpHtml(IOutputStream& out) const override;
    bool IsAlignedDataEnabled() const override;

private:
    // called from external thread
    void HandleConnectionEvent(
        NVerbs::TConnectionEventPtr event) noexcept override;

    // called from CM thread
    void Reconnect(TClientEndpoint* endpont) noexcept override;
    void BeginResolveAddress(TClientEndpoint* endpoint) noexcept;
    void BeginResolveRoute(TClientEndpoint* endpoint) noexcept;
    void BeginConnect(TClientEndpoint* endpoint) noexcept;
    void HandleDisconnected(TClientEndpoint* endpoint) noexcept;
    void HandleConnected(
        TClientEndpoint* endpoint,
        NVerbs::TConnectionEventPtr event) noexcept;
    void HandleRejected(
        TClientEndpoint* endpoint,
        NVerbs::TConnectionEventPtr event) noexcept;
    TCompletionPoller& PickPoller() noexcept;
    void StopEndpoint(TClientEndpoint* endpoint) noexcept;
};

////////////////////////////////////////////////////////////////////////////////

TClient::TClient(
        NVerbs::IVerbsPtr verbs,
        ILoggingServicePtr logging,
        IMonitoringServicePtr monitoring,
        TClientConfigPtr config)
    : Verbs(std::move(verbs))
    , Logging(std::move(logging))
    , Monitoring(std::move(monitoring))
    , Config(std::move(config))
    , Counters(new TEndpointCounters())
{
    // check basic functionality for early problem detection
    Verbs->GetDeviceList();
    Verbs->GetAddressInfo("localhost", 10020, nullptr);
}

void TClient::Start() noexcept
{
    Log = Logging->CreateLog("BLOCKSTORE_RDMA");

    RDMA_DEBUG("start client");

    auto counters = Monitoring->GetCounters();
    auto rootGroup = counters->GetSubgroup("counters", "blockstore");
    Counters->Register(*rootGroup->GetSubgroup("component", "rdma_client"));

    CompletionPollers.resize(Config->PollerThreads);
    for (size_t i = 0; i < CompletionPollers.size(); ++i) {
        CompletionPollers[i] = std::make_unique<TCompletionPoller>(
            Verbs,
            Config,
            Log);
        CompletionPollers[i]->Start();
    }

    try {
        ConnectionPoller = std::make_unique<TConnectionPoller>(Verbs, this, Log);
        ConnectionPoller->Start();

    } catch (const TServiceError &e) {
        RDMA_ERROR("unable to start client: " << e.what());
        Stop();
    }
}

void TClient::Stop() noexcept
{
    RDMA_DEBUG("stop client");

    if (ConnectionPoller) {
        ConnectionPoller->Stop();
        ConnectionPoller.reset();
    }

    for (auto& poller: CompletionPollers) {
        poller->Stop();
    }
    CompletionPollers.clear();
}

// implements IClient
TFuture<IClientEndpointPtr> TClient::StartEndpoint(
    TString host,
    ui32 port) noexcept
{
    auto unavailable = [&](TString message) {
        return MakeErrorFuture<IClientEndpointPtr>(
            std::make_exception_ptr(TServiceError(
                MakeError(E_RDMA_UNAVAILABLE, std::move(message)))));
    };

    if (ConnectionPoller == nullptr) {
        return unavailable("rdma client is down");
    }

    try {
        auto endpoint = std::make_shared<TClientEndpoint>(
            Verbs,
            ConnectionPoller->CreateConnection(),
            std::move(host),
            port,
            Config,
            Counters,
            Log);

        auto future = endpoint->StartResult.GetFuture();

        ConnectionPoller->Attach(endpoint.get());
        PickPoller().Acquire(endpoint);
        BeginResolveAddress(endpoint.get());
        return future;

    } catch (const TServiceError& e) {
        return unavailable("unable to start rdma endpoint");
    }
}

////////////////////////////////////////////////////////////////////////////////

// implements IConnectionEventHandler
void TClient::HandleConnectionEvent(NVerbs::TConnectionEventPtr event) noexcept
{
    TClientEndpoint* endpoint = TClientEndpoint::FromEvent(event.get());

    RDMA_DEBUG(endpoint->Log, "received " << NVerbs::GetEventName(event->event));

    switch (event->event) {
        case RDMA_CM_EVENT_CONNECT_REQUEST:
            // not relevant for the client
            break;

        case RDMA_CM_EVENT_MULTICAST_JOIN:
        case RDMA_CM_EVENT_MULTICAST_ERROR:
            // multicast is not used
            break;

        case RDMA_CM_EVENT_TIMEWAIT_EXIT:
            // QPs is not re-used
            break;

        case RDMA_CM_EVENT_CONNECT_RESPONSE:
            // generated only if rdma_id doesn't have associated QP
            break;

        case RDMA_CM_EVENT_ADDR_RESOLVED:
            BeginResolveRoute(endpoint);
            break;

        case RDMA_CM_EVENT_ROUTE_RESOLVED:
            BeginConnect(endpoint);
            break;

        case RDMA_CM_EVENT_ESTABLISHED:
            HandleConnected(endpoint, std::move(event));
            break;

        case RDMA_CM_EVENT_REJECTED:
            HandleRejected(endpoint, std::move(event));
            break;

        case RDMA_CM_EVENT_ADDR_ERROR:
        case RDMA_CM_EVENT_ROUTE_ERROR:
        case RDMA_CM_EVENT_CONNECT_ERROR:
        case RDMA_CM_EVENT_UNREACHABLE:
        case RDMA_CM_EVENT_DISCONNECTED:
            HandleDisconnected(endpoint);
            break;

        case RDMA_CM_EVENT_DEVICE_REMOVAL:
        case RDMA_CM_EVENT_ADDR_CHANGE:
            // TODO
            break;
    }
}

void TClient::StopEndpoint(TClientEndpoint* endpoint) noexcept
{
    RDMA_INFO(endpoint->Log, "detach pollers and close connection");

    ConnectionPoller->Detach(endpoint);
    endpoint->Poller->Detach(endpoint);
    endpoint->DestroyQP();
    endpoint->Connection.reset();
    endpoint->StopResult.SetValue();
    endpoint->Poller->Release(endpoint);
}

// implements IConnectionEventHandler
void TClient::Reconnect(TClientEndpoint* endpoint) noexcept
{
    if (endpoint->ShouldStop()) {
        if (endpoint->CheckState(EEndpointState::Disconnecting)) {
            // wait for completion poller to flush WRs
            endpoint->Reconnect.Schedule();
        } else {
            // detach pollers and close connection
            StopEndpoint(endpoint);
        }
        return;
    }

    if (endpoint->Reconnect.Hanging()) {
        // if this is our first connection, fail over to IC
        if (endpoint->StartResult.Initialized()) {
            RDMA_ERROR(endpoint->Log, "connection timeout");

            auto startResult = std::move(endpoint->StartResult);
            startResult.SetException(std::make_exception_ptr(TServiceError(
                MakeError(endpoint->Status, "connection timeout"))));

            StopEndpoint(endpoint);
            return;
        }
        // otherwise keep trying
        RDMA_WARN(endpoint->Log, "connection is hanging");
    }

    RDMA_DEBUG(endpoint->Log, "reconnect timer hit in "
        << GetEndpointStateName(endpoint->State) << " state");

    switch (endpoint->State) {
        // wait for completion poller to flush WRs
        case EEndpointState::Disconnecting:
            endpoint->Reconnect.Schedule();
            return;

        // didn't even start to connect, try again
        case EEndpointState::ResolvingAddress:
        case EEndpointState::ResolvingRoute:
            break;

        // create new connection and try again
        case EEndpointState::Connecting:
        case EEndpointState::Disconnected:
            endpoint->Poller->Detach(endpoint);
            endpoint->DestroyQP();
            endpoint->SetConnection(ConnectionPoller->CreateConnection());
            break;

        // reconnect timer hit at the same time connection was established
        case EEndpointState::Connected:
            return;
    }

    endpoint->ChangeState(EEndpointState::Disconnected);
    BeginResolveAddress(endpoint);
}

void TClient::BeginResolveAddress(TClientEndpoint* endpoint) noexcept
{
    try {
        rdma_addrinfo hints = {
            .ai_port_space = RDMA_PS_TCP,
        };

        auto addrinfo = Verbs->GetAddressInfo(
            endpoint->Host, endpoint->Port, &hints);

        RDMA_DEBUG(endpoint->Log, "resolve rdma address");

        endpoint->ChangeState(
            EEndpointState::Disconnected,
            EEndpointState::ResolvingAddress);

        Verbs->ResolveAddress(endpoint->Connection.get(), addrinfo->ai_src_addr,
            addrinfo->ai_dst_addr, RESOLVE_TIMEOUT);

    } catch (const TServiceError& e) {
        RDMA_ERROR(endpoint->Log, e.what());
        endpoint->Disconnect();
    }
}

void TClient::BeginResolveRoute(TClientEndpoint* endpoint) noexcept
{
    RDMA_DEBUG(endpoint->Log, "resolve route");

    endpoint->ChangeState(
        EEndpointState::ResolvingAddress,
        EEndpointState::ResolvingRoute);

    try {
        Verbs->ResolveRoute(endpoint->Connection.get(), RESOLVE_TIMEOUT);

    } catch (const TServiceError& e) {
        RDMA_ERROR(endpoint->Log, e.what());
        endpoint->Disconnect();
    }
}

void TClient::BeginConnect(TClientEndpoint* endpoint) noexcept
{
    Y_ABORT_UNLESS(endpoint);

    try {
        endpoint->ChangeState(
            EEndpointState::ResolvingRoute,
            EEndpointState::Connecting);

        endpoint->CreateQP();
        endpoint->Poller->Attach(endpoint);
        endpoint->Reconnect.Schedule(MIN_CONNECT_TIMEOUT);

        TConnectMessage message = {
            .QueueSize = SafeCast<ui16>(endpoint->Config.QueueSize),
            .MaxBufferSize = SafeCast<ui32>(endpoint->Config.MaxBufferSize),
        };
        InitMessageHeader(&message, RDMA_PROTO_VERSION);

        rdma_conn_param param = {
            .private_data = &message,
            .private_data_len = sizeof(TConnectMessage),
            .responder_resources = RDMA_MAX_RESP_RES,
            .initiator_depth = RDMA_MAX_INIT_DEPTH,
            .flow_control = 1,
            .retry_count = 7,
            .rnr_retry_count = 7,
        };

        RDMA_INFO(endpoint->Log, "connect "
            << NVerbs::PrintConnectionParams(&param));

        Verbs->Connect(endpoint->Connection.get(), &param);

    } catch (const TServiceError& e) {
        RDMA_ERROR(endpoint->Log, e.what());
        endpoint->Disconnect();
    }
}

void TClient::HandleConnected(
    TClientEndpoint* endpoint,
    NVerbs::TConnectionEventPtr event) noexcept
{
    const rdma_conn_param* param = &event->param.conn;

    RDMA_DEBUG(endpoint->Log, "validate "
        << NVerbs::PrintConnectionParams(param));

    if (param->private_data == nullptr ||
        param->private_data_len < sizeof(TAcceptMessage) ||
        ParseMessageHeader(param->private_data) != RDMA_PROTO_VERSION)
    {
        RDMA_ERROR(endpoint->Log, "unable to parse accept message");
        endpoint->Disconnect();
        return;
    }

    endpoint->ChangeState(
        EEndpointState::Connecting,
        EEndpointState::Connected);

    endpoint->Reconnect.Cancel();
    endpoint->Status = S_OK;

    try {
        endpoint->StartReceive();

    } catch (const TServiceError& e) {
        RDMA_ERROR(endpoint->Log, e.what());
        endpoint->Disconnect();
        return;
    }

    if (endpoint->StartResult.Initialized()) {
        auto startResult = std::move(endpoint->StartResult);
        startResult.SetValue(endpoint->shared_from_this());
    }
}

void TClient::HandleRejected(
    TClientEndpoint* endpoint,
    NVerbs::TConnectionEventPtr event) noexcept
{
    const rdma_conn_param* param = &event->param.conn;

    if (param->private_data == nullptr ||
        param->private_data_len < sizeof(TRejectMessage) ||
        ParseMessageHeader(param->private_data) != RDMA_PROTO_VERSION)
    {
        endpoint->Disconnect();
        return;
    }

    const auto* msg = static_cast<const TRejectMessage*>(
        param->private_data);

    if (msg->Status == RDMA_PROTO_CONFIG_MISMATCH) {
        if (endpoint->Config.QueueSize > msg->QueueSize) {
            RDMA_INFO(endpoint->Log, "set QueueSize=" << msg->QueueSize
                << " supported by " << endpoint->Host);

            endpoint->Config.QueueSize = msg->QueueSize;
        }

        if (endpoint->Config.MaxBufferSize > msg->MaxBufferSize) {
            RDMA_INFO(endpoint->Log, "set MaxBufferSize=" << msg->MaxBufferSize
                << " supported by " << endpoint->Host);

            endpoint->Config.MaxBufferSize = msg->MaxBufferSize;
        }
    }

    endpoint->Disconnect();
}

void TClient::HandleDisconnected(TClientEndpoint* endpoint) noexcept
{
    // we can't reset config right away, because disconnect needs to know queue
    // size to reap flushed WRs
    endpoint->ResetConfig = true;
    endpoint->Disconnect();
}

TCompletionPoller& TClient::PickPoller() noexcept
{
    size_t index = RandomNumber(CompletionPollers.size());
    return *CompletionPollers[index];
}

void TClient::DumpHtml(IOutputStream& out) const
{
    HTML(out) {
        TAG(TH4) { out << "Config"; }
        Config->DumpHtml(out);

        TAG(TH4) { out << "Counters"; }
        TABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "QueuedRequests"; }
                    TABLEH() { out << "ActiveRequests"; }
                    TABLEH() { out << "AbortedRequests"; }
                    TABLEH() { out << "CompletedRequests"; }
                    TABLEH() { out << "UnknownRequests"; }
                    TABLEH() { out << "ActiveSend"; }
                    TABLEH() { out << "ActiveRecv"; }
                    TABLEH() { out << "SendErrors"; }
                    TABLEH() { out << "RecvErrors"; }
                    TABLEH() { out << "UnexpectedCompletions"; }
                }
                TABLER() {
                    TABLED() { out << Counters->QueuedRequests->Val(); }
                    TABLED() { out << Counters->ActiveRequests->Val(); }
                    TABLED() { out << Counters->AbortedRequests->Val(); }
                    TABLED() { out << Counters->CompletedRequests->Val(); }
                    TABLED() { out << Counters->UnknownRequests->Val(); }
                    TABLED() { out << Counters->ActiveSend->Val(); }
                    TABLED() { out << Counters->ActiveRecv->Val(); }
                    TABLED() { out << Counters->SendErrors->Val(); }
                    TABLED() { out << Counters->RecvErrors->Val(); }
                    TABLED() { out << Counters->UnexpectedCompletions->Val(); }
                }
            }
        }

        TAG(TH4) { out << "Endpoints"; }
        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Poller"; }
                    TABLEH() { out << "Id"; }
                    TABLEH() { out << "Host"; }
                    TABLEH() { out << "Port"; }
                    TABLEH() { out << "Magic"; }
                }
            }

            for (size_t i = 0; i < CompletionPollers.size(); ++i) {
                auto& poller = CompletionPollers[i];
                auto endpoints = poller->GetEndpoints();

                for (auto& ep: *endpoints) {
                    TABLER() {
                        TABLED() { out << i; }
                        TABLED() { out << ep->Id; }
                        TABLED() { out << ep->Host; }
                        TABLED() { out << ep->Port; }
                        TABLED()
                        {
                            Printf(
                                out,
                                "%08X:%08X:%d",
                                ep->SendMagic,
                                ep->RecvMagic,
                                ep->Generation);
                        }
                    }
                }
            }
        }
    }
}

bool TClient::IsAlignedDataEnabled() const
{
    return Config->AlignedDataEnabled;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(
    NVerbs::IVerbsPtr verbs,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    TClientConfigPtr config)
{
    return std::make_shared<TClient>(
        std::move(verbs),
        std::move(logging),
        std::move(monitoring),
        std::move(config));
}

}   // namespace NCloud::NBlockStore::NRdma
