#include "client.h"

#include "buffer.h"
#include "verbs.h"
#include "work_queue.h"
#include "adaptive_wait.h"

#include <cloud/blockstore/libs/rdma/iface/error.h>
#include <cloud/blockstore/libs/rdma/iface/list.h>
#include <cloud/blockstore/libs/rdma/iface/poll.h>
#include <cloud/blockstore/libs/rdma/iface/probes.h>
#include <cloud/blockstore/libs/rdma/iface/protocol.h>
#include <cloud/blockstore/libs/rdma/iface/rcu.h>
#include <cloud/blockstore/libs/rdma/iface/utils.h>

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/service/context.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/random/random.h>
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

constexpr TDuration MIN_RECONNECT_DELAY = TDuration::MilliSeconds(10);

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

////////////////////////////////////////////////////////////////////////////////

constexpr int EVENT_MASK = 3;   // low bits unused because of alignment

enum EPollerEvent
{
    Completion = 0,
    Request = 1,
    Disconnect = 2,
};

template <typename T>
void* PtrEventTag(T* ptr, int event)
{
    auto tag = reinterpret_cast<uintptr_t>(ptr) | (event & EVENT_MASK);
    return reinterpret_cast<void*>(tag);
}

template <typename T>
T* PtrFromTag(void* tag)
{
    auto ptr = reinterpret_cast<uintptr_t>(tag) & ~EVENT_MASK;
    return reinterpret_cast<T*>(ptr);
}

int EventFromTag(void* tag)
{
    return reinterpret_cast<uintptr_t>(tag) & EVENT_MASK;
}

////////////////////////////////////////////////////////////////////////////////

struct TRequest
    : TClientRequest
    , TListNode<TRequest>
{
    const ui64 StartedCycles;

    std::weak_ptr<TClientEndpoint> Endpoint;

    TCallContextPtr CallContext;
    ui32 ReqId = 0;

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
    THashSet<TRequest*> Pointers;

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
        Y_VERIFY(Pointers.emplace(req.get()).second);
        Y_VERIFY(Requests.emplace(req->ReqId, std::move(req)).second);
    }

    TRequestPtr Pop(ui32 reqId)
    {
        auto it = Requests.find(reqId);
        if (it != Requests.end()) {
            TRequestPtr req = std::move(it->second);
            Pointers.erase(req.get());
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
        Pointers.erase(req.get());
        Requests.erase(it);
        return req;
    }

    bool Contains(TRequest* ptr)
    {
        return Pointers.contains(ptr);
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
            // TODO: keep tombstones to distinguish between timed out requests
            // and unknown reqIds
            Pointers.erase(x.get());
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

    TDynamicCounters::TCounterPtr ActiveSend;
    TDynamicCounters::TCounterPtr ActiveRecv;

    TDynamicCounters::TCounterPtr SendErrors;
    TDynamicCounters::TCounterPtr RecvErrors;

    void Register(TDynamicCounters& counters)
    {
        QueuedRequests = counters.GetCounter("QueuedRequests");
        ActiveRequests = counters.GetCounter("ActiveRequests");
        CompletedRequests = counters.GetCounter("CompletedRequests", true);
        AbortedRequests = counters.GetCounter("AbortedRequests");

        ActiveSend = counters.GetCounter("ActiveSend");
        ActiveRecv = counters.GetCounter("ActiveRecv");

        SendErrors = counters.GetCounter("SendErrors");
        RecvErrors = counters.GetCounter("RecvErrors");
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
        ActiveSend->Dec();
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

    void ResetActiveSend()
    {
        ActiveSend->Set(0);
    }

    void ResetActiveRecv()
    {
        ActiveRecv->Set(0);
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

    // config might be adjusted during initial handshake
    TClientConfigPtr OriginalConfig;
    TClientConfig Config;
    bool ResetConfig = false;

    TCompletionPoller* Poller = nullptr;

    std::atomic<EEndpointState> State = EEndpointState::Disconnected;
    std::atomic<ui32> Status = S_OK;

    NVerbs::TCompletionChannelPtr CompletionChannel = NVerbs::NullPtr;
    NVerbs::TCompletionQueuePtr CompletionQueue = NVerbs::NullPtr;

    TPromise<IClientEndpointPtr> StartResult = NewPromise<IClientEndpointPtr>();

    TBufferPool SendBuffers;
    TBufferPool RecvBuffers;
    TMutex AllocationLock;

    TPooledBuffer SendBuffer {};
    TPooledBuffer RecvBuffer {};

    TVector<TSendWr> SendWrs;
    TVector<TRecvWr> RecvWrs;

    TWorkQueue<TSendWr> SendQueue;
    TWorkQueue<TRecvWr> RecvQueue;

    TLockFreeList<TRequest> InputRequests;
    TEventHandle RequestEvent;
    TEventHandle DisconnectEvent;

    TSimpleList<TRequest> QueuedRequests;
    TActiveRequests ActiveRequests;

public:
    static TClientEndpoint* FromEvent(rdma_cm_event* event)
    {
        Y_VERIFY(event->id && event->id->context);
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
    void SetError(ui32 error) noexcept;

    // called from CM thread
    void InitCompletionQueue();
    void StartReceive();
    void ResetConnection(NVerbs::TConnectionPtr connection) noexcept;
    int ReconnectTimerHandle() const;

    // called from client thread
    TResultOrError<TClientRequestPtr> AllocateRequest(
        IClientHandlerPtr handler,
        std::unique_ptr<TNullContext> context,
        size_t requestBytes,
        size_t responseBytes) noexcept override;

    void SendRequest(
        TClientRequestPtr creq,
        TCallContextPtr callContext) noexcept override;

    // called from CQ thread
    bool HandleInputRequests();
    bool HandleCompletionEvents();
    bool HandleDisconnect() noexcept;
    bool Flushed() const;

private:
    void HandleQueuedRequests();
    void HandleCompletionEvent(const NVerbs::TCompletion& wc) override;

    void SendRequest(TRequestPtr req, TSendWr* send);
    void SendRequestCompleted(
        TSendWr* send, ibv_wc_status status, ui64 ts) noexcept;

    void RecvResponse(TRecvWr* recv);
    void RecvResponseCompleted(TRecvWr* recv, ibv_wc_status status, ui64 ts);

    ibv_wc_opcode GetOpcode(const NVerbs::TCompletion& wc) const;

    void AbortRequest(TRequestPtr req, ui32 err, const TString& msg) noexcept;
    void FreeRequest(TRequest* creq) noexcept;
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
{
    // user data attached to connection events
    Connection->context = this;
}

TClientEndpoint::~TClientEndpoint()
{
    if (SendBuffers.Initialized()) {
        SendBuffers.ReleaseBuffer(SendBuffer);
    }

    if (RecvBuffers.Initialized()) {
        RecvBuffers.ReleaseBuffer(RecvBuffer);
    }

    // TODO detach pollers
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

    Y_VERIFY(actualState == expectedState,
        "invalid state transition (new: %s, expected: %s, actual: %s)",
        GetEndpointStateName(newState),
        GetEndpointStateName(expectedState),
        GetEndpointStateName(actualState));

    STORAGE_DEBUG("change state from %s to %s",
        GetEndpointStateName(expectedState),
        GetEndpointStateName(newState));
}

void TClientEndpoint::ChangeState(EEndpointState newState) noexcept
{
    auto currentState = State.exchange(newState);

    STORAGE_DEBUG("change state from %s to %s",
        GetEndpointStateName(currentState),
        GetEndpointStateName(newState));
}

void TClientEndpoint::InitCompletionQueue()
{
    CompletionChannel = Verbs->CreateCompletionChannel(Connection->verbs);
    SetNonBlock(CompletionChannel->fd, true);

    if (ResetConfig) {
        Config = *OriginalConfig;
        ResetConfig = false;
    }

    ibv_cq_init_attr_ex cq_attrs = {
        .cqe = 2 * Config.QueueSize,     // send + recv
        .cq_context = this,
        .channel = CompletionChannel.get(),
        .wc_flags = IBV_WC_EX_WITH_COMPLETION_TIMESTAMP,
        .comp_mask = IBV_CQ_INIT_ATTR_MASK_FLAGS,
        .flags = IBV_CREATE_CQ_ATTR_SINGLE_THREADED,
    };

    CompletionQueue = Verbs->CreateCompletionQueue(
        Connection->verbs,
        &cq_attrs);

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

    ui64 requestMsg = SendBuffer.Address;
    for (auto& wr: SendWrs) {
        wr.wr.opcode = IBV_WR_SEND;

        wr.wr.wr_id = reinterpret_cast<uintptr_t>(&wr);
        wr.wr.sg_list = wr.sg_list;
        wr.wr.num_sge = 1;

        wr.sg_list[0].lkey = SendBuffer.Key;
        wr.sg_list[0].addr = requestMsg;
        wr.sg_list[0].length = sizeof(TRequestMessage);

        SendQueue.Push(&wr);
        requestMsg += sizeof(TRequestMessage);
    }

    ui64 responseMsg = RecvBuffer.Address;
    for (auto& wr: RecvWrs) {
        wr.wr.wr_id = reinterpret_cast<uintptr_t>(&wr);
        wr.wr.sg_list = wr.sg_list;
        wr.wr.num_sge = 1;

        wr.sg_list[0].lkey = RecvBuffer.Key;
        wr.sg_list[0].addr = responseMsg;
        wr.sg_list[0].length = sizeof(TResponseMessage);

        RecvQueue.Push(&wr);
        responseMsg += sizeof(TResponseMessage);
    }
}

void TClientEndpoint::StartReceive()
{
    while (auto* recv = RecvQueue.Pop()) {
        RecvResponse(recv);
    }
}

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
        requestBytes,
    };
    req->ResponseBuffer = TStringBuf {
        reinterpret_cast<char*>(req->OutBuffer.Address),
        responseBytes,
    };

    return TClientRequestPtr(std::move(req));
}

void TClientEndpoint::SendRequest(
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
        return;
    }

    Counters->RequestEnqueued();
    InputRequests.Enqueue(std::move(req));

    if (Config.WaitMode == EWaitMode::Poll) {
        RequestEvent.Set();
    }
}

bool TClientEndpoint::HandleInputRequests()
{
    if (Config.WaitMode == EWaitMode::Poll) {
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
        Y_VERIFY(req);

        Counters->RequestDequeued();
        SendRequest(std::move(req), send);
    }
}

bool TClientEndpoint::HandleDisconnect() noexcept
{
    bool ret = false;

    if (Config.WaitMode == EWaitMode::Poll) {
        DisconnectEvent.Clear();
    }

    auto requests = InputRequests.DequeueAll();
    if (requests) {
        QueuedRequests.Append(std::move(requests));
        ret = true;
    }

    while (QueuedRequests) {
        auto req = QueuedRequests.Dequeue();
        Y_VERIFY(req);

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

    if (Config.WaitMode == EWaitMode::Poll) {
        Verbs->GetCompletionEvent(cq);
        Verbs->AckCompletionEvents(cq, 1);
        Verbs->RequestCompletionEvent(cq, 0);
    }

    if (Verbs->PollCompletionQueue(reinterpret_cast<ibv_cq_ex*>(cq), this)) {
        HandleQueuedRequests();
        return true;
    }

    return false;
}

ibv_wc_opcode TClientEndpoint::GetOpcode(const NVerbs::TCompletion& wc) const
{
    if (wc.opcode != IBV_WC_SEND && wc.opcode != IBV_WC_RECV) {
        STORAGE_WARN("unknown opcode: " << NVerbs::GetOpcodeName(wc.opcode));
    }
    if (OwnedBy(wc.wr_id, SendWrs)) {
        return IBV_WC_SEND;
    }
    if (OwnedBy(wc.wr_id, RecvWrs)) {
        return IBV_WC_RECV;
    }
    Y_FAIL("unknown work request: %" PRIu64, wc.wr_id);
}

void TClientEndpoint::HandleCompletionEvent(const NVerbs::TCompletion& wc)
{
    auto opcode = GetOpcode(wc);

    STORAGE_TRACE(NVerbs::GetOpcodeName(opcode) << " #" << wc.wr_id
        << " completed");

    switch (opcode) {
        case IBV_WC_SEND:
            SendRequestCompleted(
                reinterpret_cast<TSendWr*>(wc.wr_id),
                wc.status,
                wc.ts);
            break;

        case IBV_WC_RECV:
            RecvResponseCompleted(
                reinterpret_cast<TRecvWr*>(wc.wr_id),
                wc.status,
                wc.ts);
            break;

        default:
            break;
    }
}

void TClientEndpoint::SendRequest(TRequestPtr req, TSendWr* send)
{
    req->ReqId = ActiveRequests.CreateId();

    auto* requestMsg = send->Message<TRequestMessage>();
    Zero(*requestMsg);

    InitMessageHeader(requestMsg, RDMA_PROTO_VERSION);

    requestMsg->ReqId = req->ReqId;
    requestMsg->In = req->InBuffer;
    requestMsg->Out = req->OutBuffer;

    STORAGE_TRACE("SEND #" << send->wr.wr_id);
    Verbs->PostSend(Connection->qp, &send->wr);

    LWTRACK(
        SendRequestStarted,
        req->CallContext->LWOrbit,
        req->CallContext->RequestId);

    Counters->SendRequestStarted();

    send->context = req.get();
    ActiveRequests.Push(std::move(req));
}

void TClientEndpoint::SendRequestCompleted(
    TSendWr* send,
    ibv_wc_status status,
    ui64 ts) noexcept
{
    // TODO
    Y_UNUSED(ts);

    if (status == IBV_WC_WR_FLUSH_ERR) {
        SendQueue.Push(send);
        return;
    }

    auto* req = static_cast<TRequest*>(send->context);

    if (!ActiveRequests.Contains(req)) {
        STORAGE_WARN("SEND #" << send->wr.wr_id << ": request "
            << reinterpret_cast<void*>(req) << " not found")

        // reclaim an actual send
        if (OwnedBy(send, SendWrs)) {
            SendQueue.Push(send);
        }
        return;
    }

    if (status != IBV_WC_SUCCESS) {
        STORAGE_ERROR("SEND #" << send->wr.wr_id << ": "
            << NVerbs::GetStatusString(status));

        ReportRdmaError();
        SetError(E_RDMA_UNAVAILABLE);
        Counters->SendRequestError();
        SendQueue.Push(send);

        return;
    }

    LWTRACK(
        SendRequestCompleted,
        req->CallContext->LWOrbit,
        req->CallContext->RequestId);

    Counters->SendRequestCompleted();
    SendQueue.Push(send);
}

void TClientEndpoint::RecvResponse(TRecvWr* recv)
{
    auto* responseMsg = recv->Message<TResponseMessage>();
    Zero(*responseMsg);

    STORAGE_TRACE("RECV #" << recv->wr.wr_id);
    Verbs->PostRecv(Connection->qp, &recv->wr);

    Counters->RecvResponseStarted();
}

void TClientEndpoint::RecvResponseCompleted(
    TRecvWr* recv,
    ibv_wc_status wc_status,
    ui64 ts)
{
    // TODO
    Y_UNUSED(ts);

    if (wc_status == IBV_WC_WR_FLUSH_ERR) {
        RecvQueue.Push(recv);
        return;
    }

    if (wc_status != IBV_WC_SUCCESS) {
        STORAGE_ERROR("RECV #" << recv->wr.wr_id << ": "
            << NVerbs::GetStatusString(wc_status));

        ReportRdmaError();
        SetError(E_RDMA_UNAVAILABLE);
        Counters->RecvResponseError();
        RecvQueue.Push(recv);

        return;
    }

    const auto* msg = recv->Message<TResponseMessage>();
    const int version = ParseMessageHeader(msg);

    if (version != RDMA_PROTO_VERSION) {
        STORAGE_ERROR("RECV #" << recv->wr.wr_id
            << ": incompatible protocol version "
            << version << " != " << int(RDMA_PROTO_VERSION));

        // should always be posted
        RecvResponse(recv);

        Counters->RecvResponseError();
        return;
    }

    const ui32 reqId = msg->ReqId;
    const ui32 status = msg->Status;
    const ui32 responseBytes = msg->ResponseBytes;

    // should always be posted
    RecvResponse(recv);

    auto req = ActiveRequests.Pop(reqId);
    if (!req) {
        STORAGE_ERROR("RECV #" << recv->wr.wr_id
            << ": request " << reqId << " not found");

        Counters->RecvResponseError();
        return;
    }

    Counters->RecvResponseCompleted();

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

void TClientEndpoint::ResetConnection(
    NVerbs::TConnectionPtr connection) noexcept
{
    Verbs->DestroyQP(Connection.get());

    with_lock (AllocationLock) {
        SendBuffers.ReleaseBuffer(SendBuffer);
        RecvBuffers.ReleaseBuffer(RecvBuffer);
    }

    SendQueue.Clear();
    RecvQueue.Clear();

    CompletionQueue.reset();
    CompletionChannel.reset();

    Connection = std::move(connection);
    Connection->context = this;

    Counters->ResetActiveRecv();
    Counters->ResetActiveSend();
}

int TClientEndpoint::ReconnectTimerHandle() const
{
    return Reconnect.Timer.Handle();
}

void TClientEndpoint::SetError(ui32 error) noexcept
{
    Status = error;

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

        // flush queues and schedule reconnect
        case EEndpointState::Connected:
            ChangeState(EEndpointState::Disconnecting);

            try {
                struct ibv_qp_attr attr = {.qp_state = IBV_QPS_ERR};
                Verbs->ModifyQP(Connection->qp, &attr, IBV_QP_STATE);
            } catch (const TServiceError& e) {
                STORAGE_ERROR("unable to flush queues. " << e.what());
            }

            if (Config.WaitMode == EWaitMode::Poll) {
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
        EventChannel = Verbs->CreateEventChannel();
        SetNonBlock(EventChannel->fd, true);

        PollHandle.Attach(StopEvent.Handle(), EPOLLIN);
        PollHandle.Attach(
            EventChannel->fd,
            EPOLLIN,
            PtrEventTag(EventChannel.get(), 0));
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
            PtrEventTag(endpoint, 1));
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

                if (event.events && event.data.ptr) {
                    if (EventFromTag(event.data.ptr)) {
                        auto* endpoint = PtrFromTag<TClientEndpoint>(event.data.ptr);
                        EventHandler->Reconnect(endpoint);
                    } else {
                        HandleConnectionEvents();
                    }
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
            STORAGE_ERROR(e.what());
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

    void Add(TClientEndpointPtr endpoint)
    {
        endpoint->Poller = this;
        Endpoints.Add(std::move(endpoint));
    }

    void Del(TClientEndpointPtr endpoint)
    {
        endpoint->Poller = nullptr;
        Endpoints.Del(std::move(endpoint));
    }

    void Attach(TClientEndpoint* endpoint)
    {
        if (Config->WaitMode == EWaitMode::Poll) {
            PollHandle.Attach(
                endpoint->CompletionChannel->fd,
                EPOLLIN,
                PtrEventTag(endpoint, EPollerEvent::Completion));

            PollHandle.Attach(
                endpoint->RequestEvent.Handle(),
                EPOLLIN,
                PtrEventTag(endpoint, EPollerEvent::Request));

            PollHandle.Attach(
                endpoint->DisconnectEvent.Handle(),
                EPOLLIN,
                PtrEventTag(endpoint, EPollerEvent::Disconnect));

            Verbs->RequestCompletionEvent(endpoint->CompletionQueue.get(), 0);
        }
    }

    void Detach(TClientEndpoint* endpoint)
    {
        if (Config->WaitMode == EWaitMode::Poll) {
            PollHandle.Detach(endpoint->CompletionChannel->fd);
            PollHandle.Detach(endpoint->RequestEvent.Handle());
            PollHandle.Detach(endpoint->DisconnectEvent.Handle());
        }
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
        if (!event.events || !event.data.ptr) {
            return;
        }

        auto* endpoint = PtrFromTag<TClientEndpoint>(event.data.ptr);

        try {
            switch (EventFromTag(event.data.ptr)) {
                case EPollerEvent::Completion:
                    endpoint->HandleCompletionEvents();
                    break;

                case EPollerEvent::Request:
                    endpoint->HandleInputRequests();
                    break;

                case EPollerEvent::Disconnect:
                    endpoint->HandleDisconnect();
                    break;
            }
        } catch (const TServiceError& e) {
            STORAGE_ERROR(e.what());
            endpoint->SetError(E_RDMA_UNAVAILABLE);
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
                    hasWork |= endpoint->HandleCompletionEvents();
                }
                if (endpoint->CheckState(EEndpointState::Disconnecting)) {
                    hasWork |= endpoint->HandleCompletionEvents();
                    hasWork |= endpoint->HandleDisconnect();
                }
            } catch (const TServiceError& e) {
                endpoint->SetError(E_RDMA_UNAVAILABLE);
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
            if (endpoint->CheckState(EEndpointState::Disconnecting)
                && endpoint->Flushed())
            {
                endpoint->ChangeState(
                    EEndpointState::Disconnecting,
                    EEndpointState::Disconnected);
            }
        }
    }

    template <EWaitMode WaitMode>
    void Execute()
    {
        TAdaptiveWait aw(
            Config->AdaptiveWaitSleepDuration,
            Config->AdaptiveWaitSleepDelay);

        while (!ShouldStop()) {
            if (WaitMode == EWaitMode::Poll) {
                // wait for completion events
                size_t signaled = PollHandle.Wait(POLL_TIMEOUT);

                for (size_t i = 0; i < signaled; ++i) {
                    HandlePollEvent(PollHandle.GetEvent(i));
                }
            } else {
                auto hasWork = HandleEvents();

                if (WaitMode == EWaitMode::AdaptiveWait) {
                    if (hasWork) {
                        aw.Reset();
                    } else {
                        aw.Sleep();
                    }
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

    void Start() noexcept override;
    void Stop() noexcept override;

    TFuture<IClientEndpointPtr> StartEndpoint(
        TString host,
        ui32 port) noexcept override;

private:
    void HandleConnectionEvent(
        NVerbs::TConnectionEventPtr event) noexcept override;

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

    STORAGE_DEBUG("Start client");

    auto counters = Monitoring->GetCounters();
    auto rootGroup = counters->GetSubgroup("counters", "blockstore");
    Counters->Register(*rootGroup->GetSubgroup("component", "rdma_client"));

    CompletionPollers.resize(Config->PollerThreads);
    for (size_t i = 0; i < CompletionPollers.size(); ++i) {
        CompletionPollers[i] = std::make_unique<TCompletionPoller>(
            Verbs, Config, Log);
        CompletionPollers[i]->Start();
    }

    try {
        ConnectionPoller = std::make_unique<TConnectionPoller>(Verbs, this, Log);
        ConnectionPoller->Start();
    } catch (const TServiceError &e) {
        STORAGE_ERROR("unable to start client: " << e.what());
        Stop();
    }
}

void TClient::Stop() noexcept
{
    STORAGE_DEBUG("Stop client");

    if (ConnectionPoller) {
        ConnectionPoller->Stop();
        ConnectionPoller.reset();
    }

    for (auto& poller: CompletionPollers) {
        poller->Stop();
    }
    CompletionPollers.clear();
}

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

        ConnectionPoller->Attach(endpoint.get());
        PickPoller().Add(endpoint);
        BeginResolveAddress(endpoint.get());

        return endpoint->StartResult.GetFuture();

    } catch (const TServiceError &e) {
        return unavailable("unable to create rdma connection");
    }
}

////////////////////////////////////////////////////////////////////////////////

void TClient::HandleConnectionEvent(NVerbs::TConnectionEventPtr event) noexcept
{
    STORAGE_DEBUG(NVerbs::GetEventName(event->event) << " received");

    TClientEndpoint* endpoint = TClientEndpoint::FromEvent(event.get());

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

void TClient::Reconnect(TClientEndpoint* endpoint) noexcept
{
    if (endpoint->Reconnect.Hanging()) {
        // if this is our first connection, fail over to IC
        if (endpoint->StartResult.Initialized()) {
            auto startResult = std::move(endpoint->StartResult);
            startResult.SetException(std::make_exception_ptr(TServiceError(
                MakeError(endpoint->Status, "connection timeout"))));
            return;
        }
        // otherwise keep trying
    }

    STORAGE_DEBUG("reconnect timer hit in %s state",
        GetEndpointStateName(endpoint->State));

    switch (endpoint->State) {
        // wait for completion poller to flush WRs
        case EEndpointState::Disconnecting:
            return;

        // didn't even start to connect, try again
        case EEndpointState::ResolvingAddress:
        case EEndpointState::ResolvingRoute:
            break;

        // reset connection and try again
        case EEndpointState::Connecting:
        case EEndpointState::Disconnected:
            endpoint->Poller->Detach(endpoint);
            endpoint->ResetConnection(ConnectionPoller->CreateConnection());
            break;

        // shouldn't happen
        case EEndpointState::Connected:
            STORAGE_ERROR("unexpected state");
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

        if (addrinfo->ai_src_addr) {
            STORAGE_DEBUG("resolve source address "
                << NVerbs::PrintAddress(addrinfo->ai_src_addr));
        }

        if (addrinfo->ai_dst_addr) {
            STORAGE_DEBUG("resolve destination address "
                << NVerbs::PrintAddress(addrinfo->ai_dst_addr));
        }

        endpoint->ChangeState(
            EEndpointState::Disconnected,
            EEndpointState::ResolvingAddress);

        Verbs->ResolveAddress(endpoint->Connection.get(), addrinfo->ai_src_addr,
            addrinfo->ai_dst_addr, RESOLVE_TIMEOUT);

    } catch (const TServiceError& e) {
        STORAGE_ERROR(e.what());
        endpoint->SetError(E_RDMA_UNAVAILABLE);
    }
}

void TClient::BeginResolveRoute(TClientEndpoint* endpoint) noexcept
{
    STORAGE_DEBUG("resolve route to "
        << NVerbs::PrintAddress(rdma_get_peer_addr(endpoint->Connection.get())));

    endpoint->ChangeState(
        EEndpointState::ResolvingAddress,
        EEndpointState::ResolvingRoute);

    try {
        Verbs->ResolveRoute(endpoint->Connection.get(), RESOLVE_TIMEOUT);
    } catch (const TServiceError& e) {
        STORAGE_ERROR(e.what());
        endpoint->SetError(E_RDMA_UNAVAILABLE);
    }
}

void TClient::BeginConnect(TClientEndpoint* endpoint) noexcept
{
    Y_VERIFY(endpoint);

    try {
        endpoint->ChangeState(
            EEndpointState::ResolvingRoute,
            EEndpointState::Connecting);

        endpoint->InitCompletionQueue();
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

        STORAGE_DEBUG("connect to "
            << NVerbs::PrintAddress(rdma_get_peer_addr(endpoint->Connection.get()))
            << " " << NVerbs::PrintConnectionParams(&param));

        Verbs->Connect(endpoint->Connection.get(), &param);

    } catch (const TServiceError& e) {
        STORAGE_ERROR(e.what());
        endpoint->SetError(E_RDMA_UNAVAILABLE);
    }
}

void TClient::HandleConnected(
    TClientEndpoint* endpoint,
    NVerbs::TConnectionEventPtr event) noexcept
{
    const rdma_conn_param* param = &event->param.conn;

    STORAGE_DEBUG("validate connection from "
        << NVerbs::PrintAddress(rdma_get_peer_addr(event->id))
        << " " << NVerbs::PrintConnectionParams(param));

    if (param->private_data == nullptr ||
        param->private_data_len < sizeof(TAcceptMessage) ||
        ParseMessageHeader(param->private_data) != RDMA_PROTO_VERSION)
    {
        STORAGE_ERROR("unable to parse accept message");
        endpoint->SetError(E_RDMA_UNAVAILABLE);
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
        STORAGE_ERROR(e.what());
        endpoint->SetError(E_RDMA_UNAVAILABLE);
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
        endpoint->SetError(E_RDMA_UNAVAILABLE);
        return;
    }

    const auto* msg = static_cast<const TRejectMessage*>(
        param->private_data);

    if (msg->Status == RDMA_PROTO_CONFIG_MISMATCH) {
        if (endpoint->Config.QueueSize > msg->QueueSize) {
            STORAGE_INFO("set QueueSize=" << msg->QueueSize
                << " supported by server");

            endpoint->Config.QueueSize = msg->QueueSize;
        }

        if (endpoint->Config.MaxBufferSize > msg->MaxBufferSize) {
            STORAGE_INFO("set MaxBufferSize=" << msg->MaxBufferSize
                << " supported by server");

            endpoint->Config.MaxBufferSize = msg->MaxBufferSize;
        }
    }

    endpoint->SetError(E_RDMA_UNAVAILABLE);
}

void TClient::HandleDisconnected(TClientEndpoint* endpoint) noexcept
{
    // we can't reset config right away, because disconnect needs to know queue
    // size to reap flushed WRs
    endpoint->ResetConfig = true;
    endpoint->SetError(E_RDMA_UNAVAILABLE);
}

TCompletionPoller& TClient::PickPoller() noexcept
{
    size_t index = RandomNumber(CompletionPollers.size());
    return *CompletionPollers[index];
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
