#include "client.h"

#include "buffer.h"
#include "error.h"
#include "list.h"
#include "poll.h"
#include "probes.h"
#include "protocol.h"
#include "rcu.h"
#include "utils.h"
#include "verbs.h"
#include "work_queue.h"

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
#include <util/system/spin_wait.h>
#include <util/system/thread.h>

namespace NCloud::NBlockStore::NRdma {

using namespace NMonitoring;
using namespace NThreading;

LWTRACE_USING(BLOCKSTORE_RDMA_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration POLL_TIMEOUT = TDuration::Seconds(1);
constexpr TDuration RESOLVE_TIMEOUT = TDuration::Seconds(10);

constexpr TDuration MIN_RECONNECT_DELAY = TDuration::MilliSeconds(10);

constexpr ui32 WAIT_TIMEOUT = 10;   // us

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

struct TRequest : TClientRequest, TListNode<TRequest>
{
    const ui64 StartedCycles;

    TClientEndpoint* Endpoint;

    TCallContextPtr CallContext;
    ui32 ReqId = 0;

    TPooledBuffer InBuffer {};
    TPooledBuffer OutBuffer {};

    TRequest(TClientEndpoint* endpoint, void* context)
        : StartedCycles(GetCycleCount())
        , Endpoint(endpoint)
    {
        Context = context;
    }
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
        {
            auto [_, inserted] = Pointers.emplace(req.get());
            Y_VERIFY(inserted);
        }

        {
            auto [_, inserted] = Requests.emplace(req->ReqId, std::move(req));
            Y_VERIFY(inserted);
        }
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
            Pointers.erase(x.get());
            // TODO: keep tombstones to distinguish between timed out requests
            // and unknown reqIds
            Requests.erase(x->ReqId);
        }

        return requests;
    }

    bool Contains(TRequest* ptr)
    {
        return Pointers.contains(ptr);
    }
};

////////////////////////////////////////////////////////////////////////////////

enum class EEndpointState
{
    Disconnected,
    ResolvingAddress,
    ResolvingRoute,
    Connecting,
    Connected,
    Error,
};

////////////////////////////////////////////////////////////////////////////////

const char* GetEndpointStateName(EEndpointState state)
{
    static const char* names[] = {
        "Disconnected",
        "ResolvingAddress",
        "ResolvingRoute",
        "Connecting",
        "Connected",
        "Error",
    };

    if ((size_t)state < Y_ARRAY_SIZE(names)) {
        return names[(size_t)state];
    } else {
        return "Undefined";
    }
}

EEndpointState GetExpectedEndpointState(rdma_cm_event_type event)
{
    switch (event) {
        case RDMA_CM_EVENT_ADDR_RESOLVED:
        case RDMA_CM_EVENT_ADDR_ERROR:
            return EEndpointState::ResolvingAddress;

        case RDMA_CM_EVENT_ROUTE_RESOLVED:
        case RDMA_CM_EVENT_ROUTE_ERROR:
            return EEndpointState::ResolvingRoute;

        case RDMA_CM_EVENT_UNREACHABLE:
        case RDMA_CM_EVENT_ESTABLISHED:
        case RDMA_CM_EVENT_REJECTED:
            return EEndpointState::Connecting;

        default:
        case RDMA_CM_EVENT_DISCONNECTED:
            return EEndpointState::Connected;
    }
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

class TClientEndpoint final
    : public IClientEndpoint
    , public NVerbs::ICompletionHandler
    , public std::enable_shared_from_this<TClientEndpoint>
{
    // TODO
    friend class TClient;
    friend class TCompletionPoller;

    struct TReconnect
    {
        const TDuration MaxDelay;

        TTimerHandle Timer;
        TDuration Delay = MIN_RECONNECT_DELAY;

        // returned in transient states
        TAtomic Error = E_REJECTED;

        TReconnect(const TDuration maxDelay)
            : MaxDelay(maxDelay)
        {
        }

        void Reset()
        {
            Timer.Clear();
            AtomicSet(Error, E_REJECTED);
            Delay = MIN_RECONNECT_DELAY;
        }

        void Schedule()
        {
            Timer.Set(Delay);

            if (Delay != MaxDelay) {
                Delay = Min(Delay * 2, MaxDelay);

                if (Delay >= MaxDelay) {
                    AtomicSet(Error, E_RDMA_CONNECT_FAILED);
                }
            }
        }
    };

private:
    NVerbs::IVerbsPtr Verbs;

    NVerbs::TConnectionPtr Connection;
    TString Host;
    ui32 Port;
    IClientHandlerPtr Handler;
    TClientConfigPtr DefaultConfig;
    TEndpointCountersPtr Counters;
    TLog Log;

    TReconnect Reconnect;
    TClientConfig Config;

    TCompletionPoller* Poller = nullptr;
    TAtomic State = intptr_t(EEndpointState::Disconnected);

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
        IClientHandlerPtr handler,
        TClientConfigPtr config,
        TEndpointCountersPtr stats,
        TLog log);
    ~TClientEndpoint();

    bool CheckState(EEndpointState expectedState) const;
    void ChangeState(EEndpointState expectedState, EEndpointState newState);

    // called in context of CM poller thread
    void InitCompletionQueue();
    void StartReceive();
    void ResetConfig();

    void AbortRequests(NVerbs::TConnectionEventPtr event);
    void ResetConnection(NVerbs::TConnectionPtr conn);

    int ReconnectTimerHandle();
    void ScheduleReconnect();

    // called in context of client thread
    TResultOrError<TClientRequestPtr> AllocateRequest(
        void* context,
        size_t requestBytes,
        size_t responseBytes) override;
    void SendRequest(
        TClientRequestPtr creq,
        TCallContextPtr callContext) override;
    void FreeRequest(TClientRequestPtr creq) override;

    // called in context of CQ poller thread
    bool HandleInputRequests();
    bool HandleCompletionEvents();

private:
    void HandleQueuedRequests();
    void HandleCompletionEvent(const NVerbs::TCompletion& wc) override;

    void SendRequest(TRequestPtr req, TSendWr* send);
    void SendRequestCompleted(TSendWr* send, ibv_wc_status status, ui64 ts);
    void SendRequestError(ui32 reqId, TSendWr* send, ibv_wc_status status);

    void RecvResponse(TRecvWr* recv);
    void RecvResponseCompleted(TRecvWr* recv, ibv_wc_status status, ui64 ts);
    void RecvResponseError(TRecvWr* recv, ibv_wc_status status);

    void HandleError(TRequestPtr req, ui32 err, const TString& msg);
    ui32 GetError() const;
};

////////////////////////////////////////////////////////////////////////////////

TClientEndpoint::TClientEndpoint(
        NVerbs::IVerbsPtr verbs,
        NVerbs::TConnectionPtr connection,
        TString host,
        ui32 port,
        IClientHandlerPtr handler,
        TClientConfigPtr config,
        TEndpointCountersPtr stats,
        TLog log)
    : Verbs(std::move(verbs))
    , Connection(std::move(connection))
    , Host(std::move(host))
    , Port(port)
    , Handler(std::move(handler))
    , DefaultConfig(std::move(config))
    , Counters(std::move(stats))
    , Log(log)
    , Reconnect(DefaultConfig->MaxReconnectDelay)
{
    Connection->context = this;
    Config = *DefaultConfig;
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

ui32 TClientEndpoint::GetError() const
{
    return AtomicGet(Reconnect.Error);
}

bool TClientEndpoint::CheckState(EEndpointState expectedState) const
{
    auto actualState = static_cast<EEndpointState>(AtomicGet(State));
    return actualState == expectedState;
}

void TClientEndpoint::ChangeState(
    EEndpointState expectedState,
    EEndpointState newState)
{
    do {
        auto actualState = static_cast<EEndpointState>(AtomicGet(State));
        Y_VERIFY(actualState == expectedState,
            "invalid state transition (new: %s, expected: %s, actual: %s)",
            GetEndpointStateName(newState),
            GetEndpointStateName(expectedState),
            GetEndpointStateName(actualState));
    } while (!AtomicCas(&State, intptr_t(newState), intptr_t(expectedState)));
}

void TClientEndpoint::InitCompletionQueue()
{
    CompletionChannel = Verbs->CreateCompletionChannel(Connection->verbs);
    SetNonBlock(CompletionChannel->fd, true);

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

void TClientEndpoint::ResetConfig()
{
    Config = *DefaultConfig;
}

TResultOrError<TClientRequestPtr> TClientEndpoint::AllocateRequest(
    void* context,
    size_t requestBytes,
    size_t responseBytes)
{
    if (!CheckState(EEndpointState::Connected)) {
        return MakeError(GetError(), "unable to allocate request");
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

    auto req = std::make_unique<TRequest>(this, context);

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

void TClientEndpoint::FreeRequest(TClientRequestPtr creq)
{
    TRequestPtr req(static_cast<TRequest*>(creq.release()));

    with_lock (AllocationLock) {
        SendBuffers.ReleaseBuffer(req->InBuffer);
        RecvBuffers.ReleaseBuffer(req->OutBuffer);
    }

    // request will be deleted now
}

void TClientEndpoint::SendRequest(
    TClientRequestPtr creq,
    TCallContextPtr callContext)
{
    TRequestPtr req(static_cast<TRequest*>(creq.release()));
    req->CallContext = std::move(callContext);

    LWTRACK(
        RequestEnqueued,
        req->CallContext->LWOrbit,
        req->CallContext->RequestId);

    if (!CheckState(EEndpointState::Connected)) {
        HandleError(std::move(req), GetError(), "endpoint is unavailable");
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

void TClientEndpoint::AbortRequests(NVerbs::TConnectionEventPtr event)
{
    auto msg = TStringBuilder()
        << "endpoint is unavailable: "
        << NVerbs::GetEventName(event->event);

    auto requests = InputRequests.DequeueAll();
    if (requests) {
        QueuedRequests.Append(std::move(requests));
    }

    while (QueuedRequests) {
        auto req = QueuedRequests.Dequeue();
        Y_VERIFY(req);

        Counters->RequestDequeued();
        HandleError(std::move(req), GetError(), msg);
    }

    while (auto req = ActiveRequests.Pop()) {
        Counters->RequestAborted();
        HandleError(std::move(req), GetError(), msg);
    }
}

void TClientEndpoint::HandleError(TRequestPtr req, ui32 err, const TString& msg)
{
    auto len = SerializeError(
        err,
        msg,
        static_cast<TStringBuf>(req->OutBuffer));
    Handler->HandleResponse(std::move(req), RDMA_PROTO_FAIL, len);
}

bool TClientEndpoint::HandleCompletionEvents()
{
    ibv_cq* cq = CompletionQueue.get();

    if (Config.WaitMode == EWaitMode::Poll) {
        Verbs->GetCompletionEvent(cq);
        Verbs->RequestCompletionEvent(cq, 0);
    }

    int rc = Verbs->PollCompletionQueue((ibv_cq_ex*)cq, this);

    if (rc != 0 && rc != ENOENT) {
        char buf[64];
        STORAGE_ERROR("poll error: " << strerror_r(rc, buf, sizeof(buf)));
    }

    if (Config.WaitMode == EWaitMode::Poll) {
        Verbs->AckCompletionEvents(cq, 1);
    }

    HandleQueuedRequests();
    return rc == 0;
}

void TClientEndpoint::HandleCompletionEvent(const NVerbs::TCompletion& wc)
{
    STORAGE_TRACE(NVerbs::GetOpcodeName(wc.opcode) << " #" << wc.wr_id
        << " completed");

    switch (wc.opcode) {
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
            Y_FAIL("unexpected opcode: %d", wc.opcode);
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
    ui64 ts)
{
    // TODO
    Y_UNUSED(ts);

    auto* req = static_cast<TRequest*>(send->context);

    if (!ActiveRequests.Contains(req)) {
        STORAGE_WARN("SEND #" << send->wr.wr_id
            << ": request " << (void*)req << " not found");

        Counters->SendRequestCompleted();
        SendQueue.Push(send);

        return;
    }

    if (status != IBV_WC_SUCCESS) {
        return SendRequestError(req->ReqId, send, status);
    }

    LWTRACK(
        SendRequestCompleted,
        req->CallContext->LWOrbit,
        req->CallContext->RequestId);

    Counters->SendRequestCompleted();
    SendQueue.Push(send);
}

void TClientEndpoint::SendRequestError(
    ui32 reqId,
    TSendWr* send,
    ibv_wc_status status)
{
    STORAGE_ERROR("SEND #" << send->wr.wr_id << ": "
        << NVerbs::GetStatusString(status));

    if (auto req = ActiveRequests.Pop(reqId)) {
        HandleError(std::move(req), E_FAIL, TStringBuilder()
            << "send request error: " << NVerbs::GetStatusString(status));
    } else {
        STORAGE_WARN("SEND #" << send->wr.wr_id << ": request "
            << reqId << " not found");
    }

    ReportRdmaError();
    Counters->SendRequestError();

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
    if (wc_status != IBV_WC_SUCCESS) {
        return RecvResponseError(recv, wc_status);
    }

    // TODO
    Y_UNUSED(ts);

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

    Handler->HandleResponse(
        std::move(req),
        status,
        responseBytes);
}

void TClientEndpoint::RecvResponseError(TRecvWr* recv, ibv_wc_status status)
{
    STORAGE_ERROR("RECV #" << recv->wr.wr_id << ": "
        << NVerbs::GetStatusString(status));

    ReportRdmaError();
    Counters->RecvResponseError();

    // should always be posted
    RecvResponse(recv);
}

void TClientEndpoint::ResetConnection(NVerbs::TConnectionPtr conn)
{
    Verbs->DestroyQP(Connection.get());

    with_lock (AllocationLock) {
        SendBuffers.ReleaseBuffer(SendBuffer);
        RecvBuffers.ReleaseBuffer(RecvBuffer);
    }

    SendQueue.Clear();
    RecvQueue.Clear();

    Counters->ResetActiveRecv();
    Counters->ResetActiveSend();

    CompletionQueue.reset();
    CompletionChannel.reset();

    conn->context = this;
    Connection = std::move(conn);

    ScheduleReconnect();
}

int TClientEndpoint::ReconnectTimerHandle()
{
    return Reconnect.Timer.Handle();
}

void TClientEndpoint::ScheduleReconnect()
{
    Reconnect.Schedule();

    auto error = GetError();

    if (!error || error == E_REJECTED) {
        return;
    }

    auto startResult = std::move(StartResult);
    if (!startResult.Initialized()) {
        return;
    }

    startResult.SetException(std::make_exception_ptr(TServiceError(
        MakeError(error, TStringBuilder() << "Reconnect timer hit"))));
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
        NCloud::SetCurrentThreadName("CM");

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

    void HandleConnectionEvents()
    {
        while (auto event = Verbs->GetConnectionEvent(EventChannel.get())) {
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
                PtrEventTag(endpoint, 0));

            PollHandle.Attach(
                endpoint->RequestEvent.Handle(),
                EPOLLIN,
                PtrEventTag(endpoint, 1));

            Verbs->RequestCompletionEvent(endpoint->CompletionQueue.get(), 0);
        }
    }

    void Detach(TClientEndpoint* endpoint)
    {
        if (Config->WaitMode == EWaitMode::Poll) {
            PollHandle.Detach(endpoint->CompletionChannel->fd);
            PollHandle.Detach(endpoint->RequestEvent.Handle());
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
        NCloud::SetCurrentThreadName("CQ");

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

    template <EWaitMode WaitMode>
    void Execute()
    {
        TSpinWait sw;
        sw.T = WAIT_TIMEOUT;

        while (!ShouldStop()) {
            if (WaitMode == EWaitMode::Poll) {
                // wait for completion events
                size_t signaled = PollHandle.Wait(POLL_TIMEOUT);
                for (size_t i = 0; i < signaled; ++i) {
                    const auto& event = PollHandle.GetEvent(i);
                    if (event.events && event.data.ptr) {
                        auto* endpoint = PtrFromTag<TClientEndpoint>(event.data.ptr);
                        if (EventFromTag(event.data.ptr)) {
                            endpoint->HandleInputRequests();
                        } else {
                            endpoint->HandleCompletionEvents();
                        }
                    }
                }
            } else {
                bool hasWork = false;
                auto endpoints = Endpoints.Get();

                // just loop through all registered endpoints and do polling
                for (const auto& endpoint: *endpoints) {
                    if (endpoint->CheckState(EEndpointState::Connected)) {
                        if (endpoint->HandleInputRequests()) {
                            hasWork = true;
                        }
                        if (endpoint->HandleCompletionEvents()) {
                            hasWork = true;
                        }
                    }
                }

                if (WaitMode == EWaitMode::AdaptiveWait) {
                    if (hasWork) {
                        // reset spin wait
                        sw.T = WAIT_TIMEOUT;
                        sw.C = 0;
                    } else {
                        sw.Sleep();
                    }
                }
            }

            auto endpoints = Endpoints.Get();
            for (const auto& endpoint: *endpoints) {
                auto requests = endpoint->ActiveRequests.PopTimedOutRequests(
                    DurationToCyclesSafe(Config->MaxResponseDelay));

                for (auto& request: requests) {
                    endpoint->HandleError(
                        std::move(request),
                        E_TIMEOUT,
                        "timed out");
                }
            }
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

    void Start() override;
    void Stop() override;

    TFuture<IClientEndpointPtr> StartEndpoint(
        TString host,
        ui32 port,
        IClientHandlerPtr handler) override;

private:
    void HandleConnectionEvent(NVerbs::TConnectionEventPtr event) override;
    void Reconnect(TClientEndpoint* endpont) override;

    void BeginResolveAddress(TClientEndpoint* endpoint);
    void BeginResolveRoute(TClientEndpoint* endpoint);
    void BeginConnect(TClientEndpoint* endpoint);

    void HandleConnected(
        TClientEndpoint* endpoint,
        NVerbs::TConnectionEventPtr event);

    void HandleRejected(
        TClientEndpoint* endpoint,
        NVerbs::TConnectionEventPtr event);

    void HandleDisconnected(
        TClientEndpoint* endpoint,
        NVerbs::TConnectionEventPtr event);

    void ResetConnection(
        TClientEndpoint* endpoint,
        NVerbs::TConnectionEventPtr event);

    void ResetIncompleteConnection(TClientEndpoint* endpoint);

    TCompletionPoller& PickPoller();
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

void TClient::Start()
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

    ConnectionPoller = std::make_unique<TConnectionPoller>(Verbs, this, Log);
    ConnectionPoller->Start();
}

void TClient::Stop()
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
    ui32 port,
    IClientHandlerPtr handler)
{
    auto endpoint = std::make_shared<TClientEndpoint>(
        Verbs,
        ConnectionPoller->CreateConnection(),
        std::move(host),
        port,
        std::move(handler),
        Config,
        Counters,
        Log);

    auto f = endpoint->StartResult.GetFuture();

    auto& poller = PickPoller();
    poller.Add(endpoint);

    ConnectionPoller->Attach(endpoint.get());

    BeginResolveAddress(endpoint.get());
    return f;
}

////////////////////////////////////////////////////////////////////////////////

void TClient::HandleConnectionEvent(NVerbs::TConnectionEventPtr event)
{
    STORAGE_DEBUG(NVerbs::GetEventName(event->event) << " received");

    TClientEndpoint* endpoint = nullptr;

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
            BeginResolveRoute(TClientEndpoint::FromEvent(event.get()));
            break;

        case RDMA_CM_EVENT_ROUTE_RESOLVED:
            BeginConnect(TClientEndpoint::FromEvent(event.get()));
            break;

        case RDMA_CM_EVENT_ESTABLISHED:
            endpoint = TClientEndpoint::FromEvent(event.get());
            HandleConnected(endpoint, std::move(event));
            break;

        case RDMA_CM_EVENT_REJECTED:
            endpoint = TClientEndpoint::FromEvent(event.get());
            HandleRejected(endpoint, std::move(event));
            break;

        case RDMA_CM_EVENT_ADDR_ERROR:
        case RDMA_CM_EVENT_ROUTE_ERROR:
        case RDMA_CM_EVENT_CONNECT_ERROR:
        case RDMA_CM_EVENT_UNREACHABLE:
        case RDMA_CM_EVENT_DISCONNECTED:
            endpoint = TClientEndpoint::FromEvent(event.get());
            HandleDisconnected(endpoint, std::move(event));
            break;

        case RDMA_CM_EVENT_DEVICE_REMOVAL:
        case RDMA_CM_EVENT_ADDR_CHANGE:
            // TODO
            break;
    }
}

void TClient::Reconnect(TClientEndpoint* endpoint)
{
    // XXX a temporary hack, will fix this properly later
    auto state = static_cast<EEndpointState>(AtomicGet(endpoint->State));

    STORAGE_INFO("reconnect attempt in "
        << GetEndpointStateName(state) << " state");

    switch (state) {
        case EEndpointState::Connecting:
            ResetIncompleteConnection(endpoint);
            [[fallthrough]];

        case EEndpointState::Disconnected:
            BeginResolveAddress(endpoint);
            break;

        default:
            break;
    }
}

void TClient::BeginResolveAddress(TClientEndpoint* endpoint)
{
    rdma_addrinfo hints = {
        .ai_port_space = RDMA_PS_TCP,
    };

    auto addrinfo = Verbs->GetAddressInfo(
        endpoint->Host,
        endpoint->Port,
        &hints);

    if (addrinfo->ai_src_addr) {
        STORAGE_DEBUG("RESOLVE src address "
            << NVerbs::PrintAddress(addrinfo->ai_src_addr));
    }

    if (addrinfo->ai_dst_addr) {
        STORAGE_DEBUG("RESOLVE dst address "
            << NVerbs::PrintAddress(addrinfo->ai_dst_addr));
    }

    endpoint->ChangeState(
        EEndpointState::Disconnected,
        EEndpointState::ResolvingAddress);

    Verbs->ResolveAddress(
        endpoint->Connection.get(),
        addrinfo->ai_src_addr,
        addrinfo->ai_dst_addr,
        RESOLVE_TIMEOUT);
}

void TClient::BeginResolveRoute(TClientEndpoint* endpoint)
{
    STORAGE_DEBUG("RESOLVE route to "
        << NVerbs::PrintAddress(rdma_get_peer_addr(endpoint->Connection.get())));

    endpoint->ChangeState(
        EEndpointState::ResolvingAddress,
        EEndpointState::ResolvingRoute);

    Verbs->ResolveRoute(endpoint->Connection.get(), RESOLVE_TIMEOUT);
}

void TClient::BeginConnect(TClientEndpoint* endpoint)
{
    Y_VERIFY(endpoint);
    endpoint->InitCompletionQueue();

    endpoint->Poller->Attach(endpoint);

    TConnectMessage connectMsg = {
        .QueueSize = SafeCast<ui16>(endpoint->Config.QueueSize),
        .MaxBufferSize = SafeCast<ui32>(endpoint->Config.MaxBufferSize),
    };
    InitMessageHeader(&connectMsg, RDMA_PROTO_VERSION);

    rdma_conn_param connectParams = {
        .private_data = &connectMsg,
        .private_data_len = sizeof(TConnectMessage),
        .responder_resources = RDMA_MAX_RESP_RES,
        .initiator_depth = RDMA_MAX_INIT_DEPTH,
        .flow_control = 1,
        .retry_count = 7,
        .rnr_retry_count = 7,
    };

    STORAGE_DEBUG("CONNECT to "
        << NVerbs::PrintAddress(rdma_get_peer_addr(endpoint->Connection.get()))
        << " " << NVerbs::PrintConnectionParams(&connectParams));

    endpoint->ChangeState(
        EEndpointState::ResolvingRoute,
        EEndpointState::Connecting);

    endpoint->ScheduleReconnect();

    Verbs->Connect(endpoint->Connection.get(), &connectParams);
}

void TClient::HandleConnected(
    TClientEndpoint* endpoint,
    NVerbs::TConnectionEventPtr event)
{
    const rdma_conn_param* acceptParams = &event->param.conn;
    STORAGE_DEBUG("VALIDATE connection from "
        << NVerbs::PrintAddress(rdma_get_peer_addr(event->id))
        << " " << NVerbs::PrintConnectionParams(acceptParams));

    if (acceptParams->private_data == nullptr ||
        acceptParams->private_data_len < sizeof(TAcceptMessage))
    {
        STORAGE_ERROR("CONNECT: unable to parse accept message");
        return ResetConnection(endpoint, std::move(event));
    }

    const auto* msg = static_cast<const TAcceptMessage*>(
        acceptParams->private_data);
    const int version = ParseMessageHeader(msg);

    if (version != RDMA_PROTO_VERSION) {
        STORAGE_ERROR("CONNECT: incompatible protocol version "
            << version << " != " << int(RDMA_PROTO_VERSION));

        return ResetConnection(endpoint, std::move(event));
    }

    const ui32 keepAliveTimeout = msg->KeepAliveTimeout;

    // TODO
    Y_UNUSED(keepAliveTimeout);

    endpoint->ChangeState(
        EEndpointState::Connecting,
        EEndpointState::Connected);

    endpoint->Reconnect.Reset();
    endpoint->StartReceive();

    // only the first connect is passed up
    if (endpoint->StartResult.Initialized()) {
        auto startResult = std::move(endpoint->StartResult);
        startResult.SetValue(endpoint->shared_from_this());
    }
}

void TClient::HandleRejected(
    TClientEndpoint* endpoint,
    NVerbs::TConnectionEventPtr event)
{
    const rdma_conn_param* params = &event->param.conn;

    if (params->private_data == nullptr ||
        params->private_data_len < sizeof(TRejectMessage) ||
        ParseMessageHeader(params->private_data) != RDMA_PROTO_VERSION)
    {
        ResetConnection(endpoint, std::move(event));
        return;
    }

    const auto* msg = static_cast<const TRejectMessage*>(
        params->private_data);

    const ui32 status = msg->Status;
    const ui32 queueSize = msg->QueueSize;
    const ui32 maxBufferSize = msg->MaxBufferSize;

    if (status == RDMA_PROTO_CONFIG_MISMATCH) {
        if (endpoint->Config.QueueSize > queueSize) {
            STORAGE_INFO("set QueueSize=" << queueSize
                << " supported by server");

            endpoint->Config.QueueSize = queueSize;
        }

        if (endpoint->Config.MaxBufferSize > maxBufferSize) {
            STORAGE_INFO("set MaxBufferSize=" << maxBufferSize
                << " supported by server");

            endpoint->Config.MaxBufferSize = maxBufferSize;
        }
    }

    ResetConnection(endpoint, std::move(event));
}

void TClient::HandleDisconnected(
    TClientEndpoint* endpoint,
    NVerbs::TConnectionEventPtr event)
{
    endpoint->ResetConfig();
    ResetConnection(endpoint, std::move(event));
}

void TClient::ResetConnection(
    TClientEndpoint* endpoint,
    NVerbs::TConnectionEventPtr event)
{
    endpoint->ChangeState(
        GetExpectedEndpointState(event->event),
        EEndpointState::Disconnected);

    endpoint->Poller->Detach(endpoint);

    // event must be freed before reset, otherwise it will block indefinitely
    endpoint->AbortRequests(std::move(event));
    endpoint->ResetConnection(ConnectionPoller->CreateConnection());
}

void TClient::ResetIncompleteConnection(TClientEndpoint* endpoint)
{
    endpoint->ChangeState(
        EEndpointState::Connecting,
        EEndpointState::Disconnected);

    endpoint->Poller->Detach(endpoint);
    endpoint->ResetConnection(ConnectionPoller->CreateConnection());
}

TCompletionPoller& TClient::PickPoller()
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
