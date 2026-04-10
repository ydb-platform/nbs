#include "client.h"

#include "adaptive_wait.h"
#include "buffer.h"
#include "event.h"
#include "list.h"
#include "log.h"
#include "poll.h"
#include "rcu.h"
#include "utils.h"
#include "verbs.h"
#include "work_queue.h"

#include <cloud/blockstore/libs/rdma/iface/probes.h>
#include <cloud/blockstore/libs/rdma/iface/protobuf.h>
#include <cloud/blockstore/libs/rdma/iface/protocol.h>
#include <cloud/blockstore/libs/service/context.h>

#include <cloud/storage/core/libs/common/backoff_delay_provider.h>
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
#include <util/generic/map.h>
#include <util/generic/vector.h>
#include <util/network/interface.h>
#include <util/random/random.h>
#include <util/stream/format.h>
#include <util/system/datetime.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>

namespace NCloud::NBlockStore::NRdma {

using namespace NMonitoring;
using namespace NThreading;

LWTRACE_USING(BLOCKSTORE_RDMA_PROVIDER);

////////////////////////////////////////////////////////////////////////////////
//
// Threading model
// ===============
//
// Three thread groups interact with TClientEndpoint:
//
//  1. External threads (actor system / caller)
//     - AllocateRequest, SendRequest (public), CancelRequest,
//       TryForceReconnect, Stop, GetNewReqId
//     - Enqueue work via lock-free lists (InputRequests, CancelRequests)
//       and signal via EventHandle.
//
//  2. RDMA.CM  (TConnectionPoller thread, one per TClient)
//     - Connection lifecycle: resolve address/route, connect, disconnect,
//       reconnect, create/destroy QP, flush queues.
//     - Drives state machine: Disconnected -> ResolvingAddress ->
//       ResolvingRoute -> Connecting -> Connected -> Disconnecting -> ...
//
//  3. RDMA.CQ  (TCompletionPoller thread, one per poller)
//     - Request processing: dequeue from lock-free lists, post send/recv,
//       handle completions, abort/timeout requests.
//     - Owns QueuedRequests / ActiveRequests (no concurrent access: CQ thread
//       is the only writer/reader once requests leave the lock-free list).
//
// Synchronization
// ---------------
//  - State (atomic)           : read from any thread, exchanged by CM/CQ
//  - InputRequests/CancelRequests (TLockFreeList) : produced by external,
//                                                   consumed by CQ
//  - AllocationLock (TMutex)  : protects buffer pool alloc/free across
//                               external and any-thread (~TRequest)
//  - Reconnect (internal lock): protects timer schedule/cancel across CM,
//                               CQ, and external threads
//  - DisconnectEvent          : set by any thread, consumed by CM
//  - FlushStartCycles (atomic): written by CM, read by CQ
//
// Thread safety notes
// -------------------
//  - RecvResponse is called from RDMA.CM (initial posting via StartReceive)
//    and RDMA.CQ (re-posting after completion). This is safe because the
//    initial posting finishes before any completions can arrive.
//
//  - Disconnect() can be called from any thread. For the Connected state it
//    only sets DisconnectEvent (idempotent); the CM thread is the sole
//    consumer that performs the actual Connected -> Disconnecting transition.
//
//  - HandleCancelRequests cancels active requests whose RDMA send/recv may
//    still be in-flight. The freed request buffers (OutBuffer) could be
//    reused by the pool while the remote server performs RDMA WRITE to the
//    old address. The current code shares MR rkeys directly; switching to
//    Type 2 Memory Windows with IBV_WR_LOCAL_INV would allow revoking
//    remote access per-request on cancel (at the cost of extra bind/inv WRs
//    per request and server-side handling of remote access errors).
//
////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration POLL_TIMEOUT = TDuration::Seconds(1);
constexpr TDuration RESOLVE_TIMEOUT = TDuration::Seconds(10);
constexpr TDuration MIN_CONNECT_TIMEOUT = TDuration::Seconds(1);
constexpr TDuration FLUSH_TIMEOUT = TDuration::Seconds(10);
constexpr TDuration MIN_RECONNECT_DELAY = TDuration::MilliSeconds(10);
constexpr TDuration INSTANT_RECONNECT_DELAY = TDuration::MicroSeconds(1);

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

ui32 GetRequestId(const TSendWr& send) {
    return SafeCast<ui32>(reinterpret_cast<uintptr_t>(send.context));
}

ui32 GetRequestId(const TRecvWr& recv) {
    return recv.Message<TResponseMessage>()->ReqId;
}

TString
GetLogTitle(TStringBuf opcode, const TWorkRequestId& id, ui32 reqId)
{
    return TStringBuilder() << opcode << " [" << id << "][" << reqId << "]: ";
}

////////////////////////////////////////////////////////////////////////////////

struct TRequest
    : TClientRequest
    , TListNode<TRequest>
{
    const ui64 StartedCycles;

    std::weak_ptr<TClientEndpoint> Endpoint;

    TCallContextPtr CallContext;
    ui32 ReqId = 0;   // 16-bit RDMA request id
    ui64 ClientReqId = 0;

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
    ui32 RequestIdGenerator = 0;
    TMap<ui32, TRequestPtr> Requests;

public:
    [[nodiscard]] ui32 CreateId()
    {
        for (;;) {
            Y_DEBUG_ABORT_UNLESS(Requests.size() < RDMA_MAX_REQID - 1);

            if (RequestIdGenerator >= RDMA_MAX_REQID) {
                RequestIdGenerator = 0;
            }
            const ui32 reqId = ++RequestIdGenerator;
            // must be unique through all in-flight requests
            if (Requests.find(reqId) == Requests.end()) {
                Y_DEBUG_ABORT_UNLESS(reqId > 0 && reqId <= RDMA_MAX_REQID);
                return reqId;
            }
        }
    }

    [[nodiscard]] ui32 GetCurrentId() const
    {
        return RequestIdGenerator;
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

    TSimpleList<TRequest> PopCancelledRequests(
        const THashSet<ui64>& clientRequestIdToCancel)
    {
        TSimpleList<TRequest> cancelledReqs;
        for (auto& [rdmaReqId, req]: Requests) {
            if (clientRequestIdToCancel.contains(req->ClientReqId)) {
                cancelledReqs.Enqueue(std::move(req));
            }
        }

        for (const auto& req: cancelledReqs) {
            Requests.erase(req.ReqId);
        }

        return cancelledReqs;
    }

    TVector<TRequestPtr> PopTimedOutRequests(ui64 timeoutCycles)
    {
        TVector<TRequestPtr> requests;
        const ui64 now = GetCycleCount();

        auto popTimedOut = [&](decltype(Requests.begin()) it)
        {
            for (; it != Requests.end();) {
                TRequestPtr& request = it->second;
                if (request->StartedCycles &&
                    request->StartedCycles + timeoutCycles < now)
                {
                    requests.push_back(std::move(request));
                    it = Requests.erase(it);
                } else {
                    break;
                }
            }
        };

        // Since identifiers are reused in a circle, the oldest identifiers need
        // to be searched in two places - at the very beginning and after the
        // last one used.
        // [ old ... GetCurrentId() ... older]
        popTimedOut(Requests.begin());
        popTimedOut(Requests.upper_bound(GetCurrentId()));

        return requests;
    }

    bool Empty() const
    {
        return Requests.empty();
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

inline IOutputStream& operator<<(
    IOutputStream& out,
    const std::atomic<EEndpointState>& state)
{
    return out << GetEndpointStateName(state);
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

    TDynamicCounters::TCounterPtr Errors;

    void Register(TDynamicCounters& counters)
    {
        QueuedRequests = counters.GetCounter("QueuedRequests");
        ActiveRequests = counters.GetCounter("ActiveRequests");
        CompletedRequests = counters.GetCounter("CompletedRequests", true);
        AbortedRequests = counters.GetCounter("AbortedRequests", true);

        ActiveSend = counters.GetCounter("ActiveSend");
        ActiveRecv = counters.GetCounter("ActiveRecv");

        Errors = counters.GetCounter("Errors", true);
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

    void RecvResponseCompleted()
    {
        ActiveRecv->Dec();
    }

    void RequestCompleted()
    {
        ActiveRequests->Dec();
        CompletedRequests->Inc();
    }

    void RequestAborted()
    {
        ActiveRequests->Dec();
        AbortedRequests->Inc();
    }

    void Error()
    {
        Errors->Inc();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TReconnect
{
private:
    const TDuration MaxDelay;

    std::optional<TBackoffDelayProvider> DelayProvider;
    TTimerHandle Timer;
    TAdaptiveLock Lock;

public:
    explicit TReconnect(TDuration maxDelay)
        : MaxDelay(maxDelay)
    {}

    ~TReconnect() = default;

    void Cancel()
    {
        auto guard = Guard(Lock);
        CancelLocked();
    }

    void Schedule()
    {
        Schedule(MIN_RECONNECT_DELAY);
    }

    void Schedule(TDuration minDelay)
    {
        auto guard = Guard(Lock);
        ScheduleLocked(minDelay, TDuration());
    }

    void InstantReschedule(TDuration minDelay)
    {
        auto guard = Guard(Lock);
        CancelLocked();
        ScheduleLocked(minDelay, INSTANT_RECONNECT_DELAY);
    }

    bool Hanging() const
    {
        auto guard = Guard(Lock);
        if (!DelayProvider) {
            return false;
        }
        return DelayProvider->GetDelay() >= MaxDelay / 2;
    }

    int Handle() const
    {
        return Timer.Handle();
    }

private:
    void CancelLocked()
    {
        DelayProvider.reset();
        Timer.Clear();
    }

    void ScheduleLocked(TDuration minDelay, TDuration initialDelay)
    {
        if (!DelayProvider) {
            DelayProvider.emplace(minDelay, MaxDelay);
        }
        const auto delay =
            initialDelay ? initialDelay : DelayProvider->GetDelayAndIncrease();
        Timer.Set(delay);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TClientRequestId: TListNode<TClientRequestId>
{
    ui64 ClientRequestId = 0;
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
    const EWaitMode WaitMode;
    bool ResetConfig = false;

    TCompletionPoller* Poller = nullptr;

    std::atomic<EEndpointState> State = EEndpointState::Disconnected;
    std::atomic_flag StopFlag = ATOMIC_FLAG_INIT;

    NVerbs::TCompletionChannelPtr CompletionChannel = NVerbs::NullPtr;
    NVerbs::TCompletionQueuePtr CompletionQueue = NVerbs::NullPtr;

    TPromise<IClientEndpointPtr> StartResult = NewPromise<IClientEndpointPtr>();
    TPromise<void> StopResult = NewPromise<void>();

    std::atomic<ui64> FlushStartCycles = 0;

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
    TLockFreeList<TClientRequestId> CancelRequests;
    TEventHandle CancelRequestEvent;
    TEventHandle RequestEvent;
    TEventHandle AbortRequestsEvent;
    TEventHandle DisconnectEvent;

    TSimpleList<TRequest> QueuedRequests;
    TActiveRequests ActiveRequests;

    std::atomic<ui64> ReqIdPool{0};

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
    void ChangeState(EEndpointState expectedState, EEndpointState newState) noexcept;
    void Disconnect() noexcept;

    // called from CM thread
    void CreateQP();
    void DestroyQP() noexcept;
    void StartReceive() noexcept;
    void SetConnection(NVerbs::TConnectionPtr connection) noexcept;
    void FlushQueues() noexcept;
    void ClearDisconnectEvent() noexcept;
    bool ShouldStop() const;
    int ReconnectTimerHandle() const;
    int DisconnectEventHandle() const;

    // called from external thread
    TResultOrError<TClientRequestPtr> AllocateRequest(
        IClientHandlerPtr handler,
        std::unique_ptr<TNullContext> context,
        size_t requestBytes,
        size_t responseBytes) noexcept override;
    ui64 SendRequest(
        TClientRequestPtr creq,
        TCallContextPtr callContext) noexcept override;
    void CancelRequest(ui64 reqId) noexcept override;
    void TryForceReconnect() noexcept override;
    TFuture<void> Stop() noexcept override;

    // called from CQ thread
    void HandleCompletionEvent(ibv_wc* wc) noexcept override;
    bool HandleInputRequests() noexcept;
    bool HandleCancelRequests() noexcept;
    bool HandleCompletionEvents() noexcept;
    void AbortRequests() noexcept;
    bool Flushed() const;
    bool FlushHanging() const;

private:
    // called from CQ thread
    void HandleQueuedRequests() noexcept;
    void SendRequest(TRequestPtr req, TSendWr* send) noexcept;
    void SendRequestCompleted(TSendWr* send) noexcept;
    void RecvResponse(TRecvWr* recv) noexcept;
    void RecvResponseCompleted(TRecvWr* recv) noexcept;
    void AbortRequest(TRequestPtr req, ui32 err, const TString& msg) noexcept;
    void FreeRequest(TRequest* creq) noexcept;
    int ValidateCompletion(ibv_wc* wc) noexcept;
    ui64 GetNewReqId() noexcept;
};

////////////////////////////////////////////////////////////////////////////////

// Thread: any (destructor runs on whichever thread releases the last reference)
TRequest::~TRequest()
{
    auto clientEndpoint = Endpoint.lock();
    if (clientEndpoint) {
        clientEndpoint->FreeRequest(this);
    }
}

////////////////////////////////////////////////////////////////////////////////

// Thread: external (created by TClient::StartEndpoint on caller's thread)
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

    Log.SetFormatter([=, this](ELogPriority p, TStringBuf msg) {
        Y_UNUSED(p);
        return TStringBuilder() << "[" << Id << "] " << msg;
    });

    RDMA_INFO(
        "start endpoint [host="
        << Host << " send_magic=" << Hex(SendMagic, HF_FULL)
        << " recv_magic=" << Hex(RecvMagic, HF_FULL) << "]");
}

// Thread: any (destructor, typically RDMA.CQ via TCompletionPoller::Release)
TClientEndpoint::~TClientEndpoint()
{
    // release any leftover resources if endpoint hasn't been properly stopped
    if (Connection) {
        RDMA_INFO("release resources");
    }
    DestroyQP();
    RDMA_INFO("stop endpoint");
}

// Thread: any (thread-safe: atomic read)
bool TClientEndpoint::CheckState(EEndpointState expectedState) const
{
    return State == expectedState;
}

// Thread: RDMA.CM or RDMA.CQ (atomic exchange; each transition is owned by one thread)
void TClientEndpoint::ChangeState(
    EEndpointState expectedState,
    EEndpointState newState) noexcept
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

// Thread: RDMA.CM (called from BeginConnect)
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

    if (Config.VerbsQP) {
        Connection->qp = Verbs->CreateQP(Connection->pd, &qp_attrs);
    } else {
        Verbs->RdmaCreateQP(Connection.get(), &qp_attrs);
    }

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

// Thread: RDMA.CM (called from Reconnect, StopEndpoint; state is Disconnected)
void TClientEndpoint::DestroyQP() noexcept
{
    if (Connection && Connection->qp) {
        if (Config.VerbsQP) {
            Verbs->DestroyQP(Connection->qp);
            Connection->qp = nullptr;
        } else {
            Verbs->RdmaDestroyQP(Connection.get());
        }
    }

    CompletionQueue.reset();
    CompletionChannel.reset();

    with_lock (AllocationLock) {
        if (SendBuffers.Initialized()) {
            SendBuffers.ReleaseBuffer(SendBuffer);
        }

        if (RecvBuffers.Initialized()) {
            RecvBuffers.ReleaseBuffer(RecvBuffer);
        }
    }

    SendQueue.Clear();
    RecvQueue.Clear();

    FlushStartCycles = 0;
}

// Thread: RDMA.CM (called from HandleConnected after state becomes Connected)
void TClientEndpoint::StartReceive() noexcept
{
    while (auto* recv = RecvQueue.Pop()) {
        RecvResponse(recv);
    }
}

// Thread: external (actor system thread, e.g. partition_nonrepl)
// implements IClientEndpoint
TResultOrError<TClientRequestPtr> TClientEndpoint::AllocateRequest(
    IClientHandlerPtr handler,
    std::unique_ptr<TNullContext> context,
    size_t requestBytes,
    size_t responseBytes) noexcept
{
    if (!CheckState(EEndpointState::Connected)) {
        return MakeError(E_RDMA_UNAVAILABLE, "unable to allocate request");
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

// Thread: external (actor system thread)
// implements IClientEndpoint
ui64 TClientEndpoint::SendRequest(
    TClientRequestPtr creq,
    TCallContextPtr callContext) noexcept
{
    TRequestPtr req(static_cast<TRequest*>(creq.release()));
    req->CallContext = std::move(callContext);

    auto clientReqId = GetNewReqId();

    LWTRACK(
        RequestEnqueued,
        req->CallContext->LWOrbit,
        req->CallContext->RequestId);

    if (!CheckState(EEndpointState::Connected)) {
        AbortRequest(std::move(req), E_RDMA_UNAVAILABLE, "endpoint is unavailable");
        return clientReqId;
    }

    req->ClientReqId = clientReqId;

    Counters->RequestEnqueued();
    InputRequests.Enqueue(std::move(req));

    if (WaitMode == EWaitMode::Poll) {
        RequestEvent.Set();
    }
    return clientReqId;
}

// Thread: RDMA.CQ
bool TClientEndpoint::HandleInputRequests() noexcept
{
    if (WaitMode == EWaitMode::Poll) {
        RequestEvent.Clear();
    }

    if (!CheckState(EEndpointState::Connected)) {
        return false;
    }

    auto requests = InputRequests.DequeueAll();
    if (!requests) {
        return false;
    }

    QueuedRequests.Append(std::move(requests));
    HandleQueuedRequests();
    return true;
}

// Thread: RDMA.CQ
void TClientEndpoint::HandleQueuedRequests() noexcept
{
    if (!CheckState(EEndpointState::Connected)) {
        return;
    }

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

// Thread: external (actor system thread)
void TClientEndpoint::CancelRequest(ui64 clientRequestId) noexcept
{
    if (!CheckState(EEndpointState::Connected)) {
        return;
    }

    auto reqIdForQueue = std::make_unique<TClientRequestId>();
    reqIdForQueue->ClientRequestId = clientRequestId;
    CancelRequests.Enqueue(std::move(reqIdForQueue));

    if (WaitMode == EWaitMode::Poll) {
        CancelRequestEvent.Set();
    }
}

// Thread: external (actor system thread)
void TClientEndpoint::TryForceReconnect() noexcept
{
    switch (State) {
        case EEndpointState::ResolvingAddress:
        case EEndpointState::ResolvingRoute:
        case EEndpointState::Connected:
            return;
        case EEndpointState::Connecting:
        case EEndpointState::Disconnecting:
        case EEndpointState::Disconnected:
            break;
    }

    RDMA_DEBUG("scheduling force reconnect");
    Reconnect.InstantReschedule(MIN_CONNECT_TIMEOUT / 2);
}

// Thread: RDMA.CQ
bool TClientEndpoint::HandleCancelRequests() noexcept
{
    if (WaitMode == EWaitMode::Poll) {
        CancelRequestEvent.Clear();
    }

    if (!CheckState(EEndpointState::Connected) &&
        !CheckState(EEndpointState::Disconnecting))
    {
        return false;
    }

    THashSet<ui64> clientRequestIdToCancel;
    for (auto req: CancelRequests.DequeueAll()) {
        clientRequestIdToCancel.emplace(req.ClientRequestId);
    }
    if (clientRequestIdToCancel.empty()) {
        return false;
    }
    auto wasCancelled = [&](const TRequest& req) {
        return clientRequestIdToCancel.contains(req.ClientReqId);
    };

    // cancel input and queued requests
    auto requests = InputRequests.DequeueAll();
    auto cancelled = requests.DequeueIf(wasCancelled);
    cancelled.Append(QueuedRequests.DequeueIf(wasCancelled));
    while (auto req = cancelled.Dequeue()) {
        RDMA_TRACE("cancel request " << req->ReqId);
        Counters->RequestDequeued();
        AbortRequest(std::move(req), E_CANCELLED, "request was cancelled");
    }

    // cancel active requests
    cancelled.Append(ActiveRequests.PopCancelledRequests(clientRequestIdToCancel));
    while (auto req = cancelled.Dequeue()) {
        RDMA_TRACE("cancel request " << req->ReqId);
        Counters->RequestAborted();
        AbortRequest(std::move(req), E_CANCELLED, "request was cancelled");
    }

    if (!requests) {
        return false;
    }

    QueuedRequests.Append(std::move(requests));
    HandleQueuedRequests();
    return true;
}

// Thread: RDMA.CQ
void TClientEndpoint::AbortRequests() noexcept
{
    if (WaitMode == EWaitMode::Poll) {
        AbortRequestsEvent.Clear();
    }

    if (!CheckState(EEndpointState::Disconnecting)) {
        return;
    }

    auto requests = InputRequests.DequeueAll();
    if (requests) {
        QueuedRequests.Append(std::move(requests));
    }

    while (QueuedRequests) {
        auto req = QueuedRequests.Dequeue();
        Y_ABORT_UNLESS(req);
        RDMA_DEBUG("abort request " << req->ReqId);
        Counters->RequestDequeued();
        AbortRequest(std::move(req), E_RDMA_UNAVAILABLE, "endpoint is unavailable");
    }

    while (auto req = ActiveRequests.Pop()) {
        RDMA_DEBUG("abort request " << req->ReqId);
        Counters->RequestAborted();
        AbortRequest(std::move(req), E_RDMA_UNAVAILABLE, "endpoint is unavailable");
    }
}

// Thread: RDMA.CQ or external (external only in SendRequest error path
// when request hasn't been enqueued yet, so no concurrent access)
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

// Thread: RDMA.CQ
bool TClientEndpoint::HandleCompletionEvents() noexcept
{
    if (!CheckState(EEndpointState::Connected) &&
        !CheckState(EEndpointState::Disconnecting))
    {
        return false;
    }

    try {
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

    } catch (const TServiceError& e) {
        RDMA_ERROR(e.what());
        Counters->Error();
        Disconnect();
        return true;
    }

    return false;
}

// Thread: RDMA.CQ
int TClientEndpoint::ValidateCompletion(ibv_wc* wc) noexcept
{
    auto id = TWorkRequestId(wc->wr_id);

    if (id.Magic == SendMagic && id.Index < SendWrs.size()) {
        if (wc->status == IBV_WC_WR_FLUSH_ERR) {
            RDMA_TRACE(
                "SEND " << id << " " << NVerbs::GetStatusString(wc->status));
            Counters->SendRequestCompleted();
            SendQueue.Push(&SendWrs[id.Index]);
            return -1;
        }
        if (wc->status != IBV_WC_SUCCESS) {
            RDMA_ERROR(
                "SEND " << id << " " << NVerbs::GetStatusString(wc->status));
            Counters->Error();
            Counters->SendRequestCompleted();
            SendQueue.Push(&SendWrs[id.Index]);
            return -1;
        }
        if (wc->opcode != IBV_WC_SEND) {
            RDMA_ERROR(
                "SEND " << id << " unexpected opcode "
                        << NVerbs::GetOpcodeName(wc->opcode));
            Counters->Error();
            Counters->SendRequestCompleted();
            SendQueue.Push(&SendWrs[id.Index]);
            return -1;
        }
        return 0;
    }

    if (id.Magic == RecvMagic && id.Index < RecvWrs.size()) {
        if (wc->status == IBV_WC_WR_FLUSH_ERR) {
            RDMA_TRACE(
                "RECV " << id << " " << NVerbs::GetStatusString(wc->status));
            Counters->RecvResponseCompleted();
            RecvQueue.Push(&RecvWrs[id.Index]);
            return -1;
        }
        if (wc->status != IBV_WC_SUCCESS) {
            RDMA_ERROR(
                "RECV " << id << " " << NVerbs::GetStatusString(wc->status));
            Counters->Error();
            Counters->RecvResponseCompleted();
            RecvQueue.Push(&RecvWrs[id.Index]);
            return -1;
        }
        if (wc->opcode != IBV_WC_RECV) {
            RDMA_ERROR(
                "RECV " << id << " unexpected opcode "
                        << NVerbs::GetOpcodeName(wc->opcode));
            Counters->Error();
            Counters->RecvResponseCompleted();
            RecvQueue.Push(&RecvWrs[id.Index]);
            return -1;
        }
        return 0;
    }

    RDMA_ERROR("unexpected wr_id " << NVerbs::PrintCompletion(wc));
    Counters->Error();
    return -1;
}

// Thread: RDMA.CQ (called via Verbs->PollCompletionQueue callback)
// implements NVerbs::ICompletionHandler
void TClientEndpoint::HandleCompletionEvent(ibv_wc* wc) noexcept
{
    auto id = TWorkRequestId(wc->wr_id);

    if (ValidateCompletion(wc)) {
        Disconnect();
        return;
    }

    switch (wc->opcode) {
        case IBV_WC_SEND: {
            TSendWr* send = &SendWrs[id.Index];
            RDMA_TRACE(
                GetLogTitle("SEND", id, GetRequestId(*send)) << "completed");
            SendRequestCompleted(send);
            break;
        }

        case IBV_WC_RECV: {
            TRecvWr* recv = &RecvWrs[id.Index];
            RDMA_TRACE(
                GetLogTitle("RECV", id, GetRequestId(*recv)) << "completed");
            RecvResponseCompleted(recv);
            break;
        }

        default:
            RDMA_TRACE(
                "HandleCompletionEvent: unhandeled opcode "
                << NVerbs::GetOpcodeName(wc->opcode));
            break;
    }
}

// Thread: RDMA.CQ (private overload: posts RDMA send for a queued request)
void TClientEndpoint::SendRequest(TRequestPtr req, TSendWr* send) noexcept
{
    req->ReqId = ActiveRequests.CreateId();

    auto id = TWorkRequestId(send->wr.wr_id);
    auto* requestMsg = send->Message<TRequestMessage>();
    Zero(*requestMsg);

    InitMessageHeader(requestMsg, RDMA_PROTO_VERSION);

    requestMsg->ReqId = req->ReqId;
    requestMsg->In = req->InBuffer;
    requestMsg->Out = req->OutBuffer;

    try {
        Verbs->PostSend(Connection->qp, &send->wr);
        RDMA_TRACE(GetLogTitle("SEND", id, req->ReqId) << "posted");

    } catch (const TServiceError& e) {
        RDMA_ERROR(GetLogTitle("SEND", id, req->ReqId) << e.what());
        SendQueue.Push(send);
        Counters->Error();
        Counters->RequestEnqueued();
        QueuedRequests.Enqueue(std::move(req));
        Disconnect();
        return;
    }

    LWTRACK(
        SendRequestStarted,
        req->CallContext->LWOrbit,
        req->CallContext->RequestId);

    Counters->SendRequestStarted();

    send->context = reinterpret_cast<void*>(static_cast<uintptr_t>(req->ReqId));
    ActiveRequests.Push(std::move(req));
}

// Thread: RDMA.CQ
void TClientEndpoint::SendRequestCompleted(TSendWr* send) noexcept
{
    const ui32 reqId = GetRequestId(*send);

    Counters->SendRequestCompleted();
    SendQueue.Push(send);

    if (auto* req = ActiveRequests.Get(reqId)) {
        LWTRACK(
            SendRequestCompleted,
            req->CallContext->LWOrbit,
            req->CallContext->RequestId);
    }
    // request has already been completed
}

// Thread: RDMA.CM (from StartReceive) or RDMA.CQ (from RecvResponseCompleted)
// safe: initial recv posting happens before any completions can arrive
void TClientEndpoint::RecvResponse(TRecvWr* recv) noexcept
{
    auto id = TWorkRequestId(recv->wr.wr_id);
    auto* responseMsg = recv->Message<TResponseMessage>();
    Zero(*responseMsg);

    try {
        Verbs->PostRecv(Connection->qp, &recv->wr);
        RDMA_TRACE(GetLogTitle("RECV", id, GetRequestId(*recv)) << "posted");

    } catch (const TServiceError& e) {
        RDMA_ERROR(GetLogTitle("RECV", id, GetRequestId(*recv)) << e.what());
        Counters->Error();
        RecvQueue.Push(recv);
        Disconnect();
        return;
    }

    Counters->RecvResponseStarted();
}

// Thread: RDMA.CQ
void TClientEndpoint::RecvResponseCompleted(TRecvWr* recv) noexcept
{
    const auto wrId = TWorkRequestId(recv->wr.wr_id);
    auto* msg = recv->Message<TResponseMessage>();

    int version = ParseMessageHeader(msg);
    if (version != RDMA_PROTO_VERSION) {
        RDMA_ERROR(
            GetLogTitle("RECV", wrId, GetRequestId(*recv))
            << "incompatible protocol version " << version << ", expected "
            << static_cast<int>(RDMA_PROTO_VERSION));
        Counters->RecvResponseCompleted();
        Counters->Error();
        RecvResponse(recv);
        Disconnect();
        return;
    }

    const ui32 reqId = msg->ReqId;
    const ui32 status = msg->Status;
    const ui32 responseBytes = msg->ResponseBytes;

    Counters->RecvResponseCompleted();
    RecvResponse(recv);

    auto req = ActiveRequests.Pop(reqId);
    if (!req) {
        RDMA_WARN(
            GetLogTitle("RECV", wrId, GetRequestId(*recv))
            << "request " << reqId << " not found, last active request id "
            << ActiveRequests.GetCurrentId());
        Counters->Error();
        return;
    }
    Counters->RequestCompleted();

    RDMA_TRACE("complete request " << reqId);

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

// Thread: external
TFuture<void> TClientEndpoint::Stop() noexcept
{
    if (!StopFlag.test_and_set()) {
        Disconnect();
    }
    return StopResult.GetFuture();
}

// Thread: RDMA.CM (atomic read, also safe from any thread)
bool TClientEndpoint::ShouldStop() const
{
    return StopFlag.test();
}

// Thread: RDMA.CM
void TClientEndpoint::SetConnection(NVerbs::TConnectionPtr connection) noexcept
{
    connection->context = this;
    Connection = std::move(connection);
}

// Thread: RDMA.CM
int TClientEndpoint::ReconnectTimerHandle() const
{
    return Reconnect.Handle();
}

// Thread: RDMA.CM (called from TClient::Disconnect)
void TClientEndpoint::FlushQueues() noexcept
{
    RDMA_DEBUG("flush queues");

    try {
        ibv_qp_attr attr = {.qp_state = IBV_QPS_ERR};
        Verbs->ModifyQP(Connection->qp, &attr, IBV_QP_STATE);
        FlushStartCycles = GetCycleCount();

    } catch (const TServiceError& e) {
        RDMA_ERROR(e.what());
        Counters->Error();
    }
}

// Thread: RDMA.CM
int TClientEndpoint::DisconnectEventHandle() const
{
    return DisconnectEvent.Handle();
}

// Thread: RDMA.CM
void TClientEndpoint::ClearDisconnectEvent() noexcept
{
    DisconnectEvent.Clear();
}

// Thread: any (RDMA.CM, RDMA.CQ, or external via Stop)
// Safe: reads atomic State, then either calls Reconnect.Schedule (has internal
// lock) or sets DisconnectEvent (idempotent). The CM thread is the sole
// consumer of DisconnectEvent and performs the actual state transition.
void TClientEndpoint::Disconnect() noexcept
{
    switch (State) {
        // queues are empty, reconnect is scheduled, nothing to do
        case EEndpointState::Disconnecting:
        case EEndpointState::Disconnected:
        case EEndpointState::Connecting:
            return;

        // schedule reconnect
        case EEndpointState::ResolvingAddress:
        case EEndpointState::ResolvingRoute:
            Reconnect.Schedule();
            return;

        // disconnect
        case EEndpointState::Connected:
            DisconnectEvent.Set();
            return;
    }
}

// Thread: RDMA.CQ (called from DisconnectFlushed)
bool TClientEndpoint::Flushed() const
{
    return SendQueue.Size() == Config.QueueSize
        && RecvQueue.Size() == Config.QueueSize
        && ActiveRequests.Empty()
        && !InputRequests
        && !QueuedRequests
        && !CancelRequests;
}

// Thread: RDMA.CQ (called from DisconnectFlushed; FlushStartCycles is atomic)
bool TClientEndpoint::FlushHanging() const
{
    auto start = FlushStartCycles.load();
    return start &&
           CyclesToDurationSafe(GetCycleCount() - start) >= FLUSH_TIMEOUT;
}

// Thread: any (called from ~TRequest; protected by AllocationLock)
void TClientEndpoint::FreeRequest(TRequest* req) noexcept
{
    with_lock (AllocationLock) {
        SendBuffers.ReleaseBuffer(req->InBuffer);
        RecvBuffers.ReleaseBuffer(req->OutBuffer);
    }
}

// Thread: external (atomic increment, thread-safe)
ui64 TClientEndpoint::GetNewReqId() noexcept
{
    return ReqIdPool.fetch_add(1);
}

////////////////////////////////////////////////////////////////////////////////

struct IConnectionEventHandler
{
    virtual ~IConnectionEventHandler() = default;

    virtual void HandleConnectionEvent(NVerbs::TConnectionEventPtr event) = 0;
    virtual void Reconnect(TClientEndpoint* endpoint) = 0;
    virtual void Disconnect(TClientEndpoint* endpoint) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TConnectionPoller final
    : public IStartable
    , private ISimpleThread
{
private:
    // events must fit into EVENT_MASK
    enum EPollEvent
    {
        ConnectionEvent = 0,
        ReconnectTimer = 1,
        DisconnectEvent = 2,
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

    NVerbs::TConnectionPtr CreateConnection(ui8 tos)
    {
        return Verbs->CreateConnection(
            EventChannel.get(),
            nullptr,    // context
            RDMA_PS_TCP,
            tos);
    }

    void Attach(TClientEndpoint* endpoint)
    {
        PollHandle.Attach(
            endpoint->DisconnectEventHandle(),
            EPOLLIN,
            PtrEventTag(endpoint, EPollEvent::DisconnectEvent));

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

                    case EPollEvent::DisconnectEvent:
                        EventHandler->Disconnect(
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
    // events must fit into EVENT_MASK
    enum EPollEvent
    {
        Completion = 0,
        Request = 1,
        AbortRequests = 2,
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
                endpoint->AbortRequestsEvent.Handle(),
                EPOLLIN,
                PtrEventTag(endpoint, EPollEvent::AbortRequests));

            Verbs->RequestCompletionEvent(endpoint->CompletionQueue.get(), 0);
        }
    }

    void Detach(TClientEndpoint* endpoint)
    {
        if (Config->WaitMode == EWaitMode::Poll) {
            PollHandle.Detach(endpoint->CompletionChannel->fd);
            PollHandle.Detach(endpoint->RequestEvent.Handle());
            PollHandle.Detach(endpoint->CancelRequestEvent.Handle());
            PollHandle.Detach(endpoint->AbortRequestsEvent.Handle());
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

            case EPollEvent::AbortRequests:
                endpoint->AbortRequests();
                break;
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
            hasWork |= endpoint->HandleCancelRequests();
            hasWork |= endpoint->HandleInputRequests();
            hasWork |= endpoint->HandleCompletionEvents();
            endpoint->AbortRequests();
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
                RDMA_DEBUG(endpoint->Log, "timeout request " << request->ReqId);
                endpoint->Counters->RequestAborted();
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

            RDMA_INFO(endpoint->Log, "disconnected");
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
    void Disconnect(TClientEndpoint* endpont) noexcept override;
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

// Thread: external
void TClient::Start() noexcept
{
    Log = Logging->CreateLog("BLOCKSTORE_RDMA");

    RDMA_INFO("start client");

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

// Thread: external
void TClient::Stop() noexcept
{
    if (ConnectionPoller) {
        ConnectionPoller->Stop();
        ConnectionPoller.reset();
    }

    for (auto& poller: CompletionPollers) {
        poller->Stop();
    }
    CompletionPollers.clear();

    RDMA_INFO("stop client");
}

// Thread: external (actor system thread)
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
            ConnectionPoller->CreateConnection(Config->IpTypeOfService),
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

// Thread: RDMA.CM
// implements IConnectionEventHandler
void TClient::HandleConnectionEvent(NVerbs::TConnectionEventPtr event) noexcept
{
    TClientEndpoint* endpoint = TClientEndpoint::FromEvent(event.get());

    RDMA_INFO(endpoint->Log, NVerbs::GetEventName(event->event) << " received");

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

// Thread: RDMA.CM
void TClient::StopEndpoint(TClientEndpoint* endpoint) noexcept
{
    RDMA_INFO(endpoint->Log, "release resources");
    ConnectionPoller->Detach(endpoint);
    if (endpoint->CompletionChannel) {
        endpoint->Poller->Detach(endpoint);
    }
    endpoint->DestroyQP();
    endpoint->Connection.reset();
    endpoint->StopResult.SetValue();
    endpoint->Poller->Release(endpoint);
}

// Thread: RDMA.CM
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
                MakeError(E_RDMA_UNAVAILABLE, "connection timeout"))));

            StopEndpoint(endpoint);
            return;
        }
        // otherwise keep trying
    }

    RDMA_DEBUG(
        endpoint->Log,
        "reconnect timer hit in " << endpoint->State << " state");

    switch (endpoint->State) {
        // wait for completion poller to flush WRs
        case EEndpointState::Disconnecting:
            endpoint->Reconnect.Schedule();
            return;

        // didn't even start to connect, try again
        case EEndpointState::ResolvingAddress:
            endpoint->ChangeState(
                EEndpointState::ResolvingAddress,
                EEndpointState::Disconnected);
            break;

        case EEndpointState::ResolvingRoute:
            endpoint->ChangeState(
                EEndpointState::ResolvingRoute,
                EEndpointState::Disconnected);
            break;

        // create new connection and try again
        case EEndpointState::Connecting:
            endpoint->ChangeState(
                EEndpointState::Connecting,
                EEndpointState::Disconnected);
            // fallthrough

        case EEndpointState::Disconnected:
            endpoint->Poller->Detach(endpoint);
            endpoint->DestroyQP();
            endpoint->SetConnection(
                ConnectionPoller->CreateConnection(Config->IpTypeOfService));
            break;

        // reconnect timer hit at the same time connection was established
        case EEndpointState::Connected:
            return;
    }

    RDMA_WARN(endpoint->Log, "reconnect");
    BeginResolveAddress(endpoint);
}

// Thread: RDMA.CM (triggered by DisconnectEvent from any thread)
// implements IConnectionEventHandler
void TClient::Disconnect(TClientEndpoint* endpoint) noexcept
{
    endpoint->ClearDisconnectEvent();

    if (!endpoint->CheckState(EEndpointState::Connected)) {
        return;
    }

    RDMA_INFO(endpoint->Log, "disconnect from " << endpoint->Host);

    endpoint->ChangeState(
        EEndpointState::Connected,
        EEndpointState::Disconnecting);

    endpoint->FlushQueues();
    endpoint->Reconnect.Schedule();

    if (endpoint->WaitMode == EWaitMode::Poll) {
        endpoint->AbortRequestsEvent.Set();
    }
}

// Thread: RDMA.CM
void TClient::BeginResolveAddress(TClientEndpoint* endpoint) noexcept
{
    try {
        rdma_addrinfo hints = {
            .ai_port_space = RDMA_PS_TCP,
        };
        NAddr::IRemoteAddrRef src;

        // find the first non local address of the specified interface
        for (auto& interface: NAddr::GetNetworkInterfaces()) {
            if (interface.Name == Config->SourceInterface &&
                GetScopeId(interface.Address->Addr()) == 0)
            {
                src = interface.Address;

                RDMA_INFO(endpoint->Log, "bind to " << interface.Name
                    << " address " << NAddr::PrintHost(*src));

                // it's a TOpaqueAddr, so it's safe to cast the const away
                hints.ai_src_addr = const_cast<sockaddr*>(src->Addr());
                hints.ai_src_len = src->Len();
                break;
            }
        }

        endpoint->ChangeState(
            EEndpointState::Disconnected,
            EEndpointState::ResolvingAddress);

        auto addrinfo = Verbs->GetAddressInfo(
            endpoint->Host, endpoint->Port, &hints);

        RDMA_DEBUG(endpoint->Log, "resolve address");

        Verbs->ResolveAddress(endpoint->Connection.get(), addrinfo->ai_src_addr,
            addrinfo->ai_dst_addr, RESOLVE_TIMEOUT);

    } catch (const TServiceError& e) {
        RDMA_ERROR(endpoint->Log, e.what());
        Counters->Error();
        endpoint->Disconnect();
    }
}

// Thread: RDMA.CM
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
        Counters->Error();
        endpoint->Disconnect();
    }
}

// Thread: RDMA.CM
void TClient::BeginConnect(TClientEndpoint* endpoint) noexcept
{
    Y_ABORT_UNLESS(endpoint);

    try {
        RDMA_INFO(endpoint->Log, "connect to " << endpoint->Host);

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

        Verbs->Connect(endpoint->Connection.get(), &param);

    } catch (const TServiceError& e) {
        RDMA_ERROR(endpoint->Log, e.what());
        Counters->Error();
        endpoint->Disconnect();
    }
}

// Thread: RDMA.CM
void TClient::HandleConnected(
    TClientEndpoint* endpoint,
    NVerbs::TConnectionEventPtr event) noexcept
{
    const rdma_conn_param* param = &event->param.conn;

    RDMA_DEBUG(endpoint->Log, "validate");

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
    endpoint->StartReceive();

    RDMA_INFO(endpoint->Log, "connected");

    if (endpoint->StartResult.Initialized()) {
        auto startResult = std::move(endpoint->StartResult);
        startResult.SetValue(endpoint->shared_from_this());
    }
}

// Thread: RDMA.CM
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

// Thread: RDMA.CM
void TClient::HandleDisconnected(TClientEndpoint* endpoint) noexcept
{
    // we can't reset config right away, because we need to know queue size to
    // clean up flushed WRs
    endpoint->ResetConfig = true;
    endpoint->Disconnect();
}

// Thread: external (called from StartEndpoint)
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
                    TABLEH() { out << "ActiveSend"; }
                    TABLEH() { out << "ActiveRecv"; }
                    TABLEH() { out << "Errors"; }
                }
                TABLER() {
                    TABLED() { out << Counters->QueuedRequests->Val(); }
                    TABLED() { out << Counters->ActiveRequests->Val(); }
                    TABLED() { out << Counters->AbortedRequests->Val(); }
                    TABLED() { out << Counters->CompletedRequests->Val(); }
                    TABLED() { out << Counters->ActiveSend->Val(); }
                    TABLED() { out << Counters->ActiveRecv->Val(); }
                    TABLED() { out << Counters->Errors->Val(); }
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
