#include "client.h"

#include "adaptive_wait.h"
#include "buffer.h"
#include "event.h"
#include "list.h"
#include "poll.h"
#include "rcu.h"
#include "utils.h"
#include "verbs.h"
#include "work_queue.h"

#include <cloud/storage/core/libs/common/backoff_delay_provider.h>
#include <cloud/storage/core/libs/common/context.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/history.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/rdma/iface/log.h>
#include <cloud/storage/core/libs/rdma/iface/probes.h>
#include <cloud/storage/core/libs/rdma/iface/protobuf.h>
#include <cloud/storage/core/libs/rdma/iface/protocol.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/intrlist.h>
#include <util/generic/map.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/network/interface.h>
#include <util/random/random.h>
#include <util/stream/format.h>
#include <util/system/datetime.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>

namespace NCloud::NStorage::NRdma {

using namespace NMonitoring;
using namespace NThreading;

using TSendWr = TSendWrBase<TRequestMessage>;
using TRecvWr = TRecvWrBase<TResponseMessage>;

LWTRACE_USING(STORAGE_RDMA_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration POLL_TIMEOUT = TDuration::Seconds(1);
constexpr TDuration MIN_CONNECT_TIMEOUT = TDuration::Seconds(1);
constexpr TDuration MIN_RECONNECT_DELAY = TDuration::MilliSeconds(10);
constexpr TDuration INSTANT_RECONNECT_DELAY = TDuration::MicroSeconds(1);

////////////////////////////////////////////////////////////////////////////////

struct TRequestResources
{
    enum class ERecyclePolicy
    {
        Recycle,
        Drop,
    };

    enum class EOutMode
    {
        None,
        RemoteInvalidate,
        LocalInvalidate,
    };

    TPooledBuffer InBuffer{};
    TPooledBuffer OutBuffer{};

    NVerbs::TMemoryWindowPtr InMemoryWindow = NVerbs::NullPtr;
    NVerbs::TMemoryWindowPtr OutMemoryWindow = NVerbs::NullPtr;

    ERecyclePolicy RecyclePolicy = ERecyclePolicy::Recycle;
    EOutMode OutMode = EOutMode::None;
    ui32 RKey = 0;
    ui64 BufferPoolGeneration = 0;
    NProto::TError Error;

    ui32 Status = RDMA_PROTO_FAIL;
    ui32 ResponseBytes = 0;
};

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

enum class ERequestState
{
    SendRequest,
    RecvResponse,
};

////////////////////////////////////////////////////////////////////////////////

struct TRequest
    : TClientRequest
    , TListNode<TRequest>
{
    const ui64 StartedCycles;

    std::weak_ptr<TClientEndpoint> Endpoint;

    TCallContextBasePtr CallContext;
    ui32 ReqId = 0;   // 16-bit RDMA request id
    ui64 ClientReqId = 0;

    TRequestResources Resources;

    ERequestState State;

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

struct TInvalidationRequest
    : TIntrusiveListItem<TInvalidationRequest>
{
    TRequestPtr Request;
    bool QpOwnedSendSlot = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TInvalidationRequestDelete
{
    static void Destroy(TInvalidationRequest* req)
    {
        delete req;
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
    std::atomic<ui64> BufferPoolGeneration = 0;
    TMutex AllocationLock;
    TVector<NVerbs::TMemoryWindowPtr> FreeMemoryWindows;
    size_t MaxFreeMemoryWindows = 0;

    TPooledBuffer SendBuffer {};
    TPooledBuffer RecvBuffer {};

    TVector<TSendWr> SendWrs;
    TVector<TRecvWr> RecvWrs;

    TWorkQueue<TSendWr> SendQueue;
    TWorkQueue<TRecvWr> RecvQueue;

    const ui64 Id = RandomNumber(Max<ui64>());
    ui16 Generation = Max<ui16>();

    const ui32 RecvMagic = RandomNumber(Max<ui32>());
    const ui32 SendMagic = RecvMagic ^ (1u << 31);

    TLockFreeList<TRequest> InputRequests;
    TLockFreeList<TClientRequestId> CancelRequests;
    TEventHandle CancelRequestEvent;
    TEventHandle RequestEvent;
    TEventHandle AbortRequestsEvent;
    TEventHandle DisconnectEvent;

    TSimpleList<TRequest> QueuedRequests;
    TSimpleList<TRequest> PartiallyPostedRequests;
    // Send slots whose terminal CQE is no longer required because the current
    // QP is committed to destruction.
    std::atomic<size_t> QpOwnedSendSlots = 0;
    // Invalidations have only two ownership states: waiting for a send slot or
    // already visible to hardware. Posted entries stay in one list until their
    // terminal CQE or QP destruction. QpOwnedSendSlot tracks send-slot
    // bookkeeping while the QP is committed to destruction.
    TIntrusiveListWithAutoDelete<TInvalidationRequest, TInvalidationRequestDelete>
        PendingInvalidationRequests;
    TIntrusiveListWithAutoDelete<TInvalidationRequest, TInvalidationRequestDelete>
        PostedInvalidationRequests;
    TActiveRequests ActiveRequests;

    std::atomic<ui64> ReqIdPool{0};

    int NegotiatedProtocolVersion = RDMA_PROTO_VERSION;
    bool PeerSupportsSendWithInvalidate = false;

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
    void SetupQP();
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
        TCallContextBasePtr callContext) noexcept override;
    void CancelRequest(ui64 reqId) noexcept override;
    void TryForceReconnect() noexcept override;
    TFuture<void> Stop() noexcept override;

    // called from CQ thread
    void HandleCompletionEvent(ibv_wc* wc) noexcept override;
    bool HandleInputRequests() noexcept;
    bool HandleCancelRequests() noexcept;
    bool HandleCompletionEvents() noexcept;
    void AbortRequests() noexcept;
    bool ClientRequestsFlushed() const;
    bool WorkRequestsFlushed() const;
    bool FlushHanging() const;

    void SetNegotiatedProtocolVersion(int negotiatedProtocolVersion);
    int GetNegotiatedProtocolVersion() const;

private:
    enum class EAbortCounter
    {
        Dequeued,
        Aborted,
    };

private:
    // called from CQ thread

    // Request scheduling and completion flow.
    void HandleQueuedRequests() noexcept;
    void SendRequest(TRequestPtr req, TSendWr* send) noexcept;
    void SendRequest(TRequest* req, TSendWr* send) noexcept;
    void SendRequestCompleted(TSendWr* send) noexcept;
    int ValidateCompletion(ibv_wc* wc) noexcept;
    void RecvResponse(TRecvWr* recv) noexcept;
    void RecvResponseCompleted(TRecvWr* recv, ibv_wc* wc) noexcept;
    void FinalizeRequest(TRequestPtr req) noexcept;

    // Abort/defer flow.
    void TryAbortRequest(
        TRequestPtr req,
        NProto::TError error) noexcept;
    void AbortRequest(ui32 reqId) noexcept;
    void AbortRequest(ui32 reqId, NProto::TError error) noexcept;
    void AbortRequest(TRequestPtr req, const NProto::TError& error) noexcept;
    void AbortRequest(
        TRequestPtr req,
        EAbortCounter counter,
        const NProto::TError& error) noexcept;

    // Invalidation flow.
    void CompleteInvalidationRequest(TInvalidationRequest* req) noexcept;
    void MarkInvalidationAsQpOwned(TInvalidationRequest* req) noexcept;
    void DrainInvalidationRequests() noexcept;
    void PostNextPendingInvalidation(TSendWr* send) noexcept;
    void PostLocalInvalidation(TInvalidationRequest* req, TSendWr* send) noexcept;
    void HandlePendingInvalidationRequests() noexcept;
    void LocalInvalidationCompleted(TSendWr* send) noexcept;
    void ConfigureOutInvalidation(
        TRequestResources& resources,
        TRequestMessage* msg,
        ui32 outRKey) noexcept;
    bool ValidateRemoteInvalidation(
        TRecvWr* recv,
        ui32 reqId,
        ibv_wc* wc,
        TRequestResources& resources) noexcept;
    bool NeedLocalInvalidation(const TRequestResources& resources) const noexcept;

    // Resource lifetime and allocation helpers.
    void CleanupRequestResourcesLocked(TRequestResources& resources) noexcept;
    void FreeRequest(TRequest* creq) noexcept;
    NVerbs::TMemoryWindowPtr AcquireMemoryWindowLocked();
    void RecycleMemoryWindowLocked(NVerbs::TMemoryWindowPtr& memoryWindow);
    static ui32 GetNextMemoryWindowRKey(ibv_mw* memoryWindow);

    // Misc.
    ui64 GetNewReqId() noexcept;
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
    , SendBuffers(Config.BufferPool)
    , RecvBuffers(Config.BufferPool)
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

TClientEndpoint::~TClientEndpoint()
{
    // release any leftover resources if endpoint hasn't been properly stopped
    if (Connection) {
        RDMA_INFO("release resources");
    }
    DestroyQP();
    RDMA_INFO("stop endpoint");
}

bool TClientEndpoint::CheckState(EEndpointState expectedState) const
{
    return State == expectedState;
}

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

void TClientEndpoint::CreateQP()
{
    ++BufferPoolGeneration;

    CompletionChannel = Verbs->CreateCompletionChannel(Connection->verbs);
    SetNonBlock(CompletionChannel->fd, true);

    if (ResetConfig) {
        Config = *OriginalConfig;
        ResetConfig = false;
    }

    const ui32 sendWrPerRequest = Config.UseMemoryWindows ? 3 : 1;
    const ui32 maxSendWr = Config.SendQueueSize * sendWrPerRequest;

    CompletionQueue = Verbs->CreateCompletionQueue(
        Connection->verbs,
        maxSendWr + Config.RecvQueueSize,
        this,
        CompletionChannel.get(),
        0);   // comp_vector

    ibv_qp_init_attr qp_attrs = {
        .qp_context = nullptr,
        .send_cq = CompletionQueue.get(),
        .recv_cq = CompletionQueue.get(),
        .cap = {
            .max_send_wr = maxSendWr,
            .max_recv_wr = Config.RecvQueueSize,
            .max_send_sge = RDMA_MAX_SEND_SGE,
            .max_recv_sge = RDMA_MAX_RECV_SGE,
            .max_inline_data = 16,
        },
        .qp_type = IBV_QPT_RC,
        .sq_sig_all = 0,
    };

    if (Config.VerbsQP) {
        Connection->qp = Verbs->CreateQP(Connection->pd, &qp_attrs);
    } else {
        Verbs->RdmaCreateQP(Connection.get(), &qp_attrs);
    }

    int sendAccessFlags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ;
    int recvAccessFlags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE;
    if (Config.UseMemoryWindows) {
        sendAccessFlags |= IBV_ACCESS_MW_BIND;
        recvAccessFlags |= IBV_ACCESS_MW_BIND;
    }

    SendBuffers.Init(Verbs, Connection->pd, sendAccessFlags);
    RecvBuffers.Init(Verbs, Connection->pd, recvAccessFlags);

    if (Config.UseMemoryWindows) {
        // Endpoint-local pool: windows are reused only inside this endpoint/QP
        // flow (Type 2A semantics at application level).
        MaxFreeMemoryWindows = static_cast<size_t>(Config.SendQueueSize) * 2;
        FreeMemoryWindows.reserve(MaxFreeMemoryWindows);
        for (size_t i = 0; i < MaxFreeMemoryWindows; ++i) {
            FreeMemoryWindows.push_back(Verbs->CreateMemoryWindow(Connection->pd));
        }
    } else {
        MaxFreeMemoryWindows = 0;
        FreeMemoryWindows.clear();
    }

    SendBuffer = SendBuffers.AcquireBuffer(
        Config.SendQueueSize * sizeof(TRequestMessage), true);

    RecvBuffer = RecvBuffers.AcquireBuffer(
        Config.RecvQueueSize * sizeof(TResponseMessage), true);

    SendWrs.resize(Config.SendQueueSize);
    RecvWrs.resize(Config.RecvQueueSize);

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
        wr.wr.send_flags = IBV_SEND_SIGNALED;
        wr.wr.next = nullptr;

        wr.sg_list[0].lkey = SendBuffer.LKey;
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

        wr.sg_list[0].lkey = RecvBuffer.LKey;
        wr.sg_list[0].addr = responseMsg;
        wr.sg_list[0].length = sizeof(TResponseMessage);

        RecvQueue.Push(&wr);
        responseMsg += sizeof(TResponseMessage);
    }
}

void TClientEndpoint::SetupQP()
{
    ibv_qp_attr qpAttr{};
    int mask = 0;
    if (Config.QpTimeout > 0) {
        qpAttr.timeout = Config.QpTimeout;
        mask |= IBV_QP_TIMEOUT;
    }
    if (Config.QpMinRnrTimer > 0) {
        qpAttr.min_rnr_timer = Config.QpMinRnrTimer;
        mask |= IBV_QP_MIN_RNR_TIMER;
    }
    if (mask != 0) {
        Verbs->ModifyQP(Connection->qp, &qpAttr, mask);
    }
}

void TClientEndpoint::DestroyQP() noexcept
{
    // QP destruction is the lifetime boundary for every MW which may have
    // been referenced by a posted WQE.
    if (Connection && Connection->qp) {
        if (Config.VerbsQP) {
            Verbs->DestroyQP(Connection->qp);
            Connection->qp = nullptr;
        } else {
            Verbs->RdmaDestroyQP(Connection.get());
            Connection->qp = nullptr;
        }
    }

    while (auto req = PartiallyPostedRequests.Dequeue()) {
        AbortRequest(
            std::move(req),
            EAbortCounter::Aborted,
            MakeError(E_RDMA_UNAVAILABLE, "failed to post complete send chain"));
    }

    while (PostedInvalidationRequests) {
        auto* req = PostedInvalidationRequests.PopFront();
        Y_ABORT_UNLESS(req);
        CompleteInvalidationRequest(req);
        delete req;
    }
    QpOwnedSendSlots = 0;

    // No WQE can reference windows from the endpoint-local pool anymore.
    with_lock (AllocationLock) {
        MaxFreeMemoryWindows = 0;
        FreeMemoryWindows.clear();
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

void TClientEndpoint::StartReceive() noexcept
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
    req->Resources.BufferPoolGeneration = BufferPoolGeneration.load();

    try {
        with_lock (AllocationLock) {
            if (requestBytes) {
                req->Resources.InBuffer = SendBuffers.AcquireBuffer(requestBytes);
                if (Config.UseMemoryWindows) {
                    req->Resources.InMemoryWindow = AcquireMemoryWindowLocked();
                }
            }

            if (responseBytes) {
                req->Resources.OutBuffer = RecvBuffers.AcquireBuffer(responseBytes);
                if (Config.UseMemoryWindows) {
                    req->Resources.OutMemoryWindow = AcquireMemoryWindowLocked();
                }
            }
        }
    } catch (...) {
        RDMA_ERROR(
            "failed to allocate request with exception: "
            << CurrentExceptionMessage());
        Counters->Error();
        Disconnect();
        return MakeError(
            E_RDMA_UNAVAILABLE,
            TStringBuilder()
                << "failed to allocate request with exception: "
                << CurrentExceptionMessage());
    }

    req->RequestBuffer = TStringBuf {
        reinterpret_cast<char*>(req->Resources.InBuffer.Address),
        req->Resources.InBuffer.Length,
    };
    req->ResponseBuffer = TStringBuf {
        reinterpret_cast<char*>(req->Resources.OutBuffer.Address),
        req->Resources.OutBuffer.Length,
    };

    return TClientRequestPtr(std::move(req));
}

// implements IClientEndpoint
ui64 TClientEndpoint::SendRequest(
    TClientRequestPtr creq,
    TCallContextBasePtr callContext) noexcept
{
    TRequestPtr req(static_cast<TRequest*>(creq.release()));
    req->CallContext = std::move(callContext);

    auto clientReqId = GetNewReqId();
    req->ClientReqId = clientReqId;

    if (!CheckState(EEndpointState::Connected)) {
        AbortRequest(
            std::move(req),
            MakeError(E_RDMA_UNAVAILABLE, "endpoint is unavailable"));
        return clientReqId;
    }

    if (req->CallContext) {
        LWTRACK(
            RequestEnqueued,
            req->CallContext->LWOrbit,
            req->CallContext->RequestId);
    }

    Counters->RequestEnqueued();
    InputRequests.Enqueue(std::move(req));

    if (WaitMode == EWaitMode::Poll) {
        RequestEvent.Set();
    }

    return clientReqId;
}

bool TClientEndpoint::HandleInputRequests() noexcept
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

void TClientEndpoint::HandleQueuedRequests() noexcept
{
    while (QueuedRequests || PendingInvalidationRequests) {
        if (CheckState(EEndpointState::Connected)) {
            auto* send = SendQueue.Pop();
            if (!send) {
                // no more WRs available
                break;
            }

            if (PendingInvalidationRequests) {
                PostNextPendingInvalidation(send);
                continue;
            }

            auto req = QueuedRequests.Dequeue();
            Y_ABORT_UNLESS(req);

            Counters->RequestDequeued();
            SendRequest(std::move(req), send);
        }
        else {
            DrainInvalidationRequests();

            auto req = QueuedRequests.Dequeue();
            if (!req) {
                break;
            }

            AbortRequest(
                std::move(req),
                EAbortCounter::Dequeued,
                MakeError(E_RDMA_UNAVAILABLE, "endpoint is unavailable"));
        }
    }
}

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

bool TClientEndpoint::HandleCancelRequests() noexcept
{
    if (WaitMode == EWaitMode::Poll) {
        CancelRequestEvent.Clear();
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
        RDMA_TRACE("request " << req->ReqId << " cancelled");
        AbortRequest(
            std::move(req),
            EAbortCounter::Dequeued,
            MakeError(E_CANCELLED, "request was cancelled"));
    }

    // cancel active requests
    cancelled.Append(ActiveRequests.PopCancelledRequests(clientRequestIdToCancel));
    while (auto req = cancelled.Dequeue()) {
        RDMA_TRACE("request " << req->ReqId << " cancelled");
        TryAbortRequest(
            std::move(req),
            MakeError(E_CANCELLED, "request was cancelled"));
    }

    if (!requests) {
        return false;
    }

    QueuedRequests.Append(std::move(requests));
    HandleQueuedRequests();
    return true;
}

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
        AbortRequest(
            std::move(req),
            EAbortCounter::Dequeued,
            MakeError(E_RDMA_UNAVAILABLE, "endpoint is unavailable"));
    }

    TSimpleList<TRequest> activeRequests;
    while (auto req = ActiveRequests.Pop()) {
        activeRequests.Enqueue(std::move(req));
    }

    while (auto req = activeRequests.Dequeue()) {
        RDMA_DEBUG("abort request " << req->ReqId);
        TryAbortRequest(
            std::move(req),
            MakeError(E_RDMA_UNAVAILABLE, "endpoint is unavailable"));
    }

    DrainInvalidationRequests();
}

void TClientEndpoint::AbortRequest(
    TRequestPtr req,
    const NProto::TError& error) noexcept
{
    // For timed out/cancelled/aborted requests we cannot guarantee that remote
    // peer invalidated keys, so force MW deallocation instead of recycling.
    req->Resources.RecyclePolicy = TRequestResources::ERecyclePolicy::Drop;

    auto len = SerializeError(
        error.GetCode(),
        error.GetMessage(),
        static_cast<TStringBuf>(req->Resources.OutBuffer));

    auto* handler = req->Handler.get();
    handler->HandleResponse(std::move(req), RDMA_PROTO_FAIL, len);
}

bool TClientEndpoint::HandleCompletionEvents() noexcept
{
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

int TClientEndpoint::ValidateCompletion(ibv_wc* wc) noexcept
{
    auto id = TWorkRequestId(wc->wr_id);

    if (id.Magic == SendMagic && id.Index < SendWrs.size()) {
        auto* send = &SendWrs[id.Index];
        auto handleLocalInvalidationCompletion = [&] {
            if (!send->context) {
                RDMA_TRACE(send << " local invalidate completion already handled");
                return;
            }
            auto* req = static_cast<TInvalidationRequest*>(send->context);
            send->context = nullptr;
            // A failed LOCAL_INV may be followed by another WQE from the same
            // chain. Quarantine the slot and retain resources until QP destroy.
            MarkInvalidationAsQpOwned(req);
        };
        auto handleRequestSendCompletion = [&] (NProto::TError error) {
            const ui32 reqId =
                SafeCast<ui32>(reinterpret_cast<uintptr_t>(send->context));
            Counters->SendRequestCompleted();
            SendQueue.Push(send);
            AbortRequest(reqId, std::move(error));
        };

        switch (wc->status) {
            case IBV_WC_WR_FLUSH_ERR:
                RDMA_TRACE(send << " " << NVerbs::GetStatusString(wc->status));
                switch (wc->opcode) {
                    case IBV_WC_LOCAL_INV:
                        handleLocalInvalidationCompletion();
                        break;
                    case IBV_WC_SEND:
                        handleRequestSendCompletion(MakeError(
                            E_RDMA_UNAVAILABLE,
                            TStringBuilder()
                                << "send request flushed: "
                                << NVerbs::GetStatusString(wc->status)));
                        break;
                    default:
                        break;
                }
                return -1;

            case IBV_WC_SUCCESS:
                switch (wc->opcode) {
                    case IBV_WC_SEND:
                    case IBV_WC_BIND_MW:
                    case IBV_WC_LOCAL_INV:
                        return 0;
                    default:
                        RDMA_ERROR(
                            send << " unexpected opcode "
                                 << NVerbs::GetOpcodeName(wc->opcode));
                        Counters->Error();
                        Counters->SendRequestCompleted();
                        SendQueue.Push(send);
                        return -1;
                }

            default:
                RDMA_ERROR(send << " " << NVerbs::GetStatusString(wc->status));
                Counters->Error();
                switch (wc->opcode) {
                    case IBV_WC_LOCAL_INV:
                        handleLocalInvalidationCompletion();
                        break;
                    case IBV_WC_SEND:
                        handleRequestSendCompletion(MakeError(
                            E_RDMA_UNAVAILABLE,
                            TStringBuilder()
                                << "send request failed: "
                                << NVerbs::GetStatusString(wc->status)));
                        break;
                    default:
                        break;
                }
                return -1;
        }
    }

    if (id.Magic == RecvMagic && id.Index < RecvWrs.size()) {
        auto* recv = &RecvWrs[id.Index];

        switch (wc->status) {
            case IBV_WC_WR_FLUSH_ERR:
                RDMA_TRACE(recv << " " << NVerbs::GetStatusString(wc->status));
                Counters->RecvResponseCompleted();
                RecvQueue.Push(recv);
                return -1;

            case IBV_WC_SUCCESS:
                switch (wc->opcode) {
                    case IBV_WC_RECV:
                        return 0;
                    default:
                        RDMA_ERROR(
                            recv << " unexpected opcode "
                                 << NVerbs::GetOpcodeName(wc->opcode));
                        Counters->Error();
                        Counters->RecvResponseCompleted();
                        RecvQueue.Push(recv);
                        return -1;
                }

            default:
                RDMA_ERROR(recv << " " << NVerbs::GetStatusString(wc->status));
                Counters->Error();
                Counters->RecvResponseCompleted();
                RecvQueue.Push(recv);
                return -1;
        }
    }

    RDMA_ERROR("unexpected wr_id " << NVerbs::PrintCompletion(wc));
    Counters->Error();
    return -1;
}

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
            RDMA_TRACE(send << " completed");
            SendRequestCompleted(send);
            break;
        }

        case IBV_WC_LOCAL_INV: {
            TSendWr* send = &SendWrs[id.Index];
            RDMA_TRACE(send << " local invalidate completed");
            LocalInvalidationCompleted(send);
            break;
        }

        case IBV_WC_BIND_MW:
            // Auxiliary completions for bind WRs in a SEND chain.
            // Send WR slot is returned by terminal SEND/LOCAL_INV completion.
            break;

        case IBV_WC_RECV: {
            TRecvWr* recv = &RecvWrs[id.Index];
            RDMA_TRACE(recv << " completed");
            RecvResponseCompleted(recv, wc);
            break;
        }

        default:
            RDMA_WARN("unhandled completion " << NVerbs::PrintCompletion(wc));
            break;
    }
}

void TClientEndpoint::SendRequest(TRequestPtr request, TSendWr* send) noexcept
{
    // hand request over to simplify error handling
    auto* req = request.get();
    request->ReqId = ActiveRequests.CreateId();
    ActiveRequests.Push(std::move(request));

    send->context = reinterpret_cast<void*>(static_cast<uintptr_t>(req->ReqId));

    auto* msg = send->Message();
    Zero(*msg);

    InitMessageHeader(msg, NegotiatedProtocolVersion);

    msg->ReqId = req->ReqId;
    msg->Unused = RDMA_REQUEST_FLAG_NONE;
    msg->In = req->Resources.InBuffer;
    msg->Out = req->Resources.OutBuffer;

    SendRequest(req, send);
}

void TClientEndpoint::SendRequest(TRequest* req, TSendWr* send) noexcept
{
    req->State = ERequestState::SendRequest;
    auto* msg = send->Message();
    Counters->SendRequestStarted();

    // We only expect SEND completion with sq_sig_all=0.
    send->wr.send_flags = IBV_SEND_SIGNALED;
    send->wr.next = nullptr;

    ibv_send_wr* head = &send->wr;
    ibv_send_wr* tail = nullptr;
    ibv_send_wr bindIn = {};
    ibv_send_wr bindOut = {};

    if (Config.UseMemoryWindows) {
        if (req->Resources.InMemoryWindow) {
            if (req->CallContext) {
                LWTRACK(
                    BindInBufferStarted,
                    req->CallContext->LWOrbit,
                    req->CallContext->RequestId);
            }

            const ui32 inRKey = GetNextMemoryWindowRKey(
                req->Resources.InMemoryWindow.get());
            msg->In.RKey = inRKey;

            bindIn.wr_id = send->wr.wr_id;
            bindIn.opcode = IBV_WR_BIND_MW;
            bindIn.bind_mw.mw = req->Resources.InMemoryWindow.get();
            bindIn.bind_mw.rkey = inRKey;
            bindIn.bind_mw.bind_info.mr = req->Resources.InBuffer.GetMemoryRegion();
            bindIn.bind_mw.bind_info.addr = req->Resources.InBuffer.Address;
            bindIn.bind_mw.bind_info.length = req->Resources.InBuffer.Length;
            bindIn.bind_mw.bind_info.mw_access_flags = IBV_ACCESS_REMOTE_READ;

            head = &bindIn;
            tail = &bindIn;
        }

        if (req->Resources.OutMemoryWindow) {
            if (req->CallContext) {
                LWTRACK(
                    BindOutBufferStarted,
                    req->CallContext->LWOrbit,
                    req->CallContext->RequestId);
            }

            const ui32 outRKey = GetNextMemoryWindowRKey(
                req->Resources.OutMemoryWindow.get());
            msg->Out.RKey = outRKey;
            ConfigureOutInvalidation(req->Resources, msg, outRKey);

            bindOut.wr_id = send->wr.wr_id;
            bindOut.opcode = IBV_WR_BIND_MW;
            bindOut.bind_mw.mw = req->Resources.OutMemoryWindow.get();
            bindOut.bind_mw.rkey = outRKey;
            bindOut.bind_mw.bind_info.mr = req->Resources.OutBuffer.GetMemoryRegion();
            bindOut.bind_mw.bind_info.addr = req->Resources.OutBuffer.Address;
            bindOut.bind_mw.bind_info.length = req->Resources.OutBuffer.Length;
            bindOut.bind_mw.bind_info.mw_access_flags = IBV_ACCESS_REMOTE_WRITE;

            if (tail) {
                tail->next = &bindOut;
            } else {
                head = &bindOut;
            }
            tail = &bindOut;
        }
    }

    if (tail) {
        // BIND_MW is asynchronous on HCA, SEND must be fenced to ensure
        // memory window bindings become visible before request delivery.
        send->wr.send_flags |= IBV_SEND_FENCE;
        tail->next = &send->wr;
    }

    ibv_send_wr* badWr = nullptr;
    try {
        Verbs->PostSend(Connection->qp, head, &badWr);
        RDMA_TRACE(send << " posted");
    }
    catch (const TServiceError& e) {
        RDMA_ERROR(send << " " << e.what());
        Counters->SendRequestCompleted();
        if (auto failedReq = ActiveRequests.Pop(req->ReqId)) {
            if (badWr == head) {
                // The first WR was rejected, hence nothing from this chain can
                // still reference the request resources.
                SendQueue.Push(send);
                AbortRequest(
                    std::move(failedReq),
                    EAbortCounter::Aborted,
                    MakeError(E_RDMA_UNAVAILABLE, "failed to post send request"));
            } else {
                // ibv_post_send may accept a prefix of a linked list. Keep all
                // resources alive until the QP has been destroyed.
                PartiallyPostedRequests.Enqueue(std::move(failedReq));
                ++QpOwnedSendSlots;
            }
        }
        Counters->Error();
        Disconnect();
        return;
    }

    if (req->CallContext) {
        LWTRACK(
            SendRequestStarted,
            req->CallContext->LWOrbit,
            req->CallContext->RequestId);
    }

}

void TClientEndpoint::ConfigureOutInvalidation(
    TRequestResources& resources,
    TRequestMessage* msg,
    ui32 outRKey) noexcept
{
    if (PeerSupportsSendWithInvalidate) {
        msg->Unused |= RDMA_REQUEST_FLAG_USE_MEMORY_WINDOWS;
        resources.OutMode =
            TRequestResources::EOutMode::RemoteInvalidate;
        resources.RKey = outRKey;
    } else {
        resources.OutMode =
            TRequestResources::EOutMode::LocalInvalidate;
    }
}

bool TClientEndpoint::ValidateRemoteInvalidation(
    TRecvWr* recv,
    ui32 reqId,
    ibv_wc* wc,
    TRequestResources& resources) noexcept
{
    if (resources.OutMode !=
        TRequestResources::EOutMode::RemoteInvalidate)
    {
        return true;
    }

    const bool hasInv = wc && (wc->wc_flags & IBV_WC_WITH_INV);
    if (!hasInv) {
        RDMA_WARN(recv << " missing remote invalidate for request " << reqId);
        resources.RecyclePolicy = TRequestResources::ERecyclePolicy::Drop;
        Counters->Error();
        return false;
    }

    if (wc->invalidated_rkey != resources.RKey) {
        RDMA_WARN(
            recv << " unexpected invalidated rkey for request " << reqId
                 << ", expected " << resources.RKey
                 << ", got " << wc->invalidated_rkey);
        resources.RecyclePolicy = TRequestResources::ERecyclePolicy::Drop;
        Counters->Error();
        return false;
    }

    return true;
}

bool TClientEndpoint::NeedLocalInvalidation(
    const TRequestResources& resources) const noexcept
{
    return Config.UseMemoryWindows &&
        resources.RecyclePolicy == TRequestResources::ERecyclePolicy::Recycle &&
        (resources.InMemoryWindow ||
         (resources.OutMode ==
              TRequestResources::EOutMode::LocalInvalidate &&
          resources.OutMemoryWindow));
}

void TClientEndpoint::AbortRequest(
    TRequestPtr req,
    EAbortCounter counter,
    const NProto::TError& error) noexcept
{
    if (!req) {
        return;
    }

    switch (counter) {
        case EAbortCounter::Dequeued:
            Counters->RequestDequeued();
            break;
        case EAbortCounter::Aborted:
            Counters->RequestAborted();
            break;
    }

    AbortRequest(std::move(req), error);
}

void TClientEndpoint::TryAbortRequest(
    TRequestPtr req,
    NProto::TError error) noexcept
{
    Y_ABORT_UNLESS(req);

    if (Config.UseMemoryWindows &&
        req->State == ERequestState::SendRequest)
    {
        auto& resources = req->Resources;
        if (!HasError(resources.Error)) {
            resources.Error = std::move(error);
        }

        ActiveRequests.Push(std::move(req));
        return;
    }

    AbortRequest(
        std::move(req),
        EAbortCounter::Aborted,
        std::move(error));
}

void TClientEndpoint::AbortRequest(ui32 reqId) noexcept
{
    AbortRequest(reqId, NProto::TError());
}

void TClientEndpoint::AbortRequest(ui32 reqId, NProto::TError error) noexcept
{
    auto* req = ActiveRequests.Get(reqId);
    if (!req) {
        return;
    }
    if (!HasError(req->Resources.Error)) {
        if (!HasError(error)) {
            return;
        }
        req->Resources.Error = std::move(error);
    }

    auto abortedReq = ActiveRequests.Pop(reqId);
    Y_ABORT_UNLESS(abortedReq);

    auto abortError = std::move(abortedReq->Resources.Error);

    AbortRequest(
        std::move(abortedReq),
        EAbortCounter::Aborted,
        abortError);
}

void TClientEndpoint::SendRequestCompleted(TSendWr* send) noexcept
{
    const ui32 reqId =
        SafeCast<ui32>(reinterpret_cast<uintptr_t>(send->context));

    Counters->SendRequestCompleted();
    SendQueue.Push(send);

    if (auto* req = ActiveRequests.Get(reqId)) {
        if (req->Resources.InMemoryWindow) {
            if (req->CallContext) {
                LWTRACK(
                    BindInBufferCompleted,
                    req->CallContext->LWOrbit,
                    req->CallContext->RequestId);
            }
        }
        if (req->Resources.OutMemoryWindow) {
            if (req->CallContext) {
                LWTRACK(
                    BindOutBufferCompleted,
                    req->CallContext->LWOrbit,
                    req->CallContext->RequestId);
            }
        }

        if (req->CallContext) {
            LWTRACK(
                SendRequestCompleted,
                req->CallContext->LWOrbit,
                req->CallContext->RequestId);
        }

        req->State = ERequestState::RecvResponse;
        AbortRequest(reqId);
    }
    // request has already been completed
    HandlePendingInvalidationRequests();
}

void TClientEndpoint::RecvResponse(TRecvWr* recv) noexcept
{
    auto* responseMsg = recv->Message();
    Zero(*responseMsg);

    try {
        Verbs->PostRecv(Connection->qp, &recv->wr);
        RDMA_TRACE(recv << " posted");

    } catch (const TServiceError& e) {
        RDMA_ERROR(recv << " " << e.what());
        Counters->Error();
        RecvQueue.Push(recv);
        Disconnect();
        return;
    }

    Counters->RecvResponseStarted();
}

void TClientEndpoint::RecvResponseCompleted(TRecvWr* recv, ibv_wc* wc) noexcept
{
    auto* msg = recv->Message();

    int version = ParseMessageHeader(msg);
    if (version != NegotiatedProtocolVersion) {
        RDMA_ERROR(
            recv << " incompatible protocol version " << version
                 << ", expected " << NegotiatedProtocolVersion);
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

    auto req = ActiveRequests.Pop(reqId);
    if (!req) {
        RDMA_WARN(
            recv << " request not found, last active request id "
                 << ActiveRequests.GetCurrentId());
        Counters->Error();
        RecvResponse(recv);
        return;
    }

    NProto::TError abortError;
    if (HasError(req->Resources.Error)) {
        abortError = std::move(req->Resources.Error);
        RecvResponse(recv);
        AbortRequest(
            std::move(req),
            EAbortCounter::Aborted,
            abortError);
        return;
    }

    auto& resources = req->Resources;
    ValidateRemoteInvalidation(recv, reqId, wc, resources);

    resources.Status = status;
    resources.ResponseBytes = responseBytes;
    RecvResponse(recv);

    RDMA_TRACE("request " << reqId << " completed");

    const bool needsLocalInvalidate = NeedLocalInvalidation(resources);

    if (needsLocalInvalidate) {
        auto* invalidationRequest = new TInvalidationRequest();
        invalidationRequest->Request = std::move(req);
        PendingInvalidationRequests.PushBack(invalidationRequest);
        HandlePendingInvalidationRequests();
        return;
    }

    FinalizeRequest(std::move(req));
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

void TClientEndpoint::SetConnection(NVerbs::TConnectionPtr connection) noexcept
{
    connection->context = this;
    Connection = std::move(connection);
}

int TClientEndpoint::ReconnectTimerHandle() const
{
    return Reconnect.Handle();
}

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

int TClientEndpoint::DisconnectEventHandle() const
{
    return DisconnectEvent.Handle();
}

void TClientEndpoint::ClearDisconnectEvent() noexcept
{
    DisconnectEvent.Clear();
}

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

bool TClientEndpoint::ClientRequestsFlushed() const
{
    return ActiveRequests.Empty()
        && !InputRequests
        && !QueuedRequests
        && !CancelRequests
        && !PendingInvalidationRequests;
}

bool TClientEndpoint::WorkRequestsFlushed() const
{
    return SendQueue.Size() + QpOwnedSendSlots.load() ==
            Config.SendQueueSize
        && RecvQueue.Size() == Config.RecvQueueSize
        && !PendingInvalidationRequests;
}

bool TClientEndpoint::FlushHanging() const
{
    auto start = FlushStartCycles.load();
    return start &&
           CyclesToDurationSafe(GetCycleCount() - start) >= Config.FlushTimeout;
}

void TClientEndpoint::CleanupRequestResourcesLocked(
    TRequestResources& resources) noexcept
{
    const ui64 currentBufferPoolGeneration = BufferPoolGeneration.load();
    const bool sameBufferPoolGeneration =
        resources.BufferPoolGeneration == currentBufferPoolGeneration;

    if (sameBufferPoolGeneration) {
        if (Config.UseMemoryWindows &&
            resources.RecyclePolicy == TRequestResources::ERecyclePolicy::Recycle &&
            CheckState(EEndpointState::Connected))
        {
            RecycleMemoryWindowLocked(resources.InMemoryWindow);
            RecycleMemoryWindowLocked(resources.OutMemoryWindow);
        } else {
            resources.InMemoryWindow = NVerbs::NullPtr;
            resources.OutMemoryWindow = NVerbs::NullPtr;
        }

        SendBuffers.ReleaseBuffer(resources.InBuffer);
        RecvBuffers.ReleaseBuffer(resources.OutBuffer);
        return;
    }

    // Late cleanup after reconnect: buffers belong to an older pool generation
    // and cannot be released through the current pool.
    resources.InMemoryWindow = NVerbs::NullPtr;
    resources.OutMemoryWindow = NVerbs::NullPtr;
    resources.InBuffer = {};
    resources.OutBuffer = {};
}

void TClientEndpoint::FinalizeRequest(TRequestPtr req) noexcept
{
    Y_ABORT_UNLESS(req);

    const ui32 status = req->Resources.Status;
    const ui32 responseBytes = req->Resources.ResponseBytes;

    Counters->RequestCompleted();

    if (req->CallContext) {
        LWTRACK(
            RecvResponseCompleted,
            req->CallContext->LWOrbit,
            req->CallContext->RequestId);
    }

    auto* handler = req->Handler.get();
    handler->HandleResponse(
        std::move(req),
        status,
        responseBytes);
}

void TClientEndpoint::CompleteInvalidationRequest(
    TInvalidationRequest* req) noexcept
{
    Y_ABORT_UNLESS(req);
    Y_ABORT_UNLESS(req->Request);
    req->Request->Resources.RecyclePolicy =
        TRequestResources::ERecyclePolicy::Drop;
    FinalizeRequest(std::move(req->Request));
}

void TClientEndpoint::MarkInvalidationAsQpOwned(
    TInvalidationRequest* req) noexcept
{
    Y_ABORT_UNLESS(req);
    if (!req->QpOwnedSendSlot) {
        req->QpOwnedSendSlot = true;
        ++QpOwnedSendSlots;
    }
}

void TClientEndpoint::DrainInvalidationRequests() noexcept
{
    while (PendingInvalidationRequests) {
        auto* req = PendingInvalidationRequests.PopFront();
        Y_ABORT_UNLESS(req);
        CompleteInvalidationRequest(req);
        delete req;
    }

    for (auto& req: PostedInvalidationRequests) {
        MarkInvalidationAsQpOwned(&req);
    }
}

void TClientEndpoint::PostNextPendingInvalidation(TSendWr* send) noexcept
{
    Y_ABORT_UNLESS(send);
    auto* req = PendingInvalidationRequests.PopFront();
    Y_ABORT_UNLESS(req);
    PostLocalInvalidation(req, send);
}

void TClientEndpoint::PostLocalInvalidation(
    TInvalidationRequest* req,
    TSendWr* send) noexcept
{
    Y_ABORT_UNLESS(req);
    Y_ABORT_UNLESS(req->Request);
    auto& resources = req->Request->Resources;
    const bool needLocalInvalidateOut =
        resources.OutMode ==
        TRequestResources::EOutMode::LocalInvalidate;

    ibv_send_wr* head = nullptr;
    ibv_send_wr* tail = nullptr;
    ibv_send_wr invIn = {};
    ibv_send_wr invOut = {};

    if (resources.InMemoryWindow) {
        invIn.wr_id = send->wr.wr_id;
        invIn.opcode = IBV_WR_LOCAL_INV;
        invIn.invalidate_rkey = resources.InMemoryWindow->rkey;

        head = &invIn;
        tail = &invIn;
    }

    if (needLocalInvalidateOut &&
        resources.OutMemoryWindow)
    {
        invOut.wr_id = send->wr.wr_id;
        invOut.opcode = IBV_WR_LOCAL_INV;
        invOut.invalidate_rkey = resources.OutMemoryWindow->rkey;

        if (tail) {
            tail->next = &invOut;
        } else {
            head = &invOut;
        }
        tail = &invOut;
    }

    Y_ABORT_UNLESS(head && tail);
    PostedInvalidationRequests.PushBack(req);
    send->context = req;
    tail->send_flags |= IBV_SEND_SIGNALED;

    ibv_send_wr* badWr = nullptr;
    try {
        Verbs->PostSend(Connection->qp, head, &badWr);
        RDMA_TRACE(send << " local invalidate posted");
    } catch (const TServiceError& e) {
        RDMA_ERROR(send << " " << e.what());
        send->context = nullptr;
        Counters->Error();
        PostedInvalidationRequests.Remove(req);
        if (badWr == head) {
            SendQueue.Push(send);
            CompleteInvalidationRequest(req);
            delete req;
        } else {
            // At least one invalidation was accepted. Its MW must remain alive
            // until QP destruction even though no terminal WR was posted.
            PostedInvalidationRequests.PushBack(req);
            MarkInvalidationAsQpOwned(req);
        }
        Disconnect();
    }
}

void TClientEndpoint::HandlePendingInvalidationRequests() noexcept
{
    while (PendingInvalidationRequests && CheckState(EEndpointState::Connected)) {
        auto* send = SendQueue.Pop();
        if (!send) {
            return;
        }

        PostNextPendingInvalidation(send);
    }
}

void TClientEndpoint::LocalInvalidationCompleted(TSendWr* send) noexcept
{
    auto* rawReq = static_cast<TInvalidationRequest*>(send->context);
    if (!rawReq) {
        RDMA_TRACE(send << " duplicate local invalidation completion");
        return;
    }

    send->context = nullptr;
    SendQueue.Push(send);

    PostedInvalidationRequests.Remove(rawReq);
    if (rawReq->QpOwnedSendSlot) {
        Y_ABORT_UNLESS(QpOwnedSendSlots.load() > 0);
        --QpOwnedSendSlots;
    }
    FinalizeRequest(std::move(rawReq->Request));
    delete rawReq;
    HandlePendingInvalidationRequests();
}

void TClientEndpoint::FreeRequest(TRequest* req) noexcept
{
    with_lock (AllocationLock) {
        CleanupRequestResourcesLocked(req->Resources);
    }
}

NVerbs::TMemoryWindowPtr TClientEndpoint::AcquireMemoryWindowLocked()
{
    if (!FreeMemoryWindows.empty()) {
        auto memoryWindow = std::move(FreeMemoryWindows.back());
        FreeMemoryWindows.pop_back();
        return memoryWindow;
    }

    // Slow path for request spikes over the preallocated pool size.
    return Verbs->CreateMemoryWindow(Connection->pd);
}

void TClientEndpoint::RecycleMemoryWindowLocked(
    NVerbs::TMemoryWindowPtr& memoryWindow)
{
    if (!memoryWindow) {
        return;
    }

    if (FreeMemoryWindows.size() < MaxFreeMemoryWindows) {
        FreeMemoryWindows.push_back(std::move(memoryWindow));
    } else {
        memoryWindow = NVerbs::NullPtr;
    }
}

ui32 TClientEndpoint::GetNextMemoryWindowRKey(ibv_mw* memoryWindow)
{
    Y_ABORT_UNLESS(memoryWindow);

    const ui32 current = memoryWindow->rkey;
    const ui32 next = (current & 0xFFFFFF00u) | ((current + 1u) & 0x000000FFu);
    memoryWindow->rkey = next;
    return next;
}

ui64 TClientEndpoint::GetNewReqId() noexcept
{
    return ReqIdPool.fetch_add(1);
}

void TClientEndpoint::SetNegotiatedProtocolVersion(
    int negotiatedProtocolVersion)
{
    NegotiatedProtocolVersion = negotiatedProtocolVersion;
}

int TClientEndpoint::GetNegotiatedProtocolVersion() const
{
    return NegotiatedProtocolVersion;
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
        PollHandle.Detach(endpoint->DisconnectEventHandle());
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
            if (endpoint->CheckState(EEndpointState::Connected)) {
                hasWork |= endpoint->HandleCancelRequests();
                hasWork |= endpoint->HandleInputRequests();
                hasWork |= endpoint->HandleCompletionEvents();
            }
            if (endpoint->CheckState(EEndpointState::Disconnecting)) {
                hasWork |= endpoint->HandleCancelRequests();
                hasWork |= endpoint->HandleCompletionEvents();
                endpoint->AbortRequests();
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
                const ui32 reqId = request->ReqId;
                const bool alreadyPendingAbort =
                    HasError(request->Resources.Error);
                if (!alreadyPendingAbort) {
                    RDMA_DEBUG(endpoint->Log, "request " << reqId << " timed out");
                }
                TString timeoutMessage = TStringBuilder()
                    << "request " << reqId << " timed out "
                    << "[peer=" << endpoint->Host << ":"
                    << endpoint->Port << "]";

                endpoint->TryAbortRequest(
                    std::move(request),
                    MakeError(E_TIMEOUT, std::move(timeoutMessage)));
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

            if (!endpoint->ClientRequestsFlushed()) {
                continue;
            }

            if (!endpoint->WorkRequestsFlushed()) {
                if (!endpoint->FlushHanging()) {
                    continue;
                }
                // either we have a bug or underlying layer didn't flush WRs in time
                RDMA_ERROR(endpoint->Log, "flush timeout "
                    << "[send_queue.size=" << endpoint->SendQueue.Size()
                    << " recv_queue.size=" << endpoint->RecvQueue.Size() << "]");
            }

            // detach immediately to prevent completions from arriving during
            // destruction sequence
            Detach(endpoint.get());

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

    TObservabilityProvider ObservabilityProvider;

    TClientConfigPtr Config;
    TEndpointCountersPtr Counters;
    TLog Log;

    TConnectionPollerPtr ConnectionPoller;
    TVector<TCompletionPollerPtr> CompletionPollers;

public:
    TClient(
        NVerbs::IVerbsPtr verbs,
        TObservabilityProvider observabilityProvider,
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
    void ReleaseResources(TClientEndpoint* endpoint) noexcept;
};

////////////////////////////////////////////////////////////////////////////////

TClient::TClient(
        NVerbs::IVerbsPtr verbs,
        TObservabilityProvider observabilityProvider,
        TClientConfigPtr config)
    : Verbs(std::move(verbs))
    , ObservabilityProvider(std::move(observabilityProvider))
    , Config(std::move(config))
    , Counters(new TEndpointCounters())
{
    // check basic functionality for early problem detection
    Verbs->GetDeviceList();
    Verbs->GetAddressInfo("localhost", 10020, nullptr);
}

void TClient::Start() noexcept
{
    Log = ObservabilityProvider.CreateLog();

    RDMA_INFO("start client");

    auto countersGroup = ObservabilityProvider.CreateCounters();
    Counters->Register(*countersGroup);

    CompletionPollers.resize(Config->PollerThreads);
    for (size_t i = 0; i < CompletionPollers.size(); ++i) {
        CompletionPollers[i] = std::make_unique<TCompletionPoller>(
            Verbs,
            Config,
            Log);
        CompletionPollers[i]->Start();
    }

    Config->Validate(Log);

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

void TClient::ReleaseResources(TClientEndpoint* endpoint) noexcept
{
    switch (endpoint->State) {
        // reconnect timer hit before disconnect event, reschedule
        case EEndpointState::Connected:
            endpoint->Reconnect.Schedule();
            return;

        // wait for completion poller to flush WRs
        case EEndpointState::Disconnecting:
            endpoint->Reconnect.Schedule();
            return;

        // QP hasn't been created yet
        case EEndpointState::ResolvingAddress:
        case EEndpointState::ResolvingRoute:
            break;

        // only Connected endpoints would be detached during flush
        case EEndpointState::Connecting:
            endpoint->Poller->Detach(endpoint);
            endpoint->DestroyQP();
            break;

        // endpoint has been detached by the poller
        case EEndpointState::Disconnected:
            endpoint->DestroyQP();
            break;
    }

    RDMA_INFO(endpoint->Log, "release resources");
    ConnectionPoller->Detach(endpoint);
    endpoint->Connection.reset();
    endpoint->StopResult.SetValue();
    endpoint->Poller->Release(endpoint);
}

// implements IConnectionEventHandler
void TClient::Reconnect(TClientEndpoint* endpoint) noexcept
{
    if (endpoint->ShouldStop()) {
        ReleaseResources(endpoint);
        return;
    }

    if (endpoint->Reconnect.Hanging()) {
        // if this is our first connection, fail over to IC
        if (endpoint->StartResult.Initialized()) {
            RDMA_ERROR(endpoint->Log, "connection timeout");

            auto startResult = std::move(endpoint->StartResult);
            startResult.SetException(std::make_exception_ptr(TServiceError(
                MakeError(E_RDMA_UNAVAILABLE, "connection timeout"))));

            ReleaseResources(endpoint);
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
            endpoint->Poller->Detach(endpoint);
            endpoint->ChangeState(
                EEndpointState::Connecting,
                EEndpointState::Disconnected);
            // fallthrough

        case EEndpointState::Disconnected:
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
            addrinfo->ai_dst_addr, Config->ResolveTimeout);

    } catch (const TServiceError& e) {
        RDMA_ERROR(endpoint->Log, e.what());
        Counters->Error();
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
        Verbs->ResolveRoute(endpoint->Connection.get(), Config->ResolveTimeout);

    } catch (const TServiceError& e) {
        RDMA_ERROR(endpoint->Log, e.what());
        Counters->Error();
        endpoint->Disconnect();
    }
}

void TClient::BeginConnect(TClientEndpoint* endpoint) noexcept
{
    Y_ABORT_UNLESS(endpoint);

    try {
        RDMA_INFO(endpoint->Log, "connect to " << endpoint->Host);
        endpoint->PeerSupportsSendWithInvalidate = false;

        endpoint->ChangeState(
            EEndpointState::ResolvingRoute,
            EEndpointState::Connecting);

        endpoint->CreateQP();
        endpoint->Poller->Attach(endpoint);
        endpoint->Reconnect.Schedule(MIN_CONNECT_TIMEOUT);

        TConnectMessage message = {
            .SendQueueSize = SafeCast<ui16>(endpoint->Config.SendQueueSize),
            .RecvQueueSize = SafeCast<ui16>(endpoint->Config.RecvQueueSize),
            .MaxBufferSize = SafeCast<ui32>(endpoint->Config.MaxBufferSize),
        };
        InitMessageHeader(&message, endpoint->GetNegotiatedProtocolVersion());

        rdma_conn_param param = {
            .private_data = &message,
            .private_data_len = sizeof(TConnectMessage),
            .responder_resources = RDMA_MAX_RESP_RES,
            .initiator_depth = RDMA_MAX_INIT_DEPTH,
            .flow_control = 1,
            .retry_count = Config->QpRetryCount,
            .rnr_retry_count = Config->QpRnrRetryCount,
        };

        Verbs->Connect(endpoint->Connection.get(), &param);

    } catch (const TServiceError& e) {
        RDMA_ERROR(endpoint->Log, e.what());
        Counters->Error();
        endpoint->Disconnect();
    }
}

void TClient::HandleConnected(
    TClientEndpoint* endpoint,
    NVerbs::TConnectionEventPtr event) noexcept
{
    const rdma_conn_param* param = &event->param.conn;

    RDMA_DEBUG(endpoint->Log, "validate accept message");

    if (param->private_data == nullptr ||
        param->private_data_len < sizeof(TAcceptMessage))
    {
        RDMA_ERROR(endpoint->Log, "unable to parse accept message");
        endpoint->Disconnect();
        return;
    }

    const int version = ParseMessageHeader(param->private_data);
    if (version < RDMA_PROTO_PREV_VERSION || version > RDMA_PROTO_VERSION)
    {
        RDMA_ERROR(
            endpoint->Log,
            "unsupported message version: " << version);
        endpoint->Disconnect();
        return;
    }

    const auto* acceptMsg =
        static_cast<const TAcceptMessage*>(param->private_data);
    endpoint->PeerSupportsSendWithInvalidate =
        (acceptMsg->Unused & RDMA_ACCEPT_FLAG_SEND_WITH_INV) != 0;
    if (endpoint->PeerSupportsSendWithInvalidate) {
        RDMA_INFO(endpoint->Log, "send with invalidate enabled");
    }

    endpoint->SetNegotiatedProtocolVersion(version);
    endpoint->ChangeState(
        EEndpointState::Connecting,
        EEndpointState::Connected);

    endpoint->Reconnect.Cancel();
    try {
        endpoint->SetupQP();
    } catch (const TServiceError& e) {
        RDMA_ERROR(endpoint->Log, e.what());
        Counters->Error();
        endpoint->Disconnect();
        return;
    }
    endpoint->StartReceive();

    RDMA_INFO(endpoint->Log, "connected");

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

    RDMA_DEBUG(endpoint->Log, "validate reject message");

    if (param->private_data == nullptr ||
        param->private_data_len < sizeof(TRejectMessage))
    {
        RDMA_ERROR(endpoint->Log, "unable to parse reject message");
        endpoint->Disconnect();
        return;
    }

    const int version = ParseMessageHeader(param->private_data);
    switch (version) {
        case RDMA_PROTO_PREV_VERSION: {
            const auto* msg =
                static_cast<const TRejectMessage*>(param->private_data);
            // NOTE: Previous version of the server can't reply with
            // "RDMA_PROTO_CONFIG_MISMATCH", since "StrictValidation" couldn't
            // be enabled before.
            if (msg->Status == RDMA_PROTO_INVALID_REQUEST &&
                endpoint->GetNegotiatedProtocolVersion() !=
                    RDMA_PROTO_PREV_VERSION)
            {
                RDMA_WARN(
                    endpoint->Log,
                    "connection rejected, retry connect with previous protocol "
                    "version");
                endpoint->SetNegotiatedProtocolVersion(RDMA_PROTO_PREV_VERSION);
            }
            break;
        }
        case RDMA_PROTO_VERSION: {
            const auto* msg =
                static_cast<const TRejectMessage2*>(param->private_data);
            if (msg->Status == RDMA_PROTO_CONFIG_MISMATCH) {
                bool changed = false;
                if (endpoint->Config.SendQueueSize > msg->RecvQueueSize) {
                    endpoint->Config.SendQueueSize =
                        std::max(1, msg->RecvQueueSize / 2);
                    changed = true;

                    RDMA_WARN(
                        endpoint->Log,
                        "set SendQueueSize=" << endpoint->Config.SendQueueSize
                                             << " supported by "
                                             << endpoint->Host);
                }
                if (msg->SendQueueSize > endpoint->Config.RecvQueueSize) {
                    endpoint->Config.RecvQueueSize = std::min<ui32>(
                        std::numeric_limits<ui16>::max(),
                        msg->SendQueueSize * 2);
                    changed = true;

                    RDMA_WARN(
                        endpoint->Log,
                        "set RecvQueueSize=" << endpoint->Config.RecvQueueSize
                                             << " supported by "
                                             << endpoint->Host);
                }
                if (endpoint->Config.MaxBufferSize > msg->MaxBufferSize) {
                    endpoint->Config.MaxBufferSize = msg->MaxBufferSize;
                    changed = true;

                    RDMA_WARN(
                        endpoint->Log,
                        "set MaxBufferSize=" << endpoint->Config.MaxBufferSize
                                             << " supported by "
                                             << endpoint->Host);
                }

                if (changed) {
                    endpoint->TryForceReconnect();
                    return;
                }
            }
            break;
        }
        default:
            RDMA_ERROR(
                endpoint->Log,
                "unknown protocol version in reject message: " << version);
            break;
    }

    endpoint->Disconnect();
}

void TClient::HandleDisconnected(TClientEndpoint* endpoint) noexcept
{
    // we can't reset config right away, because we need to know queue size to
    // clean up flushed WRs
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

inline IOutputStream& operator<<(IOutputStream& out, TSendWr* send)
{
    out << "SEND " << TWorkRequestId(send->wr.wr_id);
    if (auto msg = send->Message()) {
        if (auto ver = ParseMessageHeader(msg);
            ver == RDMA_PROTO_VERSION || ver == RDMA_PROTO_PREV_VERSION)
        {
            out << " [request=" << msg->ReqId << "]";
        }
    }
    return out;
}

inline IOutputStream& operator<<(IOutputStream& out, TRecvWr* recv)
{
    out << "RECV " << TWorkRequestId(recv->wr.wr_id);
    if (auto msg = recv->Message()) {
        if (auto ver = ParseMessageHeader(msg);
            ver == RDMA_PROTO_VERSION || ver == RDMA_PROTO_PREV_VERSION)
        {
            out << " [request=" << msg->ReqId << "]";
        }
    }
    return out;
}

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(
    NVerbs::IVerbsPtr verbs,
    TObservabilityProvider observabilityProvider,
    TClientConfigPtr config)
{
    return std::make_shared<TClient>(
        std::move(verbs),
        std::move(observabilityProvider),
        std::move(config));
}

}   // namespace NCloud::NStorage::NRdma
