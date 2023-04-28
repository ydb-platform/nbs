#include "server.h"

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

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>
#include <util/random/random.h>
#include <util/system/mutex.h>
#include <util/system/spin_wait.h>
#include <util/system/thread.h>

namespace NCloud::NBlockStore::NRdma {

using namespace NMonitoring;

LWTRACE_USING(BLOCKSTORE_RDMA_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration LOG_THROTTLER_PERIOD = TDuration::Minutes(20);

constexpr TDuration POLL_TIMEOUT = TDuration::Seconds(1);

constexpr ui32 WAIT_TIMEOUT = 10;   // us

////////////////////////////////////////////////////////////////////////////////

struct TRequest;
using TRequestPtr = std::unique_ptr<TRequest>;

struct TEndpointCounters;
using TEndpointCountersPtr = std::shared_ptr<TEndpointCounters>;

class TClientEndpoint;
using TClientEndpointPtr = std::shared_ptr<TClientEndpoint>;

class TServerEndpoint;
using TServerEndpointPtr = std::shared_ptr<TServerEndpoint>;

class TConnectionPoller;
using TConnectionPollerPtr = std::unique_ptr<TConnectionPoller>;

class TCompletionPoller;
using TCompletionPollerPtr = std::unique_ptr<TCompletionPoller>;

////////////////////////////////////////////////////////////////////////////////

constexpr int EVENT_MASK = 3;   // low bits unused because of alignment

void* EndpointEventTag(TClientEndpoint* endpoint, int event)
{
    auto tag = reinterpret_cast<uintptr_t>(endpoint) | (event & EVENT_MASK);
    return reinterpret_cast<void*>(tag);
}

TClientEndpoint* EndpointFromTag(void* tag)
{
    auto ptr = reinterpret_cast<uintptr_t>(tag) & ~EVENT_MASK;
    return reinterpret_cast<TClientEndpoint*>(ptr);
}

int EventFromTag(void* tag)
{
    return reinterpret_cast<uintptr_t>(tag) & EVENT_MASK;
}

////////////////////////////////////////////////////////////////////////////////

NRdma::EWaitMode Convert(NProto::EWaitMode mode)
{
    switch (mode) {
        case NProto::WAIT_MODE_POLL:
            return NRdma::EWaitMode::Poll;

        case NProto::WAIT_MODE_BUSY_WAIT:
            return NRdma::EWaitMode::BusyWait;

        case NProto::WAIT_MODE_ADAPTIVE_WAIT:
            return NRdma::EWaitMode::AdaptiveWait;;

        default:
            Y_FAIL("unsupported wait mode %d", mode);
    }
}

////////////////////////////////////////////////////////////////////////////////

enum class ERequestState
{
    RecvRequest,
    ReadRequestData,
    ExecuteRequest,
    WriteResponseData,
    SendResponse,
};

////////////////////////////////////////////////////////////////////////////////

struct TRequest : TListNode<TRequest>
{
    TClientEndpoint* Endpoint;

    ERequestState State = ERequestState::RecvRequest;

    TCallContextPtr CallContext;
    ui32 ReqId = 0;
    ui32 Status = 0;
    ui32 ResponseBytes = 0;

    TBufferDesc In {};
    TBufferDesc Out {};

    TPooledBuffer InBuffer {};
    TPooledBuffer OutBuffer {};

    TRequest(TClientEndpoint* endpoint)
        : Endpoint(endpoint)
    {}
};

////////////////////////////////////////////////////////////////////////////////

void StoreRequest(TSendWr* send, TRequestPtr req)
{
    Y_VERIFY(send->context == nullptr);
    send->context = req.release();  // ownership transferred
}

TRequestPtr ExtractRequest(TSendWr* send)
{
    TRequestPtr req(static_cast<TRequest*>(send->context));
    send->context = nullptr;
    return req;
}

////////////////////////////////////////////////////////////////////////////////

enum class EEndpointState
{
    Disconnected,
    Connecting,
    Connected,
    Error,
};

////////////////////////////////////////////////////////////////////////////////

const char* GetEndpointStateName(EEndpointState state)
{
    static const char* names[] = {
        "Disconnected",
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
        case RDMA_CM_EVENT_CONNECT_ERROR:
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
    TDynamicCounters::TCounterPtr CompletedRequests;
    TDynamicCounters::TCounterPtr ThrottledRequests;

    TDynamicCounters::TCounterPtr ActiveSend;
    TDynamicCounters::TCounterPtr ActiveRecv;
    TDynamicCounters::TCounterPtr ActiveRead;
    TDynamicCounters::TCounterPtr ActiveWrite;

    TDynamicCounters::TCounterPtr SendErrors;
    TDynamicCounters::TCounterPtr RecvErrors;
    TDynamicCounters::TCounterPtr ReadErrors;
    TDynamicCounters::TCounterPtr WriteErrors;

    void Register(TDynamicCounters& counters)
    {
        QueuedRequests = counters.GetCounter("QueuedRequests");
        ActiveRequests = counters.GetCounter("ActiveRequests");
        CompletedRequests = counters.GetCounter("CompletedRequests", true);
        ThrottledRequests = counters.GetCounter("ThrottledRequests", true);

        ActiveSend = counters.GetCounter("ActiveSend");
        ActiveRecv = counters.GetCounter("ActiveRecv");
        ActiveRead = counters.GetCounter("ActiveRead");
        ActiveWrite = counters.GetCounter("ActiveWrite");

        SendErrors = counters.GetCounter("SendErrors");
        RecvErrors = counters.GetCounter("RecvErrors");
        ReadErrors = counters.GetCounter("ReadErrors");
        WriteErrors = counters.GetCounter("WriteErrors");
    }

    void RequestEnqueued()
    {
        QueuedRequests->Inc();
    }

    void RequestDequeued()
    {
        QueuedRequests->Dec();
    }

    void RecvRequestStarted()
    {
        ActiveRecv->Inc();
    }

    void RecvRequestCompleted()
    {
        ActiveRecv->Dec();
        ActiveRequests->Inc();
    }

    void ReadRequestStarted()
    {
        ActiveRead->Inc();
    }

    void ReadRequestCompleted()
    {
        ActiveRead->Dec();
    }

    void WriteResponseStarted()
    {
        ActiveWrite->Inc();
    }

    void WriteResponseCompleted()
    {
        ActiveWrite->Dec();
    }

    void SendResponseStarted()
    {
        ActiveSend->Inc();
    }

    void SendResponseCompleted()
    {
        ActiveSend->Dec();
        ActiveRequests->Dec();
        CompletedRequests->Inc();
    }

    void RecvRequestError()
    {
        ActiveRecv->Dec();
        RecvErrors->Inc();
    }

    void ReadRequestError()
    {
        ActiveRead->Dec();
        ActiveRequests->Dec();
        ReadErrors->Inc();
    }

    void WriteResponseError()
    {
        ActiveWrite->Dec();
        ActiveRequests->Dec();
        WriteErrors->Inc();
    }

    void SendResponseError()
    {
        ActiveSend->Dec();
        ActiveRequests->Dec();
        SendErrors->Inc();
    }

    void RequestThrottled()
    {
        ActiveSend->Dec();
        ActiveRequests->Dec();
        ThrottledRequests->Inc();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TClientEndpoint final
    : public NVerbs::ICompletionHandler
{
    // TODO
    friend class TServer;
    friend class TCompletionPoller;

private:
    NVerbs::IVerbsPtr Verbs;

    NVerbs::TConnectionPtr Connection;
    IServerHandlerPtr Handler;
    TServerConfigPtr Config;
    TEndpointCountersPtr Counters;
    TLog Log;
    TLogThrottler LogThrottler = TLogThrottler(LOG_THROTTLER_PERIOD);

    TCompletionPoller* Poller = nullptr;
    TAtomic State = intptr_t(EEndpointState::Disconnected);

    NVerbs::TCompletionChannelPtr CompletionChannel = NVerbs::NullPtr;
    NVerbs::TCompletionQueuePtr CompletionQueue = NVerbs::NullPtr;

    TBufferPool SendBuffers;
    TBufferPool RecvBuffers;

    TPooledBuffer SendBuffer {};
    TPooledBuffer RecvBuffer {};

    TVector<TSendWr> SendWrs;
    TVector<TRecvWr> RecvWrs;

    TWorkQueue<TSendWr> SendQueue;
    TWorkQueue<TRecvWr> RecvQueue;

    TLockFreeList<TRequest> InputRequests;
    TEventHandle RequestEvent;

    TSimpleList<TRequest> QueuedRequests;

    size_t MaxInflightBytes;

public:
    static TClientEndpoint* FromEvent(rdma_cm_event* event)
    {
        Y_VERIFY(event->id && event->id->context);
        return static_cast<TClientEndpoint*>(event->id->context);
    }

    TClientEndpoint(
        NVerbs::IVerbsPtr Verbs,
        NVerbs::TConnectionPtr connection,
        IServerHandlerPtr handler,
        TServerConfigPtr config,
        TEndpointCountersPtr stats,
        TLog log);
    ~TClientEndpoint();

    bool CheckState(EEndpointState expectedState) const;
    void ChangeState(EEndpointState expectedState, EEndpointState newState);

    // called in context of CM poller thread
    void InitCompletionQueue(ui32 queueSize);
    void StartReceive();

    // called in context of client thread
    void EnqueueRequest(TRequestPtr req);

    // called in context of CQ poller thread
    bool HandleInputRequests();
    bool HandleCompletionEvents();

private:
    void HandleQueuedRequests();
    void HandleCompletionEvent(const NVerbs::TCompletion& wc) override;

    void RecvRequest(TRecvWr* recv);
    void RecvRequestCompleted(TRecvWr* recv, ibv_wc_status status);
    void RecvRequestError(TRecvWr* recv, ibv_wc_status status);

    void ReadRequestData(TRequestPtr req, TSendWr* send);
    void ReadRequestDataCompleted(TSendWr* send, ibv_wc_status status);
    void ReadRequestDataError(TRequestPtr req, TSendWr* send,
        ibv_wc_status status);

    void ExecuteRequest(TRequestPtr req);

    void WriteResponseData(TRequestPtr req, TSendWr* send);
    void WriteResponseDataCompleted(TSendWr* send, ibv_wc_status status);
    void WriteResponseDataError(TRequestPtr req, TSendWr* send,
        ibv_wc_status status);

    void SendResponse(TRequestPtr req, TSendWr* send);
    void SendResponseCompleted(TSendWr* send, ibv_wc_status status);
    void SendResponseError(TRequestPtr req, TSendWr* send,
        ibv_wc_status status);

    void RejectRequest(TRequestPtr req, ui32 status, TStringBuf message);
};

////////////////////////////////////////////////////////////////////////////////

TClientEndpoint::TClientEndpoint(
        NVerbs::IVerbsPtr verbs,
        NVerbs::TConnectionPtr connection,
        IServerHandlerPtr handler,
        TServerConfigPtr config,
        TEndpointCountersPtr stats,
        TLog log)
    : Verbs(std::move(verbs))
    , Connection(std::move(connection))
    , Handler(std::move(handler))
    , Config(std::move(config))
    , Counters(std::move(stats))
    , Log(log)
{
    Connection->context = this;
    MaxInflightBytes = Config->MaxInflightBytes;
}

TClientEndpoint::~TClientEndpoint()
{
    SendBuffers.ReleaseBuffer(SendBuffer);
    RecvBuffers.ReleaseBuffer(RecvBuffer);

    // TODO detach pollers
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

void TClientEndpoint::InitCompletionQueue(ui32 queueSize)
{
    CompletionChannel = Verbs->CreateCompletionChannel(Connection->verbs);
    SetNonBlock(CompletionChannel->fd, true);

    ibv_cq_init_attr_ex cq_attrs = {
        .cqe = 2*queueSize,     // send + recv
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
            .max_send_wr = queueSize,
            .max_recv_wr = queueSize,
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

    SendBuffer = SendBuffers.AcquireBuffer(queueSize * sizeof(TResponseMessage), true);
    RecvBuffer = RecvBuffers.AcquireBuffer(queueSize * sizeof(TRequestMessage), true);

    SendWrs.resize(queueSize);
    RecvWrs.resize(queueSize);

    ui64 responseMsg = SendBuffer.Address;
    for (auto& wr: SendWrs) {
        wr.wr.opcode = IBV_WR_SEND;

        wr.wr.wr_id = reinterpret_cast<uintptr_t>(&wr);
        wr.wr.sg_list = wr.sg_list;
        wr.wr.num_sge = 1;

        wr.sg_list[0].lkey = SendBuffer.Key;
        wr.sg_list[0].addr = responseMsg;
        wr.sg_list[0].length = sizeof(TResponseMessage);

        SendQueue.Push(&wr);
        responseMsg += sizeof(TResponseMessage);
    }

    ui64 requestMsg = RecvBuffer.Address;
    for (auto& wr: RecvWrs) {
        wr.wr.wr_id = reinterpret_cast<uintptr_t>(&wr);
        wr.wr.sg_list = wr.sg_list;
        wr.wr.num_sge = 1;

        wr.sg_list[0].lkey = RecvBuffer.Key;
        wr.sg_list[0].addr = requestMsg;
        wr.sg_list[0].length = sizeof(TRequestMessage);

        RecvQueue.Push(&wr);
        requestMsg += sizeof(TRequestMessage);
    }
}

void TClientEndpoint::StartReceive()
{
    while (auto* recv = RecvQueue.Pop()) {
        RecvRequest(recv);
    }
}

void TClientEndpoint::EnqueueRequest(TRequestPtr req)
{
    Counters->RequestEnqueued();
    InputRequests.Enqueue(std::move(req));

    if (Config->WaitMode == EWaitMode::Poll) {
        RequestEvent.Set();
    }
}

bool TClientEndpoint::HandleInputRequests()
{
    if (Config->WaitMode == EWaitMode::Poll) {
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

        switch (req->State) {
            case ERequestState::RecvRequest:
                Y_VERIFY(req->In.Length);
                ReadRequestData(std::move(req), send);
                break;

            case ERequestState::ExecuteRequest:
                if (req->ResponseBytes) {
                    WriteResponseData(std::move(req), send);
                } else {
                    SendResponse(std::move(req), send);
                }
                break;

            default:
                Y_FAIL("unexpected state: %d", (int)req->State);
        }
    }
}

bool TClientEndpoint::HandleCompletionEvents()
{
    ibv_cq* cq = CompletionQueue.get();

    if (Config->WaitMode == EWaitMode::Poll) {
        Verbs->GetCompletionEvent(cq);
        Verbs->RequestCompletionEvent(cq, 0);
    }

    int rc = Verbs->PollCompletionQueue((ibv_cq_ex*)cq, this);

    if (rc != 0 && rc != ENOENT) {
        char buf[64];
        STORAGE_ERROR("poll error: " << strerror_r(rc, buf, sizeof(buf)));
    }

    if (Config->WaitMode == EWaitMode::Poll) {
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
        case IBV_WC_RECV:
            RecvRequestCompleted(
                reinterpret_cast<TRecvWr*>(wc.wr_id),
                wc.status);
            break;

        case IBV_WC_RDMA_READ:
            ReadRequestDataCompleted(
                reinterpret_cast<TSendWr*>(wc.wr_id),
                wc.status);
            break;

        case IBV_WC_RDMA_WRITE:
            WriteResponseDataCompleted(
                reinterpret_cast<TSendWr*>(wc.wr_id),
                wc.status);
            break;

        case IBV_WC_SEND:
            SendResponseCompleted(
                reinterpret_cast<TSendWr*>(wc.wr_id),
                wc.status);
            break;

        default:
            Y_FAIL("unexpected opcode: %d", wc.opcode);
    }
}

void TClientEndpoint::RecvRequest(TRecvWr* recv)
{
    auto* requestMsg = recv->Message<TRequestMessage>();
    Zero(*requestMsg);

    STORAGE_TRACE("RECV #" << recv->wr.wr_id);
    Verbs->PostRecv(Connection->qp, &recv->wr);

    Counters->RecvRequestStarted();
}

void TClientEndpoint::RecvRequestCompleted(TRecvWr* recv, ibv_wc_status status)
{
    if (status != IBV_WC_SUCCESS) {
        return RecvRequestError(recv, status);
    }

    const auto* msg = recv->Message<TRequestMessage>();
    const int version = ParseMessageHeader(msg);

    if (version != RDMA_PROTO_VERSION) {
        STORAGE_ERROR("RECV #" << recv->wr.wr_id
            << ": incompatible protocol version "
            << version << " != "<< int(RDMA_PROTO_VERSION));

        // should always be posted
        RecvRequest(recv);

        Counters->RecvRequestError();
        return;
    }

    auto req = std::make_unique<TRequest>(this);

    // TODO restore context from request meta
    req->CallContext = MakeIntrusive<TCallContext>();

    req->ReqId = msg->ReqId;
    req->In = msg->In;
    req->Out = msg->Out;

    // should always be posted
    RecvRequest(recv);

    Counters->RecvRequestCompleted();

    if (req->In.Length > Config->MaxBufferSize) {
        STORAGE_ERROR("RECV #" << recv->wr.wr_id
            << ": request exceeds maximum supported size "
            << req->In.Length << " > " << Config->MaxBufferSize);

        Counters->RecvRequestError();
        return;
    }

    if (req->Out.Length > Config->MaxBufferSize) {
        STORAGE_ERROR("RECV #" << recv->wr.wr_id
            << ": request exceeds maximum supported size "
            << req->Out.Length << " > " << Config->MaxBufferSize);

        Counters->RecvRequestError();
        return;
    }

    LWTRACK(
        RecvRequestCompleted,
        req->CallContext->LWOrbit,
        req->CallContext->RequestId);

    if (MaxInflightBytes < req->In.Length + req->Out.Length) {
        STORAGE_INFO_T(LogThrottler, "reached inflight limit, "
            << MaxInflightBytes << "/" << Config->MaxInflightBytes
            << " bytes available");

        return RejectRequest(std::move(req), RDMA_PROTO_THROTTLED,
            "throttled");
    }

    MaxInflightBytes -= req->In.Length + req->Out.Length;

    if (req->Out.Length) {
        req->OutBuffer = SendBuffers.AcquireBuffer(req->Out.Length);
    }

    if (req->In.Length) {
        req->InBuffer = RecvBuffers.AcquireBuffer(req->In.Length);

        if (auto* send = SendQueue.Pop()) {
            ReadRequestData(std::move(req), send);
        } else {
            // no more WRs available
            Counters->RequestEnqueued();
            QueuedRequests.Enqueue(std::move(req));
        }
    } else {
        ExecuteRequest(std::move(req));
    }
}

void TClientEndpoint::RejectRequest(
    TRequestPtr req,
    ui32 status,
    TStringBuf message)
{
    req->State = ERequestState::ExecuteRequest;

    req->Status = status;
    req->In.Length = 0;
    req->Out.Length = 1_KB;
    req->OutBuffer = SendBuffers.AcquireBuffer(req->Out.Length);
    req->ResponseBytes = SerializeError(
        E_REJECTED,
        message,
        static_cast<TStringBuf>(req->OutBuffer));

    MaxInflightBytes -= req->Out.Length;

    if (auto* send = SendQueue.Pop()) {
        WriteResponseData(std::move(req), send);
    } else {
        // no more WRs available
        Counters->RequestEnqueued();
        QueuedRequests.Enqueue(std::move(req));
    }
}

void TClientEndpoint::RecvRequestError(TRecvWr* recv, ibv_wc_status status)
{
    STORAGE_ERROR("RECV #" << recv->wr.wr_id << ": "
        << NVerbs::GetStatusString(status));

    Counters->RecvRequestError();
    ReportRdmaError();

    // should always be posted
    RecvRequest(recv);
}

void TClientEndpoint::ReadRequestData(TRequestPtr req, TSendWr* send)
{
    req->State = ERequestState::ReadRequestData;

    ibv_sge sg_list = {
        .addr = req->InBuffer.Address,
        .length = req->In.Length,
        .lkey = req->InBuffer.Key,
    };

    ibv_send_wr wr = {
        .wr_id = reinterpret_cast<uintptr_t>(send),
        .sg_list = &sg_list,
        .num_sge = 1,
        .opcode = IBV_WR_RDMA_READ,
    };

    wr.wr.rdma.rkey = req->In.Key;
    wr.wr.rdma.remote_addr = req->In.Address;

    STORAGE_TRACE("READ #" << wr.wr_id);
    Verbs->PostSend(Connection->qp, &wr);

    Counters->ReadRequestStarted();

    LWTRACK(
        ReadRequestDataStarted,
        req->CallContext->LWOrbit,
        req->CallContext->RequestId);

    StoreRequest(send, std::move(req));
}

void TClientEndpoint::ReadRequestDataCompleted(
    TSendWr* send,
    ibv_wc_status status)
{
    auto req = ExtractRequest(send);

    if (req == nullptr) {
        STORAGE_WARN("READ #" << send->wr.wr_id << ": request is empty");
        return;
    }

    if (status != IBV_WC_SUCCESS) {
        return ReadRequestDataError(std::move(req), send, status);
    }

    Y_VERIFY(req->State == ERequestState::ReadRequestData);

    Counters->ReadRequestCompleted();
    SendQueue.Push(send);

    LWTRACK(
        ReadRequestDataCompleted,
        req->CallContext->LWOrbit,
        req->CallContext->RequestId);

    ExecuteRequest(std::move(req));
}

void TClientEndpoint::ReadRequestDataError(
    TRequestPtr req,
    TSendWr* send,
    ibv_wc_status status)
{
    STORAGE_ERROR("READ #" << send->wr.wr_id << ": "
        << NVerbs::GetStatusString(status));

    ReportRdmaError();
    Counters->ReadRequestError();

    SendQueue.Push(send);

    RecvBuffers.ReleaseBuffer(req->InBuffer);
    SendBuffers.ReleaseBuffer(req->OutBuffer);

    MaxInflightBytes += req->In.Length + req->Out.Length;
}

void TClientEndpoint::ExecuteRequest(TRequestPtr req)
{
    req->State = ERequestState::ExecuteRequest;

    TStringBuf in = {
        reinterpret_cast<char*>(req->InBuffer.Address),
        req->In.Length,
    };

    TStringBuf out = {
        reinterpret_cast<char*>(req->OutBuffer.Address),
        req->Out.Length,
    };

    LWTRACK(
        ExecuteRequest,
        req->CallContext->LWOrbit,
        req->CallContext->RequestId);

    Handler->HandleRequest(req.get(), req->CallContext, in, out);

    req.release();  // ownership transferred
}

void TClientEndpoint::WriteResponseData(TRequestPtr req, TSendWr* send)
{
    req->State = ERequestState::WriteResponseData;

    ibv_sge sg_list = {
        .addr = req->OutBuffer.Address,
        .length = req->ResponseBytes,
        .lkey = req->OutBuffer.Key,
    };

    ibv_send_wr wr = {
        .wr_id = reinterpret_cast<uintptr_t>(send),
        .sg_list = &sg_list,
        .num_sge = 1,
        .opcode = IBV_WR_RDMA_WRITE,
    };

    wr.wr.rdma.rkey = req->Out.Key;
    wr.wr.rdma.remote_addr = req->Out.Address;

    STORAGE_TRACE("WRITE #" << wr.wr_id);
    Verbs->PostSend(Connection->qp, &wr);

    Counters->WriteResponseStarted();

    LWTRACK(
        WriteResponseDataStarted,
        req->CallContext->LWOrbit,
        req->CallContext->RequestId);

    StoreRequest(send, std::move(req));
}

void TClientEndpoint::WriteResponseDataCompleted(
    TSendWr* send,
    ibv_wc_status status)
{
    auto req = ExtractRequest(send);

    if (req == nullptr) {
        STORAGE_WARN("WRITE #" << send->wr.wr_id << ": request is empty");
        return;
    }

    if (status != IBV_WC_SUCCESS) {
        return WriteResponseDataError(std::move(req), send, status);
    }

    Y_VERIFY(req->State == ERequestState::WriteResponseData);

    Counters->WriteResponseCompleted();

    LWTRACK(
        WriteResponseDataCompleted,
        req->CallContext->LWOrbit,
        req->CallContext->RequestId);

    SendResponse(std::move(req), send);
}

void TClientEndpoint::WriteResponseDataError(
    TRequestPtr req,
    TSendWr* send,
    ibv_wc_status status)
{
    if (status != IBV_WC_SUCCESS) {
        STORAGE_ERROR("WRITE #" << send->wr.wr_id << ": "
            << NVerbs::GetStatusString(status));
    }

    ReportRdmaError();
    Counters->WriteResponseError();

    RecvBuffers.ReleaseBuffer(req->InBuffer);
    SendBuffers.ReleaseBuffer(req->OutBuffer);

    MaxInflightBytes += req->In.Length + req->Out.Length;
}

void TClientEndpoint::SendResponse(TRequestPtr req, TSendWr* send)
{
    req->State = ERequestState::SendResponse;

    auto* responseMsg = send->Message<TResponseMessage>();
    Zero(*responseMsg);

    InitMessageHeader(responseMsg, RDMA_PROTO_VERSION);

    responseMsg->ReqId = req->ReqId;
    responseMsg->Status = req->Status;
    responseMsg->ResponseBytes = req->ResponseBytes;

    STORAGE_TRACE("SEND #" << send->wr.wr_id);
    Verbs->PostSend(Connection->qp, &send->wr);

    Counters->SendResponseStarted();

    LWTRACK(
        SendResponseStarted,
        req->CallContext->LWOrbit,
        req->CallContext->RequestId);

    StoreRequest(send, std::move(req));
}

void TClientEndpoint::SendResponseCompleted(TSendWr* send, ibv_wc_status status)
{
    auto req = ExtractRequest(send);

    if (req == nullptr) {
        STORAGE_WARN("SEND #" << send->wr.wr_id << ": request is empty");
        return;
    }

    if (status != IBV_WC_SUCCESS) {
        return SendResponseError(std::move(req), send, status);
    }

    Y_VERIFY(req->State == ERequestState::SendResponse);

    if (req->Status == RDMA_PROTO_THROTTLED) {
        Counters->RequestThrottled();
    } else {
        Counters->SendResponseCompleted();
    }

    SendQueue.Push(send);

    RecvBuffers.ReleaseBuffer(req->InBuffer);
    SendBuffers.ReleaseBuffer(req->OutBuffer);

    MaxInflightBytes += req->In.Length + req->Out.Length;

    LWTRACK(
        SendResponseCompleted,
        req->CallContext->LWOrbit,
        req->CallContext->RequestId);

    // request will be deleted
}

void TClientEndpoint::SendResponseError(
    TRequestPtr req,
    TSendWr* send,
    ibv_wc_status status)
{
    if (status != IBV_WC_SUCCESS) {
        STORAGE_ERROR("SEND #" << send->wr.wr_id << ": "
            << NVerbs::GetStatusString(status));
    }

    ReportRdmaError();
    Counters->SendResponseError();

    SendQueue.Push(send);

    RecvBuffers.ReleaseBuffer(req->InBuffer);
    SendBuffers.ReleaseBuffer(req->OutBuffer);

    MaxInflightBytes += req->In.Length + req->Out.Length;
}

////////////////////////////////////////////////////////////////////////////////

class TServerEndpoint final
    : public IServerEndpoint
{
    // TODO
    friend class TServer;

private:
    NVerbs::IVerbsPtr Verbs;

    NVerbs::TConnectionPtr Connection;
    TString Host;
    ui32 Port;
    IServerHandlerPtr Handler;

public:
    TServerEndpoint(
            NVerbs::IVerbsPtr verbs,
            NVerbs::TConnectionPtr connection,
            TString host,
            ui32 port,
            IServerHandlerPtr handler)
        : Verbs(std::move(verbs))
        , Connection(std::move(connection))
        , Host(std::move(host))
        , Port(port)
        , Handler(std::move(handler))
    {
        Connection->context = this;
    }

    static TServerEndpoint* FromEvent(rdma_cm_event* event)
    {
        Y_VERIFY(event->listen_id && event->listen_id->context);
        return static_cast<TServerEndpoint*>(event->listen_id->context);
    }

    void SendResponse(void* context, size_t responseBytes) override
    {
        TRequestPtr req(static_cast<TRequest*>(context));
        req->Status = RDMA_PROTO_OK;
        req->ResponseBytes = responseBytes;
        req->Endpoint->EnqueueRequest(std::move(req));
    }

    void SendError(void* context, ui32 error, TStringBuf message) override
    {
        TRequestPtr req(static_cast<TRequest*>(context));
        req->Status = RDMA_PROTO_FAIL;
        req->ResponseBytes = SerializeError(
            error,
            message,
            static_cast<TStringBuf>(req->OutBuffer));
        req->Endpoint->EnqueueRequest(std::move(req));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct IConnectionEventHandler
{
    virtual ~IConnectionEventHandler() = default;

    virtual void HandleConnectionEvent(rdma_cm_event* event) = 0;
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
        PollHandle.Attach(EventChannel->fd, EPOLLIN, EventChannel.get());
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
                    Y_VERIFY(event.data.ptr == EventChannel.get());
                    HandleConnectionEvents();
                }
            }
        }

        return nullptr;
    }

    void HandleConnectionEvents()
    {
        while (auto event = Verbs->GetConnectionEvent(EventChannel.get())) {
            EventHandler->HandleConnectionEvent(event.get());
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

    TServerConfigPtr Config;
    TLog Log;

    TRCUList<TClientEndpointPtr> Endpoints;
    TPollHandle PollHandle;

    TAtomic StopFlag = 0;
    TEventHandle StopEvent;

public:
    TCompletionPoller(
            NVerbs::IVerbsPtr verbs,
            TServerConfigPtr config,
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

    void Attach(TClientEndpoint* endpoint)
    {
        if (Config->WaitMode == EWaitMode::Poll) {
            PollHandle.Attach(
                endpoint->CompletionChannel->fd,
                EPOLLIN,
                EndpointEventTag(endpoint, 0));

            PollHandle.Attach(
                endpoint->RequestEvent.Handle(),
                EPOLLIN,
                EndpointEventTag(endpoint, 1));

            Verbs->RequestCompletionEvent(endpoint->CompletionQueue.get(), 0);
        }
    }

    void Detach(TClientEndpoint* endpoint)
    {
        if (Config->WaitMode == EWaitMode::Poll) {
            PollHandle.Detach(endpoint->CompletionChannel->fd);
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
                        auto* endpoint = EndpointFromTag(event.data.ptr);
                        if (EventFromTag(event.data.ptr)) {
                            endpoint->HandleInputRequests();
                        } else {
                            endpoint->HandleCompletionEvents();
                        }
                    }
                }
            } else {
                auto endpoints = Endpoints.Get();
                bool hasWork = false;

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
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TServer final
    : public IServer
    , public IConnectionEventHandler
{
private:
    NVerbs::IVerbsPtr Verbs;

    const ILoggingServicePtr Logging;
    const IMonitoringServicePtr Monitoring;

    TServerConfigPtr Config;
    TEndpointCountersPtr Counters;
    TLog Log;

    TVector<TServerEndpointPtr> Endpoints;
    TMutex EndpointsLock;

    TConnectionPollerPtr ConnectionPoller;
    TVector<TCompletionPollerPtr> CompletionPollers;

public:
    TServer(
        NVerbs::IVerbsPtr verbs,
        ILoggingServicePtr logging,
        IMonitoringServicePtr monitoring,
        TServerConfigPtr config);

    void Start() override;
    void Stop() override;

    IServerEndpointPtr StartEndpoint(
        TString host,
        ui32 port,
        IServerHandlerPtr handler) override;

private:
    void HandleConnectionEvent(rdma_cm_event* event) override;
    void HandleConnectRequest(TServerEndpoint* endpoint, rdma_cm_event* event);

    void BeginListen(TServerEndpoint* endpoint);
    void BeginConnect(TClientEndpointPtr endpoint);

    void HandleEndpointConnected(TClientEndpoint* endpoint);
    void HandleEndpointDisconnected(TClientEndpoint* endpoint, rdma_cm_event* event);

    void RejectWithStatus(rdma_cm_id* id, int status);

    TCompletionPoller& PickPoller();
};

////////////////////////////////////////////////////////////////////////////////

TServer::TServer(
        NVerbs::IVerbsPtr verbs,
        ILoggingServicePtr logging,
        IMonitoringServicePtr monitoring,
        TServerConfigPtr config)
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

void TServer::Start()
{
    Log = Logging->CreateLog("BLOCKSTORE_RDMA");

    auto counters = Monitoring->GetCounters();
    auto rootGroup = counters->GetSubgroup("counters", "blockstore");
    Counters->Register(*rootGroup->GetSubgroup("component", "rdma_server"));

    STORAGE_DEBUG("Start server");

    CompletionPollers.resize(Config->PollerThreads);
    for (size_t i = 0; i < CompletionPollers.size(); ++i) {
        CompletionPollers[i] = std::make_unique<TCompletionPoller>(
            Verbs,
            Config,
            Log);
        CompletionPollers[i]->Start();
    }

    ConnectionPoller = std::make_unique<TConnectionPoller>(Verbs, this, Log);
    ConnectionPoller->Start();
}

void TServer::Stop()
{
    STORAGE_DEBUG("Stop server");

    if (ConnectionPoller) {
        ConnectionPoller->Stop();
        ConnectionPoller.reset();
    }

    for (auto& poller: CompletionPollers) {
        poller->Stop();
    }
    CompletionPollers.clear();
}

IServerEndpointPtr TServer::StartEndpoint(
    TString host,
    ui32 port,
    IServerHandlerPtr handler)
{
    auto endpoint = std::make_shared<TServerEndpoint>(
        Verbs,
        ConnectionPoller->CreateConnection(),
        std::move(host),
        port,
        std::move(handler));

    with_lock (EndpointsLock) {
        Endpoints.emplace_back(endpoint);
    }

    BeginListen(endpoint.get());
    return endpoint;
}

void TServer::BeginListen(TServerEndpoint* endpoint)
{
    rdma_addrinfo hints = {
        .ai_flags = RAI_PASSIVE,
        .ai_port_space = RDMA_PS_TCP,
    };

    auto addrinfo = Verbs->GetAddressInfo(
        endpoint->Host,
        endpoint->Port,
        &hints);

    STORAGE_DEBUG("LISTEN address "
        << NVerbs::PrintAddress(addrinfo->ai_src_addr));

    Verbs->BindAddress(endpoint->Connection.get(), addrinfo->ai_src_addr);
    Verbs->Listen(endpoint->Connection.get(), Config->Backlog);
}

////////////////////////////////////////////////////////////////////////////////

void TServer::HandleConnectionEvent(rdma_cm_event* event)
{
    STORAGE_DEBUG(NVerbs::GetEventName(event->event) << " received");

    switch (event->event) {
        case RDMA_CM_EVENT_ADDR_RESOLVED:
        case RDMA_CM_EVENT_ADDR_ERROR:
        case RDMA_CM_EVENT_ROUTE_RESOLVED:
        case RDMA_CM_EVENT_ROUTE_ERROR:
        case RDMA_CM_EVENT_CONNECT_RESPONSE:
        case RDMA_CM_EVENT_UNREACHABLE:
        case RDMA_CM_EVENT_REJECTED:
            // not relevant for the server
            break;

        case RDMA_CM_EVENT_MULTICAST_JOIN:
        case RDMA_CM_EVENT_MULTICAST_ERROR:
            // multicast is not used
            break;

        case RDMA_CM_EVENT_TIMEWAIT_EXIT:
            // QPs is not re-used
            break;

        case RDMA_CM_EVENT_CONNECT_REQUEST:
            HandleConnectRequest(TServerEndpoint::FromEvent(event), event);
            break;

        case RDMA_CM_EVENT_ESTABLISHED:
            HandleEndpointConnected(TClientEndpoint::FromEvent(event));
            break;

        case RDMA_CM_EVENT_CONNECT_ERROR:
        case RDMA_CM_EVENT_DISCONNECTED:
            HandleEndpointDisconnected(TClientEndpoint::FromEvent(event), event);
            break;

        case RDMA_CM_EVENT_DEVICE_REMOVAL:
        case RDMA_CM_EVENT_ADDR_CHANGE:
            // TODO
            break;
    }
}

void TServer::HandleConnectRequest(
    TServerEndpoint* endpoint,
    rdma_cm_event* event)
{
    const rdma_conn_param* connectParams = &event->param.conn;
    STORAGE_DEBUG("VALIDATE connection from "
        << NVerbs::PrintAddress(rdma_get_peer_addr(event->id))
        << " " << NVerbs::PrintConnectionParams(connectParams));

    if (connectParams->private_data == nullptr ||
        connectParams->private_data_len < sizeof(TConnectMessage) ||
        ParseMessageHeader(connectParams->private_data) < RDMA_PROTO_VERSION)
    {
        return RejectWithStatus(event->id, RDMA_PROTO_INVALID_REQUEST);
    }

    const auto* connectMsg = static_cast<const TConnectMessage*>(
        connectParams->private_data);

    if (Config->StrictValidation) {
        if (connectMsg->QueueSize > Config->QueueSize ||
            connectMsg->MaxBufferSize > Config->MaxBufferSize)
        {
            return RejectWithStatus(event->id, RDMA_PROTO_CONFIG_MISMATCH);
        }
    }

    auto client = std::make_shared<TClientEndpoint>(
        Verbs,
        NVerbs::WrapPtr(event->id),
        endpoint->Handler,
        Config,
        Counters,
        Log);

    auto& poller = PickPoller();
    poller.Add(client);

    BeginConnect(std::move(client));
}

void TServer::BeginConnect(TClientEndpointPtr endpoint)
{
    endpoint->InitCompletionQueue(Config->QueueSize);

    endpoint->Poller->Attach(endpoint.get());

    TAcceptMessage acceptMsg = {
        .KeepAliveTimeout = SafeCast<ui16>(Config->KeepAliveTimeout.MilliSeconds()),
    };
    InitMessageHeader(&acceptMsg, RDMA_PROTO_VERSION);

    rdma_conn_param acceptParams = {
        .private_data = &acceptMsg,
        .private_data_len = sizeof(TAcceptMessage),
        .responder_resources = RDMA_MAX_RESP_RES,
        .initiator_depth = RDMA_MAX_INIT_DEPTH,
        .flow_control = 1,
        .retry_count = 7,
        .rnr_retry_count = 7,
    };

    STORAGE_DEBUG("ACCEPT connection from "
        << NVerbs::PrintAddress(rdma_get_peer_addr(endpoint->Connection.get()))
        << " " << NVerbs::PrintConnectionParams(&acceptParams));

    endpoint->ChangeState(
        EEndpointState::Disconnected,
        EEndpointState::Connecting);

    int err = Verbs->Accept(endpoint->Connection.get(), &acceptParams);
    if (err) {
        char buf[64];
        STORAGE_ERROR("ACCEPT failed: "
            << strerror_r(err, buf, sizeof(buf)) << "(" << err << ")");

        RejectWithStatus(endpoint->Connection.get(), RDMA_PROTO_FAIL);

        endpoint->ChangeState(
            EEndpointState::Connecting,
            EEndpointState::Error);

        endpoint->Poller->Detach(endpoint.get());

        Verbs->DestroyQP(endpoint->Connection.get());
    }
}

void TServer::HandleEndpointConnected(TClientEndpoint* endpoint)
{
    endpoint->ChangeState(
        EEndpointState::Connecting,
        EEndpointState::Connected);

    endpoint->StartReceive();
}

void TServer::HandleEndpointDisconnected(
    TClientEndpoint* endpoint,
    rdma_cm_event* event)
{
    endpoint->ChangeState(
        GetExpectedEndpointState(event->event),
        EEndpointState::Disconnected);

    endpoint->Poller->Detach(endpoint);

    Verbs->DestroyQP(endpoint->Connection.get());

    // TODO abort
}

void TServer::RejectWithStatus(rdma_cm_id* id, int status)
{
    STORAGE_DEBUG("REJECT status=" << status);

    TRejectMessage rejectMsg = {
        .Status = SafeCast<ui16>(status),
        .QueueSize = SafeCast<ui16>(Config->QueueSize),
        .MaxBufferSize = SafeCast<ui32>(Config->MaxBufferSize),
    };
    InitMessageHeader(&rejectMsg, RDMA_PROTO_VERSION);

    int err = Verbs->Reject(id, &rejectMsg, sizeof(TRejectMessage));
    if (err) {
        char buf[64];
        STORAGE_ERROR("REJECT failed: "
            << strerror_r(err, buf, sizeof(buf)) << "(" << err << ")");
    }
}

TCompletionPoller& TServer::PickPoller()
{
    size_t index = RandomNumber(CompletionPollers.size());
    return *CompletionPollers[index];
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

#define SET(param, ...) \
    if (const auto& value = config.Get##param()) { \
        param = __VA_ARGS__(value); \
    }

TServerConfig::TServerConfig(const NProto::TRdmaServer& config)
{
    SET(Backlog);
    SET(QueueSize);
    SET(MaxBufferSize);
    SET(KeepAliveTimeout, TDuration::MilliSeconds);
    SET(WaitMode, Convert);
    SET(PollerThreads);
    SET(MaxInflightBytes);
}

#undef SET

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    NVerbs::IVerbsPtr verbs,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    TServerConfigPtr config)
{
    return std::make_shared<TServer>(
        std::move(verbs),
        std::move(logging),
        std::move(monitoring),
        std::move(config));
}

}   // namespace NCloud::NBlockStore::NRdma
