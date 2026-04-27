#include "server.h"

#include "buffer.h"
#include "event.h"
#include "list.h"
#include "poll.h"
#include "rcu.h"
#include "utils.h"
#include "verbs.h"
#include "work_queue.h"
#include "adaptive_wait.h"

#include <cloud/storage/core/libs/rdma/iface/log.h>
#include <cloud/storage/core/libs/rdma/iface/probes.h>
#include <cloud/storage/core/libs/rdma/iface/protobuf.h>

#include <cloud/storage/core/libs/common/context.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>
#include <util/network/interface.h>
#include <util/random/random.h>
#include <util/stream/format.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>

namespace NCloud::NStorage::NRdma {

using namespace NMonitoring;

using TSendWr = TSendWrBase<TResponseMessage>;
using TRecvWr = TRecvWrBase<TRequestMessage>;

LWTRACE_USING(STORAGE_RDMA_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration LOG_THROTTLER_PERIOD = TDuration::Minutes(20);
constexpr TDuration POLL_TIMEOUT = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

struct TRequest;
using TRequestPtr = std::unique_ptr<TRequest>;

struct TEndpointCounters;
using TEndpointCountersPtr = std::shared_ptr<TEndpointCounters>;

class TServerSession;
using TServerSessionPtr = std::shared_ptr<TServerSession>;

class TServerEndpoint;
using TServerEndpointPtr = std::shared_ptr<TServerEndpoint>;

class TConnectionPoller;
using TConnectionPollerPtr = std::unique_ptr<TConnectionPoller>;

class TCompletionPoller;
using TCompletionPollerPtr = std::unique_ptr<TCompletionPoller>;

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

struct TRequest
    : TListNode<TRequest>
{
    std::weak_ptr<TServerSession> Session;

    ERequestState State = ERequestState::RecvRequest;

    TCallContextBasePtr CallContext;
    ui32 ReqId = 0;
    ui32 Status = 0;
    ui32 ResponseBytes = 0;

    TBufferDesc In {};
    TBufferDesc Out {};

    TPooledBuffer InBuffer {};
    TPooledBuffer OutBuffer {};

    TRequest(std::weak_ptr<TServerSession> session)
        : Session(std::move(session))
    {}
};

////////////////////////////////////////////////////////////////////////////////

void StoreRequest(TSendWr* send, TRequestPtr req)
{
    Y_ABORT_UNLESS(send->context == nullptr);
    send->context = req.release();  // ownership transferred
}

TRequestPtr ExtractRequest(TSendWr* send)
{
    TRequestPtr req(static_cast<TRequest*>(send->context));
    send->context = nullptr;
    return req;
}

TString GetOpcode(TSendWr* send)
{
    auto opcode = "SEND";
    if (auto* req = send->Context<TRequest*>()) {
        if (req->State == ERequestState::ReadRequestData) {
            opcode = "READ";
        }
        if (req->State == ERequestState::WriteResponseData) {
            opcode = "WRITE";
        }
    }
    return opcode;
}

////////////////////////////////////////////////////////////////////////////////

struct TEndpointCounters
{
    TDynamicCounters::TCounterPtr QueuedRequests;
    TDynamicCounters::TCounterPtr ActiveRequests;
    TDynamicCounters::TCounterPtr CompletedRequests;
    TDynamicCounters::TCounterPtr AbortedRequests;
    TDynamicCounters::TCounterPtr ThrottledRequests;

    TDynamicCounters::TCounterPtr ActiveSend;
    TDynamicCounters::TCounterPtr ActiveRecv;
    TDynamicCounters::TCounterPtr ActiveRead;
    TDynamicCounters::TCounterPtr ActiveWrite;

    TDynamicCounters::TCounterPtr Errors;

    void Register(TDynamicCounters& counters)
    {
        QueuedRequests = counters.GetCounter("QueuedRequests");
        ActiveRequests = counters.GetCounter("ActiveRequests");
        CompletedRequests = counters.GetCounter("CompletedRequests", true);
        AbortedRequests = counters.GetCounter("AbortedRequests", true);
        ThrottledRequests = counters.GetCounter("ThrottledRequests", true);

        ActiveSend = counters.GetCounter("ActiveSend");
        ActiveRecv = counters.GetCounter("ActiveRecv");
        ActiveRead = counters.GetCounter("ActiveRead");
        ActiveWrite = counters.GetCounter("ActiveWrite");

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

    void RecvRequestStarted()
    {
        ActiveRecv->Inc();
    }

    void RecvRequestCompleted()
    {
        ActiveRecv->Dec();
    }

    void RequestStarted()
    {
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
    }

    void RequestThrottled()
    {
        ThrottledRequests->Inc();
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

class TServerSession final
    : public NVerbs::ICompletionHandler
    , public std::enable_shared_from_this<TServerSession>
{
    // TODO
    friend class TServer;
    friend class TCompletionPoller;

private:
    NVerbs::IVerbsPtr Verbs;
    NVerbs::TConnectionPtr Connection;
    TCompletionPoller* CompletionPoller = nullptr;
    IServerHandlerPtr Handler;
    TServerConfigPtr Config;
    TEndpointCountersPtr Counters;
    TLog Log;
    size_t MaxInflightBytes;

    struct {
        TLogThrottler Inflight = TLogThrottler(LOG_THROTTLER_PERIOD);
    } LogThrottler;

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

    union {
        const ui64 Id = RandomNumber(Max<ui64>());
        struct {
            const ui32 RecvMagic;
            const ui32 SendMagic;
        };
    };

    TLockFreeList<TRequest> InputRequests;
    TSimpleList<TRequest> QueuedRequests;
    TEventHandle RequestEvent;
    bool FlushStarted = false;

public:
    static TServerSession* FromEvent(rdma_cm_event* event)
    {
        Y_ABORT_UNLESS(event->id && event->id->context);
        return static_cast<TServerSession*>(event->id->context);
    }

    TServerSession(
        NVerbs::IVerbsPtr Verbs,
        NVerbs::TConnectionPtr connection,
        TCompletionPoller* completionPoller,
        IServerHandlerPtr handler,
        TServerConfigPtr config,
        TEndpointCountersPtr stats,
        TLog log);

    ~TServerSession();

    // called from CM thread
    void CreateQP();
    void Start() noexcept;
    void Stop() noexcept;
    void Flush() noexcept;

    // called from external thread
    void EnqueueRequest(TRequestPtr req) noexcept;

    // called from CQ thread
    void HandleCompletionEvent(ibv_wc* wc) noexcept override;
    bool HandleInputRequests() noexcept;
    bool HandleCompletionEvents() noexcept;
    bool IsFlushed() const;

private:
    // called from CQ thread
    void HandleQueuedRequests() noexcept;
    void RecvRequest(TRecvWr* recv) noexcept;
    void RecvRequestCompleted(TRecvWr* recv) noexcept;
    void ReadRequestData(TRequestPtr req, TSendWr* send) noexcept;
    void ReadRequestDataCompleted(TSendWr* send) noexcept;
    void ExecuteRequest(TRequestPtr req) noexcept;
    void WriteResponseData(TRequestPtr req, TSendWr* send) noexcept;
    void WriteResponseDataCompleted(TSendWr* send) noexcept;
    void SendResponse(TRequestPtr req, TSendWr* send) noexcept;
    void SendResponseCompleted(TSendWr* send) noexcept;
    void FreeRequest(TRequestPtr req, TSendWr* send) noexcept;
    void RejectRequest(
        TRequestPtr req,
        ui32 status,
        TStringBuf message) noexcept;
    int ValidateCompletion(ibv_wc* wc) noexcept;
    void HandleSendError(TSendWr* send) noexcept;
};

////////////////////////////////////////////////////////////////////////////////

TServerSession::TServerSession(
        NVerbs::IVerbsPtr verbs,
        NVerbs::TConnectionPtr connection,
        TCompletionPoller* completionPoller,
        IServerHandlerPtr handler,
        TServerConfigPtr config,
        TEndpointCountersPtr stats,
        TLog log)
    : Verbs(std::move(verbs))
    , Connection(std::move(connection))
    , CompletionPoller(completionPoller)
    , Handler(std::move(handler))
    , Config(std::move(config))
    , Counters(std::move(stats))
    , Log(log)
    , MaxInflightBytes(Config->MaxInflightBytes)
    , SendBuffers(Config->BufferPool)
    , RecvBuffers(Config->BufferPool)
{
    Connection->context = this;

    Log.SetFormatter([=, this](ELogPriority p, TStringBuf msg) {
        Y_UNUSED(p);
        return TStringBuilder() << "[" << Id << "] " << msg;
    });

    RDMA_INFO(
        "start session [host=" << Verbs->GetPeer(Connection.get())
                               << " send_magic=" << Hex(SendMagic, HF_FULL)
                               << " recv_magic=" << Hex(RecvMagic, HF_FULL)
                               << "]");
}

void TServerSession::CreateQP()
{
    CompletionChannel = Verbs->CreateCompletionChannel(Connection->verbs);
    SetNonBlock(CompletionChannel->fd, true);

    CompletionQueue = Verbs->CreateCompletionQueue(
        Connection->verbs,
        Config->SendQueueSize + Config->RecvQueueSize,
        this,
        CompletionChannel.get(),
        0);   // comp_vector

    ibv_qp_init_attr qp_attrs = {
        .qp_context = nullptr,
        .send_cq = CompletionQueue.get(),
        .recv_cq = CompletionQueue.get(),
        .cap = {
            .max_send_wr = Config->SendQueueSize,
            .max_recv_wr = Config->RecvQueueSize,
            .max_send_sge = RDMA_MAX_SEND_SGE,
            .max_recv_sge = RDMA_MAX_RECV_SGE,
            .max_inline_data = 16,
        },
        .qp_type = IBV_QPT_RC,
        .sq_sig_all = 1,
    };

    if (Config->VerbsQP) {
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
        Config->SendQueueSize * sizeof(TResponseMessage),
        true);

    RecvBuffer = RecvBuffers.AcquireBuffer(
        Config->RecvQueueSize * sizeof(TRequestMessage),
        true);

    SendWrs.resize(Config->SendQueueSize);
    RecvWrs.resize(Config->RecvQueueSize);

    ui32 i = 0;
    ui64 responseMsg = SendBuffer.Address;
    for (auto& wr: SendWrs) {
        wr.wr.opcode = IBV_WR_SEND;

        wr.wr.wr_id = TWorkRequestId(SendMagic, 0, i++).Id;
        wr.wr.sg_list = wr.sg_list;
        wr.wr.num_sge = 1;

        wr.sg_list[0].lkey = SendBuffer.LKey;
        wr.sg_list[0].addr = responseMsg;
        wr.sg_list[0].length = sizeof(TResponseMessage);

        SendQueue.Push(&wr);
        responseMsg += sizeof(TResponseMessage);
    }

    ui32 j = 0;
    ui64 requestMsg = RecvBuffer.Address;
    for (auto& wr: RecvWrs) {
        wr.wr.wr_id = TWorkRequestId(RecvMagic, 0, j++).Id;
        wr.wr.sg_list = wr.sg_list;
        wr.wr.num_sge = 1;

        wr.sg_list[0].lkey = RecvBuffer.LKey;
        wr.sg_list[0].addr = requestMsg;
        wr.sg_list[0].length = sizeof(TRequestMessage);

        RecvQueue.Push(&wr);
        requestMsg += sizeof(TRequestMessage);
    }
}

TServerSession::~TServerSession()
{
    RDMA_INFO("stop session");

    if (Connection->qp) {
        if (Config->VerbsQP) {
            Verbs->DestroyQP(Connection->qp);
            Connection->qp = nullptr;
        } else {
            Verbs->RdmaDestroyQP(Connection.get());
        }
    }

    CompletionQueue.reset();
    CompletionChannel.reset();

    if (SendBuffers.Initialized()) {
        SendBuffers.ReleaseBuffer(SendBuffer);
    }

    if (RecvBuffers.Initialized()) {
        RecvBuffers.ReleaseBuffer(RecvBuffer);
    }

    SendQueue.Clear();
    RecvQueue.Clear();
}

void TServerSession::Start() noexcept
{
    while (auto* recv = RecvQueue.Pop()) {
        RecvRequest(recv);
    }
}

void TServerSession::Stop() noexcept
{
    try {
        Verbs->Disconnect(Connection.get());

    } catch (const TServiceError& e) {
        RDMA_ERROR("unable to disconnect session");
    }
}

void TServerSession::EnqueueRequest(TRequestPtr req) noexcept
{
    Counters->RequestEnqueued();
    InputRequests.Enqueue(std::move(req));

    if (Config->WaitMode == EWaitMode::Poll) {
        RequestEvent.Set();
    }
}

bool TServerSession::HandleInputRequests() noexcept
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

void TServerSession::HandleQueuedRequests() noexcept
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

        switch (req->State) {
            case ERequestState::RecvRequest:
                Y_ABORT_UNLESS(req->In.Length);
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
                Y_ABORT("unexpected state %d", (int)req->State);
        }
    }
}

bool TServerSession::HandleCompletionEvents() noexcept
{
    try {
        ibv_cq* cq = CompletionQueue.get();

        if (Config->WaitMode == EWaitMode::Poll) {
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
        return true;
    }

    return false;
}

void TServerSession::Flush() noexcept
{
    RDMA_DEBUG("flush queues");

    try {
        struct ibv_qp_attr attr = {.qp_state = IBV_QPS_ERR};
        Verbs->ModifyQP(Connection->qp, &attr, IBV_QP_STATE);
        FlushStarted = true;

    } catch (const TServiceError& e) {
        RDMA_ERROR("flush error: " << e.what());
        Counters->Error();
    }
}

bool TServerSession::IsFlushed() const
{
    return FlushStarted
        && SendQueue.Size() == Config->SendQueueSize
        && RecvQueue.Size() == Config->RecvQueueSize;
}

int TServerSession::ValidateCompletion(ibv_wc* wc) noexcept
{
    auto id = TWorkRequestId(wc->wr_id);

    if (id.Magic == SendMagic && id.Index < SendWrs.size()) {
        auto* send = &SendWrs[id.Index];

        if (wc->status == IBV_WC_WR_FLUSH_ERR) {
            RDMA_TRACE(send << " " << NVerbs::GetStatusString(wc->status));
            HandleSendError(send);
            return -1;
        }

        if (wc->status != IBV_WC_SUCCESS) {
            RDMA_ERROR(send << " " << NVerbs::GetStatusString(wc->status));
            HandleSendError(send);
            Counters->Error();
            return -1;
        }

        if (wc->opcode != IBV_WC_SEND && wc->opcode != IBV_WC_RDMA_READ &&
            wc->opcode != IBV_WC_RDMA_WRITE)
        {
            RDMA_ERROR(
                send << " unexpected opcode "
                     << NVerbs::GetOpcodeName(wc->opcode));
            HandleSendError(send);
            Counters->Error();
            return -1;
        }

        return 0;
    }

    if (id.Magic == RecvMagic && id.Index < RecvWrs.size()) {
        auto* recv = &RecvWrs[id.Index];

        if (wc->status == IBV_WC_WR_FLUSH_ERR) {
            RDMA_TRACE(recv << " " << NVerbs::GetStatusString(wc->status));
            Counters->RecvRequestCompleted();
            RecvQueue.Push(recv);
            return -1;
        }

        if (wc->status != IBV_WC_SUCCESS) {
            RDMA_ERROR(recv << " " << NVerbs::GetStatusString(wc->status));
            Counters->Error();
            Counters->RecvRequestCompleted();
            RecvQueue.Push(recv);
            return -1;
        }

        if (wc->opcode != IBV_WC_RECV) {
            RDMA_ERROR(
                recv << " unexpected opcode "
                     << NVerbs::GetOpcodeName(wc->opcode));
            Counters->Error();
            Counters->RecvRequestCompleted();
            RecvQueue.Push(recv);
            return -1;
        }

        return 0;
    }

    RDMA_ERROR("unexpected wr_id " << NVerbs::PrintCompletion(wc));
    Counters->Error();
    return -1;
}

// implements NVerbs::ICompletionHandler
void TServerSession::HandleCompletionEvent(ibv_wc* wc) noexcept
{
    auto id = TWorkRequestId(wc->wr_id);

    if (ValidateCompletion(wc)) {
        return;
    }

    switch (wc->opcode) {
        case IBV_WC_RECV: {
            TRecvWr* recv = &RecvWrs[id.Index];
            RDMA_TRACE(recv << " completed");
            RecvRequestCompleted(recv);
            break;
        }

        case IBV_WC_RDMA_READ: {
            TSendWr* send = &SendWrs[id.Index];
            RDMA_TRACE(send << " completed");
            ReadRequestDataCompleted(send);
            break;
        }

        case IBV_WC_RDMA_WRITE: {
            TSendWr* send = &SendWrs[id.Index];
            RDMA_TRACE(send << " completed");
            WriteResponseDataCompleted(send);
            break;
        }

        case IBV_WC_SEND: {
            TSendWr* send = &SendWrs[id.Index];
            RDMA_TRACE(send << " completed");
            SendResponseCompleted(send);
            break;
        }

        default:
            RDMA_WARN("unhandled completion " << NVerbs::PrintCompletion(wc));
            break;
    }
}

void TServerSession::RecvRequest(TRecvWr* recv) noexcept
{
    auto* requestMsg = recv->Message();
    Zero(*requestMsg);

    try {
        Verbs->PostRecv(Connection->qp, &recv->wr);
        RDMA_TRACE(recv << " posted");

    } catch (const TServiceError& e) {
        RDMA_ERROR(recv << " " << e.what());
        Counters->Error();
        return;
    }

    Counters->RecvRequestStarted();
}

void TServerSession::HandleSendError(TSendWr* send) noexcept
{
    auto req = ExtractRequest(send);
    if (req) {
        if (req->State == ERequestState::ReadRequestData) {
            Counters->ReadRequestCompleted();
        }
        if (req->State == ERequestState::WriteResponseData) {
            Counters->WriteResponseCompleted();
        }
        if (req->State == ERequestState::SendResponse) {
            Counters->SendResponseCompleted();
        }
    }
    Counters->RequestAborted();
    FreeRequest(std::move(req), send);
}

void TServerSession::FreeRequest(TRequestPtr req, TSendWr* send) noexcept
{
    if (req == nullptr) {
        return;
    }
    RecvBuffers.ReleaseBuffer(req->InBuffer);
    SendBuffers.ReleaseBuffer(req->OutBuffer);
    MaxInflightBytes += req->In.Length + req->Out.Length;
    SendQueue.Push(send);
}

void TServerSession::RecvRequestCompleted(TRecvWr* recv) noexcept
{
    const auto* msg = recv->Message();
    const int version = ParseMessageHeader(msg);

    if (version != RDMA_PROTO_VERSION) {
        RDMA_ERROR(
            recv << " incompatible protocol version " << version
                 << ", expected " << static_cast<int>(RDMA_PROTO_VERSION));

        Counters->RecvRequestCompleted();
        Counters->Error();
        RecvRequest(recv);  // should always be posted
        return;
    }

    auto req = std::make_unique<TRequest>(weak_from_this());

    // TODO restore context from request meta
    req->CallContext = Handler->CreateCallContext();
    Y_ABORT_UNLESS(req->CallContext);
    req->ReqId = msg->ReqId;
    req->In = msg->In;
    req->Out = msg->Out;

    LWTRACK(
        RecvRequestCompleted,
        req->CallContext->LWOrbit,
        req->CallContext->RequestId);

    Counters->RecvRequestCompleted();
    Counters->RequestStarted();

    if (req->In.Length > Config->MaxBufferSize) {
        RDMA_ERROR(
            recv << " request exceeds maximum supported size " << req->In.Length
                 << " > " << Config->MaxBufferSize);

        Counters->Error();
        RecvRequest(recv);  // should always be posted
        RejectRequest(
            std::move(req),
            RDMA_PROTO_INVALID_REQUEST,
            "request is too big");
        return;
    }

    if (req->Out.Length > Config->MaxBufferSize) {
        RDMA_ERROR(
            recv << " response exceeds maximum supported size "
                 << req->Out.Length << " > " << Config->MaxBufferSize);

        Counters->Error();
        RecvRequest(recv);  // should always be posted
        RejectRequest(
            std::move(req),
            RDMA_PROTO_INVALID_REQUEST,
            "response is too big");
        return;
    }

    if (MaxInflightBytes < req->In.Length + req->Out.Length) {
        RDMA_INFO(
            LogThrottler.Inflight,
            Log,
            recv << " reached inflight limit, " << MaxInflightBytes << "/"
                 << Config->MaxInflightBytes << " bytes available");

        Counters->RequestThrottled();
        RecvRequest(recv);  // should always be posted
        RejectRequest(std::move(req), RDMA_PROTO_THROTTLED, "throttled");
        return;
    }
    RecvRequest(recv);  // should always be posted

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
            RDMA_TRACE("no more send WRs available");
            Counters->RequestEnqueued();
            QueuedRequests.Enqueue(std::move(req));
        }
    } else {
        ExecuteRequest(std::move(req));
    }
}

void TServerSession::RejectRequest(
    TRequestPtr req,
    ui32 status,
    TStringBuf message) noexcept
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

void TServerSession::ReadRequestData(TRequestPtr req, TSendWr* send) noexcept
{
    req->State = ERequestState::ReadRequestData;

    ibv_sge sg_list = {
        .addr = req->InBuffer.Address,
        .length = req->In.Length,
        .lkey = req->InBuffer.LKey,
    };

    ibv_send_wr wr = {
        .wr_id = send->wr.wr_id,
        .sg_list = &sg_list,
        .num_sge = 1,
        .opcode = IBV_WR_RDMA_READ,
    };

    wr.wr.rdma.rkey = req->In.RKey;
    wr.wr.rdma.remote_addr = req->In.Address;

    try {
        auto& cc = req->CallContext;

        StoreRequest(send, std::move(req));
        Verbs->PostSend(Connection->qp, &wr);
        RDMA_TRACE(send << " posted");

        LWTRACK(ReadRequestDataStarted, cc->LWOrbit, cc->RequestId);
        Counters->ReadRequestStarted();

    } catch (const TServiceError& e) {
        RDMA_ERROR(send << " " << e.what());
        Counters->Error();
        HandleSendError(send);
        return;
    }
}

void TServerSession::ReadRequestDataCompleted(TSendWr* send) noexcept
{
    auto req = ExtractRequest(send);

    if (req == nullptr) {
        RDMA_WARN(
            "RDMA_READ " << TWorkRequestId(send->wr.wr_id)
                         << " request is empty");
        return;
    }

    Y_ABORT_UNLESS(req->State == ERequestState::ReadRequestData);

    SendQueue.Push(send);
    Counters->ReadRequestCompleted();

    LWTRACK(
        ReadRequestDataCompleted,
        req->CallContext->LWOrbit,
        req->CallContext->RequestId);

    ExecuteRequest(std::move(req));
}

void TServerSession::ExecuteRequest(TRequestPtr req) noexcept
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

void TServerSession::WriteResponseData(TRequestPtr req, TSendWr* send) noexcept
{
    req->State = ERequestState::WriteResponseData;

    ibv_sge sg_list = {
        .addr = req->OutBuffer.Address,
        .length = req->ResponseBytes,
        .lkey = req->OutBuffer.LKey,
    };

    ibv_send_wr wr = {
        .wr_id = send->wr.wr_id,
        .sg_list = &sg_list,
        .num_sge = 1,
        .opcode = IBV_WR_RDMA_WRITE,
    };

    wr.wr.rdma.rkey = req->Out.RKey;
    wr.wr.rdma.remote_addr = req->Out.Address;

    try {
        auto& cc = req->CallContext;

        StoreRequest(send, std::move(req));
        Verbs->PostSend(Connection->qp, &wr);
        RDMA_TRACE(send << " posted");

        LWTRACK(WriteResponseDataStarted, cc->LWOrbit, cc->RequestId);
        Counters->WriteResponseStarted();

    } catch (const TServiceError& e) {
        RDMA_ERROR(send << " " << e.what());
        Counters->Error();
        HandleSendError(send);
        return;
    }
}

void TServerSession::WriteResponseDataCompleted(TSendWr* send) noexcept
{
    auto req = ExtractRequest(send);

    if (req == nullptr) {
        RDMA_WARN(
            "RDMA_WRITE " << TWorkRequestId(send->wr.wr_id)
                          << " request is empty");
        return;
    }

    Y_ABORT_UNLESS(req->State == ERequestState::WriteResponseData);

    Counters->WriteResponseCompleted();

    LWTRACK(
        WriteResponseDataCompleted,
        req->CallContext->LWOrbit,
        req->CallContext->RequestId);

    SendResponse(std::move(req), send);
}

void TServerSession::SendResponse(TRequestPtr req, TSendWr* send) noexcept
{
    req->State = ERequestState::SendResponse;

    auto* responseMsg = send->Message();
    Zero(*responseMsg);

    InitMessageHeader(responseMsg, RDMA_PROTO_VERSION);

    responseMsg->ReqId = req->ReqId;
    responseMsg->Status = req->Status;
    responseMsg->ResponseBytes = req->ResponseBytes;

    try {
        auto& cc = req->CallContext;

        StoreRequest(send, std::move(req));
        Verbs->PostSend(Connection->qp, &send->wr);
        RDMA_TRACE(send << " posted");

        LWTRACK(SendResponseStarted, cc->LWOrbit, cc->RequestId);
        Counters->SendResponseStarted();

    } catch (const TServiceError& e) {
        RDMA_ERROR(send << " " << e.what());
        Counters->Error();
        HandleSendError(send);
        return;
    }
}

void TServerSession::SendResponseCompleted(TSendWr* send) noexcept
{
    auto req = ExtractRequest(send);

    if (req == nullptr) {
        RDMA_WARN(send << " request is empty");
        return;
    }

    Y_ABORT_UNLESS(req->State == ERequestState::SendResponse);

    LWTRACK(
        SendResponseCompleted,
        req->CallContext->LWOrbit,
        req->CallContext->RequestId);

    Counters->SendResponseCompleted();
    Counters->RequestCompleted();
    FreeRequest(std::move(req), send);
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
        Y_ABORT_UNLESS(event->listen_id && event->listen_id->context);
        return static_cast<TServerEndpoint*>(event->listen_id->context);
    }

    void SendResponse(void* context, size_t responseBytes) override
    {
        TRequestPtr req(static_cast<TRequest*>(context));
        req->Status = RDMA_PROTO_OK;
        req->ResponseBytes = responseBytes;

        if (auto session = req->Session.lock()) {
            session->EnqueueRequest(std::move(req));
        }
    }

    void SendError(void* context, ui32 error, TStringBuf message) override
    {
        TRequestPtr req(static_cast<TRequest*>(context));
        req->Status = RDMA_PROTO_FAIL;
        req->ResponseBytes = SerializeError(
            error,
            message,
            static_cast<TStringBuf>(req->OutBuffer));

        if (auto session = req->Session.lock()) {
            session->EnqueueRequest(std::move(req));
        }
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

    NVerbs::TConnectionPtr CreateConnection(ui8 tos)
    {
        return Verbs->CreateConnection(
            EventChannel.get(),
            nullptr,   // context
            RDMA_PS_TCP,
            tos);
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
                    Y_ABORT_UNLESS(event.data.ptr == EventChannel.get());
                    HandleConnectionEvents();
                }
            }
        }

        return nullptr;
    }

    NVerbs::TConnectionEventPtr GetConnectionEvent()
    {
        try {
            return Verbs->GetConnectionEvent(EventChannel.get());

        } catch (const TServiceError& e) {
            RDMA_ERROR(e.what());
            return NVerbs::NullPtr;
        }
    }

    void HandleConnectionEvents()
    {
        while (auto event = GetConnectionEvent()) {
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
    enum EPollEvent
    {
        Completion = 0,
        Request = 1,
    };

private:
    NVerbs::IVerbsPtr Verbs;

    TServerConfigPtr Config;
    TLog Log;

    TRCUList<TServerSessionPtr> Sessions;
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

    // save session so it won't get destroyed
    void Acquire(TServerSessionPtr session)
    {
        Sessions.Add(std::move(session));
    }

    // stop polling and forget about this session
    void Release(TServerSession* session)
    {
        if (Config->WaitMode == EWaitMode::Poll) {
            PollHandle.Detach(session->CompletionChannel->fd);
        }

        Sessions.Delete([=](auto other) {
            return session == other.get();
        });
    }

    // start polling session events
    void Attach(TServerSession* session)
    {
        if (Config->WaitMode == EWaitMode::Poll) {
            PollHandle.Attach(
                session->CompletionChannel->fd,
                EPOLLIN,
                PtrEventTag(session, EPollEvent::Completion));

            PollHandle.Attach(
                session->RequestEvent.Handle(),
                EPOLLIN,
                PtrEventTag(session, EPollEvent::Request));

            Verbs->RequestCompletionEvent(session->CompletionQueue.get(), 0);
        }
    }

    auto GetSessions() {
        return Sessions.Get();
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
        auto* session = PtrFromTag<TServerSession>(event.data.ptr);

        switch (EventFromTag(event.data.ptr)) {
            case EPollEvent::Completion:
                session->HandleCompletionEvents();
                break;

            case EPollEvent::Request:
                session->HandleInputRequests();
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
        bool hasWork = false;
        auto sessions = Sessions.Get();

        for (const auto& session: *sessions) {
            hasWork |= session->HandleInputRequests();
            hasWork |= session->HandleCompletionEvents();
        }

        return hasWork;
    }

    void DisconnectFlushed()
    {
        auto sessions = Sessions.Get();

        for (const auto& session: *sessions) {
            if (session->IsFlushed()) {
                Release(session.get());
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

            DisconnectFlushed();
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

    TRdmaObservabilityProvider ObservabilityProvider;

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
        TRdmaObservabilityProvider observabilityProvider,
        TServerConfigPtr config);

    // called from external thread
    void Start() override;
    void Stop() override;
    IServerEndpointPtr StartEndpoint(
        TString host,
        ui32 port,
        IServerHandlerPtr handler) override;
    void DumpHtml(IOutputStream& out) const override;

private:
    // called from external thread
    void Listen(TServerEndpoint* endpoint);

    // called from CM thread
    void HandleConnectionEvent(rdma_cm_event* event) noexcept override;
    void HandleConnectRequest(
        TServerEndpoint* endpoint, rdma_cm_event* event) noexcept;
    void Accept(TServerEndpoint* endpoint, rdma_cm_event* event) noexcept;
    void HandleConnected(TServerSession* session) noexcept;
    void HandleDisconnected(TServerSession* session) noexcept;
    void Reject(rdma_cm_id* id, int status) noexcept;
    TCompletionPoller* PickPoller() noexcept;
};

////////////////////////////////////////////////////////////////////////////////

TServer::TServer(
        NVerbs::IVerbsPtr verbs,
        TRdmaObservabilityProvider observabilityProvider,
        TServerConfigPtr config)
    : Verbs(std::move(verbs))
    , ObservabilityProvider(std::move(observabilityProvider))
    , Config(std::move(config))
    , Counters(new TEndpointCounters())
{
    // check basic functionality for early problem detection
    Verbs->GetDeviceList();
    Verbs->GetAddressInfo("localhost", 10020, nullptr);
}

void TServer::Start()
{
    Log = ObservabilityProvider.CreateLog();

    RDMA_DEBUG("start server");

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

    } catch (const TServiceError& e) {
        RDMA_ERROR("unable to start server: " << e.what());
        Counters->Error();
        Stop();
    }
}

void TServer::Stop()
{
    RDMA_DEBUG("stop server");

    if (ConnectionPoller) {
        ConnectionPoller->Stop();
        ConnectionPoller.reset();
    }

    for (auto& poller: CompletionPollers) {
        poller->Stop();
    }

    CompletionPollers.clear();
}

// implements IServer
IServerEndpointPtr TServer::StartEndpoint(
    TString host,
    ui32 port,
    IServerHandlerPtr handler)
{
    if (ConnectionPoller == nullptr) {
        RDMA_ERROR("unable to start endpoint: connection poller is down");
        return nullptr;
    }

    try {
        auto endpoint = std::make_shared<TServerEndpoint>(
            Verbs,
            ConnectionPoller->CreateConnection(Config->IpTypeOfService),
            std::move(host),
            port,
            std::move(handler));

        Listen(endpoint.get());

        with_lock (EndpointsLock) {
            Endpoints.emplace_back(endpoint);
        }

        return endpoint;

    } catch (const TServiceError& e) {
        RDMA_ERROR("unable to start endpoint: " << e.what());
        Counters->Error();
        return nullptr;
    }
}

void TServer::Listen(TServerEndpoint* endpoint)
{
    endpoint->Connection.get();

    // find the first non local address of the specified interface
    for (auto& interface: NAddr::GetNetworkInterfaces()) {
        if (interface.Name == Config->SourceInterface &&
            GetScopeId(interface.Address->Addr()) == 0)
        {
            endpoint->Host = NAddr::PrintHost(*interface.Address);
            break;
        }
    }

    rdma_addrinfo hints = {
        .ai_flags = RAI_PASSIVE,
        .ai_port_space = RDMA_PS_TCP,
    };

    auto addrinfo = Verbs->GetAddressInfo(
        endpoint->Host,
        endpoint->Port,
        &hints);

    RDMA_INFO("listen on " << NVerbs::PrintAddressAndPort(addrinfo->ai_src_addr));

    Verbs->BindAddress(endpoint->Connection.get(), addrinfo->ai_src_addr);
    Verbs->Listen(endpoint->Connection.get(), Config->Backlog);
}

// implements IServer
void TServer::DumpHtml(IOutputStream& out) const
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
                    TABLEH() { out << "CompletedRequests"; }
                    TABLEH() { out << "ThrottledRequests"; }
                    TABLEH() { out << "AbortedRequests"; }
                    TABLEH() { out << "ActiveSend"; }
                    TABLEH() { out << "ActiveRecv"; }
                    TABLEH() { out << "ActiveRead"; }
                    TABLEH() { out << "ActiveWrite"; }
                    TABLEH() { out << "Errors"; }
                }
                TABLER() {
                    TABLED() { out << Counters->QueuedRequests->Val(); }
                    TABLED() { out << Counters->ActiveRequests->Val(); }
                    TABLED() { out << Counters->CompletedRequests->Val(); }
                    TABLED() { out << Counters->ThrottledRequests->Val(); }
                    TABLED() { out << Counters->AbortedRequests->Val(); }
                    TABLED() { out << Counters->ActiveSend->Val(); }
                    TABLED() { out << Counters->ActiveRecv->Val(); }
                    TABLED() { out << Counters->ActiveRead->Val(); }
                    TABLED() { out << Counters->ActiveWrite->Val(); }
                    TABLED() { out << Counters->Errors->Val(); }
                }
            }
        }

        TAG(TH4) { out << "Endpoints"; }
        TABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Host"; }
                    TABLEH() { out << "Port"; }
                }
            }

            with_lock (EndpointsLock) {
                for (auto& ep : Endpoints) {
                    TABLER() {
                        TABLED() { out << ep->Host; }
                        TABLED() { out << ep->Port; }
                    }
                }
            }
        }

        for (size_t i = 0; i < CompletionPollers.size(); ++i) {
            auto& poller = CompletionPollers[i];
            auto sessions = poller->GetSessions();

            if (sessions->empty()) {
                continue;
            }

            TAG(TH4) {
                out << "Poller-" << i << " ServerSessions"
                    << " <font color=gray>" << sessions->size() << "</font>";
            }

            TABLE_SORTABLE_CLASS("table table-bordered") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "Id"; }
                        TABLEH() { out << "Address"; }
                        TABLEH() { out << "Magic"; }
                    }
                }

                for (auto& session: *sessions) {
                    TABLER() {
                        TABLED() { out << session->Id; }
                        TABLED() {
                            out << Verbs->GetPeer(session->Connection.get());;
                        }
                        TABLED() {
                            Printf(
                                out,
                                "%08X:%08X",
                                session->SendMagic,
                                session->RecvMagic);
                        }
                    }
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

// implements IConnectionEventHandler
void TServer::HandleConnectionEvent(rdma_cm_event* event) noexcept
{
    RDMA_INFO(NVerbs::GetEventName(event->event) << " received");

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
            HandleConnected(TServerSession::FromEvent(event));
            break;

        case RDMA_CM_EVENT_CONNECT_ERROR:
        case RDMA_CM_EVENT_DISCONNECTED:
            HandleDisconnected(TServerSession::FromEvent(event));
            break;

        case RDMA_CM_EVENT_DEVICE_REMOVAL:
        case RDMA_CM_EVENT_ADDR_CHANGE:
            // TODO
            break;
    }
}

void TServer::HandleConnectRequest(
    TServerEndpoint* endpoint,
    rdma_cm_event* event) noexcept
{
    const rdma_conn_param* connectParams = &event->param.conn;

    RDMA_DEBUG("validate " << Verbs->GetPeer(event->id));

    if (connectParams->private_data == nullptr ||
        connectParams->private_data_len < sizeof(TConnectMessage) ||
        ParseMessageHeader(connectParams->private_data) < RDMA_PROTO_VERSION)
    {
        return Reject(event->id, RDMA_PROTO_INVALID_REQUEST);
    }

    const auto* connectMsg = static_cast<const TConnectMessage*>(
        connectParams->private_data);

    if (Config->StrictValidation) {
        if (connectMsg->SendQueueSize > Config->RecvQueueSize) {
            RDMA_ERROR(
                "Failed to validate connect message. Client's send queue size "
                << connectMsg->SendQueueSize
                << " is greater than server's recv queue size "
                << Config->RecvQueueSize);
            return Reject(event->id, RDMA_PROTO_CONFIG_MISMATCH);
        }
        if (Config->SendQueueSize > connectMsg->RecvQueueSize) {
            RDMA_ERROR(
                "Failed to validate connect message. Client's recv queue size "
                << connectMsg->RecvQueueSize
                << " is less than server's send queue size "
                << Config->SendQueueSize);
            return Reject(event->id, RDMA_PROTO_CONFIG_MISMATCH);
        }
        if (connectMsg->MaxBufferSize > Config->MaxBufferSize) {
            RDMA_ERROR(
                "Failed to validate connect message. Client's max buffer size "
                << connectMsg->MaxBufferSize
                << " is greater than server's max buffer size "
                << Config->MaxBufferSize);
            return Reject(event->id, RDMA_PROTO_CONFIG_MISMATCH);
        }
    }

    Accept(endpoint, event);
}

void TServer::Accept(TServerEndpoint* endpoint, rdma_cm_event* event) noexcept
{
    auto session = std::make_shared<TServerSession>(
        Verbs,
        NVerbs::WrapPtr(event->id),
        PickPoller(),
        endpoint->Handler,
        Config,
        Counters,
        Log);

    try {
        session->CreateQP();

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

        RDMA_DEBUG("accept " << Verbs->GetPeer(event->id));
        Verbs->Accept(event->id, &acceptParams);

        // transfer session ownership to the poller
        session->CompletionPoller->Acquire(session);

    } catch (const TServiceError& e) {
        RDMA_ERROR(e.what())
        Counters->Error();
        Reject(event->id, RDMA_PROTO_FAIL);
    }
}

void TServer::HandleConnected(TServerSession* session) noexcept
{
    try {
        session->Start();
        session->CompletionPoller->Attach(session);
        RDMA_INFO(session->Log, "connected");

    } catch (const TServiceError& e) {
        RDMA_ERROR(e.what());
        Counters->Error();
        session->Stop();
    }
}

void TServer::HandleDisconnected(TServerSession* session) noexcept
{
    RDMA_INFO(
        session->Log,
        "disconnect from " << Verbs->GetPeer(session->Connection.get()));
    session->Flush();
}

void TServer::Reject(rdma_cm_id* id, int status) noexcept
{
    RDMA_INFO("reject " << Verbs->GetPeer(id) << " with status " << status);

    TRejectMessage rejectMsg = {
        .Status = SafeCast<ui16>(status),
        .QueueSize =
            SafeCast<ui16>(Config->SendQueueSize + Config->RecvQueueSize),
        .MaxBufferSize = SafeCast<ui32>(Config->MaxBufferSize),
    };
    InitMessageHeader(&rejectMsg, RDMA_PROTO_VERSION);

    try {
        Verbs->Reject(id, &rejectMsg, sizeof(TRejectMessage));

    } catch (const TServiceError& e) {
        Counters->Error();
        RDMA_ERROR(e.what());
    }
}

TCompletionPoller* TServer::PickPoller() noexcept
{
    size_t index = RandomNumber(CompletionPollers.size());
    return CompletionPollers[index].get();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

inline IOutputStream& operator<<(IOutputStream& out, TSendWr* send)
{
    out << GetOpcode(send) << " " << TWorkRequestId(send->wr.wr_id);
    if (auto req = send->Context<TRequest*>()) {
        out << " [request=" << req->ReqId << "]";
    }
    return out;
}

inline IOutputStream& operator<<(IOutputStream& out, TRecvWr* recv)
{
    out << "RECV " << TWorkRequestId(recv->wr.wr_id);
    if (auto msg = recv->Message()) {
        if (auto ver = ParseMessageHeader(msg); ver == RDMA_PROTO_VERSION) {
            out << " [request=" << msg->ReqId << "]";
        }
    }
    return out;
}

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    NVerbs::IVerbsPtr verbs,
    TRdmaObservabilityProvider observabilityProvider,
    TServerConfigPtr config)
{
    return std::make_shared<TServer>(
        std::move(verbs),
        std::move(observabilityProvider),
        std::move(config));
}

}   // namespace NCloud::NStorage::NRdma
