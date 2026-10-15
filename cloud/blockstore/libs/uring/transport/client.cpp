#include "client.h"

#include "connection.h"
#include "counters.h"
#include "protocol.h"
#include "ring.h"
#include "socket.h"

#include <cloud/storage/core/libs/common/backoff_delay_provider.h>
#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/hash.h>
#include <util/generic/utility.h>
#include <util/generic/vector.h>
#include <util/network/socket.h>
#include <util/stream/format.h>
#include <util/string/builder.h>
#include <util/system/guard.h>
#include <util/system/spinlock.h>

#include <errno.h>
#include <sys/socket.h>

#include <atomic>
#include <functional>
#include <memory>
#include <optional>
#include <utility>

namespace NCloud::NBlockStore::NUring {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 TAG_CONNECT = 1;
constexpr TDuration INITIAL_RECONNECT_DELAY = TDuration::MilliSeconds(100);

struct TAddress
{
    sockaddr_storage Addr{};
    socklen_t Len = 0;
};

TResultOrError<TAddress> Resolve(const TString& host, ui16 port)
{
    try {
        TNetworkAddress address(host, port);
        const addrinfo& ai = *address.Begin();
        TAddress result;
        memcpy(&result.Addr, ai.ai_addr, ai.ai_addrlen);
        result.Len = ai.ai_addrlen;
        return result;
    } catch (...) {
        return MakeError(
            E_FAIL,
            TStringBuilder() << "cannot resolve " << host << ": "
                             << CurrentExceptionMessage());
    }
}

size_t GetGuardedSize(const TGuardedSgList& sgList)
{
    auto guard = sgList.Acquire();
    return guard ? SgListGetSize(guard.Get()) : 0;
}

void Deliver(
    TClientRequestPtr req,
    ui32 status,
    TStringBuf proto,
    size_t dataBytes)
{
    auto handler = req->Handler;
    handler->HandleResponse(std::move(req), status, proto, dataBytes);
}

void DeliverError(TClientRequestPtr req, const NProto::TError& error)
{
    Deliver(std::move(req), error.GetCode(), error.SerializeAsString(), 0);
}

////////////////////////////////////////////////////////////////////////////////

class TClientEndpoint final
    : public IClientEndpoint
    , public IConnectionHandler
    , public IIoHandler
    , public ITickable
    , public std::enable_shared_from_this<TClientEndpoint>
{
private:
    enum class EState
    {
        Idle,
        Resolving,
        Connecting,
        Connected,
        Stopping,
    };

    struct TActive
    {
        TClientRequestPtr Request;
        TCallContextBasePtr CallContext;
        ui64 Bytes = 0;
        TInstant Deadline;
        // TGuard has no default ctor and no move assignment, so both guards
        // live in an optional: emplace to take, reset to release
        std::optional<TGuardedSgList::TGuard> SendGuard;
        std::optional<TGuardedSgList::TGuard> RecvGuard;
        // nothing refers to the buffers until a frame is handed over
        bool SendDone = true;
        bool RecvInFlight = false;
        // The first outcome decided wins and is delivered once no op refers
        // to the buffers of the request. Until then Proto may hold the proto
        // of a response whose payload is still being read.
        std::optional<ui32> Status;
        TString Proto;
        size_t DataBytes = 0;
    };

    using TActiveMap = THashMap<ui64, TActive>;

    // Exactly one op is in flight from the first connect to the last reply.
    struct THandshake
    {
        TExactIo Io;
        TIoOp* ConnectOp = nullptr;
        std::function<void(int error)> OnConnect;
        TAddress Address;
        int MainFd = -1;
        int DataFd = -1;
        THello Hello;
        TWelcome Welcome;
        TDataHello DataHello;
        TDataWelcome DataWelcome;
        TInstant Deadline;
        bool Cancelled = false;

        explicit THandshake(TRing& ring)
            : Io(ring)
        {}
    };

    TRing& Ring;
    ITaskQueue& Resolver;
    const TClientConfigPtr Config;
    const TString Host;
    const ui16 Port;
    IClientEndpointHandlerPtr EventHandler;
    const TCountersPtr Counters;
    TLog Log;

    // ring thread only
    EState State = EState::Idle;
    std::unique_ptr<THandshake> Handshake;
    std::unique_ptr<TConnection> Connection;
    TConnectionParams Params;
    TActiveMap Active;
    TBackoffDelayProvider Backoff;
    TInstant NextConnectAttempt;
    bool UnavailableReported = false;

    std::atomic<ui64> NextRequestId = 1;
    std::atomic<ui32> InflightRequests = 0;
    std::atomic<ui64> InflightBytes = 0;
    std::atomic<ui64> MaxInflightBytes;

    // Guards the handoff to the ring: once Stopped is set the ring may be
    // gone, so a request is failed inline instead. Written on the ring thread
    // under the lock, read there without it and under it anywhere else.
    TAdaptiveLock Lock;
    bool StopRequested = false;
    bool Stopped = false;
    NThreading::TPromise<void> StopPromise = NThreading::NewPromise();

public:
    TClientEndpoint(
        TRing& ring,
        ITaskQueue& resolver,
        TClientConfigPtr config,
        TString host,
        ui16 port,
        IClientEndpointHandlerPtr eventHandler,
        TCountersPtr counters,
        TLog log)
        : Ring(ring)
        , Resolver(resolver)
        , Config(std::move(config))
        , Host(std::move(host))
        , Port(port)
        , EventHandler(std::move(eventHandler))
        , Counters(std::move(counters))
        , Log(std::move(log))
        , Backoff(INITIAL_RECONNECT_DELAY, Config->MaxReconnectDelay)
        , MaxInflightBytes(Config->MaxInflightBytes)
    {}

    void Start()
    {
        Ring.Execute(
            [self = shared_from_this()]
            {
                self->Ring.AddTickable(self.get());
                self->BeginConnect();
            });
    }

    ui64 SendRequest(
        TClientRequestPtr req,
        TCallContextBasePtr callContext) override
    {
        const ui64 id = NextRequestId.fetch_add(1);
        const ui64 bytes = req->Proto.size() + GetGuardedSize(req->SendData) +
                           GetGuardedSize(req->RecvData);
        if (!Admit(bytes)) {
            Counters->Rejected->Inc();
            Reject(std::move(req), MakeError(E_REJECTED, "inflight limit"));
            return id;
        }

        // a command has to be copyable, so the request travels as a raw
        // pointer; a command posted before Stopped always runs
        auto* raw = req.release();
        const bool posted = TryPost(
            [self = shared_from_this(), raw, callContext, id, bytes] {
                self->StartRequest(
                    TClientRequestPtr(raw),
                    callContext,
                    id,
                    bytes);
            });
        if (!posted) {
            Counters->Rejected->Inc();
            Release(bytes);
            DeliverError(
                TClientRequestPtr(raw),
                MakeError(E_REJECTED, "endpoint is stopped"));
        }
        return id;
    }

    void CancelRequest(ui64 id) override
    {
        TryPost(
            [self = shared_from_this(), id]
            {
                auto it = self->Active.find(id);
                if (it != self->Active.end()) {
                    self->Finish(
                        it,
                        MakeError(E_CANCELLED, "request cancelled"));
                }
            });
    }

    NThreading::TFuture<void> Stop() override
    {
        with_lock (Lock) {
            if (!StopRequested) {
                StopRequested = true;
                Ring.Execute([self = shared_from_this()]
                             { self->StopOnRing(); });
            }
        }
        return StopPromise.GetFuture();
    }

    void TryForceReconnect() override
    {
        TryPost(
            [self = shared_from_this()]
            {
                self->Backoff.Reset();
                self->NextConnectAttempt = TInstant::Zero();
            });
    }

    void OnTick(TInstant now) override
    {
        TVector<ui64> expired;
        for (const auto& [id, active]: Active) {
            if (!active.Status && active.Deadline <= now) {
                expired.push_back(id);
            }
        }
        for (ui64 id: expired) {
            auto it = Active.find(id);
            if (it != Active.end()) {
                Counters->Timeouts->Inc();
                Finish(it, MakeError(E_TIMEOUT, "request timed out"));
            }
        }

        if (Handshake && Handshake->Deadline <= now) {
            CancelHandshake();
        }
        if (State == EState::Idle && now >= NextConnectAttempt) {
            BeginConnect();
        }
    }

private:
    bool TryPost(TRing::TCommand command)
    {
        with_lock (Lock) {
            if (Stopped) {
                return false;
            }
            Ring.Execute(std::move(command));
            return true;
        }
    }

    void Reject(TClientRequestPtr req, NProto::TError error)
    {
        auto* raw = req.release();
        const bool posted = TryPost(
            [raw, error] { DeliverError(TClientRequestPtr(raw), error); });
        if (!posted) {
            DeliverError(TClientRequestPtr(raw), error);
        }
    }

    bool Admit(ui64 bytes)
    {
        ui32 requests = InflightRequests.load();
        do {
            if (requests >= Config->MaxInflightRequests) {
                return false;
            }
        } while (
            !InflightRequests.compare_exchange_weak(requests, requests + 1));

        const ui64 limit = MaxInflightBytes.load();
        ui64 current = InflightBytes.load();
        do {
            if (current + bytes > limit) {
                --InflightRequests;
                return false;
            }
        } while (
            !InflightBytes.compare_exchange_weak(current, current + bytes));
        return true;
    }

    void Release(ui64 bytes)
    {
        InflightBytes -= bytes;
        --InflightRequests;
    }

    ////////////////////////////////////////////////////////////////////////////
    // requests

    void StartRequest(
        TClientRequestPtr req,
        TCallContextBasePtr callContext,
        ui64 id,
        ui64 bytes)
    {
        if (State != EState::Connected || Connection->IsClosing()) {
            Counters->Rejected->Inc();
            Release(bytes);
            DeliverError(
                std::move(req),
                MakeError(E_REJECTED, "not connected"));
            return;
        }

        const TDuration timeout =
            req->Timeout ? req->Timeout : Config->MaxResponseDelay;
        auto it = Active.emplace(id, TActive{}).first;
        auto& active = it->second;
        active.Request = std::move(req);
        active.CallContext = std::move(callContext);
        active.Bytes = bytes;
        active.Deadline = TInstant::Now() + timeout;

        active.SendGuard.emplace(active.Request->SendData.Acquire());
        if (!*active.SendGuard) {
            Counters->Errors->Inc();
            Finish(it, MakeError(E_CANCELLED, "send buffer is closed"));
            return;
        }
        const TSgList& sgList = active.SendGuard->Get();
        const size_t dataLen = SgListGetSize(sgList);
        const TString& proto = active.Request->Proto;
        if (proto.size() > Params.MaxProtoSize || dataLen > Params.MaxDataSize)
        {
            Counters->Errors->Inc();
            Finish(
                it,
                MakeError(
                    E_ARGUMENT,
                    TStringBuilder()
                        << "request of " << proto.size() << " proto and "
                        << dataLen << " data bytes exceeds the negotiated "
                        << Params.MaxProtoSize << " and "
                        << Params.MaxDataSize));
            return;
        }

        auto frame = std::make_shared<TOutFrame>();
        frame->Head =
            TString::Uninitialized(sizeof(TFrameHeader) + proto.size());
        TFrameHeader h;
        h.Type = FRAME_REQUEST;
        h.RequestId = id;
        h.ProtoLen = proto.size();
        memcpy(frame->Head.begin(), &h, sizeof(h));
        memcpy(frame->Head.begin() + sizeof(h), proto.data(), proto.size());

        if (Config->DirectSgIoEnabled) {
            frame->Data = sgList;
            frame->OnDone = [this, id]
            {
                OnSent(id);
            };
            Counters->DirectSendBytes->Add(dataLen);
        } else {
            std::shared_ptr<TString> staging;
            if (dataLen) {
                staging =
                    std::make_shared<TString>(TString::Uninitialized(dataLen));
                SgListCopy(sgList, TBlockDataRef{staging->data(), dataLen});
                frame->Data = {TBlockDataRef{staging->data(), dataLen}};
                Counters->StagingCopyBytes->Add(dataLen);
            }
            active.SendGuard.reset();
            frame->OnDone = [this, id, staging = std::move(staging)]
            {
                OnSent(id);
            };
        }

        active.SendDone = false;
        Counters->Requests->Inc();
        Connection->Send(std::move(frame));
    }

    void OnSent(ui64 id)
    {
        auto it = Active.find(id);
        Y_ABORT_UNLESS(it != Active.end(), "request %lu sent twice", id);
        it->second.SendDone = true;
        it->second.SendGuard.reset();
        TryComplete(it);
    }

    void Finish(TActiveMap::iterator it, const NProto::TError& error)
    {
        Finish(it, error.GetCode(), error.SerializeAsString(), 0);
    }

    void Finish(
        TActiveMap::iterator it,
        ui32 status,
        TStringBuf proto,
        size_t dataBytes)
    {
        auto& active = it->second;
        if (!active.Status) {
            active.Status = status;
            active.Proto = TString(proto);
            active.DataBytes = dataBytes;
        }
        TryComplete(it);
    }

    void TryComplete(TActiveMap::iterator it)
    {
        const auto& active = it->second;
        if (!active.Status || !active.SendDone || active.RecvInFlight) {
            return;
        }
        auto done = std::move(it->second);
        Active.erase(it);
        done.SendGuard.reset();
        done.RecvGuard.reset();
        Release(done.Bytes);
        Deliver(
            std::move(done.Request),
            *done.Status,
            done.Proto,
            done.DataBytes);
    }

    void FinishWithErrorFrame(TActiveMap::iterator it, TStringBuf proto)
    {
        Counters->Errors->Inc();
        Finish(it, ParseError(proto));
    }

    void ProtocolError(TStringBuf what)
    {
        STORAGE_WARN(
            "protocol error from " << Host << ":" << Port
                                   << ", closing: " << what);
        Connection->Close(MakeError(
            E_REJECTED,
            TStringBuilder() << "protocol error: " << what));
    }

    // The request a response fills, or end() when there is none: the id is
    // unknown, the request is decided, or it is a second response.
    TActiveMap::iterator FindResponseTarget(ui64 id)
    {
        auto it = Active.find(id);
        if (it == Active.end()) {
            return it;
        }
        if (it->second.RecvInFlight) {
            // its payload is still being read into the very same buffer
            ProtocolError(
                TStringBuilder() << "second response for request " << id);
            return Active.end();
        }
        if (it->second.Status) {
            return Active.end();   // timed out or cancelled
        }
        return it;
    }

    NProto::TError TakeRecvGuard(TActive& active, ui32 dataLen)
    {
        active.RecvGuard.emplace(active.Request->RecvData.Acquire());
        if (!*active.RecvGuard) {
            Counters->Errors->Inc();
            return MakeError(E_CANCELLED, "receive buffer is closed");
        }
        const size_t size = SgListGetSize(active.RecvGuard->Get());
        if (size != dataLen) {
            Counters->Errors->Inc();
            return MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "response carries " << dataLen
                    << " bytes, the receive buffer holds " << size);
        }
        return {};
    }

    void OnFrame(
        const TFrameHeader& header,
        TStringBuf proto,
        TStringBuf inlineData) override
    {
        if (header.Type == FRAME_REQUEST) {
            Connection->Close(MakeError(E_REJECTED, "unexpected frame type"));
            return;
        }
        auto it = FindResponseTarget(header.RequestId);
        if (it == Active.end()) {
            return;
        }
        if (header.Type == FRAME_ERROR) {
            FinishWithErrorFrame(it, proto);
            return;
        }
        if (inlineData.empty()) {
            Finish(it, S_OK, proto, 0);
            return;
        }
        auto& active = it->second;
        if (auto error = TakeRecvGuard(active, header.DataLen); HasError(error))
        {
            Finish(it, error);
            return;
        }
        SgListCopy(
            TBlockDataRef{inlineData.data(), inlineData.size()},
            active.RecvGuard->Get());
        Counters->InlineBytes->Add(inlineData.size());
        Finish(it, S_OK, proto, inlineData.size());
    }

    TDataSink OnOobFrame(const TFrameHeader& header, TStringBuf proto) override
    {
        if (header.Type == FRAME_REQUEST) {
            Connection->Close(MakeError(E_REJECTED, "unexpected frame type"));
            return {};
        }
        // an empty sink makes the connection read the payload and drop it
        auto it = FindResponseTarget(header.RequestId);
        if (it == Active.end()) {
            return {};
        }
        if (header.Type == FRAME_ERROR) {
            FinishWithErrorFrame(it, proto);
            return {};
        }
        auto& active = it->second;
        if (auto error = TakeRecvGuard(active, header.DataLen); HasError(error))
        {
            Finish(it, error);
            return {};
        }

        active.Proto = TString(proto);
        active.RecvInFlight = true;
        const ui64 id = header.RequestId;
        const ui32 dataLen = header.DataLen;
        TDataSink sink;
        if (Config->DirectSgIoEnabled) {
            sink.Target = active.RecvGuard->Get();
            sink.OnDone = [this, id, dataLen](const NProto::TError& error)
            {
                OnPayload(id, dataLen, error, nullptr);
            };
        } else {
            active.RecvGuard.reset();
            auto staging =
                std::make_shared<TString>(TString::Uninitialized(dataLen));
            sink.Target = {TBlockDataRef{staging->data(), dataLen}};
            sink.OnDone =
                [this, id, dataLen, staging](const NProto::TError& error)
            {
                OnPayload(id, dataLen, error, staging.get());
            };
        }
        return sink;
    }

    // An error here means the connection is closing, and the close decides
    // the outcome of the request unless something already has.
    void OnPayload(
        ui64 id,
        ui32 dataLen,
        const NProto::TError& error,
        const TString* staging)
    {
        auto it = Active.find(id);
        if (it == Active.end()) {
            ProtocolError(
                TStringBuilder() << "payload for a finished request " << id);
            return;
        }
        auto& active = it->second;
        active.RecvInFlight = false;
        if (HasError(error) || active.Status) {
            TryComplete(it);
            return;
        }

        if (staging) {
            active.RecvGuard.emplace(active.Request->RecvData.Acquire());
            if (!*active.RecvGuard) {
                Counters->Errors->Inc();
                Finish(it, MakeError(E_CANCELLED, "receive buffer is closed"));
                return;
            }
            SgListCopy(
                TBlockDataRef{staging->data(), dataLen},
                active.RecvGuard->Get());
            Counters->StagingCopyBytes->Add(dataLen);
        } else {
            Counters->DirectRecvBytes->Add(dataLen);
        }
        Counters->OobBytes->Add(dataLen);
        active.Status = S_OK;
        active.DataBytes = dataLen;
        TryComplete(it);
    }

    void FinishAll(const NProto::TError& error)
    {
        TVector<ui64> ids;
        for (const auto& [id, active]: Active) {
            ids.push_back(id);
        }
        for (ui64 id: ids) {
            auto it = Active.find(id);
            if (it != Active.end()) {
                Finish(it, error);
            }
        }
    }

    void OnConnectionClosed(const NProto::TError& error) override
    {
        // error lives in the connection this destroys
        const auto reason = error;
        Connection.reset();
        Counters->ActiveConnections->Dec();
        FinishAll(MakeError(E_REJECTED, "connection closed"));
        if (EventHandler) {
            EventHandler->HandleDisconnected();
        }
        if (State == EState::Stopping) {
            MaybeFinishStop();
            return;
        }
        ScheduleReconnect(reason);
    }

    ////////////////////////////////////////////////////////////////////////////
    // connect

    void BeginConnect()
    {
        State = EState::Resolving;
        Resolver.ExecuteSimple(
            [weak = weak_from_this(), host = Host, port = Port]
            {
                auto address = Resolve(host, port);
                if (auto self = weak.lock()) {
                    self->TryPost([self, address]
                                  { self->OnResolved(address); });
                }
            });
    }

    void OnResolved(const TResultOrError<TAddress>& address)
    {
        if (State != EState::Resolving) {
            return;   // stopped meanwhile
        }
        if (HasError(address)) {
            ScheduleReconnect(address.GetError());
            return;
        }
        State = EState::Connecting;
        Handshake = std::make_unique<THandshake>(Ring);
        Handshake->ConnectOp = Ring.AllocOp(this, TAG_CONNECT);
        Handshake->Address = address.GetResult();
        Handshake->Deadline = TInstant::Now() + Config->MaxResponseDelay;
        Dial(Handshake->MainFd, [this] { SendHello(); });
    }

    void Dial(int& fd, std::function<void()> next)
    {
        auto& h = *Handshake;
        fd = ::socket(h.Address.Addr.ss_family, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (fd < 0) {
            FailHandshake(MakeSystemError(errno, "socket"));
            return;
        }
        TSocketOptions options{
            .TcpUserTimeout = Config->TcpUserTimeout,
            .KeepAliveIdle = Config->KeepAliveIdle,
            .KeepAliveInterval = Config->KeepAliveInterval,
            .KeepAliveCount = Config->KeepAliveCount,
        };
        if (auto error = ApplySocketOptions(fd, options); HasError(error)) {
            FailHandshake(error);
            return;
        }
        h.OnConnect = [this, next = std::move(next)](int error)
        {
            if (StepSucceeded(error, "connect")) {
                next();
            }
        };
        Ring.Connect(
            h.ConnectOp,
            fd,
            reinterpret_cast<const sockaddr*>(&h.Address.Addr),
            h.Address.Len);
    }

    void OnIoComplete(TIoOp* op, int res, ui32) override
    {
        Y_ABORT_UNLESS(op->Tag == TAG_CONNECT);
        auto next = std::move(Handshake->OnConnect);
        next(res < 0 ? -res : 0);
    }

    // Ends the handshake unless the step succeeded and it was not cancelled
    // meanwhile.
    bool StepSucceeded(int error, TStringBuf what)
    {
        if (Handshake->Cancelled) {
            FailHandshake(MakeError(E_TIMEOUT, "handshake timed out"));
            return false;
        }
        if (error) {
            FailHandshake(MakeSystemError(error, what));
            return false;
        }
        return true;
    }

    void SendHello()
    {
        auto& h = *Handshake;
        h.Hello.OobMode = ToWire(Config->OobMode);
        h.Hello.MaxProtoSize = Config->MaxProtoSize;
        h.Hello.MaxDataSize = Config->MaxDataSize;
        h.Hello.MaxInflightBytes =
            Min<ui64>(Config->MaxInflightBytes, Max<ui32>());
        h.Hello.OobThreshold = Config->OobThreshold;
        h.Io.Send(
            h.MainFd,
            &h.Hello,
            sizeof(h.Hello),
            [this](int error)
            {
                if (StepSucceeded(error, "send hello")) {
                    RecvWelcome();
                }
            });
    }

    void RecvWelcome()
    {
        auto& h = *Handshake;
        h.Io.Recv(
            h.MainFd,
            &h.Welcome,
            sizeof(h.Welcome),
            [this](int error)
            {
                if (StepSucceeded(error, "recv welcome")) {
                    OnWelcome();
                }
            });
    }

    void OnWelcome()
    {
        const auto& w = Handshake->Welcome;
        if (w.Magic != MAGIC || w.Version != PROTOCOL_VERSION) {
            FailHandshake(MakeError(
                E_REJECTED,
                TStringBuilder() << "bad welcome: magic " << Hex(w.Magic)
                                 << ", version " << w.Version));
            return;
        }
        if (w.Status != STATUS_OK) {
            FailHandshake(MakeError(
                E_REJECTED,
                TStringBuilder() << "refused with status " << ui32(w.Status)));
            return;
        }
        // FromWire turns anything unknown into Auto, and the server only ever
        // answers with what it agreed on
        const bool knownMode =
            w.OobMode == OOB_WIRE_OFF || w.OobMode == OOB_WIRE_REQUIRED;
        std::optional<bool> oob;
        if (knownMode) {
            oob = NegotiateOob(Config->OobMode, FromWire(w.OobMode));
        }
        if (!oob) {
            FailHandshake(MakeError(
                E_REJECTED,
                TStringBuilder() << "cannot agree on the out-of-band mode, "
                                 << "server sent " << ui32(w.OobMode)));
            return;
        }
        if (!*oob) {
            OnConnected(false);
            return;
        }
        Dial(Handshake->DataFd, [this] { SendDataHello(); });
    }

    void SendDataHello()
    {
        auto& h = *Handshake;
        h.DataHello.DataToken = h.Welcome.DataToken;
        h.Io.Send(
            h.DataFd,
            &h.DataHello,
            sizeof(h.DataHello),
            [this](int error)
            {
                if (StepSucceeded(error, "send data hello")) {
                    RecvDataWelcome();
                }
            });
    }

    void RecvDataWelcome()
    {
        auto& h = *Handshake;
        h.Io.Recv(
            h.DataFd,
            &h.DataWelcome,
            sizeof(h.DataWelcome),
            [this](int error)
            {
                if (StepSucceeded(error, "recv data welcome")) {
                    OnDataWelcome();
                }
            });
    }

    void OnDataWelcome()
    {
        const auto& w = Handshake->DataWelcome;
        if (w.Magic != MAGIC || w.Version != PROTOCOL_VERSION ||
            w.Status != STATUS_OK)
        {
            FailHandshake(MakeError(
                E_REJECTED,
                TStringBuilder() << "data socket refused: magic "
                                 << Hex(w.Magic) << ", version " << w.Version
                                 << ", status " << ui32(w.Status)));
            return;
        }
        OnConnected(true);
    }

    void OnConnected(bool oob)
    {
        const auto& w = Handshake->Welcome;
        Params = TConnectionParams{
            .MaxProtoSize = Min(Config->MaxProtoSize, w.MaxProtoSize),
            .MaxDataSize = Min(Config->MaxDataSize, w.MaxDataSize),
            .SendWindowBytes = Config->SendWindowBytes,
            .OobEnabled = oob,
            .OobThreshold = Min(Config->OobThreshold, w.OobThreshold),
            .SendZeroCopy = Config->SendZeroCopyMode != ESendZeroCopyMode::Off,
            .SendZeroCopyThreshold = Config->SendZeroCopyThreshold,
            .Counters = Counters,
        };
        MaxInflightBytes =
            Min<ui64>(Config->MaxInflightBytes, w.MaxInflightBytes);
        const int mainFd = std::exchange(Handshake->MainFd, -1);
        const int dataFd = std::exchange(Handshake->DataFd, -1);
        DropHandshake();

        Connection = std::make_unique<TConnection>(
            Ring,
            Params,
            mainFd,
            dataFd,
            this,
            Log);
        Connection->Start();
        State = EState::Connected;
        Backoff.Reset();
        UnavailableReported = false;
        Counters->ActiveConnections->Inc();
        STORAGE_INFO("connected to " << Host << ":" << Port);
        if (EventHandler) {
            EventHandler->HandleConnected();
        }
    }

    void CancelHandshake()
    {
        auto& h = *Handshake;
        if (h.Cancelled) {
            return;
        }
        h.Cancelled = true;
        Ring.Cancel(h.ConnectOp);
        h.Io.Cancel();
    }

    // Nothing of the handshake may be in flight.
    void DropHandshake()
    {
        auto h = std::move(Handshake);
        CloseSocket(h->MainFd);
        CloseSocket(h->DataFd);
        Ring.FreeOp(h->ConnectOp);
    }

    void FailHandshake(const NProto::TError& error)
    {
        DropHandshake();
        if (State == EState::Stopping) {
            MaybeFinishStop();
            return;
        }
        ScheduleReconnect(error);
    }

    void ScheduleReconnect(const NProto::TError& error)
    {
        State = EState::Idle;
        const TDuration delay = Backoff.GetDelayAndIncrease();
        NextConnectAttempt = TInstant::Now() + delay;
        Counters->Reconnects->Inc();
        STORAGE_WARN(
            "no connection to " << Host << ":" << Port << ": "
                                << FormatError(error) << ", retrying in "
                                << delay);
        if (delay >= Config->MaxReconnectDelay && !UnavailableReported) {
            UnavailableReported = true;
            if (EventHandler) {
                EventHandler->HandleUnavailable();
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // stop

    void StopOnRing()
    {
        State = EState::Stopping;
        Ring.RemoveTickable(this);
        FinishAll(MakeError(E_REJECTED, "endpoint is stopping"));
        if (Connection) {
            Connection->Close(MakeError(E_REJECTED, "endpoint is stopping"));
        }
        if (Handshake) {
            CancelHandshake();
        }
        MaybeFinishStop();
    }

    void MaybeFinishStop()
    {
        if (Connection || Handshake) {
            return;
        }
        Y_DEBUG_ABORT_UNLESS(Active.empty());
        with_lock (Lock) {
            Stopped = true;
        }
        // breaks the cycle with a handler holding this endpoint
        EventHandler.reset();
        StopPromise.SetValue();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TClient final: public IClient
{
private:
    const ILoggingServicePtr Logging;
    const IMonitoringServicePtr Monitoring;
    const TClientConfigPtr Config;

    TLog Log;
    TCountersPtr Counters;
    TVector<std::unique_ptr<TRing>> Rings;
    ITaskQueuePtr Resolver;
    TVector<std::shared_ptr<TClientEndpoint>> Endpoints;
    size_t NextRing = 0;
    mutable TAdaptiveLock Lock;
    bool Started = false;

public:
    TClient(
        ILoggingServicePtr logging,
        IMonitoringServicePtr monitoring,
        TClientConfigPtr config)
        : Logging(std::move(logging))
        , Monitoring(std::move(monitoring))
        , Config(std::move(config))
    {}

    ~TClient() override
    {
        // the rings would otherwise wait for the connections nobody closed
        Stop();
    }

    void Start() override
    {
        const ui64 queued = ui64(Config->MaxInflightRequests) *
                            (ui64(Config->MaxDataSize) + Config->MaxProtoSize);
        Y_ABORT_UNLESS(
            Config->MaxInflightBytes >= queued,
            "MaxInflightBytes %lu rejects what MaxInflightRequests %u allows: "
            "%u requests of MaxDataSize %u plus MaxProtoSize %u need %lu",
            Config->MaxInflightBytes,
            Config->MaxInflightRequests,
            Config->MaxInflightRequests,
            Config->MaxDataSize,
            Config->MaxProtoSize,
            queued);

        Log = Logging->CreateLog("BLOCKSTORE_URING");
        Counters = MakeCounters(Monitoring, "uring_client");
        Resolver = CreateThreadPool("URING.RESOLVE", 1);
        Resolver->Start();
        for (ui32 i = 0; i < Max<ui32>(Config->IoThreads, 1); ++i) {
            auto ring = std::make_unique<TRing>(
                TStringBuilder() << "URING.CLI" << i,
                Config->QueueSize,
                Config->SendZeroCopyMode,
                Log);
            auto error = ring->Start();
            Y_ABORT_IF(
                HasError(error),
                "cannot start the uring client: %s",
                FormatError(error).c_str());
            Rings.push_back(std::move(ring));
        }
        Counters->SendZeroCopySupported->Set(Rings[0]->SendZeroCopySupported());
        with_lock (Lock) {
            Started = true;
        }
    }

    void Stop() override
    {
        TVector<std::shared_ptr<TClientEndpoint>> endpoints;
        with_lock (Lock) {
            if (!Started) {
                return;
            }
            Started = false;
            endpoints = Endpoints;
        }
        // an endpoint is done with its ring only once its Stop has resolved
        TVector<NThreading::TFuture<void>> stopped;
        for (auto& endpoint: endpoints) {
            stopped.push_back(endpoint->Stop());
        }
        for (auto& future: stopped) {
            future.GetValueSync();
        }
        Resolver->Stop();
        for (auto& ring: Rings) {
            ring->Stop();
        }
    }

    TResultOrError<IClientEndpointPtr> StartEndpoint(
        TString host,
        ui32 port,
        IClientEndpointHandlerPtr handler) override
    {
        if (port > Max<ui16>()) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder() << "bad port " << port);
        }
        // under the lock, so that Stop() sees every endpoint that has started
        std::shared_ptr<TClientEndpoint> endpoint;
        with_lock (Lock) {
            if (!Started) {
                return MakeError(E_INVALID_STATE, "client is not started");
            }
            endpoint = std::make_shared<TClientEndpoint>(
                *Rings[NextRing++ % Rings.size()],
                *Resolver,
                Config,
                std::move(host),
                port,
                std::move(handler),
                Counters,
                Log);
            endpoint->Start();
            Endpoints.push_back(endpoint);
        }
        return IClientEndpointPtr(endpoint);
    }

    void DumpHtml(IOutputStream& out) const override
    {
        Config->DumpHtml(out);
        with_lock (Lock) {
            out << "<p>endpoints: " << Endpoints.size() << "</p>";
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    TClientConfigPtr config)
{
    return std::make_shared<TClient>(
        std::move(logging),
        std::move(monitoring),
        std::move(config));
}

}   // namespace NCloud::NBlockStore::NUring
