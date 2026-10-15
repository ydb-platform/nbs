#include "server.h"

#include "connection.h"
#include "counters.h"
#include "protocol.h"
#include "ring.h"
#include "socket.h"

#include <cloud/storage/core/libs/common/aligned_buffer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/hash.h>
#include <util/generic/utility.h>
#include <util/generic/vector.h>
#include <util/random/random.h>
#include <util/stream/format.h>
#include <util/string/builder.h>
#include <util/system/guard.h>
#include <util/system/spinlock.h>

#include <errno.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <functional>
#include <memory>
#include <utility>

namespace NCloud::NBlockStore::NUring {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 TAG_ACCEPT = 100;
constexpr ui32 BLOCK_ALIGN = 4096;

// A handshake socket is read in two steps: the prefix is a whole TDataHello
// and the common head of THello, so the role byte decides whether anything
// more is coming. One buffer serves the read and the reply.
constexpr size_t HANDSHAKE_PREFIX = sizeof(TDataHello);
constexpr size_t HANDSHAKE_BUFFER = std::max(sizeof(THello), sizeof(TWelcome));

static_assert(sizeof(TDataHello) <= sizeof(THello));
static_assert(sizeof(TDataWelcome) <= HANDSHAKE_BUFFER);
static_assert(offsetof(THello, Magic) == offsetof(TDataHello, Magic));
static_assert(offsetof(THello, Version) == offsetof(TDataHello, Version));
static_assert(offsetof(THello, Role) == offsetof(TDataHello, Role));

constexpr TDuration LOG_THROTTLER_PERIOD = TDuration::Minutes(1);

// A queued frame costs more than the bytes it carries: the frame, the chunk
// the stream holds it by, the head string and the queue slot.
constexpr ui64 FRAME_OVERHEAD = 256;

// Errors accept(2) reports for the connection it took off the queue, which it
// tells us to treat as EAGAIN.
bool IsPendingConnectionError(int error)
{
    switch (error) {
        case ECONNABORTED:
        case EPROTO:
        case ENOPROTOOPT:
        case ENETDOWN:
        case ENETUNREACH:
        case ENONET:
        case EHOSTDOWN:
        case EHOSTUNREACH:
        case EOPNOTSUPP:
            return true;
        default:
            return false;
    }
}

// Conditions that leave the connection on the queue, so accept would keep
// failing on the same one. EPERM is here because the LSM hook runs before the
// dequeue, so a standing denial would spin.
bool IsResourceError(int error)
{
    return error == EMFILE || error == ENFILE || error == ENOBUFS ||
           error == ENOMEM || error == EPERM;
}

struct TSession;
using TSessionPtr = std::shared_ptr<TSession>;

////////////////////////////////////////////////////////////////////////////////

struct TServerRequest
{
    TSessionPtr Session;
    ui64 RequestId = 0;
    TString Proto;
    TAlignedBuffer Buffer;
    size_t DataLen = 0;

    TSgList DataSgList()
    {
        if (!DataLen) {
            return {};
        }
        return {TBlockDataRef{Buffer.Begin(), DataLen}};
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TSession
    : public IConnectionHandler
    , public std::enable_shared_from_this<TSession>
{
    TRing& Ring;
    const IServerHandlerPtr Handler;
    const TServerConfigPtr Config;
    const TCountersPtr Counters;
    TLog Log;

    int MainFd = -1;
    int DataFd = -1;

    std::unique_ptr<TConnection> Connection;
    TSessionPtr Self;   // keeps the session alive until the connection closes
    std::function<void()> Unregister;

    ui32 InflightRequests = 0;
    ui64 InflightBytes = 0;
    // Unsent rejections are budgeted apart from InflightBytes: a session that
    // overshoots the inflight limit with responses it was admitted for must
    // still be able to say no, and the number of rejections a peer can
    // provoke must not depend on how much it is being sent.
    ui64 RejectionBytes = 0;
    THashMap<TServerRequest*, std::unique_ptr<TServerRequest>> Requests;

    // Guards the handoff to the ring: once Stopped is set the ring may be
    // gone, so a response must be completed inline instead. Written on the
    // ring thread under the lock, read there without it and under it anywhere
    // else.
    TAdaptiveLock Lock;
    bool Stopped = false;

    TSession(
        TRing& ring,
        IServerHandlerPtr handler,
        TServerConfigPtr config,
        TCountersPtr counters,
        TLog log,
        int mainFd,
        int dataFd)
        : Ring(ring)
        , Handler(std::move(handler))
        , Config(std::move(config))
        , Counters(std::move(counters))
        , Log(std::move(log))
        , MainFd(mainFd)
        , DataFd(dataFd)
    {}

    ~TSession() override
    {
        // only until the connection takes them over
        CloseSocket(MainFd);
        CloseSocket(DataFd);
    }

    void Start(const TConnectionParams& params)
    {
        Connection = std::make_unique<TConnection>(
            Ring,
            params,
            std::exchange(MainFd, -1),
            std::exchange(DataFd, -1),
            this,
            Log);
        Counters->ActiveConnections->Inc();
        Connection->Start();
    }

    // Any thread. Returns false when the session is done with its ring and
    // the caller has to dispose of the request itself.
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

    void SendErrorFrame(ui64 requestId, ui32 code, TStringBuf message)
    {
        auto payload = SerializeError(code, message);
        const ui64 bytes =
            sizeof(TFrameHeader) + payload.size() + FRAME_OVERHEAD;
        if (RejectionBytes + bytes > Config->MaxProtoSize) {
            // the peer keeps asking without reading the answers
            Connection->Close(
                MakeError(E_REJECTED, "too many unsent rejections"));
            return;
        }
        RejectionBytes += bytes;
        auto frame = std::make_shared<TOutFrame>();
        frame->Head =
            TString::Uninitialized(sizeof(TFrameHeader) + payload.size());
        TFrameHeader h;
        h.Type = FRAME_ERROR;
        h.RequestId = requestId;
        h.ProtoLen = payload.size();
        memcpy(frame->Head.begin(), &h, sizeof(h));
        memcpy(frame->Head.begin() + sizeof(h), payload.data(), payload.size());
        frame->OnDone = [self = shared_from_this(), bytes]
        {
            self->RejectionBytes -= bytes;
        };
        Connection->Send(std::move(frame));
        Counters->Errors->Inc();
    }

    bool Admit(const TFrameHeader& header)
    {
        if (InflightRequests >= Config->MaxInflightRequests ||
            InflightBytes + header.ProtoLen + header.DataLen >
                Config->MaxInflightBytes)
        {
            SendErrorFrame(header.RequestId, E_REJECTED, "inflight limit");
            Counters->Rejected->Inc();
            return false;
        }
        return true;
    }

    TServerRequest* MakeRequest(const TFrameHeader& header, TStringBuf proto)
    {
        auto request = std::make_unique<TServerRequest>();
        request->Session = shared_from_this();
        request->RequestId = header.RequestId;
        request->Proto = TString(proto);
        request->DataLen = header.DataLen;
        if (header.DataLen) {
            request->Buffer = TAlignedBuffer(header.DataLen, BLOCK_ALIGN);
        }
        ++InflightRequests;
        InflightBytes += header.ProtoLen + header.DataLen;
        auto* raw = request.get();
        Requests[raw] = std::move(request);
        return raw;
    }

    void Dispatch(TServerRequest* raw)
    {
        Counters->Requests->Inc();
        Handler->HandleRequest(
            raw,
            Handler->CreateCallContext(),
            raw->Proto,
            raw->DataSgList());
    }

    void OnFrame(
        const TFrameHeader& header,
        TStringBuf proto,
        TStringBuf inlineData) override
    {
        if (header.Type != FRAME_REQUEST) {
            Connection->Close(MakeError(E_REJECTED, "unexpected frame type"));
            return;
        }
        if (!Admit(header)) {
            return;
        }
        auto* raw = MakeRequest(header, proto);
        if (!inlineData.empty()) {
            memcpy(raw->Buffer.Begin(), inlineData.data(), inlineData.size());
            Counters->InlineBytes->Add(inlineData.size());
        }
        Dispatch(raw);
    }

    TDataSink OnOobFrame(const TFrameHeader& header, TStringBuf proto) override
    {
        if (header.Type != FRAME_REQUEST) {
            Connection->Close(MakeError(E_REJECTED, "unexpected frame type"));
            return {};
        }
        if (!Admit(header)) {
            return {};   // payload is discarded by the connection
        }
        auto* raw = MakeRequest(header, proto);

        TDataSink sink;
        sink.Target = raw->DataSgList();
        sink.OnDone = [this, raw](const NProto::TError& error)
        {
            if (HasError(error)) {
                ReleaseBytes(RequestBytes(raw));
                ReleaseSlot(raw);
                return;
            }
            Counters->OobBytes->Add(raw->DataLen);
            Dispatch(raw);
        };
        return sink;
    }

    void OnConnectionClosed(const NProto::TError& error) override
    {
        // error lives in the connection this destroys, so it is used here only
        STORAGE_INFO("session closed: " << FormatError(error));

        // requests still in the handler keep their contexts alive; the handler
        // frees them through SendResponse/SendError
        for (auto& [raw, owned]: Requests) {
            Y_UNUSED(raw);
            owned.release();
        }
        Requests.clear();

        with_lock (Lock) {
            Stopped = true;
        }
        if (Connection) {
            Connection.reset();
            Counters->ActiveConnections->Dec();
        }
        if (Unregister) {
            auto unregister = std::move(Unregister);
            unregister();
        }
        // the last reference may be here, so nothing below may touch a member
        auto self = std::move(Self);
        Y_UNUSED(self);
    }

    static ui64 RequestBytes(const TServerRequest* raw)
    {
        return raw->Proto.size() + raw->DataLen;
    }

    // The handler is done with the request, but the bytes of the request and
    // of its response stay accounted until the kernel is done with the frame
    // they went into: a peer that stops reading would otherwise free an
    // admission slot per response while the send queue grows without a bound,
    // and a read-shaped request pins far more on the way out than in.
    void ReleaseSlot(TServerRequest* raw)
    {
        --InflightRequests;
        Requests.erase(raw);
    }

    void ReleaseBytes(ui64 bytes)
    {
        InflightBytes -= bytes;
    }

    void Answer(
        TServerRequest* raw,
        ui8 type,
        TString proto,
        TSgList data,
        std::shared_ptr<void> owner)
    {
        if (Stopped) {
            delete raw;
            return;
        }
        const ui64 responseBytes = proto.size() + SgListGetSize(data);
        const ui64 bytes = RequestBytes(raw) + responseBytes;
        InflightBytes += responseBytes;
        auto frame = std::make_shared<TOutFrame>();
        frame->Head =
            TString::Uninitialized(sizeof(TFrameHeader) + proto.size());
        TFrameHeader h;
        h.Type = type;
        h.RequestId = raw->RequestId;
        h.ProtoLen = proto.size();
        memcpy(frame->Head.begin(), &h, sizeof(h));
        memcpy(frame->Head.begin() + sizeof(h), proto.data(), proto.size());
        frame->Data = std::move(data);
        frame->OnDone = [owner = std::move(owner),
                         self = shared_from_this(),
                         bytes]() mutable
        {
            owner.reset();
            self->ReleaseBytes(bytes);
        };
        Connection->Send(std::move(frame));
        ReleaseSlot(raw);
    }
};

////////////////////////////////////////////////////////////////////////////////

// The peer's address without its port: the two sockets of one client come
// from different ephemeral ports but the same address.
struct TPeerAddress
{
    bool Known = false;
    ui16 Family = AF_UNSPEC;
    std::array<char, 16> Bytes{};

    bool operator==(const TPeerAddress& other) const = default;
};

TPeerAddress GetPeerAddress(int fd)
{
    sockaddr_storage addr{};
    socklen_t len = sizeof(addr);
    if (::getpeername(fd, reinterpret_cast<sockaddr*>(&addr), &len) < 0) {
        return {};
    }
    TPeerAddress peer;
    peer.Known = true;
    peer.Family = addr.ss_family;
    if (addr.ss_family == AF_INET) {
        memcpy(
            peer.Bytes.data(),
            &reinterpret_cast<sockaddr_in*>(&addr)->sin_addr,
            sizeof(in_addr));
    } else if (addr.ss_family == AF_INET6) {
        memcpy(
            peer.Bytes.data(),
            &reinterpret_cast<sockaddr_in6*>(&addr)->sin6_addr,
            sizeof(in6_addr));
    }
    return peer;
}

struct TPendingSession
{
    int MainFd = -1;
    TInstant Deadline;
    TConnectionParams Params;
    TPeerAddress Peer;
};

////////////////////////////////////////////////////////////////////////////////

class TServerEndpoint final
    : public IServerEndpoint
    , public IIoHandler
    , public ITickable
    , public std::enable_shared_from_this<TServerEndpoint>
{
private:
    struct THandshake
    {
        std::unique_ptr<TExactIo> Io;
        std::array<char, HANDSHAKE_BUFFER> Buffer{};
        TInstant Deadline;
        bool Cancelled = false;
    };

    TVector<std::unique_ptr<TRing>>& Rings;
    const TServerConfigPtr Config;
    const TCountersPtr Counters;
    TLog Log;

    struct
    {
        TLogThrottler Accept = TLogThrottler(LOG_THROTTLER_PERIOD);
        TLogThrottler AcceptResources = TLogThrottler(LOG_THROTTLER_PERIOD);
    } LogThrottler;

    // ring 0 only
    int ListenFd = -1;
    TIoOp* AcceptOp = nullptr;
    bool AcceptStalled = false;
    size_t NextRing = 0;
    THashMap<ui64, TPendingSession> Tokens;
    THashMap<int, std::unique_ptr<THandshake>> Handshakes;
    NThreading::TPromise<void> TornDown;

    ui16 Port = 0;
    std::atomic<bool> Stopped = false;

    TAdaptiveLock SessionLock;
    // Dropped once the sessions are drained, which breaks the cycle with a
    // handler holding this endpoint. A weak handle instead would let the
    // handler lose the only way to release a ctx it still owns.
    IServerHandlerPtr Handler;
    THashMap<TSession*, std::weak_ptr<TSession>> Sessions;
    NThreading::TPromise<void> SessionsDrained;

public:
    TServerEndpoint(
        TVector<std::unique_ptr<TRing>>& rings,
        TServerConfigPtr config,
        IServerHandlerPtr handler,
        TCountersPtr counters,
        TLog log)
        : Rings(rings)
        , Config(std::move(config))
        , Counters(std::move(counters))
        , Log(std::move(log))
        , Handler(std::move(handler))
    {}

    NProto::TError Listen(const TString& host, ui32 port)
    {
        if (port > Max<ui16>()) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder() << "bad port " << port);
        }
        auto [fd, error] = CreateListenSocket(host, port, Config->Backlog);
        if (HasError(error)) {
            return error;
        }
        ListenFd = fd;
        auto [actual, perr] = GetLocalPort(ListenFd);
        if (HasError(perr)) {
            CloseSocket(ListenFd);
            ListenFd = -1;
            return perr;
        }
        Port = actual;

        auto* ring = Rings[0].get();
        ring->Execute(
            [this, ring]
            {
                ring->AddTickable(this);
                AcceptOp = ring->AllocOp(this, TAG_ACCEPT);
                ring->Accept(AcceptOp, ListenFd);
            });
        return {};
    }

    ui16 GetPort() const
    {
        return Port;
    }

    // Any thread but a ring one: blocks until the listener and the handshakes
    // are gone and every session has been closed, so that the rings can be
    // stopped afterwards with nothing of ours left in flight.
    void Stop()
    {
        if (Stopped.exchange(true)) {
            return;
        }

        auto tornDown = NThreading::NewPromise();
        auto* ring = Rings[0].get();
        ring->Execute(
            [this, ring, tornDown]() mutable
            {
                TornDown = std::move(tornDown);
                ring->RemoveTickable(this);
                if (AcceptOp && AcceptOp->InFlight) {
                    ring->Cancel(AcceptOp);
                } else {
                    CloseListener();
                }
                TVector<int> fds;
                for (auto& [fd, handshake]: Handshakes) {
                    if (handshake->Io->InFlight()) {
                        CancelHandshake(*handshake);
                    } else {
                        fds.push_back(fd);
                    }
                }
                for (int fd: fds) {
                    DropHandshake(fd);
                }
                for (auto& [token, pending]: Tokens) {
                    Y_UNUSED(token);
                    CloseSocket(pending.MainFd);
                }
                Tokens.clear();
                CloseSessions();
                CheckTornDown();
            });
        tornDown.GetFuture().GetValueSync();

        NThreading::TPromise<void> drained;
        with_lock (SessionLock) {
            if (!Sessions.empty()) {
                SessionsDrained = NThreading::NewPromise();
                drained = SessionsDrained;
            }
        }
        if (drained.Initialized()) {
            drained.GetFuture().GetValueSync();
        }

        IServerHandlerPtr handler;
        with_lock (SessionLock) {
            handler = std::move(Handler);
            Handler = nullptr;
        }
    }

    void SendResponse(
        void* ctx,
        TStringBuf proto,
        TSgList data,
        std::shared_ptr<void> owner) override
    {
        auto* raw = static_cast<TServerRequest*>(ctx);
        // outlives the request, whose last reference may be the one below
        auto session = raw->Session;
        const bool posted = session->TryPost(
            [session,
             raw,
             proto = TString(proto),
             data = std::move(data),
             owner = std::move(owner)]() mutable
            {
                session->Answer(
                    raw,
                    FRAME_RESPONSE,
                    std::move(proto),
                    std::move(data),
                    std::move(owner));
            });
        if (!posted) {
            delete raw;
        }
    }

    void SendError(void* ctx, ui32 error, TStringBuf message) override
    {
        auto* raw = static_cast<TServerRequest*>(ctx);
        auto session = raw->Session;
        const bool posted = session->TryPost(
            [session, raw, payload = SerializeError(error, message)]() mutable {
                session
                    ->Answer(raw, FRAME_ERROR, std::move(payload), {}, nullptr);
            });
        if (!posted) {
            delete raw;
        }
    }

    void OnTick(TInstant now) override
    {
        TVector<ui64> tokens;
        for (auto& [token, pending]: Tokens) {
            if (pending.Deadline <= now) {
                tokens.push_back(token);
            }
        }
        for (ui64 token: tokens) {
            STORAGE_WARN("data token expired, dropping the main socket");
            CloseSocket(Tokens[token].MainFd);
            Tokens.erase(token);
        }

        TVector<int> fds;
        for (auto& [fd, handshake]: Handshakes) {
            if (!handshake->Cancelled && handshake->Deadline <= now) {
                fds.push_back(fd);
            }
        }
        if (AcceptStalled && ListenFd >= 0 && AcceptOp) {
            AcceptStalled = false;
            Rings[0]->Accept(AcceptOp, ListenFd);
        }

        for (int fd: fds) {
            STORAGE_WARN("handshake timed out, closing the socket");
            auto& handshake = GetHandshake(fd);
            if (handshake.Io->InFlight()) {
                CancelHandshake(handshake);
            } else {
                DropHandshake(fd);
            }
        }
    }

private:
    void OnIoComplete(TIoOp* op, int res, ui32) override
    {
        Y_ABORT_UNLESS(op->Tag == TAG_ACCEPT);
        if (Stopped) {
            if (res >= 0) {
                CloseSocket(res);
            }
            CloseListener();
            CheckTornDown();
            return;
        }
        if (res >= 0) {
            StartHandshake(res);
        } else if (IsPendingConnectionError(-res)) {
            // the connection this reports is off the queue, so re-arming at
            // once makes progress and cannot spin; the rate is the peer's
            STORAGE_WARN_T(
                LogThrottler.Accept,
                "accept failed for one connection: "
                    << FormatError(MakeSystemError(-res, "accept")));
        } else if (IsResourceError(-res)) {
            // the connection stays on the queue, so accept would keep failing
            // at once: the next tick re-arms instead of spinning the ring
            STORAGE_ERROR_T(
                LogThrottler.AcceptResources,
                "accept failed, retrying on the next tick: "
                    << FormatError(MakeSystemError(-res, "accept")));
            AcceptStalled = true;
            return;
        } else {
            STORAGE_ERROR(
                "accept failed, no longer listening: "
                << FormatError(MakeSystemError(-res, "accept")));
            CloseListener();
            return;
        }
        Rings[0]->Accept(AcceptOp, ListenFd);
    }

    void CloseListener()
    {
        CloseSocket(ListenFd);
        ListenFd = -1;
        if (AcceptOp) {
            Rings[0]->FreeOp(AcceptOp);
            AcceptOp = nullptr;
        }
    }

    void CheckTornDown()
    {
        if (!TornDown.Initialized() || ListenFd >= 0 || AcceptOp ||
            !Handshakes.empty())
        {
            return;
        }
        auto tornDown = std::move(TornDown);
        TornDown = {};
        tornDown.SetValue();
    }

    THandshake& GetHandshake(int fd)
    {
        auto it = Handshakes.find(fd);
        Y_ABORT_UNLESS(it != Handshakes.end(), "no handshake for fd %d", fd);
        return *it->second;
    }

    void CancelHandshake(THandshake& handshake)
    {
        handshake.Cancelled = true;
        handshake.Io->Cancel();
    }

    std::unique_ptr<THandshake> ExtractHandshake(int fd)
    {
        auto it = Handshakes.find(fd);
        Y_ABORT_UNLESS(it != Handshakes.end());
        auto handshake = std::move(it->second);
        Handshakes.erase(it);
        return handshake;
    }

    void DropHandshake(int fd)
    {
        ExtractHandshake(fd);
        CloseSocket(fd);
        CheckTornDown();
    }

    void StartHandshake(int fd)
    {
        TSocketOptions options{
            .TcpUserTimeout = Config->TcpUserTimeout,
            .KeepAliveIdle = Config->KeepAliveIdle,
            .KeepAliveInterval = Config->KeepAliveInterval,
            .KeepAliveCount = Config->KeepAliveCount,
        };
        if (auto error = ApplySocketOptions(fd, options); HasError(error)) {
            STORAGE_WARN("cannot set socket options: " << FormatError(error));
            CloseSocket(fd);
            return;
        }

        auto handshake = std::make_unique<THandshake>();
        handshake->Io = std::make_unique<TExactIo>(*Rings[0]);
        // a peer that connects and stalls would hold the socket forever
        handshake->Deadline = TInstant::Now() + Config->DataTokenTtl;
        auto* raw = handshake.get();
        Handshakes[fd] = std::move(handshake);

        raw->Io->Recv(
            fd,
            raw->Buffer.data(),
            HANDSHAKE_PREFIX,
            [this, fd](int error)
            {
                if (error) {
                    DropHandshake(fd);
                    return;
                }
                OnPrefix(fd);
            });
    }

    void OnPrefix(int fd)
    {
        auto& handshake = GetHandshake(fd);
        // arming another receive here would outlive the barrier that has
        // already cancelled what it could see; a cancel that lost the race to
        // a queued completion leaves TExactIo armable again, but the deadline
        // sweep will not look at this entry a second time
        if (Stopped || handshake.Cancelled) {
            DropHandshake(fd);
            return;
        }
        ui32 magic = 0;
        ui8 role = 0;
        memcpy(
            &magic,
            handshake.Buffer.data() + offsetof(THello, Magic),
            sizeof(magic));
        memcpy(
            &role,
            handshake.Buffer.data() + offsetof(THello, Role),
            sizeof(role));

        if (magic != MAGIC) {
            STORAGE_WARN("bad handshake magic " << Hex(magic));
            DropHandshake(fd);
            return;
        }
        if (role == ROLE_DATA) {
            TDataHello hello;
            memcpy(&hello, handshake.Buffer.data(), sizeof(hello));
            OnDataHello(fd, hello);
            return;
        }
        if (role != ROLE_MAIN) {
            STORAGE_WARN("unknown handshake role " << ui32(role));
            DropHandshake(fd);
            return;
        }
        handshake.Io->Recv(
            fd,
            handshake.Buffer.data() + HANDSHAKE_PREFIX,
            sizeof(THello) - HANDSHAKE_PREFIX,
            [this, fd](int error)
            {
                if (error) {
                    DropHandshake(fd);
                    return;
                }
                OnHello(fd);
            });
    }

    // The reply is the last IO of a handshake: it takes the handshake out of
    // the map, so next owns the socket.
    void Reply(
        int fd,
        const void* data,
        size_t size,
        std::function<void(bool ok)> next)
    {
        Y_ABORT_UNLESS(size <= HANDSHAKE_BUFFER);
        auto& handshake = GetHandshake(fd);
        memcpy(handshake.Buffer.data(), data, size);
        handshake.Io->Send(
            fd,
            handshake.Buffer.data(),
            size,
            [this, fd, next = std::move(next)](int error)
            {
                auto handshake = ExtractHandshake(fd);
                next(error == 0);
                CheckTornDown();
            });
    }

    void OnHello(int fd)
    {
        auto& handshake = GetHandshake(fd);
        if (Stopped || handshake.Cancelled) {
            DropHandshake(fd);
            return;
        }
        THello hello;
        memcpy(&hello, handshake.Buffer.data(), sizeof(hello));

        if (hello.Version != PROTOCOL_VERSION) {
            STORAGE_WARN("unsupported protocol version " << hello.Version);
            TWelcome welcome;
            welcome.Status = STATUS_VERSION_MISMATCH;
            Reply(
                fd,
                &welcome,
                sizeof(welcome),
                [fd](bool) { CloseSocket(fd); });
            return;
        }

        // FromWire turns anything unknown into Auto, so the byte itself has to
        // be checked before it is converted
        const bool knownMode = hello.OobMode == OOB_WIRE_OFF ||
                               hello.OobMode == OOB_WIRE_AUTO ||
                               hello.OobMode == OOB_WIRE_REQUIRED;
        std::optional<bool> oob;
        if (knownMode) {
            oob = NegotiateOob(Config->OobMode, FromWire(hello.OobMode));
        }
        if (!oob) {
            STORAGE_WARN(
                "cannot agree on the out-of-band mode, peer sent "
                << ui32(hello.OobMode));
            TWelcome welcome;
            welcome.Status = STATUS_CONFIG_MISMATCH;
            Reply(
                fd,
                &welcome,
                sizeof(welcome),
                [fd](bool) { CloseSocket(fd); });
            return;
        }

        TWelcome welcome;
        welcome.Status = STATUS_OK;
        welcome.OobMode = *oob ? OOB_WIRE_REQUIRED : OOB_WIRE_OFF;
        welcome.MaxProtoSize = Min(Config->MaxProtoSize, hello.MaxProtoSize);
        welcome.MaxDataSize = Min(Config->MaxDataSize, hello.MaxDataSize);
        welcome.MaxInflightBytes =
            Min<ui64>(Config->MaxInflightBytes, Max<ui32>());
        welcome.OobThreshold = Min(Config->OobThreshold, hello.OobThreshold);
        welcome.DataToken = *oob ? RandomNumber<ui64>() | 1 : 0;

        const ui64 token = welcome.DataToken;
        const bool needData = *oob;
        const auto params = MakeParams(welcome, *oob);

        Reply(
            fd,
            &welcome,
            sizeof(welcome),
            [this, fd, token, needData, params](bool ok)
            {
                if (!ok || Stopped) {
                    CloseSocket(fd);
                    return;
                }
                if (!needData) {
                    StartSession(fd, -1, params);
                    return;
                }
                Tokens[token] = TPendingSession{
                    .MainFd = fd,
                    .Deadline = TInstant::Now() + Config->DataTokenTtl,
                    .Params = params,
                    .Peer = GetPeerAddress(fd),
                };
            });
    }

    void OnDataHello(int fd, const TDataHello& hello)
    {
        auto it = Tokens.find(hello.DataToken);
        // a token guessed by an off-path peer must not attach its data stream
        // to somebody else's main socket
        const bool samePeer =
            it != Tokens.end() && it->second.Peer == GetPeerAddress(fd);
        if (hello.Version != PROTOCOL_VERSION || !samePeer) {
            STORAGE_WARN(
                "rejecting a data socket: version "
                << hello.Version << ", token " << hello.DataToken);
            TDataWelcome welcome;
            welcome.Status = hello.Version != PROTOCOL_VERSION
                                 ? STATUS_VERSION_MISMATCH
                                 : STATUS_BAD_TOKEN;
            Reply(
                fd,
                &welcome,
                sizeof(welcome),
                [fd](bool) { CloseSocket(fd); });
            return;
        }

        // the token is spent here, so a second socket presenting it is
        // rejected instead of taking the main socket over
        const int mainFd = it->second.MainFd;
        const auto params = it->second.Params;
        Tokens.erase(it);

        TDataWelcome welcome;
        welcome.Status = STATUS_OK;
        Reply(
            fd,
            &welcome,
            sizeof(welcome),
            [this, fd, mainFd, params](bool ok)
            {
                if (!ok || Stopped) {
                    CloseSocket(fd);
                    CloseSocket(mainFd);
                    return;
                }
                StartSession(mainFd, fd, params);
            });
    }

    TConnectionParams MakeParams(const TWelcome& welcome, bool oob) const
    {
        return TConnectionParams{
            .MaxProtoSize = welcome.MaxProtoSize,
            .MaxDataSize = welcome.MaxDataSize,
            .SendWindowBytes = Config->SendWindowBytes,
            .OobEnabled = oob,
            .OobThreshold = welcome.OobThreshold,
            .SendZeroCopy = Config->SendZeroCopyMode != ESendZeroCopyMode::Off,
            .SendZeroCopyThreshold = Config->SendZeroCopyThreshold,
            .Counters = Counters,
        };
    }

    void StartSession(int mainFd, int dataFd, const TConnectionParams& params)
    {
        auto& ring = *Rings[NextRing++ % Rings.size()];
        TSessionPtr session;
        with_lock (SessionLock) {
            Y_ABORT_UNLESS(Handler, "starting a session without a handler");
            session = std::make_shared<TSession>(
                ring,
                Handler,
                Config,
                Counters,
                Log,
                mainFd,
                dataFd);
            session->Unregister = [weak = weak_from_this(), raw = session.get()]
            {
                if (auto self = weak.lock()) {
                    self->ForgetSession(raw);
                }
            };
            Sessions[session.get()] = session;
        }
        ring.Execute(
            [session, params]
            {
                session->Self = session;
                session->Start(params);
            });
    }

    void ForgetSession(TSession* raw)
    {
        NThreading::TPromise<void> drained;
        with_lock (SessionLock) {
            Sessions.erase(raw);
            if (Sessions.empty() && SessionsDrained.Initialized()) {
                drained = std::move(SessionsDrained);
                SessionsDrained = {};
            }
        }
        if (drained.Initialized()) {
            drained.SetValue();
        }
    }

    // Ring 0, so the close lands behind the command that builds the session.
    void CloseSessions()
    {
        TVector<TSessionPtr> sessions;
        with_lock (SessionLock) {
            for (auto& [raw, weak]: Sessions) {
                Y_UNUSED(raw);
                if (auto session = weak.lock()) {
                    sessions.push_back(std::move(session));
                }
            }
        }
        for (auto& session: sessions) {
            auto& ring = session->Ring;
            ring.Execute(
                [session = std::move(session)]
                {
                    if (session->Connection) {
                        session->Connection->Close(
                            MakeError(E_REJECTED, "server is stopping"));
                    }
                });
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TServer final
    : public IServer
    , public std::enable_shared_from_this<TServer>
{
private:
    const ILoggingServicePtr Logging;
    const IMonitoringServicePtr Monitoring;
    const TServerConfigPtr Config;

    TLog Log;
    TCountersPtr Counters;
    TVector<std::unique_ptr<TRing>> Rings;
    TVector<std::shared_ptr<TServerEndpoint>> Endpoints;
    mutable TAdaptiveLock Lock;
    bool Started = false;

public:
    TServer(
        ILoggingServicePtr logging,
        IMonitoringServicePtr monitoring,
        TServerConfigPtr config)
        : Logging(std::move(logging))
        , Monitoring(std::move(monitoring))
        , Config(std::move(config))
    {}

    ~TServer() override
    {
        // the rings would otherwise wait for the connections nobody closed
        Stop();
    }

    void Start() override
    {
        const ui64 queued = ui64(Config->MaxInflightRequests) *
                            (Config->MaxDataSize + Config->MaxProtoSize);
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
        Counters = MakeCounters(Monitoring, "uring_server");
        for (ui32 i = 0; i < Max<ui32>(Config->IoThreads, 1); ++i) {
            auto ring = std::make_unique<TRing>(
                TStringBuilder() << "URING.SRV" << i,
                Config->QueueSize,
                Config->SendZeroCopyMode,
                Log);
            auto error = ring->Start();
            Y_ABORT_IF(
                HasError(error),
                "cannot start the uring server: %s",
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
        TVector<std::shared_ptr<TServerEndpoint>> endpoints;
        with_lock (Lock) {
            if (!Started) {
                return;
            }
            Started = false;
            endpoints = Endpoints;
        }
        // the endpoints own every connection, so they go first
        for (auto& endpoint: endpoints) {
            endpoint->Stop();
        }
        for (auto& ring: Rings) {
            ring->Stop();
        }
    }

    TResultOrError<IServerEndpointPtr>
    StartEndpoint(TString host, ui32 port, IServerHandlerPtr handler) override
    {
        // the whole body is under the lock: an endpoint that armed its accept
        // after Stop() took its snapshot would never be stopped, and the ring
        // would wait for that accept forever
        std::shared_ptr<TServerEndpoint> endpoint;
        with_lock (Lock) {
            if (!Started) {
                return MakeError(E_INVALID_STATE, "server is not started");
            }
            endpoint = std::make_shared<TServerEndpoint>(
                Rings,
                Config,
                std::move(handler),
                Counters,
                Log);
            if (auto error = endpoint->Listen(host, port); HasError(error)) {
                return error;
            }
            Endpoints.push_back(endpoint);
        }
        return IServerEndpointPtr(endpoint);
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

IServerPtr CreateServer(
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    TServerConfigPtr config)
{
    return std::make_shared<TServer>(
        std::move(logging),
        std::move(monitoring),
        std::move(config));
}

TResultOrError<ui16> GetEndpointPort(const IServerEndpointPtr& endpoint)
{
    auto* impl = dynamic_cast<TServerEndpoint*>(endpoint.get());
    if (!impl) {
        return MakeError(E_ARGUMENT, "not a uring endpoint");
    }
    return impl->GetPort();
}

}   // namespace NCloud::NBlockStore::NUring
