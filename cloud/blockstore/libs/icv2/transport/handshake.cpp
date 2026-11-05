#include "handshake.h"

#include <cloud/blockstore/libs/icv2/transport/protos/handshake.pb.h>

#include <cloud/storage/core/libs/icv2/tcp/socket_helpers.h>
#include <cloud/storage/core/libs/icv2/tcp/socket_worker.h>

#include <library/cpp/time_provider/monotonic.h>

#include <util/generic/guid.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/system/error.h>
#include <util/system/guard.h>

#include <fcntl.h>
#include <netinet/in.h>

#include <climits>

namespace NCloud::NBlockStore::NIcV2 {

using NCloud::NIcV2::TSocketPtr;

using NCloud::NIcV2::CreateSocket;
using NCloud::NIcV2::ISocketWorkerHandler;
using NCloud::NIcV2::MakeSocketError;
using NCloud::NIcV2::TSocketWorker;

namespace {

////////////////////////////////////////////////////////////////////////////////

// Each hello is framed by "ICV2" and a two-byte network-order payload size.
// Main and auxiliary sockets exchange hello/reply/ack before the pair is ready.
constexpr size_t HeaderSize = 6;
constexpr ui16 Version = 1;
constexpr size_t MaxHelloSize = 1024;
constexpr size_t PairIdSize = 16;
constexpr char Ack = 1;

void AppendWord(TString& buffer, ui16 value)
{
    buffer.push_back(static_cast<char>(value >> 8));
    buffer.push_back(static_cast<char>(value & 0xff));
}

ui16 ReadWord(const TString& buffer, size_t offset)
{
    return (static_cast<ui8>(buffer[offset]) << 8) |
           static_cast<ui8>(buffer[offset + 1]);
}

enum class EStage
{
    ConnectXdc,
    SendHello,
    ReceiveHeader,
    ReceiveBody,
    SendAck,
    ReceiveAck,
    WaitXdc,
};

enum class EIoProgress
{
    Complete,
    Retry,
    Wait,
};

enum class EStepResult
{
    Continue,
    Wait,
    Complete,
};

struct TOutgoingRequest
{
    NThreading::TPromise<TConnectHandshakeResult> Promise;
};

struct TIncomingRequest
{
    NThreading::TPromise<TAcceptHandshakeResult> Promise;
    bool Auxiliary = false;
};

struct TRequest
{
    TSocketPtr Socket;
    std::variant<TOutgoingRequest, TIncomingRequest> Direction;
    const TMonotonic Deadline;
    EStage Stage;
    TString Buffer;
    size_t Offset = 0;
    bool UseXdc = false;
    bool OnXdc = false;
    TString PairId;
    TSocketPtr XdcSocket;
    TRequest* Partner = nullptr;
    NProto::TError PairError;

    bool IsOutgoing() const
    {
        return std::holds_alternative<TOutgoingRequest>(Direction);
    }

    bool IsIncomingAuxiliary() const
    {
        const auto* incoming = std::get_if<TIncomingRequest>(&Direction);
        return incoming != nullptr && incoming->Auxiliary;
    }

    const TSocketPtr& CurrentSocket() const
    {
        return OnXdc ? XdcSocket : Socket;
    }

    bool IsReady() const
    {
        return HasError(PairError) || (Stage == EStage::WaitXdc && XdcSocket);
    }

    bool IsWriting() const
    {
        return Stage == EStage::ConnectXdc || Stage == EStage::SendHello ||
               Stage == EStage::SendAck;
    }

    NProto::TError Transfer(EIoProgress& progress);
    TString EncodeHello() const;
    void Complete(const NProto::TError& error);

    void Next(EStage stage, TString buffer)
    {
        Stage = stage;
        Buffer = std::move(buffer);
        Offset = 0;
    }
};

NProto::TError TRequest::Transfer(EIoProgress& progress)
{
    if (Offset == Buffer.size()) {
        return {};
    }
    const int fd = *CurrentSocket();
    char* data = Buffer.Detach() + Offset;
    const size_t size = Buffer.size() - Offset;
    const ssize_t bytes = IsWriting() ? send(fd, data, size, MSG_NOSIGNAL)
                                      : recv(fd, data, size, 0);
    if (bytes < 0) {
        const int error = errno;
        if (error == EINTR) {
            progress = EIoProgress::Retry;
            return {};
        }
        if (error == EAGAIN || error == EWOULDBLOCK) {
            progress = EIoProgress::Wait;
            return {};
        }
        return MakeSocketError(error, "IC V2 hello I/O");
    }
    if (!bytes) {
        return MakeError(E_IO, "connection closed during IC V2 hello");
    }
    Offset += bytes;
    if (Offset != Buffer.size()) {
        progress = EIoProgress::Retry;
    }
    return {};
}

TString TRequest::EncodeHello() const
{
    NHandshakeProto::THandshakeHello hello;
    hello.SetProtocolVersion(Version);
    hello.SetEnableXdc(UseXdc);
    hello.SetAuxiliary(IsIncomingAuxiliary() || OnXdc);
    if (UseXdc) {
        hello.SetPairId(PairId);
    }
    TString payload;
    Y_PROTOBUF_SUPPRESS_NODISCARD hello.SerializeToString(&payload);
    Y_ABORT_UNLESS(payload.size() <= MaxHelloSize);
    TString buffer = "ICV2";
    AppendWord(buffer, payload.size());
    buffer.append(payload);
    return buffer;
}

using TRequests = TVector<std::unique_ptr<TRequest>>;

void TRequest::Complete(const NProto::TError& error)
{
    if (HasError(error)) {
        Socket.Reset();
        XdcSocket.Reset();
    }

    if (auto* incoming = std::get_if<TIncomingRequest>(&Direction)) {
        if (HasError(error)) {
            incoming->Promise.SetValue(error);
        } else if (incoming->Auxiliary) {
            incoming->Promise.SetValue(
                TAcceptHandshakeOutcome{THandshakeIncomplete{}});
        } else {
            incoming->Promise.SetValue(
                TAcceptHandshakeOutcome{THandshakeReady{
                    .Socket = std::move(Socket),
                    .XdcSocket = std::move(XdcSocket)}});
        }
        return;
    }

    auto* outgoing = std::get_if<TOutgoingRequest>(&Direction);
    if (HasError(error)) {
        outgoing->Promise.SetValue(error);
    } else {
        outgoing->Promise.SetValue(
            THandshakeReady{
                .Socket = std::move(Socket),
                .XdcSocket = std::move(XdcSocket)});
    }
}

class THandshakeProvider final
    : public IHandshakeProvider
    , private ISocketWorkerHandler
{
    TSocketWorker Worker;
    TMutex Mutex;
    bool Accepting = false;
    const THandshakeConfig Config;
    TRequests Queued;
    // Remove pair links before completing requests to keep partner pointers
    // valid.
    THashMap<TString, TRequest*> Pairs;

public:
    explicit THandshakeProvider(THandshakeConfig config)
        : Worker(*this)
        , Config(config)
    {}

    void Start() override
    {
        TGuard<TMutex> guard(Mutex);
        Worker.Start();
        Accepting = true;
    }

    void RequestStop() override
    {
        {
            TGuard<TMutex> guard(Mutex);
            Accepting = false;
        }
        Worker.RequestStop();
    }

    void Stop() override
    {
        RequestStop();
        Worker.Stop();
    }

    TConnectHandshakeFuture Connect(
        TSocketPtr socket,
        TDuration timeout) override
    {
        auto promise = NThreading::NewPromise<TConnectHandshakeResult>();
        auto future = promise.GetFuture();
        auto request = std::make_unique<TRequest>(
            std::move(socket),
            TOutgoingRequest{.Promise = std::move(promise)},
            TMonotonic::Now() + timeout);
        Submit(std::move(request));
        return future;
    }

    TAcceptHandshakeFuture Accept(TSocketPtr socket, TDuration timeout) override
    {
        auto promise = NThreading::NewPromise<TAcceptHandshakeResult>();
        auto future = promise.GetFuture();
        auto request = std::make_unique<TRequest>(
            std::move(socket),
            TIncomingRequest{.Promise = std::move(promise), .Auxiliary = false},
            TMonotonic::Now() + timeout);
        Submit(std::move(request));
        return future;
    }

private:
    void Submit(std::unique_ptr<TRequest> request)
    {
        if (!request->Socket) {
            request->Complete(MakeError(E_ARGUMENT, "invalid hello request"));
            return;
        }
        const int flags = fcntl(*request->Socket, F_GETFL);
        if (flags < 0) {
            request->Complete(MakeSocketError(errno, "get IC V2 socket flags"));
            return;
        }
        if (!(flags & O_NONBLOCK)) {
            request->Complete(MakeError(E_ARGUMENT, "invalid hello request"));
            return;
        }
        if (request->IsOutgoing()) {
            if (Config.EnableXdc) {
                request->UseXdc = true;
                const auto id = TGUID::Create();
                request->PairId.assign(
                    reinterpret_cast<const char*>(id.dw),
                    PairIdSize);
            }
            request->Next(EStage::SendHello, request->EncodeHello());
        } else {
            request->Next(EStage::ReceiveHeader, TString(HeaderSize, '\0'));
        }
        bool wake = false;
        {
            TGuard<TMutex> guard(Mutex);
            if (Accepting) {
                wake = Queued.empty();
                Queued.push_back(std::move(request));
            }
        }
        if (request) {
            request->Complete(
                MakeError(E_REJECTED, "IC V2 hello is not running"));
        } else if (wake) {
            Worker.Wake();
        }
    }

    TRequests TakeQueued()
    {
        TRequests queued;
        TGuard<TMutex> guard(Mutex);
        queued.swap(Queued);
        return queued;
    }

    static NProto::TError CheckHeader(TRequest& request)
    {
        const auto& buffer = request.Buffer;
        const size_t size = ReadWord(buffer, 4);
        if (!buffer.StartsWith("ICV2") || !size || size > MaxHelloSize) {
            return MakeError(E_ARGUMENT, "invalid IC V2 hello header");
        }
        request.Stage = EStage::ReceiveBody;
        request.Buffer.resize(HeaderSize + size);
        return {};
    }

    NProto::TError CheckHello(TRequest& request) const
    {
        NHandshakeProto::THandshakeHello hello;
        if (!hello.ParseFromArray(
                request.Buffer.data() + HeaderSize,
                static_cast<int>(request.Buffer.size() - HeaderSize)) ||
            hello.GetProtocolVersion() != Version)
        {
            return MakeError(
                E_ARGUMENT,
                "invalid IC V2 hello message or version");
        }
        const bool auxiliary = hello.GetAuxiliary();
        const bool xdc = hello.GetEnableXdc();
        if ((auxiliary && (!xdc || !Config.EnableXdc)) ||
            (request.IsOutgoing() &&
             (auxiliary != request.OnXdc || (request.OnXdc && !xdc))))
        {
            return MakeError(E_ARGUMENT, "incompatible IC V2 hello flags");
        }
        if (xdc) {
            const auto& id = hello.GetPairId();
            if (id.size() != PairIdSize || id == TString(PairIdSize, '\0') ||
                (request.IsOutgoing() && id != request.PairId))
            {
                return MakeError(E_ARGUMENT, "invalid IC V2 socket pair ID");
            }
            request.PairId = id;
        } else if (hello.HasPairId()) {
            return MakeError(E_ARGUMENT, "unexpected IC V2 socket pair ID");
        }
        if (auto* incoming = std::get_if<TIncomingRequest>(&request.Direction))
        {
            incoming->Auxiliary = auxiliary;
        }
        request.UseXdc = xdc && Config.EnableXdc;
        if (!request.UseXdc) {
            request.PairId.clear();
        }
        return {};
    }

    NProto::TError RegisterPair(TRequest& request)
    {
        if (request.IsOutgoing() || !request.UseXdc) {
            return {};
        }

        if (request.IsIncomingAuxiliary()) {
            const auto it = Pairs.find(request.PairId);
            if (it != Pairs.end() && TMonotonic::Now() >= it->second->Deadline)
            {
                return MakeError(E_TIMEOUT, "expired IC V2 socket pair");
            }
            if (it == Pairs.end() || it->second->Partner) {
                return MakeError(
                    E_REJECTED,
                    "unknown or occupied IC V2 socket pair");
            }
            request.Partner = it->second;
            request.Partner->Partner = &request;
        } else if (!Pairs.emplace(request.PairId, &request).second) {
            return MakeError(E_ARGUMENT, "duplicate IC V2 socket pair");
        }
        return {};
    }

    void RemovePair(TRequest& request, const NProto::TError& error)
    {
        if (request.Partner) {
            request.Partner->Partner = nullptr;
            if (HasError(error)) {
                request.Partner->PairError = error;
            }
            request.Partner = nullptr;
        }
        const auto it = Pairs.find(request.PairId);
        if (it != Pairs.end() && it->second == &request) {
            Pairs.erase(it);
        }
    }

    static NProto::TError ConnectXdc(TRequest& request)
    {
        NInterconnect::TAddress address;
        socklen_t size = sizeof(address.Addr);
        if (getpeername(*request.Socket, address.SockAddr(), &size)) {
            return MakeSocketError(errno, "get IC V2 peer address");
        }
        auto [socket, error] = CreateSocket(address);
        if (HasError(error)) {
            return error;
        }
        NInterconnect::TAddress localAddress;
        size = sizeof(localAddress.Addr);
        if (getsockname(*request.Socket, localAddress.SockAddr(), &size)) {
            return MakeSocketError(errno, "get IC V2 local address");
        }
        if (localAddress.GetFamily() == AF_INET) {
            localAddress.Addr.Ipv4.sin_port = 0;
        } else {
            localAddress.Addr.Ipv6.sin6_port = 0;
        }
        const int enable = 1;
        if (setsockopt(
                *socket,
                IPPROTO_IP,
                IP_BIND_ADDRESS_NO_PORT,
                &enable,
                sizeof(enable)))
        {
            return MakeSocketError(errno, "set IP_BIND_ADDRESS_NO_PORT");
        }
        if (bind(*socket, localAddress.SockAddr(), localAddress.Size())) {
            return MakeSocketError(errno, "bind IC V2 XDC local address");
        }
        request.XdcSocket = std::move(socket);
        request.OnXdc = true;
        const int res =
            NCloud::NIcV2::ConnectSocket(*request.XdcSocket, address);
        if (!res) {
            request.Next(EStage::SendHello, request.EncodeHello());
        } else if (res == EINPROGRESS || res == EINTR) {
            request.Next(EStage::ConnectXdc, {});
        } else {
            return MakeSocketError(res, "connect IC V2 XDC");
        }
        return {};
    }

    TResultOrError<EStepResult> FinishExchange(TRequest& request)
    {
        if (request.IsOutgoing() && request.OnXdc) {
            request.OnXdc = false;
            request.Next(EStage::SendAck, TString(1, Ack));
            return EStepResult::Continue;
        }
        if (request.IsIncomingAuxiliary()) {
            auto* main = request.Partner;
            if (!main || main->XdcSocket) {
                return MakeError(
                    E_REJECTED,
                    "unknown or occupied IC V2 socket pair");
            }
            if (TMonotonic::Now() >= main->Deadline) {
                return MakeError(E_TIMEOUT, "expired IC V2 socket pair");
            }
            main->XdcSocket = std::move(request.Socket);
            Pairs.erase(request.PairId);
            main->Partner = nullptr;
            request.Partner = nullptr;
            return EStepResult::Complete;
        }
        if (!request.IsOutgoing() && request.UseXdc && !request.XdcSocket) {
            request.Next(EStage::WaitXdc, {});
            return EStepResult::Wait;
        }
        return EStepResult::Complete;
    }

    TResultOrError<EStepResult> HandleReceivedHello(TRequest& request)
    {
        if (auto error = CheckHello(request); HasError(error)) {
            return error;
        }
        if (auto error = RegisterPair(request); HasError(error)) {
            return error;
        }
        if (!request.IsOutgoing()) {
            request.Next(EStage::SendHello, request.EncodeHello());
            return EStepResult::Continue;
        }
        if (!request.UseXdc || request.OnXdc) {
            request.Next(EStage::SendAck, TString(1, Ack));
            return EStepResult::Continue;
        }
        if (auto error = ConnectXdc(request); HasError(error)) {
            return error;
        }
        return request.Stage == EStage::ConnectXdc ? EStepResult::Wait
                                                   : EStepResult::Continue;
    }

    TResultOrError<EStepResult> AdvanceStage(TRequest& request)
    {
        switch (request.Stage) {
            case EStage::SendHello:
                if (request.IsOutgoing()) {
                    request.Next(
                        EStage::ReceiveHeader,
                        TString(HeaderSize, '\0'));
                } else {
                    request.Next(EStage::ReceiveAck, TString(1, '\0'));
                }
                break;
            case EStage::ReceiveHeader:
                if (auto error = CheckHeader(request); HasError(error)) {
                    return error;
                }
                break;
            case EStage::ReceiveBody:
                return HandleReceivedHello(request);
            case EStage::ReceiveAck:
                if (request.Buffer[0] != Ack) {
                    return MakeError(
                        E_ARGUMENT,
                        "invalid IC V2 hello acknowledgement");
                }
                [[fallthrough]];
            case EStage::SendAck:
                return FinishExchange(request);
            case EStage::ConnectXdc:
            case EStage::WaitXdc:
                Y_ABORT("handled before I/O");
        }
        return EStepResult::Continue;
    }

    TResultOrError<EStepResult> Advance(TRequest& request, short events)
    {
        while (!Worker.IsStopping()) {
            if (HasError(request.PairError)) {
                return request.PairError;
            }
            if (TMonotonic::Now() >= request.Deadline) {
                return MakeError(E_TIMEOUT, "IC V2 hello timed out");
            }
            if (request.Stage == EStage::WaitXdc) {
                if ((events & (POLLRDHUP | POLLHUP | POLLERR | POLLNVAL)) ||
                    !request.XdcSocket)
                {
                    return MakeError(
                        E_IO,
                        "IC V2 main socket closed while pairing");
                }
                return EStepResult::Complete;
            }
            if (request.Stage == EStage::ConnectXdc) {
                if (const int status = request.XdcSocket->GetConnectStatus()) {
                    return MakeSocketError(
                        status,
                        "connect IC V2 XDC completion");
                }
                request.Next(EStage::SendHello, request.EncodeHello());
            }
            EIoProgress progress = EIoProgress::Complete;
            if (auto error = request.Transfer(progress); HasError(error)) {
                return error;
            }
            if (progress == EIoProgress::Wait) {
                return EStepResult::Wait;
            }
            if (progress == EIoProgress::Retry) {
                continue;
            }
            auto result = AdvanceStage(request);
            if (HasError(result.GetError()) ||
                result.GetResult() != EStepResult::Continue)
            {
                return result;
            }
        }
        return EStepResult::Wait;
    }

    int PreparePoll(const TRequests& pending, TVector<pollfd>& fds) const
    {
        fds.resize(pending.size() + 1);
        fds[0] = {.fd = Worker.GetWakeSocket(), .events = POLLIN, .revents = 0};
        auto deadline = TMonotonic::Max();
        bool ready = false;
        for (size_t i = 0; i < pending.size(); ++i) {
            const auto& request = *pending[i];
            short events = POLLIN;
            if (request.Stage == EStage::WaitXdc) {
                events = POLLRDHUP;
            } else if (request.IsWriting()) {
                events = POLLOUT;
            }
            fds[i + 1] = {
                .fd = *request.CurrentSocket(),
                .events = events,
                .revents = 0};
            deadline = Min(deadline, request.Deadline);
            ready |= request.IsReady();
        }
        const auto now = TMonotonic::Now();
        if (ready || (!pending.empty() && deadline <= now)) {
            return 0;
        }
        if (pending.empty()) {
            return -1;
        }
        return static_cast<int>(
            Min<ui64>((deadline - now).MilliSeconds() + 1, INT_MAX));
    }

    void ProcessPending(TRequests& pending, const TVector<pollfd>& fds)
    {
        for (size_t i = pending.size(); i-- && !Worker.IsStopping();) {
            TResultOrError<EStepResult> result = EStepResult::Wait;
            if (TMonotonic::Now() >= pending[i]->Deadline) {
                result = MakeError(E_TIMEOUT, "IC V2 hello timed out");
            } else if (fds[i + 1].revents || pending[i]->IsReady()) {
                result = Advance(*pending[i], fds[i + 1].revents);
            }
            const auto& error = result.GetError();
            if (result.GetResult() == EStepResult::Complete || HasError(error))
            {
                auto request = std::move(pending[i]);
                pending[i] = std::move(pending.back());
                pending.pop_back();
                RemovePair(*request, error);
                request->Complete(error);
            }
        }
    }

    void Run() override
    {
        TRequests pending;
        TVector<pollfd> fds;
        auto failure = MakeError(E_REJECTED, "IC V2 hello is stopping");
        while (!Worker.IsStopping()) {
            for (auto& request: TakeQueued()) {
                pending.push_back(std::move(request));
            }
            const int timeout = PreparePoll(pending, fds);
            const int res = Worker.Poll(fds, timeout);
            if (res < 0) {
                failure = MakeSocketError(-res, "hello poll");
                RequestStop();
                break;
            }
            ProcessPending(pending, fds);
        }
        Pairs.clear();
        for (auto& request: pending) {
            request->Complete(failure);
            request.reset();
        }
        for (auto& request: TakeQueued()) {
            request->Complete(failure);
            request.reset();
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IHandshakeProviderPtr> CreateHandshakeProvider(
    THandshakeConfig config)
{
    try {
        return IHandshakeProviderPtr(
            std::make_shared<THandshakeProvider>(config));
    } catch (const TSystemError& error) {
        return MakeSocketError(error.Status(), "create handshake resources");
    }
}

}   // namespace NCloud::NBlockStore::NIcV2
