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

enum class EHandshakeRole
{
    Outgoing,
    Incoming,
    IncomingAuxiliary,
};

struct TRequest
{
    TSocketPtr Socket;
    EHandshakeRole Role;
    const TMonotonic Deadline;
    std::variant<
        NThreading::TPromise<TConnectHandshakeResult>,
        NThreading::TPromise<TAcceptHandshakeResult>>
        Promise;
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
        return Role == EHandshakeRole::Outgoing;
    }

    bool IsIncomingAuxiliary() const
    {
        return Role == EHandshakeRole::IncomingAuxiliary;
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
        std::visit([&](auto& promise) { promise.SetValue(error); }, Promise);
        return;
    }

    if (IsIncomingAuxiliary()) {
        std::get<NThreading::TPromise<TAcceptHandshakeResult>>(Promise)
            .SetValue(TAcceptHandshakeOutcome{THandshakeIncomplete{}});
        return;
    }

    THandshakeReady connection{
        .Socket = std::move(Socket),
        .XdcSocket = std::move(XdcSocket)};
    if (IsOutgoing()) {
        std::get<NThreading::TPromise<TConnectHandshakeResult>>(Promise)
            .SetValue(std::move(connection));
    } else {
        std::get<NThreading::TPromise<TAcceptHandshakeResult>>(Promise)
            .SetValue(TAcceptHandshakeOutcome{std::move(connection)});
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

    ~THandshakeProvider() override
    {
        Stop();
    }

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
            EHandshakeRole::Outgoing,
            TMonotonic::Now() + timeout,
            std::move(promise));
        Submit(std::move(request));
        return future;
    }

    TAcceptHandshakeFuture Accept(TSocketPtr socket, TDuration timeout) override
    {
        auto promise = NThreading::NewPromise<TAcceptHandshakeResult>();
        auto future = promise.GetFuture();
        auto request = std::make_unique<TRequest>(
            std::move(socket),
            EHandshakeRole::Incoming,
            TMonotonic::Now() + timeout,
            std::move(promise));
        Submit(std::move(request));
        return future;
    }

private:
    void Submit(std::unique_ptr<TRequest> request)
    {
        if (!request->Socket || !IsNonBlock(*request->Socket)) {
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
        {
            TGuard<TMutex> guard(Mutex);
            if (Accepting) {
                Queued.push_back(std::move(request));
                Worker.Wake();
                return;
            }
        }
        request->Complete(MakeError(E_REJECTED, "IC V2 hello is not running"));
    }

    TRequests TakeQueued()
    {
        TRequests queued;
        TGuard<TMutex> guard(Mutex);
        queued.swap(Queued);
        return queued;
    }

    NProto::TError CheckHeader(TRequest& request) const
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
                request.Buffer.size() - HeaderSize) ||
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
        if (!request.IsOutgoing() && auxiliary) {
            request.Role = EHandshakeRole::IncomingAuxiliary;
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
            if (it == Pairs.end() || it->second->Partner) {
                return MakeError(
                    E_ARGUMENT,
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
                Worker.Wake();
            }
            request.Partner = nullptr;
        }
        const auto it = Pairs.find(request.PairId);
        if (it != Pairs.end() && it->second == &request) {
            Pairs.erase(it);
        }
    }

    NProto::TError ConnectXdc(TRequest& request)
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
            const auto it = Pairs.find(request.PairId);
            if (it == Pairs.end() || TMonotonic::Now() >= it->second->Deadline)
            {
                return MakeError(E_ARGUMENT, "expired IC V2 socket pair");
            }
            it->second->XdcSocket = std::move(request.Socket);
            Pairs.erase(it);
            request.Partner->Partner = nullptr;
            request.Partner = nullptr;
            Worker.Wake();
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

    TResultOrError<EStepResult> Advance(TRequest& request)
    {
        while (!Worker.IsStopping()) {
            if (HasError(request.PairError)) {
                return request.PairError;
            }
            if (TMonotonic::Now() >= request.Deadline) {
                return MakeError(E_TIMEOUT, "IC V2 hello timed out");
            }
            if (request.Stage == EStage::WaitXdc) {
                if (!request.XdcSocket) {
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
                result = Advance(*pending[i]);
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
