#include "handshake.h"

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

using NCloud::NIcV2::CreateSocket;
using NCloud::NIcV2::ISocketWorkerHandler;
using NCloud::NIcV2::MakeSocketError;
using NCloud::NIcV2::TSocketWorker;

namespace {

////////////////////////////////////////////////////////////////////////////////

// Hello: "ICV2", version:u16, flags:u16. Integers use network byte order.
// Bit 0 selects checksums; bit 1 offers/selects XDC and appends a 16-byte
// opaque pair ID; bit 2 identifies its auxiliary socket. XDC replies echo the
// pair ID. Main and auxiliary sockets each exchange hello/reply/ack (byte 1).
// The main initiator sends its ack only after completing the auxiliary
// exchange. The acceptor publishes the pair only after receiving both
// acknowledgements. Reads never consume bytes beyond the preface. There is no
// in-flight downgrade.
constexpr size_t HeaderSize = 8;
constexpr ui16 Version = 1;
constexpr ui16 ChecksumFlag = 1;
constexpr ui16 XdcFlag = 2;
constexpr ui16 AuxiliaryFlag = 4;
constexpr size_t PairIdSize = 16;
constexpr char Ack = 1;

void AppendWord(TString& buffer, ui16 value)
{
    buffer.push_back(value >> 8);
    buffer.push_back(value & 0xff);
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

struct TRequest
{
    TSocketPtr Socket;
    const bool Initiator;
    const TMonotonic Deadline;
    NThreading::TPromise<TAcceptHandshakeResult> Promise =
        NThreading::NewPromise<TAcceptHandshakeResult>();
    EStage Stage;
    TString Buffer;
    size_t Offset = 0;
    bool HasPairId = false;
    bool UseXdc = false;
    bool Auxiliary = false;
    bool OnXdc = false;
    TString PairId;
    TSocketPtr XdcSocket;
    TRequest* Partner = nullptr;
    NProto::TError PairError;

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

    void Next(EStage stage, TString buffer)
    {
        Stage = stage;
        Buffer = std::move(buffer);
        Offset = 0;
    }
};

TString EncodeHello(const THandshakeConfig& config, const TRequest& request)
{
    TString buffer = "ICV2";
    AppendWord(buffer, Version);
    AppendWord(
        buffer,
        (config.EnableChecksum ? ChecksumFlag : 0) |
            (request.UseXdc ? XdcFlag : 0) |
            (request.Auxiliary || request.OnXdc ? AuxiliaryFlag : 0));
    buffer.append(request.PairId);
    return buffer;
}

using TRequests = TVector<std::unique_ptr<TRequest>>;

void Complete(std::unique_ptr<TRequest> request, const NProto::TError& error)
{
    if (HasError(error)) {
        request->Socket.Reset();
        request->XdcSocket.Reset();
        request->Promise.SetValue(error);
    } else if (request->Auxiliary) {
        request->Promise.SetValue(
            TAcceptHandshakeOutcome{THandshakeIncomplete{}});
    } else {
        request->Promise.SetValue(
            TAcceptHandshakeOutcome{THandshakeReady{
                std::move(request->Socket),
                std::move(request->XdcSocket)}});
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

    THandshakeFuture Connect(TSocketPtr socket, TDuration timeout) override
    {
        return Submit(std::move(socket), true, timeout)
            .Apply(
                [](const TAcceptHandshakeFuture& future) -> THandshakeResult
                {
                    const auto& result = future.GetValue();
                    if (HasError(result.GetError())) {
                        return result.GetError();
                    }

                    return std::get<THandshakeReady>(result.GetResult());
                });
    }

    TAcceptHandshakeFuture Accept(TSocketPtr socket, TDuration timeout) override
    {
        return Submit(std::move(socket), false, timeout);
    }

private:
    TAcceptHandshakeFuture
    Submit(TSocketPtr socket, bool initiator, TDuration timeout)
    {
        auto request = std::make_unique<TRequest>(
            std::move(socket),
            initiator,
            TMonotonic::Now() + timeout);
        auto future = request->Promise.GetFuture();
        if (!request->Socket || !IsNonBlock(*request->Socket)) {
            Complete(
                std::move(request),
                MakeError(E_ARGUMENT, "invalid hello request"));
            return future;
        }
        if (initiator) {
            if (Config.EnableXdc) {
                request->UseXdc = true;
                const auto id = TGUID::Create();
                request->PairId.assign(
                    reinterpret_cast<const char*>(id.dw),
                    PairIdSize);
            }
            request->Next(EStage::SendHello, EncodeHello(Config, *request));
        } else {
            request->Next(EStage::ReceiveHeader, TString(HeaderSize, '\0'));
        }
        {
            TGuard<TMutex> guard(Mutex);
            if (Accepting) {
                Queued.push_back(std::move(request));
                Worker.Wake();
                return future;
            }
        }
        Complete(
            std::move(request),
            MakeError(E_REJECTED, "IC V2 hello is not running"));
        return future;
    }

    TRequests TakeQueued()
    {
        TRequests queued;
        TGuard<TMutex> guard(Mutex);
        queued.swap(Queued);
        return queued;
    }

    NProto::TError CheckHeader(TRequest& request)
    {
        const auto& buffer = request.Buffer;
        if (buffer.substr(0, 4) != "ICV2" || ReadWord(buffer, 4) != Version) {
            return MakeError(
                E_ARGUMENT,
                "incompatible IC V2 hello version or magic");
        }
        const auto flags = ReadWord(buffer, 6);
        const auto allowed = ChecksumFlag | XdcFlag | AuxiliaryFlag;
        const bool auxiliary = flags & AuxiliaryFlag;
        const bool xdc = flags & XdcFlag;
        if ((flags & ~allowed) ||
            bool(flags & ChecksumFlag) != Config.EnableChecksum ||
            (auxiliary && (!xdc || !Config.EnableXdc)) ||
            (request.Initiator &&
             (auxiliary != request.OnXdc || (request.OnXdc && !xdc))))
        {
            return MakeError(E_ARGUMENT, "incompatible IC V2 hello flags");
        }
        request.HasPairId = xdc;
        request.Auxiliary = !request.Initiator && auxiliary;
        request.UseXdc = xdc && Config.EnableXdc;
        request.Stage = EStage::ReceiveBody;
        request.Buffer.resize(
            HeaderSize + (request.HasPairId ? PairIdSize : 0));
        return {};
    }

    NProto::TError CheckPair(TRequest& request)
    {
        if (request.HasPairId) {
            const auto id = request.Buffer.substr(HeaderSize);
            if (id == TString(PairIdSize, '\0') ||
                (request.Initiator && id != request.PairId))
            {
                return MakeError(E_ARGUMENT, "invalid IC V2 socket pair ID");
            }
            request.PairId = id;
        }
        if (!request.UseXdc) {
            request.PairId.clear();
        }
        if (!request.Initiator && request.UseXdc) {
            if (request.Auxiliary) {
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
            request.Next(EStage::SendHello, EncodeHello(Config, request));
        } else if (res == EINPROGRESS || res == EINTR) {
            request.Next(EStage::ConnectXdc, {});
        } else {
            return MakeSocketError(res, "connect IC V2 XDC");
        }
        return {};
    }

    NProto::TError FinishExchange(TRequest& request, bool& complete)
    {
        if (request.Initiator && request.OnXdc) {
            request.OnXdc = false;
            request.Next(EStage::SendAck, TString(1, Ack));
        } else if (request.Auxiliary) {
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
            complete = true;
        } else if (!request.Initiator && request.UseXdc && !request.XdcSocket) {
            request.Next(EStage::WaitXdc, {});
        } else {
            complete = true;
        }
        return {};
    }

    NProto::TError Advance(TRequest& request, bool& complete)
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
                complete = true;
                return {};
            }
            if (request.Stage == EStage::ConnectXdc) {
                if (const int status = request.XdcSocket->GetConnectStatus()) {
                    return MakeSocketError(
                        status,
                        "connect IC V2 XDC completion");
                }
                request.Next(EStage::SendHello, EncodeHello(Config, request));
            }
            if (request.Offset < request.Buffer.size()) {
                const int fd = *request.CurrentSocket();
                char* data = request.Buffer.Detach() + request.Offset;
                const size_t size = request.Buffer.size() - request.Offset;
                const ssize_t res = request.IsWriting()
                                        ? send(fd, data, size, MSG_NOSIGNAL)
                                        : recv(fd, data, size, 0);
                if (res < 0) {
                    const int error = errno;
                    if (error == EINTR) {
                        continue;
                    }
                    if (error == EAGAIN || error == EWOULDBLOCK) {
                        return {};
                    }
                    return MakeSocketError(error, "IC V2 hello I/O");
                }
                if (!res) {
                    return MakeError(
                        E_IO,
                        "connection closed during IC V2 hello");
                }
                request.Offset += res;
                if (request.Offset != request.Buffer.size()) {
                    continue;
                }
            }
            switch (request.Stage) {
                case EStage::SendHello:
                    if (request.Initiator) {
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
                    if (auto error = CheckPair(request); HasError(error)) {
                        return error;
                    }
                    if (request.Initiator) {
                        if (request.UseXdc && !request.OnXdc) {
                            if (auto error = ConnectXdc(request);
                                HasError(error))
                            {
                                return error;
                            }
                            if (request.Stage == EStage::ConnectXdc) {
                                return {};
                            }
                        } else {
                            request.Next(EStage::SendAck, TString(1, Ack));
                        }
                    } else {
                        request.Next(
                            EStage::SendHello,
                            EncodeHello(Config, request));
                    }
                    break;
                case EStage::ReceiveAck:
                    if (request.Buffer[0] != Ack) {
                        return MakeError(
                            E_ARGUMENT,
                            "invalid IC V2 hello acknowledgement");
                    }
                    [[fallthrough]];
                case EStage::SendAck:
                    if (auto error = FinishExchange(request, complete);
                        HasError(error))
                    {
                        return error;
                    }
                    if (complete || request.Stage == EStage::WaitXdc) {
                        return {};
                    }
                    break;
                case EStage::ConnectXdc:
                case EStage::WaitXdc:
                    Y_ABORT("handled before I/O");
            }
        }
        return {};
    }

    void Run() override
    {
        TRequests pending;
        auto failure = MakeError(E_REJECTED, "IC V2 hello is stopping");
        while (!Worker.IsStopping()) {
            for (auto& request: TakeQueued()) {
                pending.push_back(std::move(request));
            }
            TVector<pollfd> fds(pending.size() + 1);
            fds[0] = {Worker.GetWakeSocket(), POLLIN, 0};
            auto deadline = TMonotonic::Max();
            bool ready = false;
            for (size_t i = 0; i < pending.size(); ++i) {
                const auto& request = *pending[i];
                fds[i + 1] = {
                    *request.CurrentSocket(),
                    static_cast<short>(
                        request.Stage == EStage::WaitXdc ? POLLRDHUP
                        : request.IsWriting()            ? POLLOUT
                                                         : POLLIN),
                    0};
                deadline = Min(deadline, request.Deadline);
                ready |= request.IsReady();
            }
            const auto now = TMonotonic::Now();
            const int timeout = ready             ? 0
                                : pending.empty() ? -1
                                : deadline <= now
                                    ? 0
                                    : static_cast<int>(Min<ui64>(
                                          (deadline - now).MilliSeconds() + 1,
                                          INT_MAX));
            const int res = Worker.Poll(fds, timeout);
            if (res < 0) {
                failure = MakeSocketError(-res, "hello poll");
                RequestStop();
                break;
            }
            for (size_t i = pending.size(); i-- && !Worker.IsStopping();) {
                bool complete = false;
                NProto::TError error;
                if (TMonotonic::Now() >= pending[i]->Deadline) {
                    error = MakeError(E_TIMEOUT, "IC V2 hello timed out");
                } else if (fds[i + 1].revents || pending[i]->IsReady()) {
                    error = Advance(*pending[i], complete);
                }
                if (complete || HasError(error)) {
                    auto request = std::move(pending[i]);
                    pending[i] = std::move(pending.back());
                    pending.pop_back();
                    RemovePair(*request, error);
                    Complete(std::move(request), error);
                }
            }
        }
        Pairs.clear();
        for (auto& request: pending) {
            Complete(std::move(request), failure);
        }
        for (auto& request: TakeQueued()) {
            Complete(std::move(request), failure);
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
