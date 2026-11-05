#include "sync.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <util/generic/maybe.h>
#include <util/generic/size_literals.h>
#include <util/generic/yexception.h>
#include <util/network/sock.h>
#include <util/string/builder.h>
#include <util/system/byteorder.h>
#include <util/system/error.h>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 MaxFrameSize = 256_MB;

NProto::TError MakeSystemError(const TString& what, int error)
{
    return MakeError(
        E_IO,
        TStringBuilder() << what << ": " << LastSystemErrorText(error));
}

NProto::TError WaitFor(SOCKET fd, short events, TInstant deadline)
{
    pollfd pfd = {.fd = fd, .events = events, .revents = 0};
    const ssize_t res = PollD(&pfd, 1, deadline);
    if (res == -ETIMEDOUT) {
        return MakeError(E_TIMEOUT, "timed out");
    }
    if (res < 0) {
        return MakeSystemError("poll", -res);
    }
    return {};
}

NProto::TError SendAll(
    TSocket& socket,
    const char* data,
    size_t size,
    TInstant deadline)
{
    while (size) {
        if (auto error = WaitFor(socket, POLLOUT, deadline); HasError(error)) {
            return error;
        }

        const ssize_t res = socket.Send(data, size);
        if (res < 0) {
            if (-res == EAGAIN) {
                continue;
            }
            return MakeSystemError("send", -res);
        }

        data += res;
        size -= res;
    }

    return {};
}

NProto::TError RecvAll(
    TSocket& socket,
    char* data,
    size_t size,
    TInstant deadline)
{
    while (size) {
        if (auto error = WaitFor(socket, POLLIN, deadline); HasError(error)) {
            return error;
        }

        const ssize_t res = socket.Recv(data, size);
        if (res < 0) {
            if (-res == EAGAIN) {
                continue;
            }
            return MakeSystemError("recv", -res);
        }
        if (res == 0) {
            return MakeError(E_IO, "connection closed");
        }

        data += res;
        size -= res;
    }

    return {};
}

NProto::TError SendFrame(
    TSocket& socket,
    const NJson::TJsonValue& message,
    TInstant deadline)
{
    const TString body = NJson::WriteJson(message, false /* formatOutput */);
    const ui32 size = HostToLittle(static_cast<ui32>(body.size()));
    auto error = SendAll(
        socket,
        reinterpret_cast<const char*>(&size),
        sizeof(size),
        deadline);
    if (HasError(error)) {
        return error;
    }

    return SendAll(socket, body.data(), body.size(), deadline);
}

TResultOrError<NJson::TJsonValue> RecvFrame(
    TSocket& socket,
    TInstant deadline)
{
    ui32 size = 0;
    auto error = RecvAll(
        socket,
        reinterpret_cast<char*>(&size),
        sizeof(size),
        deadline);
    if (HasError(error)) {
        return error;
    }

    size = LittleToHost(size);
    if (size > MaxFrameSize) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "frame is too big: " << size);
    }

    TString body = TString::Uninitialized(size);
    error = RecvAll(socket, body.begin(), size, deadline);
    if (HasError(error)) {
        return error;
    }

    NJson::TJsonValue message;
    if (!NJson::ReadJsonTree(body, &message) || !message.IsMap()) {
        return MakeError(E_ARGUMENT, "malformed frame");
    }

    return message;
}

////////////////////////////////////////////////////////////////////////////////

class TSyncHub
{
private:
    TLog& Log;
    const ui16 Port;
    const ui32 InstanceCount;
    const TDuration Timeout;

    // Indexed by instance id.
    TVector<TMaybe<TSocket>> Instances;

public:
    TSyncHub(TLog& log, ui16 port, ui32 instanceCount, TDuration timeout)
        : Log(log)
        , Port(port)
        , InstanceCount(instanceCount)
        , Timeout(timeout)
    {}

    int Run()
    {
        if (auto error = AcceptInstances(); HasError(error)) {
            STORAGE_ERROR("Failed to accept instances: "
                << FormatError(error));
            return 1;
        }

        STORAGE_INFO("All " << InstanceCount << " instances connected");

        for (;;) {
            auto [finished, error] = ServeBarrier();
            if (HasError(error)) {
                //
                // The instances get the connections closed upon the hub's
                // exit and fail instead of waiting until their timeouts.
                //

                STORAGE_ERROR("Sync failed: " << FormatError(error));
                return 1;
            }

            if (finished) {
                STORAGE_INFO("All instances finished");
                return 0;
            }
        }
    }

private:
    NProto::TError AcceptInstances()
    {
        //
        // Dual-stack socket - accepts both IPv4 and IPv6 connections on all
        // addresses.
        //

        TInet6StreamSocket listener;
        if (listener.Closed()) {
            return MakeSystemError("socket", LastSystemError());
        }
        if (SetSockOpt(listener, IPPROTO_IPV6, IPV6_V6ONLY, 0)) {
            return MakeSystemError("IPV6_V6ONLY", LastSystemError());
        }
        if (SetSockOpt(listener, SOL_SOCKET, SO_REUSEADDR, 1)) {
            return MakeSystemError("SO_REUSEADDR", LastSystemError());
        }

        TSockAddrInet6 addr("::", Port);
        if (const int res = listener.Bind(&addr); res < 0) {
            return MakeSystemError("bind", -res);
        }
        if (const int res = listener.Listen(InstanceCount); res < 0) {
            return MakeSystemError("listen", -res);
        }

        STORAGE_INFO("Listening on port " << Port);

        Instances.resize(InstanceCount);
        const TInstant deadline = Timeout.ToDeadLine();
        for (ui32 i = 0; i < InstanceCount; ++i) {
            auto error = WaitFor(listener, POLLIN, deadline);
            if (HasError(error)) {
                return error;
            }

            TStreamSocket accepted;
            if (const int res = listener.Accept(&accepted); res < 0) {
                return MakeSystemError("accept", -res);
            }
            TSocket connection(accepted.Release());

            auto [hello, recvError] = RecvFrame(connection, deadline);
            if (HasError(recvError)) {
                return recvError;
            }

            const ui64 instanceId =
                hello["instance"].GetUIntegerSafe(Max<ui64>());
            if (hello["type"].GetStringSafe("") != "hello" ||
                instanceId >= InstanceCount ||
                Instances[instanceId])
            {
                return MakeError(
                    E_ARGUMENT,
                    TStringBuilder() << "unexpected hello: "
                        << NJson::WriteJson(hello, false));
            }

            STORAGE_INFO("Instance " << instanceId << " connected");
            Instances[instanceId] = std::move(connection);
        }

        return {};
    }

    TResultOrError<bool> ServeBarrier()
    {
        //
        // The instances are waited for one by one - the barrier requires all
        // of them anyway.
        //

        TVector<NJson::TJsonValue> messages;
        const TInstant deadline = Timeout.ToDeadLine();
        for (ui32 i = 0; i < InstanceCount; ++i) {
            auto [message, error] = RecvFrame(*Instances[i], deadline);
            if (HasError(error)) {
                return MakeError(
                    error.GetCode(),
                    TStringBuilder() << "instance " << i << ": "
                        << FormatError(error));
            }
            messages.push_back(std::move(message));
        }

        const auto& first = messages.front();
        const TString type = first["type"].GetStringSafe("");
        const ui64 round = first["round"].GetUIntegerSafe(0);
        const TString phase = first["phase"].GetStringSafe("");
        for (ui32 i = 0; i < InstanceCount; ++i) {
            const auto& m = messages[i];
            const bool same = m["type"].GetStringSafe("") == type &&
                m["round"].GetUIntegerSafe(0) == round &&
                m["phase"].GetStringSafe("") == phase;
            if (!same || (type != "exchange" && type != "done")) {
                return MakeError(
                    E_ARGUMENT,
                    TStringBuilder() << "instances are out of sync"
                        << ", instance 0: " << NJson::WriteJson(first, false)
                        << ", instance " << i << ": "
                        << NJson::WriteJson(m, false));
            }
        }

        if (type == "done") {
            return true;
        }

        STORAGE_INFO("Barrier round=" << round << " phase=" << phase);

        NJson::TJsonValue result(NJson::JSON_MAP);
        result["type"] = "result";
        result["round"] = first["round"];
        result["phase"] = first["phase"];
        auto& payloads = result["payloads"];
        payloads.SetType(NJson::JSON_ARRAY);
        for (auto& m: messages) {
            payloads.AppendValue(std::move(m["payload"]));
        }

        for (ui32 i = 0; i < InstanceCount; ++i) {
            auto error = SendFrame(*Instances[i], result, deadline);
            if (HasError(error)) {
                return MakeError(
                    error.GetCode(),
                    TStringBuilder() << "instance " << i << ": "
                        << FormatError(error));
            }
        }

        return false;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TSyncClient::TSyncClient(TSocket socket, TDuration timeout)
    : Socket(std::move(socket))
    , Timeout(timeout)
{}

TResultOrError<std::unique_ptr<TSyncClient>> TSyncClient::Connect(
    const TString& host,
    ui16 port,
    ui32 instanceId,
    TDuration timeout)
{
    //
    // The util networking API reports the resolution and connection errors
    // via exceptions.
    //

    TMaybe<TSocket> socket;
    try {
        socket.ConstructInPlace(TNetworkAddress(host, port), timeout);
    } catch (...) {
        return MakeError(
            E_IO,
            TStringBuilder() << "connect to " << host << ":" << port << ": "
                << CurrentExceptionMessage());
    }

    NJson::TJsonValue hello(NJson::JSON_MAP);
    hello["type"] = "hello";
    hello["instance"] = instanceId;
    auto error = SendFrame(*socket, hello, timeout.ToDeadLine());
    if (HasError(error)) {
        return error;
    }

    return std::make_unique<TSyncClient>(std::move(*socket), timeout);
}

TResultOrError<TVector<NJson::TJsonValue>> TSyncClient::Exchange(
    ui32 round,
    const TString& phase,
    const NJson::TJsonValue& payload)
{
    NJson::TJsonValue message(NJson::JSON_MAP);
    message["type"] = "exchange";
    message["round"] = round;
    message["phase"] = phase;
    message["payload"] = payload;

    const TInstant deadline = Timeout.ToDeadLine();
    if (auto error = SendFrame(Socket, message, deadline); HasError(error)) {
        return error;
    }

    auto [result, error] = RecvFrame(Socket, deadline);
    if (HasError(error)) {
        return error;
    }

    if (result["type"].GetStringSafe("") != "result" ||
        result["round"].GetUIntegerSafe(0) != round ||
        result["phase"].GetStringSafe("") != phase ||
        !result["payloads"].IsArray())
    {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "unexpected result: "
                << NJson::WriteJson(result, false));
    }

    TVector<NJson::TJsonValue> payloads;
    for (auto& p: result["payloads"].GetArraySafe()) {
        payloads.push_back(std::move(p));
    }
    return payloads;
}

NProto::TError TSyncClient::Finish()
{
    NJson::TJsonValue message(NJson::JSON_MAP);
    message["type"] = "done";
    return SendFrame(Socket, message, Timeout.ToDeadLine());
}

////////////////////////////////////////////////////////////////////////////////

int RunSyncHub(TLog& Log, ui16 port, ui32 instanceCount, TDuration timeout)
{
    TSyncHub hub(Log, port, instanceCount, timeout);
    return hub.Run();
}

}   // namespace NCloud::NFileStore
