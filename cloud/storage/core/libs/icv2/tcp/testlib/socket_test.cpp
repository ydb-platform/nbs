#include "socket_test.h"

#include <cloud/storage/core/libs/icv2/tcp/listener_private.h>

#include <library/cpp/testing/unittest/registar.h>

#include <fcntl.h>
#include <netinet/tcp.h>

#include <chrono>

namespace NCloud::NIcV2::NTests {

////////////////////////////////////////////////////////////////////////////////

TTestListenerClient::TTestListenerClient(
    std::function<void(TSocketResult)> callback)
    : Callback(std::move(callback))
{}

void TTestListenerClient::OnConnection(TSocketPtr socket)
{
    Callback(std::move(socket));
}

void TTestListenerClient::OnError(const NProto::TError& error)
{
    Callback(error);
}

TSocketResult WaitResult(const TSocketFuture& future)
{
    UNIT_ASSERT(future.Wait(TDuration::Seconds(10)));
    return future.GetValue();
}

TSocketPtr WaitSocket(const TSocketFuture& future)
{
    auto [socket, error] = WaitResult(future);
    UNIT_ASSERT_C(!HasError(error), FormatError(error));
    return socket;
}

void CheckSocket(const TSocketPtr& socket)
{
    UNIT_ASSERT(socket);
    UNIT_ASSERT(IsNonBlock(*socket));
    UNIT_ASSERT(fcntl(*socket, F_GETFD) & FD_CLOEXEC);
    int noDelay = 0;
    UNIT_ASSERT_VALUES_EQUAL(
        GetSockOpt(*socket, IPPROTO_TCP, TCP_NODELAY, noDelay),
        0);
    UNIT_ASSERT_VALUES_EQUAL(noDelay, 1);
}

std::array<TSocketPtr, 2> OccupyBacklog(const NInterconnect::TAddress& address)
{
    auto connector = CreateConnector();
    connector->Start();
    std::array<TSocketPtr, 2> sockets;
    for (auto& socket: sockets) {
        socket = WaitSocket(connector->Connect(address, TDuration::Seconds(5)));
    }
    connector->Stop();
    return sockets;
}

TTestListener::TTestListener(const char* host, int backlog)
    : Listener(
          [&]
          {
              auto [listener, error] = NPrivate::CreateListenerWithAcceptError(
                  NInterconnect::TAddress(host, 0),
                  backlog,
                  std::make_shared<TTestListenerClient>(
                      [this](TSocketResult result)
                      {
                          std::lock_guard guard(Mutex);
                          Accepted.push_back(std::move(result));
                          Changed.notify_all();
                      }));
              UNIT_ASSERT_C(!HasError(error), FormatError(error));
              return listener;
          }())
{}

TTestListener::~TTestListener()
{
    Listener->Stop();
}

TSocketPtr TTestListener::Accept()
{
    std::unique_lock lock(Mutex);
    UNIT_ASSERT(Changed.wait_for(
        lock,
        std::chrono::seconds(10),
        [&] { return !Accepted.empty(); }));
    auto [socket, error] = std::move(Accepted.front());
    Accepted.pop_front();
    UNIT_ASSERT_C(!HasError(error), FormatError(error));
    return socket;
}

}   // namespace NCloud::NIcV2::NTests
