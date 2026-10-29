#pragma once

#include <cloud/storage/core/libs/icv2/tcp/connector.h>
#include <cloud/storage/core/libs/icv2/tcp/listener.h>

#include <array>
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>

namespace NCloud::NIcV2::NTests {

////////////////////////////////////////////////////////////////////////////////

class TTestListenerClient final: public IListenerClient
{
    const std::function<void(TSocketResult)> Callback;

public:
    explicit TTestListenerClient(std::function<void(TSocketResult)> callback);

    void OnConnection(TSocketPtr socket) override;
    void OnError(const NProto::TError& error) override;
};

TSocketResult WaitResult(const TSocketFuture& future);
TSocketPtr WaitSocket(const TSocketFuture& future);
void CheckSocket(const TSocketPtr& socket);

// Keep both sockets alive without accepting to fill a listener's backlog of 1.
std::array<TSocketPtr, 2> OccupyBacklog(const NInterconnect::TAddress& address);

class TTestListener
{
    std::mutex Mutex;
    std::condition_variable Changed;
    std::deque<TSocketResult> Accepted;

public:
    const IListenerPtr Listener;

    explicit TTestListener(const char* host = "127.0.0.1", int backlog = 16);
    ~TTestListener();

    TSocketPtr Accept();
};

}   // namespace NCloud::NIcV2::NTests
