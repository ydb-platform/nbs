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

class TTestListenerHandler final: public IListenerHandler
{
    const std::function<void(TSocketResult)> Callback;

public:
    explicit TTestListenerHandler(std::function<void(TSocketResult)> callback);

    void OnSocketAccepted(TSocketPtr socket) override;
    void OnError(const NProto::TError& error) override;
};

IConnectorPtr CreateTestConnector();

TSocketResult WaitResult(const TSocketFuture& future);
TSocketPtr WaitSocket(const TSocketFuture& future);
void CheckSocket(const TSocketPtr& socket);

// Two sockets fill backlog 1; keep them alive without accepting.
// Pending connects may need SYN retransmission after accepting resumes.
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
