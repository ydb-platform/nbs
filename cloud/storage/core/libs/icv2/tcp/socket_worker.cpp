#include "socket_worker.h"

#include <util/network/pair.h>
#include <util/system/error.h>
#include <util/system/guard.h>

namespace NCloud::NIcV2 {

namespace {

thread_local const TSocketWorker* CurrentWorker = nullptr;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ISocketWorkerHandler::~ISocketWorkerHandler() = default;

void ISocketWorkerHandler::OnStopped()
{}

TSocketWorker::TSocketWorker(ISocketWorkerHandler& handler)
    : Handler(handler)
{
    SOCKET sockets[2];
    if (SocketPair(sockets, false, true) < 0) {
        ythrow TSystemError() << "TCP worker wakeup socket pair";
    }
    WakeReader = TSocketHolder(sockets[0]);
    WakeWriter = TSocketHolder(sockets[1]);
    SetNonBlock(WakeReader, true);
    SetNonBlock(WakeWriter, true);
}

TSocketWorker::~TSocketWorker()
{
    Y_ABORT_UNLESS(!Worker.joinable());
}

void TSocketWorker::Start()
{
    TGuard<TMutex> stop(StopMutex);
    Y_ABORT_UNLESS(!Worker.joinable() && !Joined && !Stopping);

    Worker = std::thread(
        [this]
        {
            CurrentWorker = this;
            Handler.Run();
            CurrentWorker = nullptr;
        });
}

void TSocketWorker::RequestStop()
{
    Stopping = true;
    Wake();
}

void TSocketWorker::Stop()
{
    Y_ABORT_UNLESS(
        CurrentWorker != this,
        "Stop() must not be called from the socket worker thread");

    RequestStop();
    TGuard<TMutex> stop(StopMutex);
    if (!Joined) {
        if (Worker.joinable()) {
            Worker.join();
        }
        Handler.OnStopped();
        Joined = true;
    }
}

bool TSocketWorker::IsStopping() const
{
    return Stopping.load();
}

void TSocketWorker::Wake()
{
    const char byte = 0;
    ssize_t res;
    do {
        res = send(WakeWriter, &byte, sizeof(byte), MSG_NOSIGNAL);
    } while (res < 0 && errno == EINTR);
    Y_ABORT_UNLESS(
        res == 1 || (res < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)));
}

SOCKET TSocketWorker::GetWakeSocket() const
{
    return WakeReader;
}

void TSocketWorker::DrainWake()
{
    char bytes[256];
    for (;;) {
        const ssize_t res = recv(WakeReader, bytes, sizeof(bytes), 0);
        if (res > 0) {
            continue;
        }
        if (res == 0 || errno != EINTR) {
            break;
        }
    }
}

int TSocketWorker::Poll(TArrayRef<pollfd> fds, int timeout)
{
    Y_ABORT_UNLESS(
        !fds.empty() && fds[0].fd == GetWakeSocket(),
        "Poll() requires the wakeup socket as the first descriptor");

    const int res = poll(fds.data(), fds.size(), timeout);
    if (res < 0) {
        return errno == EINTR ? 0 : -errno;
    }
    if (fds[0].revents & POLLIN) {
        DrainWake();
    }
    return res;
}

}   // namespace NCloud::NIcV2
