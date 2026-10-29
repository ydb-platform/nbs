#include "socket_worker.h"

#include <util/network/pair.h>
#include <util/system/error.h>
#include <util/system/guard.h>

namespace NCloud::NIcV2 {

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
    TGuard<TMutex> guard(Mutex);
    Y_ABORT_UNLESS(!Started && !Stopping);
    Worker = std::thread([this] { Handler.Run(); });
    Started = true;
}

void TSocketWorker::RequestStop()
{
    TGuard<TMutex> guard(Mutex);
    Stopping = true;
    Wake();
}

void TSocketWorker::Stop()
{
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
        if (recv(WakeReader, bytes, sizeof(bytes), 0) > 0) {
            continue;
        }
        if (errno != EINTR) {
            break;
        }
    }
}

int TSocketWorker::Poll(TArrayRef<pollfd> fds, int timeout)
{
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
