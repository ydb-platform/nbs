#pragma once

#include <util/network/init.h>

#include <memory>

namespace NCloud::NStorage::NServer {

////////////////////////////////////////////////////////////////////////////////

class TSocketPoller
{
private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    TSocketPoller();
    ~TSocketPoller();

    void WaitRead(SOCKET sock, void* cookie);
    void WaitClose(SOCKET sock, void* cookie);

    void Unwait(SOCKET sock);

    size_t Wait(void** events, size_t len);
};

}   // namespace NCloud::NStorage::NServer
