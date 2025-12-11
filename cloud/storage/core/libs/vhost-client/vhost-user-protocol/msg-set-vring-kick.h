#pragma once

#include "message.h"

namespace NVHostUser {

////////////////////////////////////////////////////////////////////////////////

class TSetVringKick: public IMessage
{
private:
    uint64_t Index;
    int Fd;

public:
    TSetVringKick(uint64_t index, int fd)
        : Index(index)
        , Fd(fd)
    {}

    bool Execute(int sock) override
    {
        struct Y_PACKED
        {
            TVhostUserMsgBase Base;
            uint64_t Index;
        } request;

        request.Base.Request = VHOST_USER_SET_VRING_KICK;
        request.Base.Flags = 1;
        request.Base.AdditionDataSize = sizeof(uint64_t);
        request.Index = Index;

        if (SendFds(sock, request.Base, {Fd}) < 0) {
            return false;
        }
        return true;
    }

    TString ToString() const override
    {
        return "VHOST_USER_SET_VRING_KICK";
    }
};

}   // namespace NVHostUser
