#pragma once

#include "message.h"

namespace NVHostUser {

////////////////////////////////////////////////////////////////////////////////

class TGetVringBase: public IMessage
{
private:
    uint32_t Index;
    uint32_t Num = 0;

public:
    explicit TGetVringBase(uint32_t index)
        : Index(index)
    {}

    bool Execute(int sock) override
    {
        struct Y_PACKED
        {
            TVhostUserMsgBase Base;
            uint32_t Index;
            uint32_t Num;
        } request;

        request.Base.Request = VHOST_USER_GET_VRING_BASE;
        request.Base.Flags = 1;
        request.Base.AdditionDataSize = sizeof(uint32_t) + sizeof(uint32_t);
        request.Index = Index;
        request.Num = Num;

        if (Send(sock, request.Base) < 0) {
            return false;
        }

        struct Y_PACKED
        {
            TVhostUserMsgBase Base;
            uint32_t Index;
            uint32_t Num;
        } response;

        response.Base.Request = VHOST_USER_NONE;
        response.Base.Flags = 0;
        response.Base.AdditionDataSize = sizeof(uint32_t) + sizeof(uint32_t);

        if (Recv(sock, response.Base) < 0) {
            return false;
        }

        Num = response.Num;
        return true;
    }

    TString ToString() const override
    {
        return "VHOST_USER_GET_VRING_BASE";
    }

    uint32_t GetResult() const
    {
        return Num;
    }
};

}   // namespace NVHostUser
