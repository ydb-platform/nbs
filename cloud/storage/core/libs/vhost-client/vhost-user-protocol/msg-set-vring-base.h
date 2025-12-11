#pragma once

#include "message.h"

namespace NVHostUser {

////////////////////////////////////////////////////////////////////////////////

class TSetVringBase: public IMessage
{
private:
    uint32_t Index;
    uint32_t Num;

public:
    TSetVringBase(uint32_t index, uint32_t num)
        : Index(index)
        , Num(num)
    {}

    bool Execute(int sock) override
    {
        struct Y_PACKED
        {
            TVhostUserMsgBase Base;
            uint32_t index;
            uint32_t num;
        } request;

        request.Base.Request = VHOST_USER_SET_VRING_BASE;
        request.Base.Flags = 1;
        request.Base.AdditionDataSize = sizeof(uint32_t) + sizeof(uint32_t);
        request.index = Index;
        request.num = Num;

        if (Send(sock, request.Base) < 0) {
            return false;
        }
        return true;
    }

    TString ToString() const override
    {
        return "VHOST_USER_SET_VRING_BASE";
    }
};

}   // namespace NVHostUser
