#pragma once

#include "message.h"

namespace NVHostUser {

////////////////////////////////////////////////////////////////////////////////

class TSetVringNum: public IMessage
{
private:
    uint32_t Index;
    uint32_t Num;

public:
    TSetVringNum(uint32_t index, uint32_t num)
        : Index(index)
        , Num(num)
    {}

    bool Execute(int sock) override
    {
        struct Y_PACKED
        {
            TVhostUserMsgBase Base;
            uint32_t Index;
            uint32_t Num;
        } request;

        request.Base.Request = VHOST_USER_SET_VRING_NUM;
        request.Base.Flags = 1;
        request.Base.AdditionDataSize = sizeof(uint32_t) + sizeof(uint32_t);
        request.Index = Index;
        request.Num = Num;

        if (Send(sock, request.Base) < 0) {
            return false;
        }
        return true;
    }

    TString ToString() const override
    {
        return "VHOST_USER_SET_VRING_NUM";
    }
};

}   // namespace NVHostUser
