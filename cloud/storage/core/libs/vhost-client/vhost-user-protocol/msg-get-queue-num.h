#pragma once

#include "message.h"

namespace NVHostUser {

////////////////////////////////////////////////////////////////////////////////

class TGetQueueNum: public IMessage
{
private:
    uint64_t QueueNum = 0;

public:
    bool Execute(int sock) override
    {
        struct Y_PACKED
        {
            TVhostUserMsgBase Base;
        } request;

        request.Base.Request = VHOST_USER_GET_QUEUE_NUM;
        request.Base.Flags = 1;
        request.Base.AdditionDataSize = 0;

        if (Send(sock, request.Base) < 0) {
            return false;
        }

        struct Y_PACKED
        {
            TVhostUserMsgBase Base;
            uint64_t Features;
        } response;

        response.Base.Request = VHOST_USER_NONE;
        response.Base.Flags = 0;
        response.Base.AdditionDataSize = sizeof(uint64_t);

        if (Recv(sock, response.Base) < 0) {
            return false;
        }

        QueueNum = response.Features;
        return true;
    }

    TString ToString() const override
    {
        return "VHOST_USER_GET_QUEUE_NUM";
    }

    uint64_t GetResult() const
    {
        return QueueNum;
    }
};

}   // namespace NVHostUser
