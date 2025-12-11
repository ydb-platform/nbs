#pragma once

#include "message.h"

namespace NVHostUser {

////////////////////////////////////////////////////////////////////////////////

class TSetOwner: public IMessage
{
public:
    bool Execute(int sock) override
    {
        struct Y_PACKED
        {
            TVhostUserMsgBase Base;
        } request;

        request.Base.Request = VHOST_USER_SET_OWNER;
        request.Base.Flags = 1;
        request.Base.AdditionDataSize = 0;

        if (Send(sock, request.Base) < 0) {
            return false;
        }
        return true;
    }

    TString ToString() const override
    {
        return "VHOST_USER_SET_OWNER";
    }
};

}   // namespace NVHostUser
