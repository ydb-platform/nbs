#pragma once

#include "message.h"

namespace NVHostUser {

////////////////////////////////////////////////////////////////////////////////

class TSetProtocolFeature: public IMessage
{
private:
    uint64_t Features = 0;

public:
    explicit TSetProtocolFeature(uint64_t features)
        : Features(features)
    {}

    bool Execute(int sock) override
    {
        struct Y_PACKED
        {
            TVhostUserMsgBase Base;
            uint64_t Features;
        } request;

        request.Base.Request = VHOST_USER_SET_PROTOCOL_FEATURES;
        request.Base.Flags = 1;
        request.Base.AdditionDataSize = sizeof(uint64_t);
        request.Features = Features;

        if (Send(sock, request.Base) < 0) {
            return false;
        }

        return true;
    }

    TString ToString() const override
    {
        return "VHOST_USER_SET_PROTOCOL_FEATURES";
    }
};

}   // namespace NVHostUser
