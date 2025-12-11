#pragma once

#include "message.h"

#include <util/string/printf.h>

namespace NVHostUser {

////////////////////////////////////////////////////////////////////////////////

class TGetFeature: public IMessage
{
private:
    uint64_t Features = 0;

public:
    enum EVirtioFeatures
    {
        // Don't rename these constants. Names are taken from specification
        VHOST_F_LOG_ALL = 26,
        VIRTIO_F_RING_INDIRECT_DESC = 28,
        VHOST_USER_F_PROTOCOL_FEATURES = 30,
        VIRTIO_F_VERSION_1 = 32
    };

    static TString ToString(EVirtioFeatures feature)
    {
        switch (feature) {
            case VHOST_F_LOG_ALL:
                return "VHOST_F_LOG_ALL";
            case VIRTIO_F_RING_INDIRECT_DESC:
                return "VIRTIO_F_RING_INDIRECT_DESC";
            case VHOST_USER_F_PROTOCOL_FEATURES:
                return "VHOST_USER_F_PROTOCOL_FEATURES";
            case VIRTIO_F_VERSION_1:
                return "VIRTIO_F_VERSION_1";
        }
        return Sprintf("UNKNOWN %d", feature);
    }

    bool Execute(int sock) override
    {
        struct Y_PACKED
        {
            TVhostUserMsgBase Base;
        } request;

        request.Base.Request = VHOST_USER_GET_FEATURES;
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

        Features = response.Features;
        return true;
    }

    TString ToString() const override
    {
        return "VHOST_USER_GET_FEATURES";
    }

    uint64_t GetResult() const
    {
        return Features;
    }
};

}   // namespace NVHostUser
