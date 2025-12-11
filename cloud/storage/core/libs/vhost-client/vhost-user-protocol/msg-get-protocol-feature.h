#pragma once

#include "message.h"

#include <util/string/printf.h>

namespace NVHostUser {

////////////////////////////////////////////////////////////////////////////////

class TGetProtocolFeature: public IMessage
{
private:
    uint64_t Features = 0;

public:
    enum EVhostUserFeatures
    {
        // Don't rename these constants. Names are taken from specification
        VHOST_USER_PROTOCOL_F_MQ = 0,
        VHOST_USER_PROTOCOL_F_LOG_SHMFD = 1,
        VHOST_USER_PROTOCOL_F_RARP = 2,
        VHOST_USER_PROTOCOL_F_REPLY_ACK = 3,
        VHOST_USER_PROTOCOL_F_MTU = 4,
        VHOST_USER_PROTOCOL_F_SLAVE_REQ = 5,
        VHOST_USER_PROTOCOL_F_CROSS_ENDIAN = 6,
        VHOST_USER_PROTOCOL_F_CRYPTO_SESSION = 7,
        VHOST_USER_PROTOCOL_F_PAGEFAULT = 8,
        VHOST_USER_PROTOCOL_F_CONFIG = 9,
        VHOST_USER_PROTOCOL_F_SLAVE_SEND_FD = 10,
        VHOST_USER_PROTOCOL_F_HOST_NOTIFIER = 11,
        VHOST_USER_PROTOCOL_F_INFLIGHT_SHMFD = 12,
        VHOST_USER_PROTOCOL_F_RESET_DEVICE = 13,
        VHOST_USER_PROTOCOL_F_INBAND_NOTIFICATIONS = 14,
        VHOST_USER_PROTOCOL_F_CONFIGURE_MEM_SLOTS = 15,
        VHOST_USER_PROTOCOL_F_STATUS = 16
    };

    static TString ToString(EVhostUserFeatures feature)
    {
        switch (feature) {
            case VHOST_USER_PROTOCOL_F_MQ:
                return "VHOST_USER_PROTOCOL_F_MQ";
            case VHOST_USER_PROTOCOL_F_LOG_SHMFD:
                return "VHOST_USER_PROTOCOL_F_LOG_SHMFD";
            case VHOST_USER_PROTOCOL_F_RARP:
                return "VHOST_USER_PROTOCOL_F_RARP";
            case VHOST_USER_PROTOCOL_F_REPLY_ACK:
                return "VHOST_USER_PROTOCOL_F_REPLY_ACK";
            case VHOST_USER_PROTOCOL_F_MTU:
                return "VHOST_USER_PROTOCOL_F_MTU";
            case VHOST_USER_PROTOCOL_F_SLAVE_REQ:
                return "VHOST_USER_PROTOCOL_F_SLAVE_REQ";
            case VHOST_USER_PROTOCOL_F_CROSS_ENDIAN:
                return "VHOST_USER_PROTOCOL_F_CROSS_ENDIAN";
            case VHOST_USER_PROTOCOL_F_CRYPTO_SESSION:
                return "VHOST_USER_PROTOCOL_F_CRYPTO_SESSION";
            case VHOST_USER_PROTOCOL_F_PAGEFAULT:
                return "VHOST_USER_PROTOCOL_F_PAGEFAULT";
            case VHOST_USER_PROTOCOL_F_CONFIG:
                return "VHOST_USER_PROTOCOL_F_CONFIG";
            case VHOST_USER_PROTOCOL_F_SLAVE_SEND_FD:
                return "VHOST_USER_PROTOCOL_F_SLAVE_SEND_FD";
            case VHOST_USER_PROTOCOL_F_HOST_NOTIFIER:
                return "VHOST_USER_PROTOCOL_F_HOST_NOTIFIER";
            case VHOST_USER_PROTOCOL_F_INFLIGHT_SHMFD:
                return "VHOST_USER_PROTOCOL_F_INFLIGHT_SHMFD";
            case VHOST_USER_PROTOCOL_F_RESET_DEVICE:
                return "VHOST_USER_PROTOCOL_F_RESET_DEVICE";
            case VHOST_USER_PROTOCOL_F_INBAND_NOTIFICATIONS:
                return "VHOST_USER_PROTOCOL_F_INBAND_NOTIFICATIONS";
            case VHOST_USER_PROTOCOL_F_CONFIGURE_MEM_SLOTS:
                return "VHOST_USER_PROTOCOL_F_CONFIGURE_MEM_SLOTS";
            case VHOST_USER_PROTOCOL_F_STATUS:
                return "VHOST_USER_PROTOCOL_F_STATUS";
        }

        return Sprintf("UNKNOWN %d", feature);
    }

    bool Execute(int sock) override
    {
        struct Y_PACKED
        {
            TVhostUserMsgBase Base;
        } request;

        request.Base.Request = VHOST_USER_GET_PROTOCOL_FEATURES;
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
        return "VHOST_USER_GET_PROTOCOL_FEATURES";
    }

    uint64_t GetResult() const
    {
        return Features;
    }
};

}   // namespace NVHostUser
