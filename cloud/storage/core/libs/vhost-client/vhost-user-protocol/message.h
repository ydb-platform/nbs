#pragma once
// https://qemu-project.gitlab.io/qemu/interop/vhost-user.html

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NVHostUser {

////////////////////////////////////////////////////////////////////////////////

template <typename Type>
bool BitIsSet(Type data, size_t bit)
{
    return data & (static_cast<Type>(1) << bit);
}

template <typename Type>
Type& SetBit(Type& data, size_t bit)
{
    return data |= (static_cast<Type>(1) << bit);
}

template <typename Type>
Type& ResetBit(Type& data, size_t bit)
{
    return data &= ~(static_cast<Type>(1) << bit);
}

////////////////////////////////////////////////////////////////////////////////

class IMessage
{
public:
    virtual ~IMessage() = default;

    virtual bool Execute(int sock) = 0;
    virtual TString ToString() const = 0;

protected:
    enum EVhostUserRequest
    {
        // Don't rename these constants. Names are taken from specification
        VHOST_USER_NONE = 0,
        VHOST_USER_GET_FEATURES = 1,
        VHOST_USER_SET_FEATURES = 2,
        VHOST_USER_SET_OWNER = 3,
        VHOST_USER_RESET_OWNER = 4,
        VHOST_USER_SET_MEM_TABLE = 5,
        VHOST_USER_SET_LOG_BASE = 6,
        VHOST_USER_SET_LOG_FD = 7,
        VHOST_USER_SET_VRING_NUM = 8,
        VHOST_USER_SET_VRING_ADDR = 9,
        VHOST_USER_SET_VRING_BASE = 10,
        VHOST_USER_GET_VRING_BASE = 11,
        VHOST_USER_SET_VRING_KICK = 12,
        VHOST_USER_SET_VRING_CALL = 13,
        VHOST_USER_SET_VRING_ERR = 14,
        VHOST_USER_GET_PROTOCOL_FEATURES = 15,
        VHOST_USER_SET_PROTOCOL_FEATURES = 16,
        VHOST_USER_GET_QUEUE_NUM = 17,
        VHOST_USER_SET_VRING_ENABLE = 18,
        VHOST_USER_SEND_RARP = 19,
        VHOST_USER_NET_SET_MTU = 20,
        VHOST_USER_SET_SLAVE_REQ_FD = 21,
        VHOST_USER_IOTLB_MSG = 22,
        VHOST_USER_CRYPTO_CREATE_SESS = 26,
        VHOST_USER_CRYPTO_CLOSE_SESS = 27,
        VHOST_USER_POSTCOPY_ADVISE = 28,
        VHOST_USER_POSTCOPY_LISTEN = 29,
        VHOST_USER_POSTCOPY_END = 30,
        VHOST_USER_GET_INFLIGHT_FD = 31,
        VHOST_USER_SET_INFLIGHT_FD = 32,
        VHOST_USER_SET_STATUS = 39,
        VHOST_USER_GET_STATUS = 40,
        VHOST_USER_MAX = 41
    };

    struct TVhostUserMsgBase
    {
        uint32_t Request;
        uint32_t Flags;
        uint32_t AdditionDataSize;
    };

    static int Send(int sock, const TVhostUserMsgBase& data);
    static int
    SendFds(int sock, const TVhostUserMsgBase& data, const TVector<int>& fds);

    static int Recv(int sock, TVhostUserMsgBase& data);
    static int RecvFds(int sock, TVhostUserMsgBase& data, TVector<int>& fds);
};

}   // namespace NVHostUser
