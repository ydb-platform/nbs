#pragma once

#include "message.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/network/socket.h>
#include <util/system/error.h>

#include <netlink/genl/ctrl.h>
#include <netlink/genl/genl.h>
#include <netlink/netlink.h>

namespace NCloud::NNetlink {

namespace NLibnl {

using TResponseHandler = std::function<int(nl_msg*)>;

class TSocket
{
private:
    std::unique_ptr<nl_sock, void(*)(nl_sock*)> Socket;
    int Family;

public:
    explicit TSocket(TString family);
    ~TSocket() = default;

    [[nodiscard]] int GetFamily() const
    {
        return Family;
    }

    void SetCallback(nl_cb_type type, TResponseHandler func);

    static int ResponseHandler(nl_msg* msg, void* arg);

    void Send(nl_msg* message);
};

}  // namespace NLibnl

////////////////////////////////////////////////////////////////////////////////

class TNetlinkSocket
{
private:
    TSocket Socket;
    ui32 SocketTimeoutMs;

public:
    TNetlinkSocket(ui32 socketTimeoutMs = 100)
        : Socket(::socket(PF_NETLINK, SOCK_RAW, NETLINK_GENERIC))
        , SocketTimeoutMs(socketTimeoutMs)
    {
        if (Socket < 0) {
            ythrow TServiceError(MAKE_SYSTEM_ERROR(LastSystemError()))
                << "Failed to create netlink socket";
        }
        Socket.SetSocketTimeout(0, SocketTimeoutMs);
    }

    template <typename TNetlinkMessage>
    void Send(const TNetlinkMessage& msg)
    {
        auto ret = Socket.Send(&msg, sizeof(msg));
        if (ret == -1) {
            ythrow TServiceError(MAKE_SYSTEM_ERROR(LastSystemError()))
                << "Failed to send netlink message";
        }
    }

    template <typename T>
    void Receive(TNetlinkResponse<T>& response)
    {
        auto ret = Socket.Recv(&response, sizeof(response));
        if (ret < 0) {
            ythrow TServiceError(MAKE_SYSTEM_ERROR(LastSystemError()))
                << "Failed to receive netlink message";
        }

        if (response.NetlinkError.MessageHeader.nlmsg_type == NLMSG_ERROR) {
            if (response.NetlinkError.MessageError.error != 0) {
                throw TServiceError(
                    MAKE_SYSTEM_ERROR(response.NetlinkError.MessageError.error))
                    << "Netlink error";
            }
        }

        if (!NLMSG_OK(&response.NetlinkError.MessageHeader, ret)) {
            throw TServiceError(MAKE_ERROR(E_FAIL))
                << "Netlink message has incorrect format";
        }

        response.Msg.Validate();
        return;
    }
};

template <size_t FamilyNameLength>
ui16 GetFamilyId(const char (&familyName)[FamilyNameLength])
{
    NNetlink::TNetlinkSocket socket;
    socket.Send(NNetlink::TNetlinkFamilyIdRequest(familyName));
    NNetlink::TNetlinkResponse<
        NNetlink::TNetlinkFamilyIdResponse<FamilyNameLength>>
        response;
    socket.Receive(response);
    return response.Msg.FamilyId;
}

}   // namespace NCloud::NNetlink
