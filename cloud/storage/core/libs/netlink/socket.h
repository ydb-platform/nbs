#pragma once

#include "message.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>

#include <netlink/genl/ctrl.h>
#include <netlink/genl/genl.h>
#include <netlink/netlink.h>

namespace NCloud::NNetlink {

using TResponseHandler = std::function<int(nl_msg*)>;

class TSocket
{
private:
    nl_sock* Socket;
    int Family;

public:
    explicit TSocket(TString family)
    {
        Socket = nl_socket_alloc();

        if (Socket == nullptr) {
            throw TServiceError(E_FAIL) << "unable to allocate netlink socket";
        }

        if (int err = genl_connect(Socket)) {
            nl_socket_free(Socket);
            throw TServiceError(E_FAIL)
                << "unable to connect to generic netlink socket: "
                << nl_geterror(err);
        }

        Family = genl_ctrl_resolve(Socket, family.c_str());

        if (Family < 0) {
            nl_socket_free(Socket);
            throw TServiceError(E_FAIL)
                << "unable to resolve netlink family: " << nl_geterror(Family);
        }
    }

    ~TSocket()
    {
        nl_socket_free(Socket);
    }

    [[nodiscard]] int GetFamily() const
    {
        return Family;
    }

    void SetCallback(nl_cb_type type, TResponseHandler func)
    {
        auto arg = std::make_unique<TResponseHandler>(std::move(func));

        if (int err = nl_socket_modify_cb(
                Socket,
                type,
                NL_CB_CUSTOM,
                TSocket::ResponseHandler,
                arg.get()))
        {
            throw TServiceError(E_FAIL)
                << "unable to set socket callback: " << nl_geterror(err);
        }
        arg.release();
    }

    static int ResponseHandler(nl_msg* msg, void* arg)
    {
        auto func = std::unique_ptr<TResponseHandler>(
            static_cast<TResponseHandler*>(arg));

        return (*func)(msg);
    }

    void Send(nl_msg* message)
    {
        if (int err = nl_send_auto(Socket, message); err < 0) {
            throw TServiceError(E_FAIL) << "send error: " << nl_geterror(err);
        }
        if (int err = nl_wait_for_ack(Socket)) {
            // this is either recv error, or an actual error message received
            // from the kernel
            throw TServiceError(E_FAIL) << "recv error: " << nl_geterror(err);
        }
    }
};

}   // namespace NCloud::NNetlink
