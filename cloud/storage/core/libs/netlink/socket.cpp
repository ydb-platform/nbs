#include "socket.h"

namespace NCloud::NNetlink {

TSocket::TSocket(TString family)
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

TSocket::~TSocket()
{
    nl_socket_free(Socket);
}

void TSocket::SetCallback(nl_cb_type type, TResponseHandler func)
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

void TSocket::Send(nl_msg* message)
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

int TSocket::ResponseHandler(nl_msg* msg, void* arg)
{
    auto func = std::unique_ptr<TResponseHandler>(
        static_cast<TResponseHandler*>(arg));

    return (*func)(msg);
}

}   // namespace NCloud::NNetlink
