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
    explicit TSocket(TString family);
    ~TSocket();

    [[nodiscard]] int GetFamily() const
    {
        return Family;
    }

    void SetCallback(nl_cb_type type, TResponseHandler func);

    static int ResponseHandler(nl_msg* msg, void* arg);

    void Send(nl_msg* message);
};

}   // namespace NCloud::NNetlink
