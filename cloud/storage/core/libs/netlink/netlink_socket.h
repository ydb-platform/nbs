#pragma once

#include "netlink_message.h"

#include <util/generic/string.h>
#include <cloud/storage/core/libs/common/error.h>

#include <netlink/netlink.h>

namespace NCloud::NNetlink {

class INetlinkSocket {
public:
    using TNetlinkSocketCallback = std::function<int(nl_msg*)>;

    virtual ~INetlinkSocket() = default;

    [[nodiscard]] virtual int GetFamily() const = 0;

    virtual void SetCallback(nl_cb_type type, TNetlinkSocketCallback func) = 0;

    virtual void Send(nl_msg* message) = 0;
};


using INetlinkSocketPtr = std::unique_ptr<INetlinkSocket>;


INetlinkSocketPtr CreateNetlinkSocket(TString netlinkFamily);

} // namespace NCloud::NNetlink
