#include "monitoring_utils.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

IOutputStream& DumpAgentState(
    IOutputStream& out,
    NProto::EAgentState state,
    bool connected)
{
    switch (state) {
        case NProto::AGENT_STATE_ONLINE:
            out << "<font color=green>online</font>";
            break;
        case NProto::AGENT_STATE_WARNING:
            out << "<font color=brown>warning</font>";
            break;
        case NProto::AGENT_STATE_UNAVAILABLE:
            out << "<font color=red>unavailable</font>";
            break;
        default:
            out << "(Unknown EAgentState value " << static_cast<int>(state)
                << ")";
            break;
    }

    if (state != NProto::AGENT_STATE_UNAVAILABLE) {
        if (!connected) {
            out << " <font color=gray>disconnected</font>";
        }
    } else if (connected) {
        out << " <font color=gray>connected</font>";
    }

    return out;
}

IOutputStream& DumpDiskState(
    IOutputStream& out,
    NProto::EDiskState state)
{
    switch (state) {
        case NProto::DISK_STATE_ONLINE:
            return out << "<font color=green>online</font>";
        case NProto::DISK_STATE_WARNING:
            return out << "<font color=blue>warning/migration</font>";
        case NProto::DISK_STATE_TEMPORARILY_UNAVAILABLE:
            return out << "temporarily unavailable";
        case NProto::DISK_STATE_ERROR:
            return out << "<font color=red>error</font>";
        default:
            return out
                << "(Unknown EDiskState value "
                << static_cast<int>(state)
                << ")";
    }
}

IOutputStream& DumpDeviceState(
    IOutputStream& out,
    NProto::EDeviceState state,
    EDeviceStateFlags flags,
    TString suffix)
{
    switch (state) {
        case NProto::DEVICE_STATE_ONLINE:
            out << "<font color=green>online" << suffix << "</font>";
            break;
        case NProto::DEVICE_STATE_WARNING:
            out << "<font color=brown>warning" << suffix << "</font>";
            break;
        case NProto::DEVICE_STATE_ERROR:
            out << "<font color=red>error" << suffix << "</font>";
            break;
        default:
            out
                << "(Unknown EDeviceState value "
                << static_cast<int>(state)
                << ")" << suffix;
    }
    if (flags & EDeviceStateFlags::FRESH) {
        out << " [<font color=blue>fresh</font>]";
    }
    if (flags & EDeviceStateFlags::DISABLED) {
        out << " [<font color=red>disabled</font>]";
    }
    if (flags & EDeviceStateFlags::DIRTY) {
        out << " [<font color=DimGrey>dirty</font>]";
    }
    if (flags & EDeviceStateFlags::SUSPENDED) {
        out << " [<font color=DarkCyan>suspended</font>]";
    }
    if (flags & EDeviceStateFlags::LAGGING) {
        out << " [<font color=Purple>lagging</font>]";
    }
    return out;
}

}   // namespace NCloud::NBlockStore::NStorage
