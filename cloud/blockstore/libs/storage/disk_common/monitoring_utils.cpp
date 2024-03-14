#include "monitoring_utils.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

IOutputStream& DumpAgentState(
    IOutputStream& out,
    NProto::EAgentState state)
{
    switch (state) {
        case NProto::AGENT_STATE_ONLINE:
            return out << "<font color=green>online</font>";
        case NProto::AGENT_STATE_WARNING:
            return out << "<font color=brown>warning</font>";
        case NProto::AGENT_STATE_UNAVAILABLE:
            return out << "<font color=red>unavailable</font>";
        default:
            return out
                << "(Unknown EAgentState value "
                << static_cast<int>(state)
                << ")";
    }
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
    return out;
}

}   // namespace NCloud::NBlockStore::NStorage
