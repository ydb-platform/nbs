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
    bool isFresh,
    bool isDisabled,
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
    if (isFresh) {
        out << " [<font color=blue>Fresh</font>]";
    }
    if (isDisabled) {
        out << " [<font color=red>Disabled</font>]";
    }
    return out;
}

}   // namespace NCloud::NBlockStore::NStorage
