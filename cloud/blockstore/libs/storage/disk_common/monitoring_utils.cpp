#include "monitoring_utils.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

IOutputStream& DumpState(
    IOutputStream& out,
    NProto::EAgentState state)
{
    switch (state) {
        case NProto::AGENT_STATE_ONLINE:
            return out << "<font color=green>online</font>";
        case NProto::AGENT_STATE_WARNING:
            return out << "warning";
        case NProto::AGENT_STATE_UNAVAILABLE:
            return out << "<font color=red>unavailable</font>";
        default:
            return out
                << "(Unknown EAgentState value "
                << static_cast<int>(state)
                << ")";
    }
}

IOutputStream& DumpState(
    IOutputStream& out,
    NProto::EDiskState state)
{
    switch (state) {
        case NProto::DISK_STATE_ONLINE:
            return out << "<font color=green>online</font>";
        case NProto::DISK_STATE_MIGRATION:
            return out << "<font color=blue>migration</font>";
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

IOutputStream& DumpState(
    IOutputStream& out,
    NProto::EDeviceState state)
{
    switch (state) {
        case NProto::DEVICE_STATE_ONLINE:
            return out << "<font color=green>online</font>";
        case NProto::DEVICE_STATE_WARNING:
            return out << "warning";
        case NProto::DEVICE_STATE_ERROR:
            return out << "<font color=red>error</font>";
        default:
            return out
                << "(Unknown EDeviceState value "
                << static_cast<int>(state)
                << ")";
    }
}

}   // namespace NCloud::NBlockStore::NStorage
