#include "helpers.h"

#include <util/generic/yexception.h>

namespace NCloud::NFileStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TString NodeTypeToString(NProto::ENodeType nodeType)
{
    switch (nodeType) {
        case NProto::E_INVALID_NODE:
            return "invalid";
        case NProto::E_REGULAR_NODE:
            return "f";
        case NProto::E_DIRECTORY_NODE:
            return "d";
        case NProto::E_LINK_NODE:
            return "l";
        case NProto::E_SOCK_NODE:
            return "s";
        case NProto::E_SYMLINK_NODE:
            return "L";
        case NProto::E_FIFO_NODE:
            return "p";
        case NProto::E_CHARDEV_NODE:
            return "c";
        case NProto::E_BLOCKDEV_NODE:
            return "b";
        default:
            ythrow yexception() << "must be unreachable";
    }
}

}   // namespace NCloud::NFileStore::NClient

////////////////////////////////////////////////////////////////////////////////
