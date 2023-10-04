#include "options.h"

namespace NCloud::NFileStore::NDaemon {

////////////////////////////////////////////////////////////////////////////////

TOptionsServer::TOptionsServer()
{
    ServerPort = 9021;
    Service = NDaemon::EServiceKind::Kikimr;
}

}   // namespace NCloud::NFileStore::NDaemon
