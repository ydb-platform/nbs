#include "options.h"

namespace NCloud::NFileStore::NDaemon {

////////////////////////////////////////////////////////////////////////////////

TOptionsVhost::TOptionsVhost()
{
    // TODO(fyodor) get rid of this option in favour --app-config in base class
    Opts.AddLongOption("vhost-file")
        .RequiredArgument("FILE")
        .StoreResult(&AppConfig);

    ServerPort = 9022;
    Service = NDaemon::EServiceKind::Local;
}

}   // namespace NCloud::NFileStore::NDaemon
