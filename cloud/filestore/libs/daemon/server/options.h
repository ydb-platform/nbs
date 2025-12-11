#pragma once

#include <cloud/filestore/libs/daemon/common/options.h>

namespace NCloud::NFileStore::NDaemon {

////////////////////////////////////////////////////////////////////////////////

struct TOptionsServer final: public TOptionsCommon
{
    TOptionsServer();
};

}   // namespace NCloud::NFileStore::NDaemon
