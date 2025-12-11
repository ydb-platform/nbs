#pragma once

#include <cloud/filestore/libs/daemon/common/options.h>

namespace NCloud::NFileStore::NDaemon {

////////////////////////////////////////////////////////////////////////////////

struct TOptionsVhost final: public TOptionsCommon
{
    ui32 LocalServicePort = 0;

    TOptionsVhost();
};

}   // namespace NCloud::NFileStore::NDaemon
