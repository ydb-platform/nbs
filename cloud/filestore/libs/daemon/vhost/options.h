#pragma once

#include <cloud/filestore/libs/daemon/common/options.h>

namespace NCloud::NFileStore::NDaemon {

////////////////////////////////////////////////////////////////////////////////

struct TOptionsVhost final
    : public TOptionsCommon
{
    TOptionsVhost();
};

}   // namespace NCloud::NFileStore::NServer
