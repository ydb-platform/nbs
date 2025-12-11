#pragma once

#include "public.h"

#include <cloud/blockstore/libs/daemon/common/options.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TOptionsLocal final: public TOptionsCommon
{
    void Parse(int argc, char** argv) override;
};

}   // namespace NCloud::NBlockStore::NServer
