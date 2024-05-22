#pragma once

#include "private.h"

#include <cloud/storage/core/libs/daemon/options.h>
#include <cloud/storage/core/libs/kikimr/options.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TOptions
    : virtual TOptionsBase
    , NCloud::NStorage::TOptionsYdbBase
{
    TString StorageConfig;
    TString DiskAgentConfig;
    TString DiskRegistryProxyConfig;
    TString FeaturesConfig;
    TString RdmaConfig;
    TString SysLogService;
    TString NodeType;
    bool TemporaryAgent = false;

    TOptions();

    void Parse(int argc, char** argv) override;
};

}   // namespace NCloud::NBlockStore::NServer
