#pragma once

#include "private.h"

#include <util/generic/string.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    TString PluginLibPath;
    TString PluginOptions;
    TString TestConfig;
    ui32 HostMajor = 0;
    ui32 HostMinor = 0;
    TString EndpointFolder;
    ui32 RunCount = 1;

    void Parse(int argc, char** argv);
};

}   // namespace NCloud::NBlockStore
