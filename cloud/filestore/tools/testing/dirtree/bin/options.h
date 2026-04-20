#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    TString TestDir;

    ui32 ProducerThreads{};
    ui32 ChildrenCount{};
    ui32 SymlinkChildrenCount{};
    double SubdirProbability{};
    double DecayFactor{};
    ui64 Seed{};

    void Parse(int argc, char** argv);
};

}   // namespace NCloud::NFileStore
