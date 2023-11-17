#pragma once

#include <util/generic/string.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    TString FilePath;
    ui64 BlockCount;
    ui16 WriteRate;
    ui16 IoDepth;
    ui64 RequestCount;

    void Parse(int argc, char** argv);
};

}   // namespace NCloud::NBlockStore
