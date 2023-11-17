#pragma once

#include "private.h"

#include <util/generic/string.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    TString FilePath;
    ui64 FileSize;
    ui64 Offset;

    ui64 Step;
    ui32 BlockSize;
    ui16 IoDepth;

    bool CheckZero;
    bool ReadOnly;

    TString VerboseLevel;

    void Parse(int argc, char** argv);
};

}   // namespace NCloud::NBlockStore
