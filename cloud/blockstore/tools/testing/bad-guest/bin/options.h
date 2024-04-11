#pragma once

#include <util/generic/string.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    TString FilePath;
    ui64 TotalBlockCount;
    ui64 ChunkBlockCount;
    ui32 BlockSize;

    void Parse(int argc, char** argv);
};

}   // namespace NCloud::NBlockStore
