#pragma once

#include "private.h"

#include <util/datetime/base.h>
#include <util/generic/size_literals.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

enum class ETestMode
{
    Target,
    Initiator
};

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    ETestMode TestMode = ETestMode::Target;
    TString VerboseLevel;

    // connection options
    TString SocketPath;
    ui32 ThreadsCount = 1;
    bool StructuredReply = false;

    // device geometry
    ui32 BlockSize = 4_KB;
    ui32 BlocksCount = 1024 * 1024;

    // test options
    ui32 MaxIoDepth = 10;
    ui32 MinBlocksCount = 1;
    ui32 MaxBlocksCount = 1;
    ui32 WriteRate = 50;

    TDuration TestDuration;

    void Parse(int argc, char** argv);
};

}   // namespace NCloud::NBlockStore
