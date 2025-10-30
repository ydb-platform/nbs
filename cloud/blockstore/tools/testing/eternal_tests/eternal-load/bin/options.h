#pragma once

#include "private.h"

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

enum class ECommand
{
    ReadConfigCmd,
    GenerateConfigCmd,
    UnknownCmd
};

////////////////////////////////////////////////////////////////////////////////

enum class EIoEngine
{
    AsyncIo,
    IoUring,
    Sync
};

////////////////////////////////////////////////////////////////////////////////

enum class EScenario
{
    Aligned,
    Unaligned
};

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    ECommand Command = ECommand::UnknownCmd;

    EScenario Scenario = EScenario::Aligned;

    EIoEngine Engine = EIoEngine::AsyncIo;
    bool RunInCallbacks = false;
    bool NoDirect = false;

    TMaybe<TString> FilePath;
    TMaybe<ui64> FileSize;

    TMaybe<TString> RestorePath;

    ui64 BlockSize;
    ui16 WriteRate;
    ui64 RequestBlockCount;
    ui16 IoDepth;
    ui64 WriteParts;
    TString AlternatingPhase;

    TString DumpPath;

    ui64 MinReadSize = 0;
    ui64 MaxReadSize = 0;
    ui64 MinWriteSize = 0;
    ui64 MaxWriteSize = 0;
    ui64 MinRegionSize = 0;
    ui64 MaxRegionSize = 0;

    bool PrintDebugStats = false;

    void Parse(int argc, char** argv);
};

}   // namespace NCloud::NBlockStore
