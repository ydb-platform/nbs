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

enum class EScenario
{
    Block,
    File
};

////////////////////////////////////////////////////////////////////////////////

enum class EIoEngine
{
    AsyncIo,
    IoUring,
    Sync
};

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    ECommand Command = ECommand::UnknownCmd;
    EScenario Scenario = EScenario::Block;

    EIoEngine Engine = EIoEngine::AsyncIo;
    bool RunFromAnyThread = false;
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

    bool PrintDebugInfo = false;

    void Parse(int argc, char** argv);
};

}   // namespace NCloud::NBlockStore
