#pragma once

#include <util/generic/string.h>

namespace NCloud::NFileStore::NWriteBackCacheStateTool {

////////////////////////////////////////////////////////////////////////////////

enum class ECommand
{
    List,
    Check,
    Dump,
    Patch,
    UnknownCmd
};

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    ECommand Command = ECommand::UnknownCmd;

    TString StateDir = "";
    TString FsId = "";
    TString SessionId = "";
    TString StateFile = "";
    TString InputFile = "";
    TString OutputFile = "";

    bool UnsafeIgnoreLock = false;
    bool UnsafeIgnoreCorruption = false;

    void Parse(int argc, char** argv);
};

}   // namespace NCloud::NFileStore::NWriteBackCacheStateTool
