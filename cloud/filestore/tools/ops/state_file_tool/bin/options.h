#pragma once

#include <cloud/filestore/tools/ops/state_file_tool/protos/state_file_tool.pb.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore::NStateFileTool {

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

    TString StateDir;
    TString FsId;
    TString SessionId;
    TString StateFile;
    NProto::EStateFileType FileType = NProto::EStateFileType::Unknown;
    TString InputFile;
    TString OutputFile;

    bool UnsafeIgnoreLock = false;
    bool UnsafeIgnoreCorruption = false;

    void Parse(int argc, char** argv);
};

}   // namespace NCloud::NFileStore::NStateFileTool
