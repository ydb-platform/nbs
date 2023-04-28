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

struct TOptions
{
    ECommand Command;
    TString CommandName;

    TMaybe<TString> FilePath;
    TMaybe<ui64> FileSize;

    TMaybe<TString> RestorePath;

    ui64 BlockSize;
    ui16 WriteRate;
    ui64 RequestBlockCount;
    ui16 IoDepth;
    ui64 WriteParts;

    TString DumpPath;

    void Parse(int argc, char** argv);
};

}   // namespace NCloud::NBlockStore
