#pragma once

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/string.h>

////////////////////////////////////////////////////////////////////////////////

enum EFormat
{
    Text,
    Map
};

struct TOptions
{
    TString Path;

    ui64 BlockSize = 4096;
    ui64 StartIndex = 0;
    ui64 BlockCount = 1;
    ui64 TestId = 0;
    EFormat Format = EFormat::Text;

    TOptions(int argc, char** argv);
};
