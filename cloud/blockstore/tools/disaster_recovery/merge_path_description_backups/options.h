#pragma once

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/string.h>

////////////////////////////////////////////////////////////////////////////////


struct TOptions
{
    TString SrcRoot;
    TString TextOutputPath;
    TString BinaryOutputPath;
    TString SchemeShardBackup;

    TOptions(int argc, char** argv);
};
