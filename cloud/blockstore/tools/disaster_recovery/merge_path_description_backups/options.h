#pragma once

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/string.h>

////////////////////////////////////////////////////////////////////////////////


struct TOptions
{
    TString SrcRoot;
    TString TextOutputPath;
    TString BinaryOutputPath;
    TString PathDescriptionBackupFileName;

    TOptions(int argc, char** argv);
};
