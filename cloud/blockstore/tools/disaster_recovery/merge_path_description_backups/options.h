#pragma once

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/string.h>

////////////////////////////////////////////////////////////////////////////////


struct TOptions
{
    TString SrcBackupsFilePath;
    TString TextOutputPath;
    TString BinaryOutputPath;
    TString PathDescriptionBackupFileName;

    TOptions(int argc, char** argv);
};
