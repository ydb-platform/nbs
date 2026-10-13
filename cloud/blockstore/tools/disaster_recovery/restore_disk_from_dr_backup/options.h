#pragma once

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/string.h>

////////////////////////////////////////////////////////////////////////////////


struct TOptions
{
    TString OldBackupPath;
    TString CurrentBackupPath;
    TVector<TString> DiskIds;
    TString OutputDir;
    TOptions(int argc, char** argv);
};
