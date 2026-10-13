#include "options.h"

using namespace NLastGetopt;

TOptions::TOptions(int argc, char** argv)
{
    TOpts opts;
    opts.AddHelpOption();

    opts.AddLongOption("old-backup-path")
        .RequiredArgument("STR")
        .Required()
        .StoreResult(&OldBackupPath)
        .Help(
            "Path to the old backup directory");

    opts.AddLongOption("current-backup-path")
        .RequiredArgument("STR")
        .Required()
        .StoreResult(&CurrentBackupPath)
        .Help(
            "Path to the current backup directory");

    opts.AddLongOption("disk-ids")
        .RequiredArgument("STR")
        .Required()
        .SplitHandler(&DiskIds, ',')
        .Help(
            "Comma-separated list of disk IDs to restore");

    opts.AddLongOption("output-dir")    
        .RequiredArgument("STR")
        .StoreResult(&OutputDir)
        .Help(
            "Path to the output directory");

    TOptsParseResultException(&opts, argc, argv);
}
