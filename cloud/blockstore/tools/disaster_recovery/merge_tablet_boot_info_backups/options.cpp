#include "options.h"

using namespace NLastGetopt;

TOptions::TOptions(int argc, char** argv)
{
    TOpts opts;
    opts.AddHelpOption();

    opts.AddLongOption("src-backups-file-path")
        .RequiredArgument("STR")
        .Required()
        .StoreResult(&SrcBackupsFilePath)
        .Help(
            "Path to the directory containing subdirectories with tablet boot "
            "info (hive) backups.");

    opts.AddLongOption("text-output")
        .RequiredArgument("STR")
        .StoreResult(&TextOutputPath);

    opts.AddLongOption("binary-output")
        .RequiredArgument("STR")
        .StoreResult(&BinaryOutputPath);

    opts.AddLongOption("tablet-boot-info-backup-file-name")
        .RequiredArgument("STR")
        .DefaultValue("nbs-tablet-boot-info-backup.txt")
        .StoreResult(&TabletBootInfoBackupFileName);

    TOptsParseResultException(&opts, argc, argv);
}
