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
            "Path to the directory containing subdirectories with path "
            "description (schemeshard) backups.");

    opts.AddLongOption("text-output")
        .RequiredArgument("STR")
        .StoreResult(&TextOutputPath);

    opts.AddLongOption("binary-output")
        .RequiredArgument("STR")
        .StoreResult(&BinaryOutputPath);

    opts.AddLongOption("path-description-backup-file-name")
        .RequiredArgument("STR")
        .DefaultValue("nbs-path-description-backup.txt")
        .StoreResult(&PathDescriptionBackupFileName);

    TOptsParseResultException(&opts, argc, argv);
}
