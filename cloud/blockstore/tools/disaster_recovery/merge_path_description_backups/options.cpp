#include "options.h"

using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

TOptions::TOptions(int argc, char** argv)
{
    TOpts opts;
    opts.AddHelpOption();

    opts.AddLongOption("src-root")
        .RequiredArgument("STR")
        .Required()
        .StoreResult(&SrcRoot);

    opts.AddLongOption("output")
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
