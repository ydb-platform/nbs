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

    opts.AddLongOption("ss-file-name")
        .RequiredArgument("STR")
        .DefaultValue("nbs-path-description-backup.txt")
        .StoreResult(&SchemeShardBackup);

    opts.AddLongOption("hive-file-name")
        .RequiredArgument("STR")
        .DefaultValue("nbs-tablet-boot-info-backup.txt")
        .StoreResult(&HiveBackup);

    TOptsParseResultException(&opts, argc, argv);
}
