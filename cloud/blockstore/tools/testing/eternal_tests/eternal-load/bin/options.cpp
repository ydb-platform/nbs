#include "options.h"

#include <library/cpp/getopt/small/last_getopt.h>

namespace NCloud::NBlockStore {

using namespace NLastGetopt;

const THashMap<TString, ECommand> NameToCommand = {
    {"file", ECommand::ReadConfigCmd},
    {"generated", ECommand::GenerateConfigCmd},
};

////////////////////////////////////////////////////////////////////////////////

void TOptions::Parse(int argc, char** argv)
{
    TOpts opts;
    opts.AddHelpOption();

    opts.AddLongOption(
        "config-type",
        R"(specify type of config:
            file - reads config from specified file
            generated - run test with generated config from specified parameters)")
        .RequiredArgument("STR")
        .Required()
        .StoreResult(&CommandName)
        .Handler0([this] {
            auto it = NameToCommand.find(CommandName);
            if (it == NameToCommand.end()) {
                Command = ECommand::UnknownCmd;
            } else {
                Command = it->second;
            }
        });

    opts.AddLongOption("file", "path to file or block device")
        .RequiredArgument("STR")
        .StoreResult(&FilePath);

    opts.AddLongOption("filesize", "size of file or block device in GB")
        .RequiredArgument("NUM")
        .StoreResult(&FileSize);

    opts.AddLongOption(
        "request-block-count",
        "specify request size in number of blocks")
        .RequiredArgument("NUM")
        .StoreResult(&RequestBlockCount)
        .DefaultValue(1);

    opts.AddLongOption(
        "blocksize",
        "specify block size in bytes")
        .RequiredArgument("NUM")
        .StoreResult(&BlockSize)
        .DefaultValue(4096);

    opts.AddLongOption("iodepth")
        .RequiredArgument("NUM")
        .DefaultValue(32)
        .StoreResult(&IoDepth);

    opts.AddLongOption("write-rate", "percentage of write requests")
        .RequiredArgument("NUM")
        .StoreResult(&WriteRate)
        .DefaultValue(0);

    opts.AddLongOption("write-parts", "number of parts to split one write")
        .RequiredArgument("NUM")
        .StoreResult(&WriteParts)
        .DefaultValue(1);

    opts.AddLongOption(
        "dump-config-path",
        "dump test configuration to specified file in json format")
        .RequiredArgument("STR")
        .StoreResult(&DumpPath)
        .DefaultValue("load-config.json");

    opts.AddLongOption(
        "restore-config-path",
        "path to test config")
        .RequiredArgument("STR")
        .StoreResult(&RestorePath);

    TOptsParseResultException(&opts, argc, argv);
}

}   // namespace NCloud::NBlockStore
