#include "options.h"

#include <library/cpp/getopt/small/last_getopt.h>

namespace NCloud::NBlockStore {

using namespace NLastGetopt;

namespace {

const THashMap<TString, ECommand> nameToCommand = {
    {"file", ECommand::ReadConfigCmd},
    {"generated", ECommand::GenerateConfigCmd},
};

const THashMap<TString, EScenario> nameToScenario = {
    {"aligned", EScenario::Aligned},
    {"unaligned", EScenario::Unaligned}
};

const THashMap<TString, EIoEngine> nameToEngine = {
    {"asyncio", EIoEngine::AsyncIo},
    {"uring", EIoEngine::IoUring},
    {"sync", EIoEngine::Sync}
};

template <class T>
struct TMapOption
{
    T* Target;
    const THashMap<TString, T>& Map;
    const T DefaultValue;

    TMapOption(T* target, const THashMap<TString, T>& map, T defaultValue)
        : Target(target)
        , Map(map)
        , DefaultValue(defaultValue)
    {}
};

template <class T>
TOpt& operator|(TOpt& opt, const TMapOption<T>& map)
{
    THashSet<TString> choices;
    TString defaultValue;
    for (const auto& [key, value]: map.Map) {
        choices.insert(key);
        if (value == map.DefaultValue) {
            defaultValue = key;
        }
    }

    opt.RequiredArgument("STR");
    opt.Choices(choices);

    if (defaultValue) {
        opt.DefaultValue(defaultValue);
    }

    return opt.StoreMappedResultT<TString>(
        map.Target,
        [map = map.Map](const TString& v) { return map.at(v); });
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TOptions::Parse(int argc, char** argv)
{
    TOpts opts;
    opts.AddHelpOption();

    opts.AddLongOption(
        "config-type",
        "specify type of config:\n"
        "- file: reads config from specified file\n"
        "- generated: run test with generated config from specified parameters")
        .Required()
        | TMapOption(&Command, nameToCommand, ECommand::UnknownCmd);

    opts.AddLongOption(
        "scenario",
        "specify the testing scenario:\n"
        "- aligned: aligned reads and writes\n"
        "    suitable for testing nbs and nfs\n"
        "    preferred options: engine=async_io\n"
        "- unaligned: arbitrary reads and writes\n"
        "    suitable for testing nfs\n"
        "    preferred options: engine=sync, no_direct\n")
        | TMapOption(&Scenario, nameToScenario, EScenario::Aligned);

    opts.AddLongOption(
        "engine",
        "specify the IO engine:\n"
        "- asyncio: AsyncIO\n"
        "- uring: io_uring\n"
        "- sync: synchronous IO + threads")
        | TMapOption(&Engine, nameToEngine, EIoEngine::AsyncIo);

    opts.AddLongOption(
        "no-direct",
        "do not set O_DIRECT flag")
        .StoreTrue(&NoDirect);

    opts.AddLongOption(
        "run-in-callbacks",
        "run test workers and post IO requests in completion "
        "callbacks instead of the single submitter thread - "
        "this may improve performance for engines that use "
        "multiple threads (like sync)")
        .StoreTrue(&RunInCallbacks);

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

    opts.AddLongOption("alternating-phase",
            "duration of a phase for tests in which write load is replaced by read load periodically")
        .OptionalArgument("STR")
        .StoreResult(&AlternatingPhase)
        .DefaultValue("");

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

    opts.AddLongOption(
        "min-read-size",
        "minimum size of read requests in bytes")
        .RequiredArgument("NUM")
        .StoreResult(&MinReadSize);

    opts.AddLongOption(
        "max-read-size",
        "maximum size of read requests in bytes")
        .RequiredArgument("NUM")
        .StoreResult(&MaxReadSize);

    opts.AddLongOption(
        "read-size",
        "minimum and maximum size of read requests in bytes")
        .RequiredArgument("NUM")
        .StoreResult(&MinReadSize)
        .StoreResult(&MaxReadSize);

    opts.AddLongOption(
        "min-write-size",
        "minimum size of write requests in bytes")
        .RequiredArgument("NUM")
        .StoreResult(&MinWriteSize);

    opts.AddLongOption(
        "max-write-size",
        "maximum size of write requests in bytes")
        .RequiredArgument("NUM")
        .StoreResult(&MaxWriteSize);

    opts.AddLongOption(
        "write-size",
        "minimum and maximum size of write requests in bytes")
        .RequiredArgument("NUM")
        .StoreResult(&MinWriteSize)
        .StoreResult(&MaxWriteSize);

    opts.AddLongOption(
        "min-region-size",
        "minimum size of file region in bytes")
        .RequiredArgument("NUM")
        .StoreResult(&MinRegionSize);

    opts.AddLongOption(
        "max-region-size",
        "maximum size of file region in bytes")
        .RequiredArgument("NUM")
        .StoreResult(&MaxRegionSize);

    opts.AddLongOption(
        "region-size",
        "minimum and maximum size of file region in bytes")
        .RequiredArgument("NUM")
        .StoreResult(&MinRegionSize)
        .StoreResult(&MaxRegionSize);

    TOptsParseResultException(&opts, argc, argv);
}

}   // namespace NCloud::NBlockStore
