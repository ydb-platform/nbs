#include "options.h"

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/serialized_enum.h>

namespace NCloud::NBlockStore {

using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

void TOptions::Parse(int argc, char** argv)
{
    TOpts opts;
    opts.AddHelpOption();

    opts.AddLongOption("test")
        .RequiredArgument("{" + GetEnumAllNames<ETestMode>() + "}")
        .DefaultValue(ToString(ETestMode::Target))
        .Handler1T<TString>([this](const auto& s)
                            { TestMode = FromString<ETestMode>(s); });

    const auto& verbose =
        opts.AddLongOption("verbose").OptionalArgument("STR").StoreResult(
            &VerboseLevel);

    opts.AddLongOption("socket-path")
        .Required()
        .RequiredArgument("STR")
        .StoreResult(&SocketPath);

    opts.AddLongOption("threads-count")
        .RequiredArgument("NUM")
        .DefaultValue(ToString(ThreadsCount))
        .StoreResult(&ThreadsCount);

    opts.AddLongOption("structured-reply")
        .NoArgument()
        .DefaultValue(ToString(StructuredReply))
        .SetFlag(&StructuredReply);

    opts.AddLongOption("block-size")
        .RequiredArgument("NUM")
        .DefaultValue(ToString(BlockSize))
        .StoreResult(&BlockSize);

    opts.AddLongOption("blocks-count")
        .RequiredArgument("NUM")
        .DefaultValue(ToString(BlocksCount))
        .StoreResult(&BlocksCount);

    opts.AddLongOption("iodepth")
        .RequiredArgument("NUM")
        .DefaultValue(ToString(MaxIoDepth))
        .StoreResult(&MaxIoDepth);

    opts.AddLongOption("min-blocks-count")
        .RequiredArgument("NUM")
        .DefaultValue(ToString(MinBlocksCount))
        .StoreResult(&MinBlocksCount);

    opts.AddLongOption("max-blocks-count")
        .RequiredArgument("NUM")
        .DefaultValue(ToString(MaxBlocksCount))
        .StoreResult(&MaxBlocksCount);

    opts.AddLongOption("write-rate")
        .RequiredArgument("NUM")
        .DefaultValue(ToString(WriteRate))
        .StoreResult(&WriteRate);

    ui32 seconds = 0;
    opts.AddLongOption("test-duration")
        .RequiredArgument("NUM")
        .StoreResult(&seconds);

    TOptsParseResultException res(&opts, argc, argv);

    if (res.Has(&verbose) && !VerboseLevel) {
        VerboseLevel = "debug";
    }

    TestDuration = TDuration::Seconds(seconds);

    Y_ENSURE(BlockSize && (BlockSize & (BlockSize - 1)) == 0);
    Y_ENSURE(BlocksCount && BlocksCount > MaxBlocksCount);
    Y_ENSURE(MinBlocksCount && MinBlocksCount <= MaxBlocksCount);
    Y_ENSURE(WriteRate <= 100);
}

}   // namespace NCloud::NBlockStore
