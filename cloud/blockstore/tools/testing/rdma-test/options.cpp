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
        .DefaultValue(ToString(TestMode))
        .Handler1T<TString>([this] (const auto& s) {
            TestMode = FromString<ETestMode>(s);
        });

    const auto& verbose = opts.AddLongOption("verbose")
        .OptionalArgument("STR")
        .StoreResult(&VerboseLevel);

    // connection options
    opts.AddLongOption("host")
        .RequiredArgument()
        .DefaultValue(Host)
        .StoreResult(&Host);

    opts.AddLongOption("port")
        .RequiredArgument("NUM")
        .DefaultValue(ToString(Port))
        .StoreResult(&Port);

    opts.AddLongOption("backlog")
        .RequiredArgument("NUM")
        .DefaultValue(ToString(Backlog))
        .StoreResult(&Backlog);

    opts.AddLongOption("queue-size")
        .RequiredArgument("NUM")
        .DefaultValue(ToString(QueueSize))
        .StoreResult(&QueueSize);

    opts.AddLongOption("poller-threads")
        .RequiredArgument("NUM")
        .DefaultValue(ToString(PollerThreads))
        .StoreResult(&PollerThreads);

    opts.AddLongOption("wait")
        .RequiredArgument("{" + GetEnumAllNames<EWaitMode>() + "}")
        .DefaultValue(ToString(WaitMode))
        .Handler1T<TString>([this] (const auto& s) {
            WaitMode = FromString<EWaitMode>(s);
        });

    opts.AddLongOption("connect-timeout")
        .RequiredArgument("SEC")
        .DefaultValue(ToString(ConnectTimeout))
        .StoreResult(&ConnectTimeout);

    opts.AddLongOption("tos")
        .RequiredArgument("NUM")
        .StoreResult(&Tos);

    opts.AddLongOption("source-interface")
        .RequiredArgument()
        .StoreResult(&SourceInterface);

    // device geometry
    opts.AddLongOption("storage")
        .RequiredArgument("{" + GetEnumAllNames<EStorageKind>() + "}")
        .DefaultValue(ToString(StorageKind))
        .Handler1T<TString>([this] (const auto& s) {
            StorageKind = FromString<EStorageKind>(s);
        });

    opts.AddLongOption("storage-path")
        .RequiredArgument("PATH")
        .StoreResult(&StoragePath);

    opts.AddLongOption("block-size")
        .RequiredArgument("NUM")
        .DefaultValue(ToString(BlockSize))
        .StoreResult(&BlockSize);

    opts.AddLongOption("blocks-count")
        .RequiredArgument("NUM")
        .DefaultValue(ToString(BlocksCount))
        .StoreResult(&BlocksCount);

    // test options
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

    opts.AddLongOption("threadpool")
        .RequiredArgument("NUM")
        .DefaultValue(ToString(ThreadPool))
        .StoreResult(&ThreadPool);

    // request tracing
    opts.AddLongOption("trace-path")
        .RequiredArgument("PATH")
        .StoreResult(&TracePath);

    opts.AddLongOption("trace-rate")
        .RequiredArgument("NUM")
        .DefaultValue(ToString(TraceRate))
        .StoreResult(&TraceRate);

    TOptsParseResultException res(&opts, argc, argv);

    if (res.Has(&verbose) && !VerboseLevel) {
        VerboseLevel = "debug";
    }

    TestDuration = TDuration::Seconds(seconds);

    if (!MaxBlocksCount) {
        MaxBlocksCount = MinBlocksCount;
    }

    Y_ENSURE(BlockSize && (BlockSize & (BlockSize - 1)) == 0);
    Y_ENSURE(BlocksCount && BlocksCount > MaxBlocksCount);
    Y_ENSURE(MinBlocksCount && MinBlocksCount <= MaxBlocksCount);
    Y_ENSURE(WriteRate <= 100);
}

}   // namespace NCloud::NBlockStore
