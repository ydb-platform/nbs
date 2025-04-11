#include "options.h"

#include <library/cpp/getopt/small/last_getopt.h>

////////////////////////////////////////////////////////////////////////////////

TString ToString(EApi api)
{
    switch (api) {
        case EApi::IoUring:
            return "io_uring";
        case EApi::LinuxAio:
            return "aio";
        case EApi::NbsAio:
            return "nbs-aio";
        default:
            Y_ABORT("unknown api: %d", (int)api);
    }
}

////////////////////////////////////////////////////////////////////////////////

TOptions::TOptions(int argc, const char** argv)
{
    using namespace NLastGetopt;

    TOpts opts;
    opts.AddHelpOption();

    opts.AddLongOption("api", "IO API to use (io_uring, aio, nbs-aio)")
        .Handler1T<TString>([this] (const TString& s) {
            if (s == "aio") {
                Api = EApi::LinuxAio;
            } else if (s == "ring" || s == "io_uring") {
                Api = EApi::IoUring;
            } else if (s == "nbs-aio" || s == "naio") {
                Api = EApi::NbsAio;
            } else {
                Y_ABORT("unknown api: %s", s.c_str());
            }
        });

    opts.AddLongOption("path", "path to the file/device")
        .Required()
        .RequiredArgument("PATH")
        .StoreResult(&Path);

    opts.AddLongOption('t', "duration", "test duration")
        .RequiredArgument("STR")
        .StoreResultDef(&Duration);

    opts.AddLongOption('b', "block-size", "size of block device")
        .RequiredArgument("INT")
        .StoreResultDef(&BlockSize);

    opts.AddLongOption('d', "depth", "IO depth")
        .RequiredArgument("INT")
        .StoreResultDef(&IoDepth);

    opts.AddLongOption('n', "ring-size", "ring size")
        .RequiredArgument("INT")
        .StoreResultDef(&RingSize);

    opts.AddLongOption('j', "threads", "test thread count")
        .RequiredArgument("INT")
        .StoreResultDef(&Threads);

    opts.AddLongOption("nbs-aio-threads", "use threaded AIO service")
        .StoreResultDef(&NbsAioThreads);

    opts.AddLongOption("io-uring-sq-polling", "enable SQ polling")
        .StoreTrue(&SqPolling);

    opts.AddLongOption('s', "quiet", "supress output")
        .StoreTrue(&Quiet);

    TOptsParseResultException(&opts, argc, argv);
}
