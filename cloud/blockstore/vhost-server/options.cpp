#include "options.h"

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/strbuf.h>
#include <util/string/cast.h>
#include <util/string/split.h>

using namespace NLastGetopt;

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

void TOptions::Parse(int argc, char** argv)
{
    TOpts opts;
    opts.AddHelpOption();

    opts.AddLongOption('s', "socket-path")
        .Required()
        .RequiredArgument("FILE")
        .StoreResult(&SocketPath);

    opts.AddLongOption('i', "serial", "disk serial")
        .Required()
        .RequiredArgument("STR")
        .StoreResult(&Serial);

    opts.AddLongOption("device", "specify device string path:size:offset "
            "(e.g. /dev/vda:1000000:0)")
        .Required()
        .RequiredArgument("STR")
        .Handler1T<TString>([this] (TStringBuf s) {
            auto i = s.find_last_of(':');
            Y_ENSURE(i != s.npos, "invalid format");

            auto j = s.find_last_of(':', i - 1);
            Y_ENSURE(j != s.npos, "invalid format");

            const i64 offset = FromString<i64>(s.substr(i + 1));
            const i64 size = FromString<i64>(s.substr(j + 1, i - j - 1));

            Layout.push_back(TChunk {
                .FilePath = ToString(s.substr(0, j)),
                .ByteCount = size,
                .Offset = offset,
            });
        });

    opts.AddLongOption('r', "read-only", "read only mode")
        .NoArgument()
        .SetFlag(&ReadOnly);

    opts.AddLongOption("no-sync", "do not use O_SYNC")
        .NoArgument()
        .SetFlag(&NoSync);

    opts.AddLongOption("no-chmod", "do not chmod socket")
        .NoArgument()
        .SetFlag(&NoChmod);

    opts.AddLongOption('B', "batch-size")
        .RequiredArgument("INT")
        .StoreResultDef(&BatchSize);

    opts.AddLongOption('q', "queue-count")
        .RequiredArgument("INT")
        .StoreResult(&QueueCount);

    opts.AddLongOption('v', "verbose", "output level for diagnostics messages")
        .OptionalArgument("STR")
        .StoreResultDef(&VerboseLevel);

    TOptsParseResultException res(&opts, argc, argv);

    if (res.FindLongOptParseResult("verbose") && VerboseLevel.empty()) {
        VerboseLevel = "debug";
    }

    if (!QueueCount) {
        QueueCount = Min<ui32>(8, Layout.size());
    }
}

}   // namespace NCloud::NBlockStore::NVHostServer
