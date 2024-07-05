#include "options.h"

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/strbuf.h>
#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/split.h>

using namespace NLastGetopt;

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

void CheckOneOf(
    const TVector<TString>& values,
    const TString& value,
    const TString& message)
{
    auto it = std::find(values.begin(), values.end(), value);
    if (it == values.end()) {
        throw TUsageException()
            << message << "[" << value << "] should be one of ["
            << JoinSeq(", ", values) << "]";
    }
}

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

    opts.AddLongOption("disk-id", "disk id")
        .RequiredArgument("STR")
        .StoreResultDef(&DiskId);

    opts.AddLongOption("client-id", "client id")
        .RequiredArgument("STR")
        .StoreResultDef(&ClientId);

    opts.AddLongOption(
            "device",
            "specify device string path:size:offset "
            "(e.g. /dev/vda:1000000:0, rdma://host:10020/abcdef:1000000:0)")
        .Required()
        .RequiredArgument("STR")
        .Handler1T<TString>(
            [this](TStringBuf s)
            {
                auto i = s.find_last_of(':');
                Y_ENSURE(i != s.npos, "invalid format");

                auto j = s.find_last_of(':', i - 1);
                Y_ENSURE(j != s.npos, "invalid format");

                const i64 offset = FromString<i64>(s.substr(i + 1));
                const i64 size = FromString<i64>(s.substr(j + 1, i - j - 1));

                Layout.push_back(TDeviceChunk{
                    .DevicePath = ToString(s.substr(0, j)),
                    .ByteCount = size,
                    .Offset = offset,
                });
            });

    opts.AddLongOption(
            "device-backend",
            "specify device backend (aio, rdma, null)")
        .RequiredArgument("STR")
        .StoreResultDef(&DeviceBackend);

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

    opts.AddLongOption("block-size", "size of block device")
        .RequiredArgument("INT")
        .StoreResultDef(&BlockSize);

    opts.AddLongOption('q', "queue-count")
        .RequiredArgument("INT")
        .StoreResult(&QueueCount);

    opts.AddLongOption('a', "socket-access-mode")
        .RequiredArgument("INT")
        .StoreResult(&SocketAccessMode);

    opts.AddLongOption('v', "verbose", "output level for diagnostics messages")
        .OptionalArgument("STR")
        .StoreResultDef(&VerboseLevel);

    opts.AddLongOption("log-type", "log type: json/console")
        .RequiredArgument("STR")
        .StoreResultDef(&LogType);

    opts.AddLongOption("rdma-queue-size", "Rdma client queue size")
        .RequiredArgument("INT")
        .StoreResultDef(&RdmaClient.QueueSize);

    opts.AddLongOption("rdma-max-buffer-size", "Rdma client queue size")
        .RequiredArgument("INT")
        .StoreResultDef(&RdmaClient.MaxBufferSize);

    opts.AddLongOption(
            "wait-after-parent-exit",
            "How many seconds keep alive after the parent process is exited")
        .RequiredArgument("INT")
        .StoreResultDef(&WaitAfterParentExit);

    TOptsParseResultException res(&opts, argc, argv);

    if (res.FindLongOptParseResult("verbose") && VerboseLevel.empty()) {
        VerboseLevel = "debug";
    }

    if (res.FindLongOptParseResult("log-type")) {
        CheckOneOf({"json", "console"}, LogType, "invalid log-type");
    }

    if (res.FindLongOptParseResult("device-backend")) {
        CheckOneOf(
            {"aio", "rdma", "null"},
            DeviceBackend,
            "invalid device-backend");
    }

    if (DiskId.empty()) {
        DiskId = Serial;
    }

    if (!QueueCount) {
        QueueCount = Min<ui32>(8, Layout.size());
    }
}

}   // namespace NCloud::NBlockStore::NVHostServer
