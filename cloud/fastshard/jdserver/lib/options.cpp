#include "options.h"

#include <library/cpp/getopt/small/last_getopt.h>
#include <library/cpp/string_utils/parse_size/parse_size.h>

#include <util/generic/yexception.h>

namespace NCloud::NFastShard::NJdServer {

using namespace NLastGetopt;

namespace {

////////////////////////////////////////////////////////////////////////////////

// "1G", "512M", "4096": the value in bytes with an optional K/M/G/T suffix
ui64 ParseSize(const TString& name, const TString& value)
{
    try {
        return NSize::ParseSize(value);
    } catch (...) {
        ythrow TUsageException()
            << "--" << name << ": " << CurrentExceptionMessage();
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TOptions::Parse(int argc, const char* argv[])
{
    TOpts opts;
    opts.AddHelpOption('h');
    opts.AddVersionOption();
    opts.SetFreeArgsNum(0);
    opts.SetTitle(
        "Standalone journalled device server: serves one file-backed device "
        "over the device protocol, without a disk agent");

    opts.AddLongOption("host", "listen on this host only")
        .RequiredArgument("STR")
        .StoreResult(&Host);

    opts.AddLongOption("port", "listen on this port")
        .RequiredArgument("NUM")
        .StoreResult(&Port);

    opts.AddLongOption("device-path", "file that backs the device")
        .RequiredArgument("PATH")
        .StoreResult(&DevicePath);

    opts.AddLongOption("device-uuid", "UUID the device is served under")
        .RequiredArgument("STR")
        .DefaultValue(DeviceUUID)
        .StoreResult(&DeviceUUID);

    TString blockSize = ToString(BlockSize);
    opts.AddLongOption("block-size", "device block size in bytes")
        .RequiredArgument("SIZE")
        .DefaultValue(blockSize)
        .StoreResult(&blockSize);

    TString logMetaSize = ToString(LogMetaSize);
    opts.AddLongOption(
            "log-meta-size",
            "journal metadata part size; a K/M/G suffix is accepted")
        .RequiredArgument("SIZE")
        .DefaultValue(logMetaSize)
        .StoreResult(&logMetaSize);

    TString logDataSize = ToString(LogDataSize);
    opts.AddLongOption(
            "log-data-size",
            "journal data part size; a K/M/G suffix is accepted")
        .RequiredArgument("SIZE")
        .DefaultValue(logDataSize)
        .StoreResult(&logDataSize);

    TString dataSize = ToString(DataSize);
    opts.AddLongOption(
            "data-size",
            "data part size; a K/M/G suffix is accepted")
        .RequiredArgument("SIZE")
        .DefaultValue(dataSize)
        .StoreResult(&dataSize);

    opts.AddLongOption(
            "no-journal",
            "serve the device without a journal: writes go straight to "
            "the data and the journal parts are not allocated")
        .NoArgument()
        .SetFlag(&NoJournal);

    opts.AddLongOption(
            "executors",
            "executor threads; the connections are spread over them")
        .RequiredArgument("NUM")
        .DefaultValue(Executors)
        .StoreResult(&Executors);

    opts.AddLongOption("aio-threads", "AIO completion threads")
        .RequiredArgument("NUM")
        .DefaultValue(AioThreads)
        .StoreResult(&AioThreads);

    opts.AddLongOption("verbose", "enable debug logging")
        .NoArgument()
        .SetFlag(&Verbose);

    TOptsParseResultException parseResult(&opts, argc, argv);

    const ui64 blockSizeValue = ParseSize("block-size", blockSize);
    if (blockSizeValue > Max<ui32>()) {
        ythrow TUsageException() << "--block-size is too large";
    }
    BlockSize = static_cast<ui32>(blockSizeValue);

    LogMetaSize = ParseSize("log-meta-size", logMetaSize);
    LogDataSize = ParseSize("log-data-size", logDataSize);
    DataSize = ParseSize("data-size", dataSize);

    if (!Port) {
        ythrow TUsageException() << "--port is required";
    }

    Validate();
}

void TOptions::Validate() const
{
    if (!DevicePath) {
        ythrow TUsageException() << "--device-path is required";
    }
    if (!DeviceUUID) {
        ythrow TUsageException() << "--device-uuid must not be empty";
    }
    if (!BlockSize) {
        ythrow TUsageException() << "--block-size must be positive";
    }
    if (!DataSize) {
        ythrow TUsageException() << "--data-size must be positive";
    }
    if (!Executors || !AioThreads) {
        ythrow TUsageException()
            << "--executors and --aio-threads must be positive";
    }
    if (DataSize % BlockSize) {
        ythrow TUsageException()
            << "--data-size must be a multiple of --block-size";
    }

    if (NoJournal) {
        return;
    }

    if (!LogMetaSize || !LogDataSize) {
        ythrow TUsageException()
            << "--log-meta-size and --log-data-size must be positive "
               "unless --no-journal is set";
    }
    if (LogMetaSize % BlockSize || LogDataSize % BlockSize) {
        ythrow TUsageException()
            << "--log-meta-size and --log-data-size must be multiples of "
               "--block-size";
    }
}

ui64 TOptions::GetLogMetaSize() const
{
    return NoJournal ? 0 : LogMetaSize;
}

ui64 TOptions::GetLogDataSize() const
{
    return NoJournal ? 0 : LogDataSize;
}

ui64 TOptions::GetDeviceSize() const
{
    return GetLogMetaSize() + GetLogDataSize() + DataSize;
}

}   // namespace NCloud::NFastShard::NJdServer
