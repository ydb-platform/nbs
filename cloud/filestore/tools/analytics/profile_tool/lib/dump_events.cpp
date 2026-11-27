#include "public.h"

#include "command.h"
#include "common_filter_params.h"

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/libs/storage/tablet/model/profile_log_events.h>
#include <cloud/filestore/tools/analytics/libs/event-log/dump.h>
#include <cloud/filestore/tools/analytics/libs/event-log/request_filter.h>

#include <cloud/storage/core/libs/common/format.h>

#include <library/cpp/eventlog/dumper/evlogdump.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/generic/xrange.h>
#include <util/string/join.h>
#include <util/system/fstat.h>
#include <util/system/platform.h>

#include <sys/stat.h>

#include <cstdint>
#include <filesystem>
#include <limits>
#if defined(_unix_)
#include <unistd.h>
#endif

namespace NCloud::NFileStore::NProfileTool {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TEventProcessor
    : public TProtobufEventProcessor
{
private:
    const IRequestFilterPtr Filter;

public:
    explicit TEventProcessor(IRequestFilterPtr filter)
        : Filter(std::move(filter))
    {}

    void DoProcessEvent(const TEvent* ev, IOutputStream* out) override
    {
        auto* message =
            dynamic_cast<const NProto::TProfileLogRecord*>(ev->GetProto());
        if (message) {
            DumpDiscardedRequestCount(*message, out);

            const auto filtered_message = Filter->GetFilteredRecord(*message);
            const auto order = GetItemOrder(filtered_message);

            for (const auto i : order) {
                DumpRequest(filtered_message, i, out);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

bool IsStdoutPipe() noexcept
{
#if defined(_unix_)
    struct stat info;
    return ::fstat(STDOUT_FILENO, &info) == 0 && S_ISFIFO(info.st_mode);
#else
    return false;
#endif
}

////////////////////////////////////////////////////////////////////////////////

class TDumpEventsCommand final
    : public TCommand
{
private:
    const TCommonFilterParams CommonFilterParams;

    IRequestFilterPtr Filter = CreateRequestFilterAccept();

    THashMap<TString, ui32> RequestName2Type;
    TVector<TString> FilterRequestNames;
    TVector<ui32> FilterRequestTypes;
    bool FilterSystemRequests = false;
    bool FilterExternalRequests = false;
    bool IgnoreDiskSpace = false;

public:
    TDumpEventsCommand()
        : CommonFilterParams(Opts)
    {
        const auto requestTypes = GetRequestTypes();
        TVector<TString> requestNames;
        TVector<TString> requestDescrs;
        requestDescrs.reserve(requestTypes.size());
        for (const auto& [id, name]: requestTypes) {
            RequestName2Type[name] = id;
            requestDescrs.push_back(TStringBuilder() << id << ": " << name);
            requestNames.push_back(name);
        }

        Opts.AddLongOption(
                "request-name",
                TStringBuilder()
                    << "comma separated request names, used for filtering."
                    << " Possible request names:\n"
                    << JoinSeq("\n", requestNames))
            .RequiredArgument("STRINGS")
            .SplitHandler(&FilterRequestNames, ',');

        Opts.AddLongOption(
                "request-type",
                TStringBuilder()
                    << "comma separated request types, used for filtering."
                    << " Possible request types:\n"
                    << JoinSeq("\n", requestDescrs))
            .RequiredArgument("NUMS")
            .SplitHandler(&FilterRequestTypes, ',');

        Opts.AddLongOption(
                "system-requests",
                "show internal tablet events")
            .NoArgument()
            .SetFlag(&FilterSystemRequests);

        Opts.AddLongOption(
                "external-requests",
                "show external requests events")
            .NoArgument()
            .SetFlag(&FilterExternalRequests);

        Opts.AddLongOption(
                "ignore-disk-space",
                "Skip the size estimate and free space check for piped output")
            .NoArgument()
            .SetFlag(&IgnoreDiskSpace);
    }

    bool Init(NLastGetopt::TOptsParseResultException& parseResult) override
    {
        const auto nodeId = CommonFilterParams.GetNodeId(parseResult);
        if (nodeId.Defined()) {
            Filter = CreateRequestFilterByNodeId(
                std::move(Filter),
                nodeId.GetRef());
        }

        const auto handle = CommonFilterParams.GetHandle(parseResult);
        if (handle.Defined()) {
            Filter = CreateRequestFilterByHandle(
                std::move(Filter),
                handle.GetRef());
        }

        for (const auto& name: FilterRequestNames) {
            FilterRequestTypes.push_back(RequestName2Type.at(name));
        }

        if (!FilterRequestTypes.empty()) {
            Filter = CreateRequestFilterByRequestType(
                std::move(Filter),
                {FilterRequestTypes.begin(), FilterRequestTypes.end()});
        }

        if (FilterSystemRequests) {
            TSet<ui32> systemRequests = ::xrange(
                NStorage::FileStoreSystemRequestStart,
                NStorage::FileStoreSystemRequestStart
                    + NStorage::FileStoreSystemRequestCount,
                1);

            Filter = CreateRequestFilterByRequestType(
                std::move(Filter),
                std::move(systemRequests));
        }

        if (FilterExternalRequests) {
            TSet<ui32> systemRequests = ::xrange(
                0u,
                static_cast<ui32>(EFileStoreRequest::MAX),
                1);

            Filter = CreateRequestFilterByRequestType(
                std::move(Filter),
                std::move(systemRequests));
        }

        const auto timeRange = CommonFilterParams.GetTimeRange(parseResult);
        if (timeRange.Until.Defined()) {
            Filter = CreateRequestFilterUntil(
                std::move(Filter),
                timeRange.Until.GetRef().MicroSeconds());
        }

        if (timeRange.Since.Defined()) {
            Filter = CreateRequestFilterSince(
                std::move(Filter),
                timeRange.Since.GetRef().MicroSeconds());
        }

        const auto fileSystemId =
            CommonFilterParams.GetFileSystemId(parseResult);
        if (fileSystemId.Defined()) {
            Filter = CreateRequestFilterByFileSystemId(
                std::move(Filter),
                fileSystemId.GetRef());
        }

        if (!parseResult.Has("no-skip-files")) {
            ProfileLogFiles = SelectProfileLogFiles(
                std::move(ProfileLogFiles), timeRange.Since, timeRange.Until);
        }

        return true;
    }

    int Execute() override
    {
        if (!IgnoreDiskSpace && IsStdoutPipe() && !CheckDiskSpace()) {
            Cerr << "Dump aborted. Use --ignore-disk-space to bypass the disk "
                    "space check."
                 << Endl;
            return 1;
        }

        TEventProcessor processor(Filter);
        const int result = ProcessProfileLogs(processor);
        Cout.Flush();
        return result;
    }

private:
    static TMaybe<ui64> ReportAvailableSpace(const char* path)
    {
        std::error_code error;
        const auto space = std::filesystem::space(path, error);
        if (error) {
            Cerr << "Cannot check free space on " << path << ": "
                 << error.message() << Endl;
            return {};
        }

        // libc++ can report the unknown-size sentinel for zero free blocks.
        const ui64 availableBytes =
            space.available == std::numeric_limits<std::uintmax_t>::max()
                ? 0
                : space.available;
        Cerr << "Free space on " << path << " is "
             << FormatByteSize(availableBytes) << " (" << availableBytes
             << " bytes)." << Endl;
        return availableBytes;
    }

    bool CheckDiskSpace()
    {
        constexpr ui64 DumpSizeMultiplier = 20;
        constexpr ui64 MaxInputBytes =
            std::numeric_limits<ui64>::max() / DumpSizeMultiplier;
        constexpr ui64 MinRemainingBytes = 1ULL << 30;

        ui64 inputBytes = 0;
        TVector<TProfileLogFile> files;
        files.reserve(ProfileLogFiles.size());
        for (const auto& file: ProfileLogFiles) {
            const TFileStat stat(file.Path);
            const bool isFile = stat.IsFile();
            if (!isFile && IgnoreErrors) {
                Cerr << "Skipping profile log " << file.Path
                     << " because its file size is unavailable." << Endl;
                continue;
            }
            if (!isFile || stat.Size > MaxInputBytes - inputBytes) {
                Cerr << "Cannot estimate dump size from profile log "
                     << file.Path << Endl;
                return false;
            }
            inputBytes += stat.Size;
            files.push_back(file);
        }
        ProfileLogFiles = std::move(files);
        if (inputBytes == 0) {
            return true;
        }

        const ui64 estimatedBytes = inputBytes * DumpSizeMultiplier;
        Cerr << "Estimated dump size is about "
             << FormatByteSize(estimatedBytes) << " (" << estimatedBytes
             << " bytes) at " << DumpSizeMultiplier
             << " times the compressed input size ("
             << FormatByteSize(inputBytes) << ")." << Endl;

        const auto availableBytes = ReportAvailableSpace("/");
        struct stat rootStat;
        struct stat cwdStat;
        if (::stat("/", &rootStat) == 0 && ::stat(".", &cwdStat) == 0 &&
            rootStat.st_dev != cwdStat.st_dev)
        {
            ReportAvailableSpace(".");
        }

        if (!availableBytes) {
            return false;
        }

        if (*availableBytes < MinRemainingBytes ||
            estimatedBytes > *availableBytes - MinRemainingBytes)
        {
            Cerr << "Insufficient free space on / for the dump. "
                    "1 GiB must remain after writing."
                 << Endl;
            return false;
        }
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewDumpEventsCommand()
{
    return std::make_shared<TDumpEventsCommand>();
}

}   // namespace NCloud::NFileStore::NProfileTool
