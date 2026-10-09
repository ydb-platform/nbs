#include "command.h"

#include "common_filter_params.h"
#include "public.h"

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/libs/storage/tablet/model/profile_log_events.h>
#include <cloud/filestore/tools/analytics/libs/event-log/dump.h>
#include <cloud/filestore/tools/analytics/libs/event-log/request_filter.h>

#include <library/cpp/eventlog/dumper/evlogdump.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/generic/xrange.h>
#include <util/string/join.h>

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

    TMaybe<TInstant> Since;
    TMaybe<TInstant> Until;

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

        Until = CommonFilterParams.GetUntil(parseResult);
        if (Until.Defined()) {
            Filter = CreateRequestFilterUntil(
                std::move(Filter),
                Until.GetRef().MicroSeconds());
        }

        Since = CommonFilterParams.GetSince(parseResult);
        if (Since.Defined()) {
            Filter = CreateRequestFilterSince(
                std::move(Filter),
                Since.GetRef().MicroSeconds());
        }

        const auto fileSystemId =
            CommonFilterParams.GetFileSystemId(parseResult);
        if (fileSystemId.Defined()) {
            Filter = CreateRequestFilterByFileSystemId(
                std::move(Filter),
                fileSystemId.GetRef());
        }

        return true;
    }

    int Execute() override
    {
        TEventProcessor processor(Filter);

        TVector<TString> argStorage{""};
        if (Since.Defined()) {
            argStorage.push_back("-s");
            argStorage.push_back(ToString(Since.GetRef().MicroSeconds()));
        }
        argStorage.push_back(PathToProfileLog);

        TVector<const char*> argv;
        argv.reserve(argStorage.size());
        for (const auto& arg: argStorage) {
            argv.push_back(arg.c_str());
        }

        return IterateEventLog(
            NEvClass::Factory(),
            &processor,
            static_cast<int>(argv.size()),
            argv.data());
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewDumpEventsCommand()
{
    return std::make_shared<TDumpEventsCommand>();
}

}   // namespace NCloud::NFileStore::NProfileTool
