#include "command.h"

#include "common_filter_params.h"
#include "public.h"

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/libs/storage/tablet/profile_log_events.h>
#include <cloud/filestore/tools/analytics/libs/event-log/dump.h>
#include <cloud/filestore/tools/analytics/libs/event-log/request_filter.h>

#include <library/cpp/eventlog/dumper/evlogdump.h>

#include <util/string/join.h>

namespace NCloud::NFileStore::NProfileTool {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf StartLabel = "start";
constexpr TStringBuf CountLabel = "count";
constexpr TStringBuf BlockSizeLabel = "block-size";

////////////////////////////////////////////////////////////////////////////////

class TEventProcessor
    : public TProtobufEventProcessor
{
private:
    TVector<IRequestFilterPtr> Filters;

public:
    explicit TEventProcessor(TVector<IRequestFilterPtr> filters)
        : Filters(std::move(filters))
    {}

    void DoProcessEvent(const TEvent* ev, IOutputStream* out) override
    {
        auto* message =
            dynamic_cast<const NProto::TProfileLogRecord*>(ev->GetProto());
        if (message) {
            NProto::TProfileLogRecord filtered_message;
            filtered_message.SetFileSystemId(message->GetFileSystemId());

            for (const auto& filter : Filters) {
                const auto current_record = filter->GetFilteredRecord(*message);
                for (const auto& request : current_record.GetRequests()) {
                    *filtered_message.AddRequests() = request;
                }
            }

            const auto order = GetItemOrder(filtered_message);
            for (const auto i : order) {
                DumpRequest(filtered_message, i, out);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFindBytesAccessCommand final
    : public TCommand
{
private:
    const TCommonFilterParams CommonFilterParams;

    ui64 Start = 0;
    ui64 Count = 0;
    ui32 BlockSize = 4_KB;

    TVector<IRequestFilterPtr> Filters;

public:
    TFindBytesAccessCommand()
        : CommonFilterParams(Opts)
    {
        Opts.AddLongOption(
                StartLabel.data(),
                "Start of the semi-interval (index of the first block)")
            .Required()
            .RequiredArgument("NUM")
            .StoreResult(&Start);

        Opts.AddLongOption(
                CountLabel.data(),
                "Size of the semi-interval (number of blocks)")
            .Required()
            .RequiredArgument("NUM")
            .StoreResult(&Count);

        Opts.AddLongOption(
                BlockSizeLabel.data(),
                "Block size (in bytes)")
            .RequiredArgument("NUM")
            .StoreResult(&BlockSize)
            .DefaultValue(4_KB);
    }

    bool Init(NLastGetopt::TOptsParseResultException& parseResult) override
    {
        auto filter = CreateRequestFilterByRange(
            CreateRequestFilterAccept(),
            Start,
            Count,
            BlockSize);

        const auto nodeId = CommonFilterParams.GetNodeId(parseResult);
        if (nodeId.Defined()) {
            filter = CreateRequestFilterByNodeId(
                std::move(filter),
                nodeId.GetRef());
        }

        const auto handle = CommonFilterParams.GetHandle(parseResult);
        if (handle.Defined()) {
            filter = CreateRequestFilterByHandle(
                std::move(filter),
                handle.GetRef());
        }

        const auto until = CommonFilterParams.GetUntil(parseResult);
        if (until.Defined()) {
            filter = CreateRequestFilterUntil(
                std::move(filter),
                until.GetRef().MicroSeconds());
        }

        const auto since = CommonFilterParams.GetSince(parseResult);
        if (since.Defined()) {
            filter = CreateRequestFilterSince(
                std::move(filter),
                since.GetRef().MicroSeconds());
        }

        const auto fileSystemId = CommonFilterParams.GetFileSystemId(parseResult);
        if (fileSystemId.Defined()) {
            filter = CreateRequestFilterByFileSystemId(
                std::move(filter),
                fileSystemId.GetRef());
        }

        TSet<ui32> requestTypes = {
            static_cast<ui32>(EFileStoreRequest::ReadData),
            static_cast<ui32>(EFileStoreRequest::WriteData),
            static_cast<ui32>(NStorage::EFileStoreSystemRequest::AddBlob),
            static_cast<ui32>(NStorage::EFileStoreSystemRequest::TruncateRange),
            static_cast<ui32>(NStorage::EFileStoreSystemRequest::Compaction),
            static_cast<ui32>(NStorage::EFileStoreSystemRequest::Cleanup),
            static_cast<ui32>(NStorage::EFileStoreSystemRequest::Flush)
        };

        Filters.push_back(
            CreateRequestFilterByRequestType(
                filter,
                std::move(requestTypes)));

        return true;
    }

    int Execute() override
    {
        TEventProcessor processor(Filters);
        const char* path[] = {"", PathToProfileLog.c_str()};
        return IterateEventLog(
            NEvClass::Factory(),
            &processor,
            2,
            path);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewFindBytesAccessCommand()
{
    return std::make_shared<TFindBytesAccessCommand>();
}

}   // namespace NCloud::NFileStore::NProfileTool
