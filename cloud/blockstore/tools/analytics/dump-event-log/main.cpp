#include <cloud/blockstore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/tools/analytics/dump-event-log/sqlite_output.h>
#include <cloud/blockstore/tools/analytics/libs/event-log/dump.h>

#include <library/cpp/eventlog/dumper/evlogdump.h>
#include <library/cpp/getopt/small/last_getopt.h>

#include <util/stream/file.h>

using namespace NCloud::NBlockStore;

namespace {

////////////////////////////////////////////////////////////////////////////////

TSet<TString> LoadDiskIds(const TString& filename)
{
    TSet<TString> diskIds;
    TFileInput input(filename);
    TString line;
    while (input.ReadLine(line)) {
        diskIds.insert(line);
    }
    return diskIds;
}

TSet<ui32> LoadRequestTypes(const TString& filename)
{
    TSet<ui32> requestIds;
    TFileInput input(filename);
    TString line;
    while (input.ReadLine(line)) {
        requestIds.insert(FromString<ui32>(line));
    }

    return requestIds;
}

struct TEventProcessor: TProtobufEventProcessor
{
    TEventLogPtr EventLog;
    TString OutputFilename;
    TString OutputDatabaseFilename;
    TString FilterByDiskId;
    TString FilterByDiskIdFile;
    TString FilterByRequestTypeFile;
    TSet<TString> FilterByDiskIdSet;
    TSet<ui32> RequestTypeSet;
    std::optional<TBlockRange64> FilterRange;
    std::unique_ptr<TSqliteOutput> SqliteOutput;

    void DoProcessEvent(const TEvent* ev, IOutputStream* out) override
    {
        const auto* message =
            dynamic_cast<const NProto::TProfileLogRecord*>(ev->GetProto());
        if (!message) {
            return;
        }

        if (OutputFilename) {
            if (!EventLog) {
                EventLog = MakeIntrusive<TEventLog>(
                    OutputFilename,
                    NEvClass::Factory()->CurrentFormat());
            }
            EventLog->LogEvent(*message);
            return;
        }

        if (FilterByDiskIdFile) {
            if (FilterByDiskIdSet.empty()) {
                FilterByDiskIdSet = LoadDiskIds(FilterByDiskIdFile);
            }
        }

        if (FilterByRequestTypeFile) {
            if (RequestTypeSet.empty()) {
                RequestTypeSet = LoadRequestTypes(FilterByRequestTypeFile);
            }
        }

        const TVector<TItemDescriptor> order = GetItemOrder(*message);
        for (const auto& [type, index]: order) {
            if (!ShouldDump(*message, type, index)) {
                continue;
            }

            if (OutputDatabaseFilename) {
                if (!SqliteOutput) {
                    SqliteOutput =
                        std::make_unique<TSqliteOutput>(OutputDatabaseFilename);
                }

                SqliteOutput->ProcessMessage(*message, type, index);
                continue;
            }

            switch (type) {
                case EItemType::Request: {
                    DumpRequest(*message, index, out);
                    break;
                }
                case EItemType::BlockInfo: {
                    DumpBlockInfoList(*message, index, out);
                    break;
                }
                case EItemType::BlockCommitId: {
                    DumpBlockCommitIdList(*message, index, out);
                    break;
                }
                case EItemType::BlobUpdate: {
                    DumpBlobUpdateList(*message, index, out);
                    break;
                }
                default: {
                    Y_ABORT("unknown item");
                }
            }
        }
    }

    bool ShouldDump(
        const NProto::TProfileLogRecord& message,
        EItemType type,
        int index) const
    {
        if (FilterByDiskId && FilterByDiskId != message.GetDiskId()) {
            return false;
        }

        if (FilterByDiskIdSet &&
            !FilterByDiskIdSet.contains(message.GetDiskId()))
        {
            return false;
        }

        if (RequestTypeSet && type == EItemType::Request &&
            !RequestTypeSet.contains(
                message.GetRequests(index).GetRequestType()))
        {
            return false;
        }

        if (!FilterRange) {
            return true;
        }

        switch (type) {
            case EItemType::BlockInfo: {
                return AnyOf(
                    message.GetBlockInfoLists(index).GetBlockInfos(),
                    [&](const auto& block)
                    { return FilterRange->Contains(block.GetBlockIndex()); });
            }
            case EItemType::Request: {
                const auto& req = message.GetRequests(index);

                if (!req.RangesSize()) {
                    return req.GetBlockCount() != 0 &&
                           FilterRange->Overlaps(
                               TBlockRange64::WithLength(
                                   req.GetBlockIndex(),
                                   req.GetBlockCount()));
                }

                return AnyOf(
                    req.GetRanges(),
                    [&](const auto& r)
                    {
                        return FilterRange->Overlaps(
                            TBlockRange64::WithLength(
                                r.GetBlockIndex(),
                                r.GetBlockCount()));
                    });
            }
            default:
                return false;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TEventProcessorProxy: public ITunableEventProcessor
{
private:
    TEventProcessor* Processor;

public:
    explicit TEventProcessorProxy(TEventProcessor* proc)
        : Processor(proc)
    {}

    void AddOptions(NLastGetopt::TOpts& opts) override
    {
        opts.AddLongOption(
                "output-binary-log-file",
                "Enables output to the specified file, original binary "
                "format is preserved")
            .Optional()
            .StoreResult(&Processor->OutputFilename);

        opts.AddLongOption(
                "filter-by-disk-id",
                "Filter by diskId. Example: --filter-by-disk-id xxx")
            .Optional()
            .StoreResult(&Processor->FilterByDiskId);

        opts.AddLongOption(
                "filter-by-disk-id-file",
                "Filter by disk names. The disk names are located in the file, "
                "each in a separate line.")
            .Optional()
            .StoreResult(&Processor->FilterByDiskIdFile);

        opts.AddLongOption(
                "filter-by-request-type-file",
                "Filter by request type. The numerical request types are "
                "located in the file, each in a separate line")
            .Optional()
            .StoreResult(&Processor->FilterByRequestTypeFile);

        opts.AddLongOption(
                "filter-by-range",
                "Show only requests that overlap with the range. Example: --filter-by-range 5000,5100")
            .Optional()
            .Handler1T<TString>([&] (TStringBuf s) {
                TStringBuf lhs;
                TStringBuf rhs;
                s.Split(',', lhs, rhs);
                Processor->FilterRange = TBlockRange64::MakeClosedInterval(
                    FromString<ui64>(lhs),
                    FromString<ui64>(rhs));
            });

        opts.AddLongOption(
                "output-sqlite-file",
                "Enables output to the sqlite database file")
            .Optional()
            .StoreResult(&Processor->OutputDatabaseFilename);
    }

    void SetOptions(const TEvent::TOutputOptions& options) override
    {
        Processor->SetOptions(options);
    }

    void ProcessEvent(const TEvent* ev) override
    {
        Processor->ProcessEvent(ev);
    }

    bool CheckedProcessEvent(const TEvent* ev) override
    {
        return Processor->CheckedProcessEvent(ev);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    TEventProcessor processor;
    TEventProcessorProxy proxy(&processor);

    return IterateEventLog(
        NEvClass::Factory(),
        static_cast<ITunableEventProcessor*>(&proxy),
        argc,
        argv);
}
