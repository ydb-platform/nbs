#include "read_journal_tail.h"

namespace NCloud::NFileStore::NStorage::NFastShard::NClient {

using namespace NLastGetopt;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TReadJournalTailCommand final: public TCommand
{
private:
    TString DeviceUUID;
    ui64 AfterLsn = 0;
    ui32 MaxRecordCount = 0;

public:
    explicit TReadJournalTailCommand(IStorageNodePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("device-uuid", "device to read the journal from")
            .RequiredArgument("STR")
            .StoreResult(&DeviceUUID);

        Opts.AddLongOption("after-lsn")
            .Help("return only the records with a log sequence number "
                  "strictly greater than this value")
            .RequiredArgument("NUM")
            .StoreResult(&AfterLsn);

        Opts.AddLongOption(
                "max-record-count",
                "maximum number of records to return (0 means no limit)")
            .RequiredArgument("NUM")
            .StoreResult(&MaxRecordCount);
    }

protected:
    void CheckOpts() const override
    {
        if (!Proto && !DeviceUUID) {
            ythrow TUsageException() << "--device-uuid is required";
        }
    }

    bool DoExecute() override
    {
        NCloud::NProto::TReadJournalTailRequest request;
        if (Proto) {
            ParseFromTextFormat(GetInputStream(), request);
        } else {
            request.SetDeviceUUID(DeviceUUID);
            request.SetAfterLogSequenceNumber(AfterLsn);
            request.SetMaxRecordCount(MaxRecordCount);
        }
        PrepareHeaders(*request.MutableHeaders());

        auto response =
            Call(&IStorageNode::ReadJournalTail, std::move(request));
        if (!HandleResponse(response)) {
            return false;
        }

        if (!Proto) {
            // a summary of the records; page contents are only available
            // via --proto
            auto& out = GetOutputStream();
            out << "LastAckedLogSequenceNumber: "
                << response.GetLastAckedLogSequenceNumber() << Endl;
            out << "Records: " << response.RecordsSize() << Endl;
            for (const auto& record: response.GetRecords()) {
                out << "  Lsn: " << record.GetLogSequenceNumber()
                    << " PrevLsn: " << record.GetPrevLogSequenceNumber();
                for (const auto& group: record.GetPageGroups()) {
                    ui64 bytes = 0;
                    for (const auto& page: group.GetContent()) {
                        bytes += page.size();
                    }
                    out << " [FirstPageNo: " << group.GetFirstPageNo()
                        << " Pages: " << group.ContentSize()
                        << " Bytes: " << bytes << "]";
                }
                out << Endl;
            }
        }
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewReadJournalTailCommand(IStorageNodePtr client)
{
    return std::make_shared<TReadJournalTailCommand>(std::move(client));
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard::NClient
