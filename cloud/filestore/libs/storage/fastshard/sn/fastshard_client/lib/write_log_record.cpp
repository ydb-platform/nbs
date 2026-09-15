#include "write_log_record.h"

#include <util/generic/yexception.h>

namespace NCloud::NFileStore::NStorage::NFastShard::NClient {

using namespace NLastGetopt;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TWriteLogRecordCommand final: public TCommand
{
private:
    TString DeviceUUID;
    ui64 FirstPageNo = 0;
    ui32 PageSize = 4096;
    ui64 Lsn = 0;
    ui64 PrevLsn = 0;

public:
    explicit TWriteLogRecordCommand(IStorageNodePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("device-uuid", "device to write to")
            .RequiredArgument("STR")
            .StoreResult(&DeviceUUID);

        Opts.AddLongOption("first-page-no", "number of the first page to write")
            .RequiredArgument("NUM")
            .StoreResult(&FirstPageNo);

        Opts.AddLongOption("page-size")
            .Help("page size in bytes; the input is split into pages of "
                  "this size")
            .RequiredArgument("NUM")
            .DefaultValue(PageSize)
            .StoreResult(&PageSize);

        Opts.AddLongOption("lsn", "log sequence number of the record")
            .RequiredArgument("NUM")
            .StoreResult(&Lsn);

        Opts.AddLongOption("prev-lsn", "log sequence number of the previous record")
            .RequiredArgument("NUM")
            .StoreResult(&PrevLsn);
    }

protected:
    void CheckOpts() const override
    {
        if (Proto) {
            return;
        }
        if (!DeviceUUID) {
            ythrow TUsageException() << "--device-uuid is required";
        }
        if (!PageSize) {
            ythrow TUsageException() << "--page-size must be positive";
        }
    }

    bool DoExecute() override
    {
        NCloud::NProto::TWriteLogRecordRequest request;
        if (Proto) {
            ParseFromTextFormat(GetInputStream(), request);
        } else {
            const TString data = GetInputStream().ReadAll();
            if (data.empty() || data.size() % PageSize) {
                ythrow yexception()
                    << "input size " << data.size()
                    << " is not a positive multiple of page size " << PageSize;
            }

            request.SetDeviceUUID(DeviceUUID);
            request.SetLogSequenceNumber(Lsn);
            request.SetPrevLogSequenceNumber(PrevLsn);

            auto* group = request.AddPageGroups();
            group->SetFirstPageNo(FirstPageNo);
            for (size_t offset = 0; offset < data.size(); offset += PageSize) {
                group->AddContent(data.substr(offset, PageSize));
            }
        }
        PrepareHeaders(*request.MutableHeaders());

        auto response = Client->WriteLogRecord(std::move(request));
        if (!HandleResponse(response)) {
            return false;
        }

        if (!Proto) {
            GetOutputStream() << "OK" << Endl;
        }
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewWriteLogRecordCommand(IStorageNodePtr client)
{
    return std::make_shared<TWriteLogRecordCommand>(std::move(client));
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard::NClient
