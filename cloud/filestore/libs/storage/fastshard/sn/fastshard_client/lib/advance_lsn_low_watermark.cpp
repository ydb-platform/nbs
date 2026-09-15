#include "advance_lsn_low_watermark.h"

namespace NCloud::NFileStore::NStorage::NFastShard::NClient {

using namespace NLastGetopt;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TAdvanceLsnLowWatermarkCommand final: public TCommand
{
private:
    TString DeviceUUID;
    ui64 LsnLowWatermark = 0;

public:
    explicit TAdvanceLsnLowWatermarkCommand(IStorageNodePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("device-uuid", "device whose watermark is advanced")
            .RequiredArgument("STR")
            .StoreResult(&DeviceUUID);

        Opts.AddLongOption("lsn-low-watermark")
            .Help("records with a log sequence number below this value may "
                  "be applied and dropped from the journal")
            .RequiredArgument("NUM")
            .StoreResult(&LsnLowWatermark);
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
        NCloud::NProto::TAdvanceLsnLowWatermarkRequest request;
        if (Proto) {
            ParseFromTextFormat(GetInputStream(), request);
        } else {
            request.SetDeviceUUID(DeviceUUID);
            request.SetLsnLowWatermark(LsnLowWatermark);
        }
        PrepareHeaders(*request.MutableHeaders());

        auto response = Client->AdvanceLsnLowWatermark(std::move(request));
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

TCommandPtr NewAdvanceLsnLowWatermarkCommand(IStorageNodePtr client)
{
    return std::make_shared<TAdvanceLsnLowWatermarkCommand>(std::move(client));
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard::NClient
