#include "format_device.h"

namespace NCloud::NFastShard::NClient {

using namespace NLastGetopt;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TFormatDeviceCommand final: public TCommand
{
private:
    TString DeviceUUID;

public:
    explicit TFormatDeviceCommand(IStorageNodePtr client)
        : TCommand(std::move(client))
    {
        AddAcquireOption();

        Opts.AddLongOption("device-uuid", "device whose journal is wiped")
            .RequiredArgument("STR")
            .StoreResult(&DeviceUUID);
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
        NCloud::NProto::TFormatDeviceRequest request;
        if (Proto) {
            ParseFromTextFormat(GetInputStream(), request);
        } else {
            request.SetDeviceUUID(DeviceUUID);
        }
        PrepareHeaders(*request.MutableHeaders());

        auto response = Call(&IStorageNode::FormatDevice, std::move(request));
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

TCommandPtr NewFormatDeviceCommand(IStorageNodePtr client)
{
    return std::make_shared<TFormatDeviceCommand>(std::move(client));
}

}   // namespace NCloud::NFastShard::NClient
