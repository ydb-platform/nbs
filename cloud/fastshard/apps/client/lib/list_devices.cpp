#include "list_devices.h"

namespace NCloud::NFastShard::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TListDevicesCommand final: public TCommand
{
public:
    explicit TListDevicesCommand(IStorageNodePtr client)
        : TCommand(std::move(client))
    {}

protected:
    bool DoExecute() override
    {
        NCloud::NProto::TListDevicesRequest request;
        if (Proto) {
            ParseFromTextFormat(GetInputStream(), request);
        }
        PrepareHeaders(*request.MutableHeaders());

        auto response = Call(&IStorageNode::ListDevices, std::move(request));
        if (!HandleResponse(response)) {
            return false;
        }

        if (!Proto) {
            auto& out = GetOutputStream();
            for (const auto& device: response.GetDevices()) {
                out << "DeviceUUID: " << device.GetDeviceUUID()
                    << " BlockSize: " << device.GetBlockSize()
                    << " LogMetaSize: " << device.GetLogMetaSize()
                    << " LogDataSize: " << device.GetLogDataSize()
                    << " DataSize: " << device.GetDataSize() << Endl;
            }
        }
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewListDevicesCommand(IStorageNodePtr client)
{
    return std::make_shared<TListDevicesCommand>(std::move(client));
}

}   // namespace NCloud::NFastShard::NClient
