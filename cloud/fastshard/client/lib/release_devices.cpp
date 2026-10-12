#include "release_devices.h"

#include <util/generic/vector.h>

namespace NCloud::NFastShard::NClient {

using namespace NLastGetopt;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TReleaseDevicesCommand final: public TCommand
{
private:
    TVector<TString> DeviceUUIDs;
    ui32 Generation = 0;
    TString FastshardId;

public:
    explicit TReleaseDevicesCommand(IStorageNodePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("device-uuid", "device to release (may be repeated)")
            .RequiredArgument("STR")
            .AppendTo(&DeviceUUIDs);

        Opts.AddLongOption(
                "generation",
                "client generation; a request with a lower generation than "
                "the last one seen from the client is rejected")
            .RequiredArgument("NUM")
            .StoreResult(&Generation);

        Opts.AddLongOption(
                "fastshard-id",
                "fastshard the devices belong to")
            .RequiredArgument("STR")
            .StoreResult(&FastshardId);
    }

protected:
    void CheckOpts() const override
    {
        if (!Proto && DeviceUUIDs.empty()) {
            ythrow TUsageException() << "--device-uuid is required";
        }
    }

    bool DoExecute() override
    {
        NCloud::NProto::TReleaseDevicesRequest request;
        if (Proto) {
            ParseFromTextFormat(GetInputStream(), request);
        } else {
            for (const auto& uuid: DeviceUUIDs) {
                request.AddDeviceUUIDs(uuid);
            }
            request.SetGeneration(Generation);
            request.SetFastshardId(FastshardId);
        }
        PrepareHeaders(*request.MutableHeaders());

        auto response = Call(&IStorageNode::ReleaseDevices, std::move(request));
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

TCommandPtr NewReleaseDevicesCommand(IStorageNodePtr client)
{
    return std::make_shared<TReleaseDevicesCommand>(std::move(client));
}

}   // namespace NCloud::NFastShard::NClient
