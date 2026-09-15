#include "acquire_devices.h"

#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage::NFastShard::NClient {

using namespace NLastGetopt;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TAcquireDevicesCommand final: public TCommand
{
private:
    TVector<TString> DeviceUUIDs;
    ui64 Generation = 0;

public:
    explicit TAcquireDevicesCommand(IStorageNodePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("device-uuid", "device to acquire (may be repeated)")
            .RequiredArgument("STR")
            .AppendTo(&DeviceUUIDs);

        Opts.AddLongOption("generation", "writer generation")
            .RequiredArgument("NUM")
            .StoreResult(&Generation);
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
        NCloud::NProto::TAcquireDevicesRequest request;
        if (Proto) {
            ParseFromTextFormat(GetInputStream(), request);
        } else {
            for (const auto& uuid: DeviceUUIDs) {
                request.AddDeviceUUIDs(uuid);
            }
            request.SetGeneration(Generation);
        }
        PrepareHeaders(*request.MutableHeaders());

        auto response = Client->AcquireDevices(std::move(request));
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

TCommandPtr NewAcquireDevicesCommand(IStorageNodePtr client)
{
    return std::make_shared<TAcquireDevicesCommand>(std::move(client));
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard::NClient
