#include "acquire_devices.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>

namespace NCloud::NFastShard::NClient {

using namespace NLastGetopt;

namespace {

////////////////////////////////////////////////////////////////////////////////

std::optional<NProto::EAccessMode> ParseAccessMode(const TString& mode)
{
    if (mode == "rw") {
        return NProto::ACCESS_READ_WRITE;
    }
    if (mode == "ro") {
        return NProto::ACCESS_READ_ONLY;
    }
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

class TAcquireDevicesCommand final: public TCommand
{
private:
    TVector<TString> DeviceUUIDs;
    ui32 Generation = 0;
    TString AccessMode;
    TString FastshardId;

public:
    explicit TAcquireDevicesCommand(IStorageNodePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("device-uuid", "device to acquire (may be repeated)")
            .RequiredArgument("STR")
            .AppendTo(&DeviceUUIDs);

        Opts.AddLongOption(
                "generation",
                "client generation; a request with a lower generation than "
                "the last one seen from the client is rejected")
            .RequiredArgument("NUM")
            .StoreResult(&Generation);

        Opts.AddLongOption("access-mode", "access mode: rw or ro")
            .RequiredArgument("STR")
            .DefaultValue("rw")
            .StoreResult(&AccessMode);

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
        if (!ParseAccessMode(AccessMode)) {
            ythrow TUsageException()
                << "unknown access mode: " << AccessMode.Quote();
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
            request.SetAccessMode(*ParseAccessMode(AccessMode));
            request.SetFastshardId(FastshardId);
        }
        PrepareHeaders(*request.MutableHeaders());

        auto response = Call(&IStorageNode::AcquireDevices, std::move(request));
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

}   // namespace NCloud::NFastShard::NClient
