#include "resize_device.h"


#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TResizeDeviceCommand final
    : public TCommand
{
private:
    TString UnixSocketPath;
    ui64 DeviceSizeInBytes = 0;

public:
    TResizeDeviceCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("socket", "unix socket path")
            .RequiredArgument("STR")
            .Required()
            .StoreResult(&UnixSocketPath);

        Opts.AddLongOption("device-size", "Device size in bytes")
            .RequiredArgument("NUM")
            .Required()
            .StoreResult(&DeviceSizeInBytes);
    }

protected:
    bool DoExecute() override
    {
        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Reading ResizeDevice request");
        auto request = std::make_shared<NProto::TResizeDeviceRequest>();
         if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            request->SetUnixSocketPath(UnixSocketPath);
            request->SetDeviceSizeInBytes(DeviceSizeInBytes);
        }

        STORAGE_DEBUG("Sending ResizeDevice request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->ResizeDevice(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received ResizeDevice response");
        if (Proto) {
            SerializeToTextFormat(result, output);
            return true;
        }

        if (HasError(result)) {
            output << FormatError(result.GetError()) << Endl;
            return false;
        }

        output << "OK" << Endl;
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewResizeDeviceCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TResizeDeviceCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
