#include "create_volume_from_device.h"

#include "volume_manipulation_params.h"

#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCreateVolumeFromDeviceCommand final: public TCommand
{
private:
    TVolumeId VolumeId;
    TVolumeParams VolumeParams;

    TString AgentId;
    TString Path;

public:
    TCreateVolumeFromDeviceCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
        , VolumeId(Opts)
        , VolumeParams(Opts)
    {
        Opts.AddLongOption(
                "agent-id",
                "FQDN of the agent on which the device is located.")
            .RequiredArgument("STR")
            .StoreResult(&AgentId);

        Opts.AddLongOption(
                "path",
                "path to the device on the specified agent "
                "(e.g. /dev/disk/by-partlabel/COMPUTENVME01).")
            .RequiredArgument("STR")
            .StoreResult(&Path);
    }

protected:
    bool DoExecute() override
    {
        if (!Proto && !CheckOpts()) {
            return false;
        }

        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Reading CreateVolumeFromDevice request");
        auto request =
            std::make_shared<NProto::TCreateVolumeFromDeviceRequest>();
        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            VolumeId.FillRequest(*request);
            VolumeParams.FillRequest(*request);

            request->SetAgentId(AgentId);
            request->SetPath(Path);
        }

        STORAGE_DEBUG("Sending CreateVolumeFromDevice request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->CreateVolumeFromDevice(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received CreateVolumeFromDevice response");
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

private:
    bool CheckOpts() const
    {
        const auto* diskId = ParseResultPtr->FindLongOptParseResult("disk-id");
        if (!diskId) {
            STORAGE_ERROR("Disk id is required");
            return false;
        }

        const auto* path = ParseResultPtr->FindLongOptParseResult("path");
        if (!path) {
            STORAGE_ERROR("Path is required");
            return false;
        }

        const auto* agentId =
            ParseResultPtr->FindLongOptParseResult("agent-id");
        if (!agentId) {
            STORAGE_ERROR("Agent id is required");
            return false;
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewCreateVolumeFromDeviceCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TCreateVolumeFromDeviceCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
