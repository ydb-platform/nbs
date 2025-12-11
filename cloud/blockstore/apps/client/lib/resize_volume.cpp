#include "resize_volume.h"

#include "volume_manipulation_params.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TResizeVolumeCommand final: public TCommand
{
private:
    TVolumeId VolumeId;
    TVolumeModelParams VolumeModelParams;

    ui32 ConfigVersion = 0;

public:
    TResizeVolumeCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
        , VolumeId(Opts)
        , VolumeModelParams(Opts)
    {
        Opts.AddLongOption("config-version", "Volume config version")
            .RequiredArgument("NUM")
            .StoreResult(&ConfigVersion);
    }

protected:
    bool DoExecute() override
    {
        if (!Proto && !CheckOpts()) {
            return false;
        }

        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Reading ResizeVolume request");
        auto request = std::make_shared<NProto::TResizeVolumeRequest>();
        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            VolumeId.FillRequest(*request);
            VolumeModelParams.FillRequest(*request);
            request->SetConfigVersion(ConfigVersion);
        }

        STORAGE_DEBUG("Sending ResizeVolume request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->ResizeVolume(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received ResizeVolume response");
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

        const auto* blocksCount =
            ParseResultPtr->FindLongOptParseResult("blocks-count");
        if (!blocksCount) {
            STORAGE_ERROR("Blocks count is required");
            return false;
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewResizeVolumeCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TResizeVolumeCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
