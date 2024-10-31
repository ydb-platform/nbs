#include "alter_volume.h"

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

class TAlterVolumeCommand final
    : public TCommand
{
private:
    TVolumeId VolumeId;
    TVolumeParams VolumeParams;
    ui32 ConfigVersion = 0;

public:
    TAlterVolumeCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
        , VolumeId(Opts)
        , VolumeParams(Opts)
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

        STORAGE_DEBUG("Reading AlterVolume request");
        auto request = std::make_shared<NProto::TAlterVolumeRequest>();
        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            VolumeId.FillRequest(*request);
            VolumeParams.FillRequest(*request);
            request->SetConfigVersion(ConfigVersion);
        }

        STORAGE_DEBUG("Sending AlterVolume request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->AlterVolume(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received AlterVolume response");
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
        if (!VolumeId) {
            STORAGE_ERROR("Disk id is required");
            return false;
        }

        if (!ConfigVersion) {
            STORAGE_ERROR("Config version is required");
            return false;
        }

        return true;
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewAlterVolumeCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TAlterVolumeCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
