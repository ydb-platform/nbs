#include "update_volume_throttling_config.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TUpdateVolumeThrottlingConfigCommand final: public TCommand
{
public:
    explicit TUpdateVolumeThrottlingConfigCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {}

protected:
    bool DoExecute() override
    {
        if (!CheckOpts()) {
            return false;
        }

        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Reading TVolumeThrottlingConfig");
        auto request =
            std::make_shared<NProto::TUpdateVolumeThrottlingConfigRequest>();
        {
            NProto::TVolumeThrottlingConfig config;
            ParseFromTextFormat(input, config);
            *request->MutableConfig() = std::move(config);
        }

        STORAGE_DEBUG("Updating TVolumeThrottlingConfig");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->UpdateVolumeThrottlingConfig(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Updated TVolumeThrottlingConfig");
        SerializeToTextFormat(result, output);

        if (HasError(result)) {
            STORAGE_ERROR(FormatError(result.GetError()));
            return false;
        }

        return true;
    }

private:
    bool CheckOpts() const
    {
        if (!Proto) {
            STORAGE_ERROR("Only proto input format is supported");
            return false;
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewUpdateVolumeThrottlingConfigCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TUpdateVolumeThrottlingConfigCommand>(
        std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
