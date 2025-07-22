#include "update_throttling_config.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TUpdateThrottlingConfigCommand final: public TCommand
{
public:
    explicit TUpdateThrottlingConfigCommand(IBlockStorePtr client)
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

        STORAGE_DEBUG("Reading TThrottlingConfig");
        auto request =
            std::make_shared<NProto::TUpdateThrottlingConfigRequest>();

        {
            NProto::TThrottlingConfig config;
            ParseFromTextFormat(input, config);
            *request->MutableConfig() = std::move(config);
        }

        STORAGE_DEBUG("Sending TUpdateThrottlingConfigRequest");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->UpdateThrottlingConfig(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received UpdateThrottlingConfig response");
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

TCommandPtr NewUpdateThrottlingConfigCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TUpdateThrottlingConfigCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
