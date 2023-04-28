#include "update_disk_registry_config.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/public/api/protos/disk.pb.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TUpdateDiskRegistryConfigCommand final
    : public TCommand
{
public:
    explicit TUpdateDiskRegistryConfigCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        // TODO: add options
    }

protected:
    bool DoExecute() override
    {
        if (!CheckOpts()) {
            return false;
        }

        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Reading UpdateDiskRegistryConfig request");
        auto request = std::make_shared<NProto::TUpdateDiskRegistryConfigRequest>();

        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            // TODO
        }

        STORAGE_DEBUG("Sending UpdateDiskRegistryConfig request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->UpdateDiskRegistryConfig(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received UpdateDiskRegistryConfig response");
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
        // TODO

        if (!Proto) {
            return false;
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewUpdateDiskRegistryConfigCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TUpdateDiskRegistryConfigCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
