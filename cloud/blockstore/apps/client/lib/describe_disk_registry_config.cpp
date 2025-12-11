#include "describe_disk_registry_config.h"

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

class TDescribeDiskRegistryConfigCommand final: public TCommand
{
public:
    explicit TDescribeDiskRegistryConfigCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {}

protected:
    bool DoExecute() override
    {
        if (!CheckOpts()) {
            return false;
        }

        auto& output = GetOutputStream();

        auto request =
            std::make_shared<NProto::TDescribeDiskRegistryConfigRequest>();

        STORAGE_DEBUG("Sending DescribeDiskRegistryConfig request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->DescribeDiskRegistryConfig(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received DescribeDiskRegistryConfig response");
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
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewDescribeDiskRegistryConfigCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TDescribeDiskRegistryConfigCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
