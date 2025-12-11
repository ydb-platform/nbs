#include "list_volumes.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TListVolumesCommand final: public TCommand
{
public:
    TListVolumesCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {}

protected:
    bool DoExecute() override
    {
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Sending ListVolumes request");
        auto result = WaitFor(ClientEndpoint->ListVolumes(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TListVolumesRequest>()));

        STORAGE_DEBUG("Received ListVolumes response");
        if (Proto) {
            SerializeToTextFormat(result, output);
            return true;
        }

        if (HasError(result)) {
            output << FormatError(result.GetError()) << Endl;
            return false;
        }

        auto volumes = result.GetVolumes();
        Sort(volumes);
        for (const auto& volume: volumes) {
            output << volume << Endl;
        }
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewListVolumesCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TListVolumesCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
