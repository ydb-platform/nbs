#include "list_volumes.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TListVolumesCommand final
    : public TCommand
{
private:
    ui32 MaxConcurrency = 0;

public:
    TListVolumesCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption(
                "max-concurrency",
                "max concurrent schemeshard requests (0 = sequential)")
            .OptionalArgument("NUM")
            .StoreResult(&MaxConcurrency);
    }

protected:
    bool DoExecute() override
    {
        auto& output = GetOutputStream();

        auto request = std::make_shared<NProto::TListVolumesRequest>();
        if (MaxConcurrency > 0) {
            request->SetMaxConcurrency(MaxConcurrency);
        }

        STORAGE_DEBUG("Sending ListVolumes request");
        auto result = WaitFor(ClientEndpoint->ListVolumes(
            MakeIntrusive<TCallContext>(),
            std::move(request)));

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

} // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewListVolumesCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TListVolumesCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
