#include "list_placement_groups.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TListPlacementGroupsCommand final: public TCommand
{
public:
    TListPlacementGroupsCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {}

protected:
    bool DoExecute() override
    {
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Sending ListPlacementGroups request");
        auto result = WaitFor(ClientEndpoint->ListPlacementGroups(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TListPlacementGroupsRequest>()));

        STORAGE_DEBUG("Received ListPlacementGroups response");
        if (Proto) {
            SerializeToTextFormat(result, output);
            return true;
        }

        if (HasError(result)) {
            output << FormatError(result.GetError()) << Endl;
            return false;
        }

        for (const auto& groupId: result.GetGroupIds()) {
            output << groupId << Endl;
        }
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewListPlacementGroupsCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TListPlacementGroupsCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
