#include "destroy_placement_group.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDestroyPlacementGroupCommand final: public TCommand
{
private:
    TString GroupId;

public:
    TDestroyPlacementGroupCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("group-id", "group identifier")
            .RequiredArgument("STR")
            .StoreResult(&GroupId);
    }

protected:
    bool DoExecute() override
    {
        if (!Proto && !CheckOpts()) {
            return false;
        }

        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Reading DestroyPlacementGroup request");
        auto request =
            std::make_shared<NProto::TDestroyPlacementGroupRequest>();
        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            request->SetGroupId(GroupId);
        }

        STORAGE_DEBUG("Sending DestroyPlacementGroup request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->DestroyPlacementGroup(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received DestroyPlacementGroup response");
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
        const auto* groupId =
            ParseResultPtr->FindLongOptParseResult("group-id");
        if (!groupId) {
            STORAGE_ERROR("Group id is required");
            return false;
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewDestroyPlacementGroupCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TDestroyPlacementGroupCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
