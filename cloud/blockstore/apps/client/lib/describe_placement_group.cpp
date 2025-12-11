#include "describe_placement_group.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDescribePlacementGroupCommand final: public TCommand
{
private:
    TString GroupId;

public:
    TDescribePlacementGroupCommand(IBlockStorePtr client)
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

        STORAGE_DEBUG("Reading DescribePlacementGroup request");
        auto request =
            std::make_shared<NProto::TDescribePlacementGroupRequest>();
        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            request->SetGroupId(GroupId);
        }

        STORAGE_DEBUG("Sending DescribePlacementGroup request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->DescribePlacementGroup(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received DescribePlacementGroup response");
        if (Proto) {
            SerializeToTextFormat(result, output);
            return true;
        }

        if (HasError(result)) {
            output << FormatError(result.GetError()) << Endl;
            return false;
        }

        output << "PlacementGroup: " << Endl;
        SerializeToTextFormat(result.GetGroup(), output);
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

TCommandPtr NewDescribePlacementGroupCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TDescribePlacementGroupCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
