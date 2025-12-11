#include "create_placement_group.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/storage/model/public.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCreatePlacementGroupCommand final: public TCommand
{
private:
    TString GroupId;
    NProto::EPlacementStrategy PlacementStrategy =
        NProto::PLACEMENT_STRATEGY_SPREAD;
    ui32 PlacementPartitionCount = 0;

public:
    TCreatePlacementGroupCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("group-id", "Group identifier")
            .RequiredArgument("STR")
            .StoreResult(&GroupId);

        Opts.AddLongOption("placement-strategy", "Placement strategy")
            .RequiredArgument("{spread, partition}")
            .DefaultValue("spread")
            .Handler1T<TString>(
                [this](const TString& s)
                {
                    if (s == "spread") {
                        PlacementStrategy = NProto::PLACEMENT_STRATEGY_SPREAD;
                    } else if (s == "partition") {
                        PlacementStrategy =
                            NProto::PLACEMENT_STRATEGY_PARTITION;
                    } else {
                        ythrow yexception()
                            << "unknown placement strategy: " << s.Quote();
                    }
                });

        Opts.AddLongOption("partition-count", "Placement partitions count")
            .RequiredArgument("NUM")
            .StoreResult(&PlacementPartitionCount);
    }

protected:
    bool DoExecute() override
    {
        if (!Proto && !CheckOpts()) {
            return false;
        }

        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Reading CreatePlacementGroup request");
        auto request = std::make_shared<NProto::TCreatePlacementGroupRequest>();
        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            request->SetGroupId(GroupId);
            request->SetPlacementStrategy(PlacementStrategy);
            request->SetPlacementPartitionCount(PlacementPartitionCount);
        }

        STORAGE_DEBUG("Sending CreatePlacementGroup request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->CreatePlacementGroup(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received CreatePlacementGroup response");
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

        switch (PlacementStrategy) {
            case NProto::PLACEMENT_STRATEGY_SPREAD: {
                if (PlacementPartitionCount != 0) {
                    STORAGE_ERROR(
                        "For spread placement strategy the number and names of "
                        "partitions must not be specified.");
                    return false;
                }
                break;
            }

            case NProto::PLACEMENT_STRATEGY_PARTITION: {
                if (PlacementPartitionCount < 2) {
                    STORAGE_ERROR(
                        "For partition placement strategy the number of "
                        "partitions must be specified."
                        " The number of partitions should be more than one.");
                    return false;
                }
                break;
            }

            default: {
                STORAGE_ERROR("Unknown placement strategy.");
                return false;
            }
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewCreatePlacementGroupCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TCreatePlacementGroupCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
