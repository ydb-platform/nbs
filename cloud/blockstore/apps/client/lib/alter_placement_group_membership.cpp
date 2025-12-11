#include "alter_placement_group_membership.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TAlterPlacementGroupMembershipCommand final: public TCommand
{
private:
    TString GroupId;
    ui32 PlacementPartitionIndex = 0;
    ui32 ConfigVersion = 0;
    TVector<TString> DisksToAdd;
    TVector<TString> DisksToRemove;

public:
    TAlterPlacementGroupMembershipCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("group-id", "Group identifier")
            .RequiredArgument("STR")
            .StoreResult(&GroupId);

        Opts.AddLongOption(
                "placement-partition-index",
                "Placement partition index")
            .RequiredArgument("NUM")
            .StoreResult(&PlacementPartitionIndex);

        Opts.AddLongOption("config-version", "Volume config version")
            .RequiredArgument("NUM")
            .StoreResult(&ConfigVersion);

        Opts.AddLongOption(
                "disk-to-add",
                "Add this disk to the group (several disks can be added at a "
                "time)")
            .RequiredArgument("STR")
            .AppendTo(&DisksToAdd);

        Opts.AddLongOption(
                "disk-to-remove",
                "Remove this disk from the group (several disks can be removed "
                "at a time)")
            .RequiredArgument("STR")
            .AppendTo(&DisksToRemove);
    }

protected:
    bool DoExecute() override
    {
        if (!Proto && !CheckOpts()) {
            return false;
        }

        auto& input = GetInputStream();
        auto& output = GetOutputStream();
        auto& error = GetErrorStream();

        STORAGE_DEBUG("Reading AlterPlacementGroupMembership request");
        auto request =
            std::make_shared<NProto::TAlterPlacementGroupMembershipRequest>();
        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            request->SetGroupId(GroupId);
            request->SetPlacementPartitionIndex(PlacementPartitionIndex);
            request->SetConfigVersion(ConfigVersion);

            for (auto& diskId: DisksToAdd) {
                *request->AddDisksToAdd() = std::move(diskId);
            }

            for (auto& diskId: DisksToRemove) {
                *request->AddDisksToRemove() = std::move(diskId);
            }
        }

        STORAGE_DEBUG("Sending AlterPlacementGroupMembership request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->AlterPlacementGroupMembership(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received AlterPlacementGroupMembership response");
        if (Proto) {
            SerializeToTextFormat(result, output);
            return true;
        }

        if (HasError(result)) {
            output << FormatError(result.GetError()) << Endl;
            if (result.DisksImpossibleToAddSize()) {
                error << "Failed to add the following disks:";
                for (const auto& diskId: result.GetDisksImpossibleToAdd()) {
                    error << " " << diskId;
                }
                error << Endl;
            }
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

        if (!DisksToAdd && !DisksToRemove) {
            STORAGE_ERROR("Some disks to add/remove should be supplied");
            return false;
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewAlterPlacementGroupMembershipCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TAlterPlacementGroupMembershipCommand>(
        std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
