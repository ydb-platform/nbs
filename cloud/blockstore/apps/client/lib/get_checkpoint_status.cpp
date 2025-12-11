#include "get_checkpoint_status.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TGetCheckpointStatusCommand final: public TCommand
{
private:
    TString DiskId;
    TString CheckpointId;

public:
    TGetCheckpointStatusCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("disk-id", "volume identifier")
            .RequiredArgument("STR")
            .StoreResult(&DiskId);

        Opts.AddLongOption("checkpoint-id", "Checkpoint identifier")
            .RequiredArgument("STR")
            .StoreResult(&CheckpointId);
    }

protected:
    bool DoExecute() override
    {
        if (!Proto && !CheckOpts()) {
            return false;
        }

        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Reading GetCheckpointStatus request");
        auto request = std::make_shared<NProto::TGetCheckpointStatusRequest>();
        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            request->SetDiskId(DiskId);
            request->SetCheckpointId(CheckpointId);
        }

        STORAGE_DEBUG("Sending GetCheckpointStatus request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->GetCheckpointStatus(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received GetCheckpointStatus response");
        if (Proto) {
            SerializeToTextFormat(result, output);
            return true;
        }

        if (HasError(result)) {
            output << FormatError(result.GetError()) << Endl;
            return false;
        }

        return true;
    }

private:
    bool CheckOpts() const
    {
        const auto* diskId = ParseResultPtr->FindLongOptParseResult("disk-id");
        if (!diskId) {
            STORAGE_ERROR("Disk id is required");
            return false;
        }

        const auto* checkpointId =
            ParseResultPtr->FindLongOptParseResult("checkpoint-id");
        if (!checkpointId) {
            STORAGE_ERROR("Checkpoint id is required");
            return false;
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewGetCheckpointStatusCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TGetCheckpointStatusCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
