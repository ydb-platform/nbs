#include "create_volume_link.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>


namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCreateVolumeLinkCommand final: public TCommand
{
private:
    TString SourceDiskId;
    TString TargetDiskId;

public:
    explicit TCreateVolumeLinkCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("source-disk-id", "source volume identifier")
            .RequiredArgument("STR")
            .StoreResult(&SourceDiskId);

        Opts.AddLongOption("target-disk-id", "target volume identifier")
            .RequiredArgument("STR")
            .StoreResult(&TargetDiskId);
    }

protected:
    bool DoExecute() override
    {
        if (!Proto && !CheckOpts()) {
            return false;
        }

        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Creating CreateVolumeLink request");
        auto request = std::make_shared<NProto::TCreateVolumeLinkRequest>();
        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            request->SetSourceDiskId(SourceDiskId);
            request->SetTargetDiskId(TargetDiskId);
        }

        STORAGE_DEBUG("Sending CreateVolumeLink request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->CreateVolumeLink(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received CreateVolumeLink response");
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
        const auto* diskId =
            ParseResultPtr->FindLongOptParseResult("source-disk-id");
        if (!diskId) {
            STORAGE_ERROR("Source Disk id is required");
            return false;
        }

        const auto* checkpointId =
            ParseResultPtr->FindLongOptParseResult("target-disk-id");
        if (!checkpointId) {
            STORAGE_ERROR("Target Disk id is required");
            return false;
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewCreateVolumeLinkCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TCreateVolumeLinkCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
