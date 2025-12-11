#include "assign_volume.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TAssignVolumeCommand final: public TCommand
{
private:
    TString DiskId;
    TString Token;
    TString InstanceId;
    TString TokenHost;

public:
    TAssignVolumeCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("disk-id", "volume identifier")
            .RequiredArgument("STR")
            .StoreResult(&DiskId);

        Opts.AddLongOption("token", "Mount token")
            .RequiredArgument("STR")
            .StoreResult(&Token);

        Opts.AddLongOption("token-host", "Mount token host")
            .RequiredArgument("STR")
            .StoreResult(&TokenHost);

        Opts.AddLongOption("instance-id", "VM information")
            .RequiredArgument("STR")
            .StoreResult(&InstanceId);
    }

protected:
    bool DoExecute() override
    {
        if (!Proto && !CheckOpts()) {
            return false;
        }

        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Reading AssignVolume request");
        auto request = std::make_shared<NProto::TAssignVolumeRequest>();
        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            request->SetDiskId(DiskId);
            request->SetToken(Token);
            request->SetHost(TokenHost);
            request->SetInstanceId(InstanceId);
        }

        STORAGE_DEBUG("Sending AssignVolume request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->AssignVolume(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received AssignVolume response");
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
        const auto* diskId = ParseResultPtr->FindLongOptParseResult("disk-id");
        if (!diskId) {
            STORAGE_ERROR("Disk id is required");
            return false;
        }

        const auto* token = ParseResultPtr->FindLongOptParseResult("token");
        if (!token) {
            STORAGE_ERROR("Mount token is required");
            return false;
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewAssignVolumeCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TAssignVolumeCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
