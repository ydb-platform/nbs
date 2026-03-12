#include "describe_volume.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDescribeVolumeCommand final
    : public TCommand
{
private:
    TString DiskId;
    bool ExactDiskIdMatch = false;

public:
    explicit TDescribeVolumeCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("disk-id", "volume identifier")
            .RequiredArgument("STR")
            .StoreResult(&DiskId);

        Opts.AddLongOption(
                "exact-disk-id-match",
                "Requires an exact match of the DiskId.  When set it does not "
                "allow to find copied disk with mangled name.")
            .StoreTrue(&ExactDiskIdMatch);
    }

protected:
    bool DoExecute() override
    {
        if (!Proto && !CheckOpts()) {
            return false;
        }

        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Reading DescribeVolume request");
        auto request = std::make_shared<NProto::TDescribeVolumeRequest>();
        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            request->SetDiskId(DiskId);
            request->MutableHeaders()->SetExactDiskIdMatch(ExactDiskIdMatch);
        }

        STORAGE_DEBUG("Sending DescribeVolume request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->DescribeVolume(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received DescribeVolume response");
        if (Proto) {
            SerializeToTextFormat(result, output);
            return true;
        }

        if (HasError(result)) {
            output << FormatError(result.GetError()) << Endl;
            return false;
        }

        output << "Volume: " << Endl;
        SerializeToTextFormat(result.GetVolume(), output);
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

        return true;
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewDescribeVolumeCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TDescribeVolumeCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
