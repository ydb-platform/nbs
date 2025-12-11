#include "destroy_volume.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDestroyVolumeCommand final: public TCommand
{
private:
    TString DiskId;
    bool Sync = false;

public:
    TDestroyVolumeCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("disk-id", "volume identifier")
            .RequiredArgument("STR")
            .StoreResult(&DiskId);

        Opts.AddLongOption("sync", "synchronous deallocation")
            .NoArgument()
            .StoreTrue(&Sync);
    }

protected:
    bool DoExecute() override
    {
        if (!Proto && !CheckOpts()) {
            return false;
        }

        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Reading DestroyVolume request");
        auto request = std::make_shared<NProto::TDestroyVolumeRequest>();
        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            request->SetDiskId(DiskId);
            request->SetSync(Sync);

            STORAGE_WARN("Waiting for confirmation");
            output << "Confirm disk destruction by typing disk id to stdin"
                   << Endl;
            TString confirmation;
            input >> confirmation;
            if (confirmation != DiskId) {
                STORAGE_ERROR(
                    "Confirmation failed: %s != %s",
                    confirmation.c_str(),
                    DiskId.c_str());

                return false;
            }
        }

        STORAGE_DEBUG("Sending DestroyVolume request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->DestroyVolume(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received DestroyVolume response");
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

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewDestroyVolumeCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TDestroyVolumeCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
