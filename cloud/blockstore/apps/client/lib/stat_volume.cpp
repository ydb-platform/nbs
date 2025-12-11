#include "stat_volume.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TStatVolumeCommand final: public TCommand
{
private:
    TString DiskId;
    ui32 Flags = 0;
    TVector<TString> StorageConfigFields;

public:
    TStatVolumeCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("disk-id", "volume identifier")
            .RequiredArgument("STR")
            .StoreResult(&DiskId);

        Opts.AddLongOption("flags").RequiredArgument("NUM").StoreResult(&Flags);

        Opts.AddLongOption(
                "storage-config-fields",
                "Get overridden value of these StorageConfig's fields")
            .RequiredArgument("STR")
            .AppendTo(&StorageConfigFields);
    }

protected:
    bool DoExecute() override
    {
        if (!Proto && !CheckOpts()) {
            return false;
        }

        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Reading StatVolume request");
        auto request = std::make_shared<NProto::TStatVolumeRequest>();
        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            request->SetDiskId(DiskId);
            request->SetFlags(Flags);
            for (auto& field: StorageConfigFields) {
                *request->AddStorageConfigFields() = std::move(field);
            }
        }

        STORAGE_DEBUG("Sending StatVolume request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->StatVolume(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received StatVolume response");
        if (!Proto && HasError(result)) {
            output << FormatError(result.GetError()) << Endl;
            return false;
        }

        SerializeToTextFormat(result, output);
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

TCommandPtr NewStatVolumeCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TStatVolumeCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
