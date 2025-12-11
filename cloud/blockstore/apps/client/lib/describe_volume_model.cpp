#include "describe_volume_model.h"

#include "volume_manipulation_params.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDescribeVolumeModelCommand final: public TCommand
{
private:
    ui64 BlocksCount = 0;
    ui32 BlockSize = DefaultBlockSize;

    ui32 TabletVersion = 1;

    NCloud::NProto::EStorageMediaKind StorageMediaKind =
        NCloud::NProto::STORAGE_MEDIA_HDD;
    TString StorageMediaKindArg;

public:
    TDescribeVolumeModelCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption(
                "blocks-count",
                "maximum number of blocks stored in volume")
            .RequiredArgument("NUM")
            .StoreResult(&BlocksCount);

        Opts.AddLongOption("block-size", "minimum addressable block size")
            .RequiredArgument("NUM")
            .StoreResult(&BlockSize);

        Opts.AddLongOption("tablet-version", "Tablet version")
            .RequiredArgument("NUM")
            .StoreResult(&TabletVersion);

        Opts.AddLongOption("storage-media-kind")
            .RequiredArgument("STR")
            .StoreResult(&StorageMediaKindArg);
    }

protected:
    bool DoExecute() override
    {
        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Reading DescribeVolumeModel request");
        auto request = std::make_shared<NProto::TDescribeVolumeModelRequest>();
        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            request->SetBlocksCount(BlocksCount);
            request->SetBlockSize(BlockSize);
            request->SetTabletVersion(TabletVersion);

            ParseStorageMediaKind(
                *ParseResultPtr,
                StorageMediaKindArg,
                StorageMediaKind);
            request->SetStorageMediaKind(StorageMediaKind);
        }

        STORAGE_DEBUG("Sending DescribeVolumeModel request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->DescribeVolumeModel(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received DescribeVolumeModel response");
        if (Proto) {
            SerializeToTextFormat(result, output);
            return true;
        }

        if (HasError(result)) {
            output << FormatError(result.GetError()) << Endl;
            return false;
        }

        output << "VolumeModel: " << Endl;
        SerializeToTextFormat(result.GetVolumeModel(), output);
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewDescribeVolumeModelCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TDescribeVolumeModelCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
