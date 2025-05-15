#include "create_volume.h"

#include "volume_manipulation_params.h"

#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/encryption/model/utils.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCreateVolumeCommand final
    : public TCommand
{
private:
    TVolumeId VolumeId;
    TVolumeParams VolumeParams;
    TVolumeModelParams VolumeModelParams;

    ui32 BlockSize = DefaultBlockSize;

    ui32 TabletVersion = 1;

    NCloud::NProto::EStorageMediaKind StorageMediaKind =
        NCloud::NProto::STORAGE_MEDIA_HDD;
    TString StorageMediaKindArg;

    TString BaseDiskId;
    TString BaseDiskCheckpointId;
    TString PlacementGroupId;
    ui32 PlacementPartitionIndex = 0;

    ui32 PartitionsCount = 0;

    NProto::EEncryptionMode EncryptionMode = NProto::NO_ENCRYPTION;
    TString EncryptionKeyPath;
    TString EncryptionKeyHash;

    TString StoragePoolName;
    TVector<TString> AgentIds;
    bool JsonOutput = false;

public:
    TCreateVolumeCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
        , VolumeId(Opts)
        , VolumeParams(Opts)
        , VolumeModelParams(Opts)
    {
        Opts.AddLongOption("block-size", "minimum addressable block size")
            .RequiredArgument("NUM")
            .StoreResult(&BlockSize);

        Opts.AddLongOption("tablet-version", "Tablet version")
            .RequiredArgument("NUM")
            .StoreResult(&TabletVersion);

        Opts.AddLongOption("storage-media-kind")
            .RequiredArgument("STR")
            .StoreResult(&StorageMediaKindArg);

        Opts.AddLongOption("base-disk-id")
            .RequiredArgument("STR")
            .StoreResult(&BaseDiskId);

        Opts.AddLongOption("base-disk-checkpoint-id")
            .RequiredArgument("STR")
            .StoreResult(&BaseDiskCheckpointId);

        Opts.AddLongOption("placement-group-id", "allowed only for nonreplicated disks")
            .RequiredArgument("STR")
            .StoreResult(&PlacementGroupId);

        Opts.AddLongOption(
                "placement-partition-index",
                "allowed only for nonreplicated disks")
            .RequiredArgument("NUM")
            .StoreResult(&PlacementPartitionIndex);

        Opts.AddLongOption("partitions-count", "explicitly specifies the number of partitions")
            .RequiredArgument("NUM")
            .StoreResult(&PartitionsCount);

        Opts.AddLongOption(
                "encryption-mode",
                "encryption mode [no|aes-xts|default|test]")
            .RequiredArgument("STR")
            .Handler1T<TString>([this] (const auto& s) {
                EncryptionMode = EncryptionModeFromString(s);
            });

        Opts.AddLongOption("encryption-key-path", "path to file with encryption key")
            .RequiredArgument("STR")
            .StoreResult(&EncryptionKeyPath);

        Opts.AddLongOption("encryption-key-hash", "key hash for snapshot mode")
            .RequiredArgument("STR")
            .StoreResult(&EncryptionKeyHash);

        Opts.AddLongOption(
                "storage-pool-name",
                "storage pool name (e.g. local-ssd). "
                "Allowed only for nonreplicated disks")
            .RequiredArgument("STR")
            .StoreResult(&StoragePoolName);

        Opts.AddLongOption(
                "agent-id",
                "list of agents where the disk can be allocated. "
                "Allowed only for nonreplicated disks")
            .RequiredArgument("STR")
            .AppendTo(&AgentIds);
        Opts.AddLongOption("json").StoreTrue(&JsonOutput);
    }

protected:
    bool DoExecute() override
    {
        if (!Proto && !CheckOpts()) {
            return false;
        }

        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Reading CreateVolume request");
        auto request = std::make_shared<NProto::TCreateVolumeRequest>();
        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            VolumeId.FillRequest(*request);
            VolumeParams.FillRequest(*request);
            VolumeModelParams.FillRequest(*request);

            request->SetBlockSize(BlockSize);
            request->SetTabletVersion(TabletVersion);

            ParseStorageMediaKind(
                *ParseResultPtr,
                StorageMediaKindArg,
                StorageMediaKind
            );
            request->SetStorageMediaKind(StorageMediaKind);

            request->SetBaseDiskId(BaseDiskId);
            request->SetBaseDiskCheckpointId(BaseDiskCheckpointId);

            request->SetPlacementGroupId(PlacementGroupId);
            request->SetPlacementPartitionIndex(PlacementPartitionIndex);

            request->SetPartitionsCount(PartitionsCount);

            request->SetStoragePoolName(StoragePoolName);
            request->MutableAgentIds()->Assign(
                std::make_move_iterator(AgentIds.begin()),
                std::make_move_iterator(AgentIds.end()));

            request->MutableEncryptionSpec()->CopyFrom(
                CreateEncryptionSpec(
                    EncryptionMode,
                    EncryptionKeyPath,
                    EncryptionKeyHash));
        }

        STORAGE_DEBUG("Sending CreateVolume request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->CreateVolume(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received CreateVolume response");
        if (Proto) {
            SerializeToTextFormat(result, output);
            return true;
        }

        if (JsonOutput){
            // We don't use result.PrintJSON(), because TError.PrintJSON()
            // writes only code, and it is more reliable to use formatted
            // error in tests and scripts.
            NJson::TJsonValue resultJson;
            if (HasError(result)){
                resultJson["Error"] = FormatErrorJson(result.GetError());
            }
            NJson::WriteJson(&output, &resultJson, false, true, true);
            return !HasError(result);
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

        const auto* tabletVersion = ParseResultPtr->FindLongOptParseResult("tablet-version");
        if (tabletVersion && TabletVersion != 1 && TabletVersion != 2) {
            STORAGE_ERROR("Tablet version should be either 1 or 2");
            return false;
        }

        const auto* pgId = ParseResultPtr->FindLongOptParseResult("placement-group-id");
        NCloud::NProto::EStorageMediaKind mediaKind;
        ParseStorageMediaKind(
            *ParseResultPtr,
            StorageMediaKindArg,
            mediaKind
        );
        if (pgId
                && mediaKind != NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED
                && mediaKind != NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED)
        {
            STORAGE_ERROR("Placement group id can be specified only for nonreplicated disks");
            return false;
        }

        return true;
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewCreateVolumeCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TCreateVolumeCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
