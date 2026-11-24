#include "command.h"

#include "performance_profile_params.h"

#include "library/cpp/json/json_writer.h"

#include <cloud/filestore/libs/service/mediakind.h>
#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <util/generic/size_literals.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCreateCommand final
    : public TFileStoreCommand
{
private:
    const TPerformanceProfileParams PerformanceProfileParams;

    TString CloudId;
    TString FolderId;
    ui32 BlockSize = 4_KB;
    ui64 BlocksCount = 0;
    NCloud::NProto::EStorageMediaKind StorageMediaKind =
        NCloud::NProto::STORAGE_MEDIA_HDD;

    TString StorageMediaKindArg;
    ui32 ShardCount = 0;

    TString FastShardStorageKindArg;
    ui32 FastShardCount = 0;

public:
    TCreateCommand()
        : PerformanceProfileParams(Opts)
    {
        Opts.AddLongOption("cloud")
            .Required()
            .RequiredArgument("STR")
            .StoreResult(&CloudId);

        Opts.AddLongOption("folder")
            .Required()
            .RequiredArgument("STR")
            .StoreResult(&FolderId);

        Opts.AddLongOption("block-size")
            .RequiredArgument("NUM")
            .StoreResult(&BlockSize);

        Opts.AddLongOption("blocks-count")
            .Required()
            .RequiredArgument("NUM")
            .StoreResult(&BlocksCount);

        Opts.AddLongOption("storage-media-kind")
            .RequiredArgument("STR")
            .StoreResult(&StorageMediaKindArg);

        Opts.AddLongOption("shard-count")
            .RequiredArgument("NUM")
            .Help("explicitly specifies the required shard count")
            .StoreResult(&ShardCount);

        Opts.AddLongOption("fast-shard-storage-kind")
            .RequiredArgument("STR")
            .Help("'mirror' or 'quorum_mirror', enables fast shards")
            .StoreResult(&FastShardStorageKindArg);

        Opts.AddLongOption("fast-shard-count")
            .RequiredArgument("NUM")
            .Help("explicitly specifies the required fast shard count")
            .StoreResult(&FastShardCount);
    }

    bool Execute() override
    {
        auto callContext = PrepareCallContext();

        auto request = std::make_shared<NProto::TCreateFileStoreRequest>();
        request->SetFileSystemId(FileSystemId);
        request->SetCloudId(CloudId);
        request->SetFolderId(FolderId);
        request->SetBlockSize(BlockSize);
        request->SetBlocksCount(BlocksCount);

        if (StorageMediaKindArg) {
            const auto mediaKind = ParseFileStoreMediaKind(StorageMediaKindArg);
            if (HasError(mediaKind)) {
                STORAGE_THROW_SERVICE_ERROR(mediaKind.GetError());
            }

            StorageMediaKind = mediaKind.GetResult();
        }

        request->SetStorageMediaKind(StorageMediaKind);
        request->SetShardCount(ShardCount);
        request->SetFastShardCount(FastShardCount);

        if (FastShardStorageKindArg) {
            const auto storageKind =
                ParseFastShardStorageKind(FastShardStorageKindArg);
            if (HasError(storageKind)) {
                STORAGE_THROW_SERVICE_ERROR(storageKind.GetError());
            }

            request->SetFastShardStorageKind(storageKind.GetResult());
        }

        PerformanceProfileParams.FillRequest(*request);

        auto response = WaitFor(
            Client->CreateFileStore(
                std::move(callContext),
                std::move(request)));

        if (JsonOutput){
            // We don't use result.PrintJSON(), because TError.PrintJSON()
            // writes only code, and it is more reliable to use formatted
            // error in tests and scripts.
            NJson::TJsonValue resultJson;
            if (HasError(response)){
                resultJson["Error"] = FormatErrorJson(response.GetError());
            }

            NJson::WriteJson(&Cout, &resultJson, false, true, true);

            if (HasError(response)){
                ProgramShouldContinue.ShouldStop(1);
                return false;
            }

            return true;
        }

        if (HasError(response)) {
            STORAGE_THROW_SERVICE_ERROR(response.GetError());
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewCreateCommand()
{
    return std::make_shared<TCreateCommand>();
}

}   // namespace NCloud::NFileStore::NClient
