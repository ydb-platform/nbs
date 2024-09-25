#include "command.h"

#include "performance_profile_params.h"

#include "library/cpp/json/json_writer.h"

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

        if (StorageMediaKindArg == "ssd") {
            StorageMediaKind = NCloud::NProto::STORAGE_MEDIA_SSD;
        } else if (StorageMediaKindArg == "hdd") {
            StorageMediaKind = NCloud::NProto::STORAGE_MEDIA_HDD;
        } else if (StorageMediaKindArg == "hybrid") {
            StorageMediaKind = NCloud::NProto::STORAGE_MEDIA_HYBRID;
        } else if (StorageMediaKindArg) {
            ythrow yexception() << "invalid storage media kind: "
                << StorageMediaKindArg
                << ", should be one of 'ssd', 'hdd', 'hybrid'";
        }

        request->SetStorageMediaKind(StorageMediaKind);

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
            return !HasError(response);
        }
        if (HasError(response)) {
            ythrow TServiceError(response.GetError());
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
