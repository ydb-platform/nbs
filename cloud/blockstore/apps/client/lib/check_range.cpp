#include "check_range.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/json/easy_parse/json_easy_parser.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/folder/path.h>
#include <util/stream/file.h>

#include <utility>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 DefaultBlocksPerRequest = 1024;

NProto::TError ExtractStatusValues(const TString& jsonStr)
{
    NJson::TJsonValue json;
    if (!NJson::ReadJsonTree(jsonStr, &json)) {
        return MakeError(E_ARGUMENT, "JSON parsing error");
    }
    NJson::TJsonValue code;
    if (json.GetValueByPath("Status.Code", code) && !code.IsUInteger()) {
        return MakeError(E_ARGUMENT, "Status.Code parsing error");
    }
    NJson::TJsonValue message;
    if (json.GetValueByPath("Status.Message", message) && !message.IsString()) {
        return MakeError(E_ARGUMENT, "Status.Message parsing error");
    }

    return MakeError(code.GetUIntegerSafe(S_OK), message.GetString());
}

struct TRequestBuilder
{
    ui32 StartIndex = 0;
    ui32 RemainingBlocks = 0;
    ui32 BlocksPerRequest = 0;

    TRequestBuilder() = default;

    TRequestBuilder(
        ui32 startIndex,
        ui32 remainingBlocks,
        ui32 blocksPerRequest)
        : StartIndex(startIndex)
        , RemainingBlocks(remainingBlocks)
        , BlocksPerRequest(blocksPerRequest)
    {}

    std::optional<TBlockRange64> Next()
    {
        if (RemainingBlocks <= 0) {
            return std::nullopt;
        }

        ui32 blocksInThisRequest = std::min(RemainingBlocks, BlocksPerRequest);

        TBlockRange64 range = TBlockRange64::MakeHalfOpenInterval(
            StartIndex,
            StartIndex + blocksInThisRequest);

        RemainingBlocks -= blocksInThisRequest;
        StartIndex += blocksInThisRequest;

        return range;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCheckRangeCommand final: public TCommand
{
private:
    TString DiskId;
    ui64 StartIndex = 0;
    ui64 BlocksCount = 0;
    ui64 BlocksPerRequest = 0;
    bool ShowReadErrorsEnabled = false;
    bool SaveResultsEnabled = false;
    bool CalculateChecksums = false;
    TString FolderPostfix;

public:
    TCheckRangeCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("disk-id", "volume identifier")
            .RequiredArgument("STR")
            .StoreResult(&DiskId);

        Opts.AddLongOption("start-index", "start block index")
            .RequiredArgument("NUM")
            .StoreResult(&StartIndex);

        Opts.AddLongOption("blocks-count", "number of blocks to check")
            .RequiredArgument("NUM")
            .StoreResult(&BlocksCount);

        Opts.AddLongOption("blocks-per-request", "blocks per request")
            .RequiredArgument("NUM")
            .StoreResultDef(&BlocksPerRequest, DefaultBlocksPerRequest);

        Opts.AddLongOption("calculate-checksums", "calulate checksums")
            .NoArgument()
            .StoreTrue(&CalculateChecksums);

        Opts.AddLongOption(
                "show-read-errors",
                "show logs for the intervals where errors occurred")
            .NoArgument()
            .StoreTrue(&ShowReadErrorsEnabled);

        Opts.AddLongOption(
                "save-results",
                "saving result of checkRange operations to the folder "
                "'./checkRange_$disk-id*', each request in own file")
            .NoArgument()
            .StoreTrue(&SaveResultsEnabled);

        Opts.AddLongOption(
                "folder-postfix",
                "select result folder postfix: full folder name - (checkRange_ "
                "+ $diskId + _$postfix)")
            .RequiredArgument("STR")
            .StoreResult(&FolderPostfix);
    }

protected:
    bool DoExecute() override
    {
        if (!CheckOpts()) {
            return false;
        }

        auto& output = GetOutputStream();

        ui32 errorCount = 0;
        ui32 requestCount = 0;

        auto [builder, error] = CreateRequestBuilder();
        if (HasError(error)) {
            output << FormatError(error) << Endl;
            return false;
        }

        while (std::optional range = builder.Next()) {
            auto request = std::make_shared<NProto::TExecuteActionRequest>();

            request->SetAction("checkrange");
            request->SetInput(CreateNextInput(*range));

            const auto requestId = GetRequestId(*request);
            auto result = WaitFor(ClientEndpoint->ExecuteAction(
                MakeIntrusive<TCallContext>(requestId),
                std::move(request)));

            ++requestCount;
            if (const auto& error = result.GetError(); HasError(error)) {
                if (error.GetCode() == E_ARGUMENT) {
                    output << "Wrong argument : " << FormatError(error) << Endl;
                    output << "Total errors caught: " << errorCount << Endl;
                    return true;
                }

                errorCount++;
                if (ShowReadErrorsEnabled) {
                    output << "CheckRange went wrong in range " << *range
                           << ": " << FormatError(error) << Endl;
                }
            } else {
                const auto& status = ExtractStatusValues(result.GetOutput());

                if (HasError(status)) {
                    errorCount++;
                    if (ShowReadErrorsEnabled) {
                        output << "ReadBlocks error in range " << *range << ": "
                               << FormatError(status) << Endl;
                    }
                }
            }

            if (SaveResultsEnabled) {
                SaveResultToFile(result.GetOutput(), *range);
            }
        }

        output << "Total requests sended: " << requestCount << Endl;
        output << "Total errors caught: " << errorCount << Endl;
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

        if (BlocksPerRequest < 1) {
            STORAGE_ERROR("BlocksPerRequest must be positive");
            return false;
        }

        return true;
    }

    TString CreateFilename(TBlockRange64 range) const
    {
        TString folderPath = "./checkRange_" + DiskId;
        if (!FolderPostfix.empty()) {
            folderPath += "_" + FolderPostfix;
        }

        TString fileName =
            Sprintf("result_%lu_%lu.json", range.Start, range.End);

        return fileName;
    }

    void SaveResultToFile(const TString& content, TBlockRange64 range) const
    {
        TFsPath fileName = CreateFilename(range);

        fileName.Parent().MkDirs();

        TOFStream file(
            fileName,
            EOpenModeFlag::CreateAlways | EOpenModeFlag::WrOnly);
        file.Write(content);
    }

    TResultOrError<TRequestBuilder> CreateRequestBuilder()
    {
        auto statVolumeRequest = std::make_shared<NProto::TStatVolumeRequest>();
        statVolumeRequest->SetDiskId(DiskId);
        const auto statVolumeRequestId = GetRequestId(*statVolumeRequest);
        auto result = WaitFor(ClientEndpoint->StatVolume(
            MakeIntrusive<TCallContext>(statVolumeRequestId),
            std::move(statVolumeRequest)));

        if (HasError(result)) {
            return result.GetError();
        }

        ui64 diskBlockCount = result.GetVolume().GetBlocksCount();

        ui64 remainingBlocks = diskBlockCount;

        if (BlocksCount) {
            if (BlocksCount + StartIndex <= diskBlockCount) {
                remainingBlocks = BlocksCount;
            } else {
                remainingBlocks = diskBlockCount - StartIndex;
            }
        }

        return TRequestBuilder(StartIndex, remainingBlocks, BlocksPerRequest);
    }

    TString CreateNextInput(TBlockRange64 range) const
    {
        NJson::TJsonValue input;
        input["DiskId"] = DiskId;
        input["StartIndex"] = range.Start;
        input["BlocksCount"] = range.Size();
        input["CalculateChecksums"] = CalculateChecksums;
        return input.GetStringRobust();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewCheckRangeCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TCheckRangeCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
