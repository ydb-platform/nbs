#include "check_range.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/media.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/json/easy_parse/json_easy_parser.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/folder/path.h>
#include <util/stream/file.h>

#include <utility>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 DefaultBlocksPerRequest = 1024;

NProto::TError ExtractStatusValues(const NJson::TJsonValue& json)
{
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
        ui32 blocksPerRequest);

    std::optional<TBlockRange64> Next();
};

////////////////////////////////////////////////////////////////////////////////

// This class builds check range result and writes it into the 'Output' with
// JSON format. Result's writing produced step by step without keeping
// intermediate results in memory.
// Full documentation is located in doc/blockstore/diagnostics/checkrange.md
class TResultManager
{
    IOutputStream& Output;
    ui32 ErrorCount{0};
    ui32 RequestCount{0};
    TVector<TBlockRange64> ProblemRanges;

    bool HasBootstrapError{false};
    bool RangeWtitten{false};

    struct TSummary
    {
        TString Summary;
        NProto::TError Err;
    };

public:
    explicit TResultManager(IOutputStream& output);
    ~TResultManager();

    void SetBootstrapError(const NProto::TError& err);

    void IncRequestCnt();

    void SetRangeResult(
        const TBlockRange64& range,
        const NProto::TError& error,
        NJson::TJsonValue response);

private:
    void AddProblemRangeWithMerging(const TBlockRange64& range);
    void WriteKV(const TString& key, const NJson::TJsonValue& val);
    void WriteRangeResult(const NJson::TJsonValue& rangeRes);

    void WriteSummary();
};

////////////////////////////////////////////////////////////////////////////////

class TCheckRangeCommand final: public TCommand
{
private:
    TString DiskId;
    ui64 StartIndex = 0;
    ui64 BlocksCount = 0;
    ui64 BlocksPerRequest = 0;
    bool IsMirror = false;

public:
    explicit TCheckRangeCommand(IBlockStorePtr client);

protected:
    bool DoExecute() override;

private:
    bool CheckOpts() const;
    TResultOrError<TRequestBuilder> CreateRequestBuilder();
    TString CreateNextInput(TBlockRange64 range) const;
};

TCheckRangeCommand::TCheckRangeCommand(IBlockStorePtr client)
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
}

bool TCheckRangeCommand::DoExecute()
{
    if (!CheckOpts()) {
        return false;
    }

    TResultManager resultManager(GetOutputStream());

    auto [builder, error] = CreateRequestBuilder();
    if (HasError(error)) {
        resultManager.SetBootstrapError(error);
        return false;
    }

    std::optional range = builder.Next();
    bool isRetry = false;
    while (range) {
        auto request = std::make_shared<NProto::TExecuteActionRequest>();

        request->SetAction("checkrange");
        request->SetInput(CreateNextInput(*range));
        resultManager.IncRequestCnt();
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->ExecuteAction(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        NJson::TJsonValue response;
        if (const auto& error = result.GetError(); HasError(error)) {
            if (error.GetCode() == E_ARGUMENT) {
                resultManager.SetRangeResult(*range, error, response);
                STORAGE_ERROR(
                    "Fatal range error: %s",
                    FormatError(error).c_str());
                return true;
            }
        } else if (
            !NJson::ReadJsonTree(result.GetOutput(), &response) ||
            !response.Has("Status"))
        {
            resultManager.SetRangeResult(
                *range,
                MakeError(E_INVALID_STATE, "Unknown response's format"),
                response);
        } else {
            const auto& status = ExtractStatusValues(response);
            if (HasError(status) && status.GetCode() == E_REJECTED &&
                IsMirror && !isRetry)
            {
                isRetry = true;
                continue;
            }
            resultManager.SetRangeResult(*range, status, response);
        }

        isRetry = false;
        range = builder.Next();
    }

    return true;
}

bool TCheckRangeCommand::CheckOpts() const
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

TResultOrError<TRequestBuilder> TCheckRangeCommand::CreateRequestBuilder()
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

    IsMirror = NCloud::IsReliableDiskRegistryMediaKind(
        result.GetVolume().GetStorageMediaKind());
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

TString TCheckRangeCommand::CreateNextInput(TBlockRange64 range) const
{
    NJson::TJsonValue input;
    input["DiskId"] = DiskId;
    input["StartIndex"] = range.Start;
    input["BlocksCount"] = range.Size();

    return input.GetStringRobust();
}

TRequestBuilder::TRequestBuilder(
    ui32 startIndex,
    ui32 remainingBlocks,
    ui32 blocksPerRequest)
    : StartIndex(startIndex)
    , RemainingBlocks(remainingBlocks)
    , BlocksPerRequest(blocksPerRequest)
{}

std::optional<TBlockRange64> TRequestBuilder::Next()
{
    if (RemainingBlocks <= 0) {
        return std::nullopt;
    }

    ui32 blocksInThisRequest = std::min(RemainingBlocks, BlocksPerRequest);

    TBlockRange64 range =
        TBlockRange64::WithLength(StartIndex, blocksInThisRequest);

    RemainingBlocks -= blocksInThisRequest;
    StartIndex += blocksInThisRequest;

    return range;
}

TResultManager::TResultManager(IOutputStream& output)
    : Output(output)
{
    Output << "{";
}

TResultManager::~TResultManager()
{
    if (!HasBootstrapError) {
        if (RangeWtitten) {
            Output << "],\n";
        }
        WriteSummary();
    }
    Output << "}";
}

void TResultManager::SetBootstrapError(const NProto::TError& err)
{
    Y_ABORT_IF(HasBootstrapError);
    HasBootstrapError = true;
    auto j = FormatErrorJson(err);
    WriteKV("GlobalError", j);
}

void TResultManager::IncRequestCnt()
{
    Y_ABORT_IF(HasBootstrapError);
    RequestCount++;
}

void TResultManager::SetRangeResult(
    const TBlockRange64& range,
    const NProto::TError& error,
    NJson::TJsonValue response)
{
    Y_ABORT_IF(HasBootstrapError);

    NJson::TJsonValue res;
    res["Start"] = range.Start;
    res["End"] = range.End;
    if (HasError(error)) {
        ErrorCount++;
        res["Error"] = FormatErrorJson(error);
    }

    if (!response.IsNull()) {
        if (response.Has("DiskChecksums") &&
            !response["DiskChecksums"]["Data"].GetArray().empty())
        {
            res["DiskChecksums"] = std::move(response["DiskChecksums"]["Data"]);
        }
        if (response.Has("InconsistentChecksums") &&
            !response["InconsistentChecksums"]["Replicas"].GetArray().empty())
        {
            res["InconsistentChecksums"] =
                std::move(response["InconsistentChecksums"]["Replicas"]);
        }
    }

    if (res.Has("Error")) {
        AddProblemRangeWithMerging(range);
    }
    WriteRangeResult(res);
}

void TResultManager::AddProblemRangeWithMerging(const TBlockRange64& range)
{
    if (ProblemRanges.size() && ProblemRanges.back().End + 1 == range.Start) {
        ProblemRanges.back().End = range.End;
    } else {
        ProblemRanges.push_back(range);
    }
}

void TResultManager::WriteKV(const TString& key, const NJson::TJsonValue& val)
{
    Output << "\"" << key << "\": " << NJson::WriteJson(val);
}

void TResultManager::WriteRangeResult(const NJson::TJsonValue& rangeRes)
{
    if (RangeWtitten) {
        Output << ",";
    } else {
        RangeWtitten = true;
        Output << "\n\"Ranges\": [";
    }
    Output << NJson::WriteJson(rangeRes);
}

void TResultManager::WriteSummary()
{
    Y_ABORT_IF(HasBootstrapError);
    NJson::TJsonValue summary;
    summary["ErrorsNum"] = ErrorCount;
    summary["RequestsNum"] = RequestCount;
    if (ErrorCount) {
        for (const auto& r: ProblemRanges) {
            NJson::TJsonValue range;
            range["Start"] = r.Start;
            range["End"] = r.End;
            summary["ProblemRanges"].AppendValue(std::move(range));
        }
    }
    WriteKV("Summary", summary);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewCheckRangeCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TCheckRangeCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
