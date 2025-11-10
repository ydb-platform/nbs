#include "check_range.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
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
        const NCloud::NBlockStore::NProto::TExecuteActionResponse& result,
        bool& fatalErr);

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

public:
    explicit TCheckRangeCommand(IBlockStorePtr client);

protected:
    bool DoExecute() override;

private:
    bool CheckOpts() const;
    TResultOrError<TRequestBuilder> CreateRequestBuilder();
    TString CreateNextInput(TBlockRange64 range) const;
};

TCheckRangeCommand::TCheckRangeCommand(IBlockStorePtr client): TCommand(std::move(client))
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
    while (range) {
        auto request = std::make_shared<NProto::TExecuteActionRequest>();

        request->SetAction("checkrange");
        request->SetInput(CreateNextInput(*range));
        resultManager.IncRequestCnt();
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->ExecuteAction(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        bool fatalErr{false};
        resultManager.SetRangeResult(*range, result, fatalErr);
        if (fatalErr) {
            return true;
        }

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

TResultManager::TResultManager(IOutputStream& output): Output(output)
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
    WriteKV("global_error", j);
}

void TResultManager::IncRequestCnt()
{
    Y_ABORT_IF(HasBootstrapError);
    RequestCount++;
}

void TResultManager::SetRangeResult(
    const TBlockRange64& range,
    const NCloud::NBlockStore::NProto::TExecuteActionResponse& result,
    bool& fatalErr)
{
    Y_ABORT_IF(HasBootstrapError);

    NJson::TJsonValue res;
    res["r_start"] = range.Start;
    res["r_end"] = range.End;

    fatalErr = false;
    if (const auto& error = result.GetError(); HasError(error)) {
        ErrorCount++;
        res["error"] = FormatErrorJson(error);
        if (error.GetCode() == E_ARGUMENT) {
            fatalErr = true;
        }
        WriteRangeResult(res);
        return;
    }

    NJson::TJsonValue resp;
    if (!NJson::ReadJsonTree(result.GetOutput(), &resp) ||
        !resp.Has("Status"))
    {
        ErrorCount++;
        res["error"]["Message"] = "Unknown response's format";
        res["error"]["Code"] = E_INVALID_STATE;
        WriteRangeResult(res);
        return;
    }

    const auto& status = ExtractStatusValues(resp);
    if (HasError(status)) {
        ErrorCount++;
        res["error"] = FormatErrorJson(status);
    }

    if (resp.Has("Checksums") && !resp["Checksums"].GetArray().empty()) {
        res["checksums"] = std::move(resp["Checksums"]);
    }
    if (resp.Has("MirrorChecksums") &&
        !resp["MirrorChecksums"].GetArray().empty())
    {
        res["mirror_checksums"] = std::move(resp["MirrorChecksums"]);
    }

    if (res.Has("error")) {
        AddProblemRangeWithMerging(range);
    }
    WriteRangeResult(res);
}

void TResultManager::AddProblemRangeWithMerging(const TBlockRange64& range)
{
    if (ProblemRanges.size() && ProblemRanges.back().End + 1 == range.Start)
    {
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
        Output << "\n\"ranges\": [";
    }
    Output << NJson::WriteJson(rangeRes);
}

void TResultManager::WriteSummary()
{
    Y_ABORT_IF(HasBootstrapError);
    NJson::TJsonValue summary;
    summary["errors_num"] = ErrorCount;
    summary["requests_num"] = RequestCount;
    if (ErrorCount) {
        for (const auto& r: ProblemRanges) {
            NJson::TJsonValue range;
            range["r_start"] = r.Start;
            range["r_end"] = r.End;
            summary["problem_ranges"].AppendValue(std::move(range));
        }
    }
    WriteKV("summary", summary);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewCheckRangeCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TCheckRangeCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
