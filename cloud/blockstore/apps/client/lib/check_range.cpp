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
#include <library/cpp/json/writer/json.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/folder/path.h>
#include <util/stream/file.h>

#include <utility>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 DefaultBlocksPerRequest = 1024;

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
    NJsonWriter::TBuf Buffer;
    ui32 ErrorCount{0};
    ui32 RequestCount{0};
    TVector<TBlockRange64> ProblemRanges;

    bool HasBootstrapError{false};
    bool RangeWritten{false};

    struct TSummary
    {
        TString Summary;
        NProto::TError Error;
    };

public:
    explicit TResultManager(IOutputStream& output);
    ~TResultManager();

    void SetBootstrapError(const NProto::TError& error);

    void IncRequestCount();

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

    while (range) {
        auto request = std::make_shared<NProto::TExecuteActionRequest>();

        request->SetAction("checkrange");
        request->SetInput(CreateNextInput(*range));
        resultManager.IncRequestCount();
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->ExecuteAction(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        NJson::TJsonValue response;
        auto rangeError = result.GetError();
        if (rangeError.GetCode() == E_ARGUMENT) {
            STORAGE_ERROR(
                "Fatal range error: %s",
                FormatError(rangeError).c_str());
            return true;
        }

        if (!NJson::ReadJsonTree(result.GetOutput(), &response)) {
            TString errorReason = "Unknown response's format";
            if (HasError(rangeError)) {
                errorReason += " with original error: " + FormatError(rangeError);
            }
            rangeError =
                MakeError(E_INVALID_STATE, errorReason);
        }
        resultManager.SetRangeResult(*range, rangeError, response);


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
    : Buffer(NJsonWriter::EHtmlEscapeMode::HEM_DONT_ESCAPE_HTML, &output)
{
    Buffer.BeginObject();
}

TResultManager::~TResultManager()
{
    if (!HasBootstrapError) {
        if (RangeWritten) {
            Buffer.EndList();
        }
        WriteSummary();
    }

    Buffer.EndObject();
}

void TResultManager::SetBootstrapError(const NProto::TError& error)
{
    Y_ABORT_IF(HasBootstrapError);
    HasBootstrapError = true;
    auto j = FormatErrorJson(error);
    WriteKV("GlobalError", j);
}

void TResultManager::IncRequestCount()
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
    Buffer.WriteKey(key);
    Buffer.WriteJsonValue(&val);
}

void TResultManager::WriteRangeResult(const NJson::TJsonValue& rangeRes)
{
    if (!RangeWritten) {
        RangeWritten = true;
        Buffer.WriteKey("Ranges");
        Buffer.BeginList();
    }

    Buffer.WriteJsonValue(&rangeRes);
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
