#include "command.h"

#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <library/cpp/json/json_reader.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TForcedCompactionCommand final
    : public TFileStoreCommand
{
private:
    ui32 MinRangeId = 0;

private:
    template <typename TRequest, typename TResponse>
    void ExecuteAction(
        const TString& action,
        const TRequest& requestProto,
        TResponse* responseProto)
    {
        auto callContext = PrepareCallContext();

        TString input;
        google::protobuf::util::MessageToJsonString(requestProto, &input);

        STORAGE_DEBUG("Reading ExecuteAction request");
        auto request = std::make_shared<NProto::TExecuteActionRequest>();
        request->SetAction(action);
        request->SetInput(std::move(input));

        STORAGE_DEBUG("Sending ExecuteAction request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(Client->ExecuteAction(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received ExecuteAction response");

        if (HasError(result)) {
            responseProto->MutableError()->CopyFrom(result.GetError());
            return;
        }

        auto parsed = google::protobuf::util::JsonStringToMessage(
            result.GetOutput(),
            responseProto).ok();

        if (!parsed) {
            responseProto->MutableError()->CopyFrom(MakeError(
                E_FAIL,
                TStringBuilder() << "failed to parse response json: "
                    << result.GetOutput()));
        }
    }

    auto RunCompaction()
    {
        NProtoPrivate::TForcedOperationRequest request;
        request.SetFileSystemId(FileSystemId);
        request.SetOpType(NProtoPrivate::TForcedOperationRequest::E_COMPACTION);
        request.SetMinRangeId(MinRangeId);
        NProtoPrivate::TForcedOperationResponse response;
        ExecuteAction("forcedoperation", request, &response);
        CheckResponse(response);
        return response;
    }

public:
    TForcedCompactionCommand()
    {
        Opts.AddLongOption("min-range-id", "initial compaction range id")
            .RequiredArgument("NUM")
            .StoreResult(&MinRangeId);
    }

public:
    bool Execute() override
    {
        auto callContext = PrepareCallContext();

        auto response = RunCompaction();

        while (true) {
            NProtoPrivate::TForcedOperationStatusRequest statusRequest;
            statusRequest.SetFileSystemId(FileSystemId);
            statusRequest.SetOperationId(response.GetOperationId());
            NProtoPrivate::TForcedOperationStatusResponse statusResponse;
            ExecuteAction(
                "forcedoperationstatus",
                statusRequest,
                &statusResponse);

            if (statusResponse.GetError().GetCode() == E_NOT_FOUND) {
                // tablet rebooted
                response = RunCompaction();
                continue;
            }

            const auto processed = statusResponse.GetProcessedRangeCount();
            const auto total = statusResponse.GetRangeCount();
            if (processed >= total) {
                // operation completed
                Cout << "finished" << Endl;
                break;
            }

            CheckResponse(statusResponse);

            Cout << "progress: " << statusResponse.GetProcessedRangeCount()
                << "/" << statusResponse.GetRangeCount() << ", last="
                << statusResponse.GetLastProcessedRangeId() << Endl;

            MinRangeId = statusResponse.GetLastProcessedRangeId();

            Sleep(TDuration::Seconds(1));
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewForcedCompactionCommand()
{
    return std::make_shared<TForcedCompactionCommand>();
}

}   // namespace NCloud::NFileStore::NClient
