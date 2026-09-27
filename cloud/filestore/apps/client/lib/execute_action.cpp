#include "command.h"

#include <cloud/filestore/private/api/protos/tablet.pb.h>
#include <cloud/filestore/public/api/protos/action.pb.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <google/protobuf/util/json_util.h>

#include <util/generic/vector.h>
#include <util/stream/file.h>
#include <util/string/builder.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString ReadFile(const TString& path)
{
    return TFileInput(path).ReadAll();
}

class TExecuteActionCommand final
    : public TFileStoreServiceCommand
{
private:
    TString Action;
    TString Input;
    TString InputFilePath;
    bool AllShards = false;

private:
    NProto::TExecuteActionResponse SendAction(
        const TString& action,
        const TString& input)
    {
        auto callContext = PrepareCallContext();
        auto request = std::make_shared<NProto::TExecuteActionRequest>();
        request->SetAction(action);
        request->SetInput(input);
        request->MutableHeaders()->SetRequestId(callContext->RequestId);

        STORAGE_DEBUG("Sending ExecuteAction request");
        return WaitFor(
            Client->ExecuteAction(std::move(callContext), std::move(request)));
    }

    NProto::TExecuteActionResponse SendAction(
        const TString& action,
        const google::protobuf::Message& request)
    {
        TString input;
        google::protobuf::util::MessageToJsonString(request, &input);
        return SendAction(action, input);
    }

    bool ReportAllShardsResults(const NJson::TJsonValue& results, bool success)
    {
        Cout << NJson::WriteJson(
                    results,
                    true /* formatOutput */,
                    true /* sortkeys */)
             << Endl;
        if (!success) {
            ProgramShouldContinue.ShouldStop(1);
        }
        return success;
    }

    template <typename TRequest>
    bool ExecuteForAllShards(const TString& action, const TString& input)
    {
        TRequest request;
        if (!google::protobuf::util::JsonStringToMessage(input, &request).ok())
        {
            STORAGE_THROW_SERVICE_ERROR(MakeError(
                E_ARGUMENT,
                "invalid storage config action input JSON"));
        }

        const TString fileSystemId = request.GetFileSystemId();
        if (fileSystemId.empty()) {
            STORAGE_THROW_SERVICE_ERROR(MakeError(
                E_ARGUMENT,
                "--all-shards requires FileSystemId of the main filesystem"));
        }

        NJson::TJsonValue results(NJson::JSON_MAP);

        NProtoPrivate::TGetFileSystemTopologyRequest topologyRequest;
        topologyRequest.SetFileSystemId(fileSystemId);
        auto topologyResult =
            SendAction("getfilesystemtopology", topologyRequest);
        if (HasError(topologyResult)) {
            results[fileSystemId]["Error"] =
                FormatError(topologyResult.GetError());
            return ReportAllShardsResults(results, false /* success */);
        }

        NProtoPrivate::TGetFileSystemTopologyResponse topology;
        if (!google::protobuf::util::JsonStringToMessage(
                topologyResult.GetOutput(),
                &topology).ok())
        {
            results[fileSystemId]["Error"] =
                "invalid getfilesystemtopology response JSON";
            return ReportAllShardsResults(results, false /* success */);
        }
        if (topology.GetShardNo()) {
            STORAGE_THROW_SERVICE_ERROR(MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "--all-shards requires the main filesystem, got shard "
                    << fileSystemId));
        }

        TVector<TString> targets{fileSystemId};
        targets.insert(
            targets.end(),
            topology.GetShardFileSystemIds().begin(),
            topology.GetShardFileSystemIds().end());

        bool success = true;
        for (const auto& target: targets) {
            NJson::TJsonValue entry(NJson::JSON_MAP);

            // WaitFor returns immediately once the program is stopped, so do
            // not send the remaining requests, but report them as skipped
            if (ProgramShouldContinue.PollState() !=
                TProgramShouldContinue::Continue)
            {
                entry.InsertValue("Error", "skipped: program was stopped");
                results.InsertValue(target, std::move(entry));
                success = false;
                continue;
            }

            request.SetFileSystemId(target);
            // if the program is stopped while this request is in flight,
            // WaitFor returns E_REJECTED "request cancelled", but the request
            // may still be applied by the server
            auto result = SendAction(action, request);
            if (HasError(result)) {
                entry.InsertValue("Error", FormatError(result.GetError()));
                success = false;
            } else {
                NJson::TJsonValue response;
                if (NJson::ReadJsonTree(result.GetOutput(), &response)) {
                    entry.InsertValue("Response", std::move(response));
                } else {
                    entry.InsertValue("Error", "invalid action response JSON");
                    success = false;
                }
            }
            results.InsertValue(target, std::move(entry));
        }

        return ReportAllShardsResults(results, success);
    }

public:
    TExecuteActionCommand()
    {
        Opts.AddLongOption("action", "name of action to execute")
            .RequiredArgument("STR")
            .StoreResult(&Action);
        const TString inputJson = "input-json";
        Opts.AddLongOption(inputJson, "action input json")
            .RequiredArgument("STR")
            .StoreResult(&Input);
        const TString inputFile = "input-file";
        Opts.AddLongOption(inputFile, "action input json file")
            .AddShortName('f')
            .RequiredArgument("STR")
            .StoreResult(&InputFilePath);
        Opts.MutuallyExclusive(inputJson, inputFile);
        Opts.AddLongOption(
                "all-shards",
                "run getstorageconfig or changestorageconfig for the main "
                "filesystem and all its shards; output JSON keyed by "
                "filesystem ID; if interrupted, the in-flight request may "
                "still be applied and the remaining filesystems are "
                "reported as skipped")
            .StoreTrue(&AllShards);
    }

    bool Execute() override
    {
        if (!InputFilePath.empty()) {
            Input = ReadFile(InputFilePath);
        }
        TStringInput inputBytes(Input);

        auto& input = Input.empty() ? Cin : inputBytes;
        auto inputJsonStr = input.ReadAll();

        if (AllShards) {
            TString action = Action;
            action.to_lower();
            if (action == "changestorageconfig") {
                return ExecuteForAllShards<
                    NProtoPrivate::TChangeStorageConfigRequest>(
                    action,
                    inputJsonStr);
            }
            if (action == "getstorageconfig") {
                return ExecuteForAllShards<
                    NProtoPrivate::TGetStorageConfigRequest>(
                    action,
                    inputJsonStr);
            }
            STORAGE_THROW_SERVICE_ERROR(MakeError(
                E_ARGUMENT,
                "--all-shards is supported only for changestorageconfig "
                "and getstorageconfig"));
        }

        auto result = SendAction(Action, inputJsonStr);

        STORAGE_DEBUG("Received ExecuteAction response");

        if (HasError(result)) {
            Cout << FormatError(result.GetError()) << Endl;
            ProgramShouldContinue.ShouldStop(1);
            return false;
        }

        Cout << result.GetOutput() << Endl;
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewExecuteActionCommand()
{
    return std::make_shared<TExecuteActionCommand>();
}

}   // namespace NCloud::NFileStore::NClient
