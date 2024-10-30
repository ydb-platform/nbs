#include "command.h"

#include <library/cpp/json/json_reader.h>

#include <util/stream/file.h>

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
    }

    bool Execute() override
    {
        auto callContext = PrepareCallContext();

        if (!InputFilePath.empty()) {
            Input = ReadFile(InputFilePath);
        }
        TStringInput inputBytes(Input);

        auto& input = Input.empty() ? Cin : inputBytes;
        auto inputJsonStr = input.ReadAll();

        STORAGE_DEBUG("Reading ExecuteAction request");
        auto request = std::make_shared<NProto::TExecuteActionRequest>();
        request->SetAction(Action);
        request->SetInput(inputJsonStr);

        STORAGE_DEBUG("Sending ExecuteAction request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(Client->ExecuteAction(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received ExecuteAction response");

        if (HasError(result)) {
            Cout << FormatError(result.GetError()) << Endl;
            return true;
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
