#include "execute_action.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TExecuteActionCommand final: public TCommand
{
private:
    TString Action;
    TString Input;

public:
    TExecuteActionCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("action", "name of action to execute")
            .RequiredArgument("STR")
            .StoreResult(&Action);
        Opts.AddLongOption("input-bytes", "input bytes")
            .RequiredArgument("STR")
            .StoreResult(&Input);
    }

protected:
    bool DoExecute() override
    {
        if (!Proto && !CheckOpts()) {
            return false;
        }

        TStringInput inputBytes(Input);

        auto& input = ParseResultPtr->FindLongOptParseResult("input-bytes")
                          ? inputBytes
                          : GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Reading ExecuteAction request");
        auto request = std::make_shared<NProto::TExecuteActionRequest>();
        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            request->SetAction(Action);
            request->SetInput(input.ReadAll());
        }

        STORAGE_DEBUG("Sending ExecuteAction request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->ExecuteAction(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received ExecuteAction response");
        if (Proto) {
            SerializeToTextFormat(result, output);
            return true;
        }

        if (HasError(result)) {
            output << FormatError(result.GetError()) << Endl;
            return false;
        }

        output << result.GetOutput() << Endl;
        return true;
    }

private:
    bool CheckOpts() const
    {
        const auto* action = ParseResultPtr->FindLongOptParseResult("action");
        if (!action) {
            STORAGE_ERROR("Action name is required");
            return false;
        }

        const auto* inputBytes =
            ParseResultPtr->FindLongOptParseResult("input-bytes");
        const auto* input = ParseResultPtr->FindLongOptParseResult("input");
        if (inputBytes && input) {
            STORAGE_ERROR("Parameter input-bytes and input can't be together");
            return false;
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewExecuteActionCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TExecuteActionCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
