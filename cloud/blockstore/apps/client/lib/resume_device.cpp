#include "query_available_storage.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TResumeDeviceCommand final: public TCommand
{
private:
    TString AgentId;
    TString Path;
    bool DryRun = false;

public:
    TResumeDeviceCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("agent-id", "host FQDN")
            .RequiredArgument("STR")
            .StoreResult(&AgentId);

        Opts.AddLongOption("path", "device path")
            .RequiredArgument("STR")
            .StoreResult(&Path);

        Opts.AddLongOption("dry-run", "dry run").NoArgument().SetFlag(&DryRun);
    }

protected:
    bool DoExecute() override
    {
        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Reading ResumeDevice request");
        auto request = std::make_shared<NProto::TResumeDeviceRequest>();
        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            request->SetAgentId(AgentId);
            request->SetPath(Path);
            request->SetDryRun(DryRun);
        }

        STORAGE_DEBUG("Sending ResumeDevice request");
        auto result = WaitFor(ClientEndpoint->ResumeDevice(
            MakeIntrusive<TCallContext>(),
            std::move(request)));

        STORAGE_DEBUG("Received ResumeDevice response");
        if (Proto) {
            SerializeToTextFormat(result, output);
            return true;
        }

        if (HasError(result)) {
            output << FormatError(result.GetError()) << Endl;
            return false;
        }

        output << result << Endl;
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewResumeDeviceCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TResumeDeviceCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
