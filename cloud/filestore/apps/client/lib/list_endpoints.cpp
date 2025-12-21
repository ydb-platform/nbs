#include "command.h"

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TListEndpointsCommand final
    : public TEndpointCommand
{
public:
    bool Execute() override
    {
        auto callContext = PrepareCallContext();

        auto request = std::make_shared<NProto::TListEndpointsRequest>();

        auto response = WaitFor(
            Client->ListEndpoints(
                std::move(callContext),
                std::move(request)));

        if (HasError(response)) {
            STORAGE_THROW_SERVICE_ERROR(response.GetError());
        }

        if (JsonOutput) {
            response.PrintJSON(Cout);
        } else {
            for (const auto& endpoint: response.GetEndpoints()) {
                Cout << DumpMessage(endpoint) << Endl;
            }
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewListEndpointsCommand()
{
    return std::make_shared<TListEndpointsCommand>();
}

}   // namespace NCloud::NFileStore::NClient
