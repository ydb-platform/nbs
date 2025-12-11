#include "discover_instances.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

using NProto::EDiscoveryPortFilter;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDiscoverInstancesCommand final: public TCommand
{
private:
    ui32 Limit;
    EDiscoveryPortFilter InstanceFilter;

public:
    TDiscoverInstancesCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("limit", "instance count limit")
            .RequiredArgument("INT")
            .DefaultValue(1)
            .StoreResult(&Limit);

        Opts.AddLongOption("instance-filter", "fitler instances by port type")
            .RequiredArgument("{insecure, secure}")
            .DefaultValue("insecure")
            .Handler1T<TString>(
                [this](const auto& s)
                {
                    if (s == "insecure") {
                        InstanceFilter =
                            EDiscoveryPortFilter::DISCOVERY_INSECURE_PORT;
                    } else if (s == "secure") {
                        InstanceFilter =
                            EDiscoveryPortFilter::DISCOVERY_SECURE_PORT;
                    } else {
                        ythrow yexception()
                            << "unknown port filter: " << s.Quote();
                    }
                });
    }

protected:
    bool DoExecute() override
    {
        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Reading DiscoverInstances request");
        auto request = std::make_shared<NProto::TDiscoverInstancesRequest>();

        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            request->SetLimit(Limit);
            request->SetInstanceFilter(InstanceFilter);
        }

        STORAGE_DEBUG("Sending DiscoverInstances request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->DiscoverInstances(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received DiscoverInstances response");
        if (Proto) {
            SerializeToTextFormat(result, output);
            return true;
        }

        if (HasError(result)) {
            output << FormatError(result.GetError()) << Endl;
            return false;
        }

        const auto& instances = result.GetInstances();
        for (const auto& instance: instances) {
            output << instance.GetHost() << ":" << instance.GetPort() << Endl;
        }
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewDiscoverInstancesCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TDiscoverInstancesCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
