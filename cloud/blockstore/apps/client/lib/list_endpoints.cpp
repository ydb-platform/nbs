#include "list_endpoints.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TListEndpointsCommand final: public TCommand
{
private:
    bool WaitForRestoring;

public:
    TListEndpointsCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("wait-for-restoring", "wait for endpoint restoring")
            .NoArgument()
            .SetFlag(&WaitForRestoring);
    }

protected:
    bool DoExecute() override
    {
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Sending ListEndpoints request");

        NProto::TListEndpointsResponse result;

        size_t attempt = 0;

        do {
            if (++attempt > 1) {
                Sleep(TDuration::MilliSeconds(500));
            }

            result = WaitFor(ClientEndpoint->ListEndpoints(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TListEndpointsRequest>()));
        } while (WaitForRestoring && !HasError(result) &&
                 !result.GetEndpointsWereRestored());

        STORAGE_DEBUG("Received ListEndpoints response");
        if (Proto) {
            SerializeToTextFormat(result, output);
            return true;
        }

        if (HasError(result)) {
            output << FormatError(result.GetError()) << Endl;
            return false;
        }

        const auto& endpoints = result.GetEndpoints();
        for (const auto& endpoint: endpoints) {
            output << "socket: " << endpoint.GetUnixSocketPath().Quote()
                   << " disk-id: " << endpoint.GetDiskId().Quote()
                   << " ipc-type: " << GetIpcTypeString(endpoint.GetIpcType())
                   << Endl;
        }
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewListEndpointsCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TListEndpointsCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
