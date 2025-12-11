#include "stop_endpoint.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDescribeEndpointCommand final: public TCommand
{
private:
    TString UnixSocketPath;

public:
    TDescribeEndpointCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("socket", "unix socket path")
            .RequiredArgument("STR")
            .StoreResult(&UnixSocketPath);
    }

protected:
    bool DoExecute() override
    {
        if (!Proto && !CheckOpts()) {
            return false;
        }

        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Reading DescribeEndpoint request");
        auto request = std::make_shared<NProto::TDescribeEndpointRequest>();
        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            request->SetUnixSocketPath(UnixSocketPath);
        }

        STORAGE_DEBUG("Sending DescribeEndpoint request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->DescribeEndpoint(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received DescribeEndpoint response");
        if (Proto) {
            SerializeToTextFormat(result, output);
            return true;
        }

        if (HasError(result)) {
            output << FormatError(result.GetError()) << Endl;
            return false;
        }

        SerializeToTextFormat(result.GetPerformanceProfile(), output);
        return true;
    }

private:
    bool CheckOpts() const
    {
        if (!UnixSocketPath) {
            STORAGE_ERROR("Unix socket path is required");
            return false;
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewDescribeEndpointCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TDescribeEndpointCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
