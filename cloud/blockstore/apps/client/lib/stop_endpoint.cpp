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

class TStopEndpointCommand final
    : public TCommand
{
private:
    TString UnixSocketPath;
    TString DiskId;
    TString ClientId;

public:
    TStopEndpointCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("socket", "unix socket path")
            .RequiredArgument("STR")
            .StoreResult(&UnixSocketPath);
        Opts.AddLongOption("disk-id", "volume identifier")
            .RequiredArgument("STR")
            .StoreResult(&DiskId);
        Opts.AddLongOption("client-id", "client identifier")
            .RequiredArgument("STR")
            .StoreResult(&ClientId);
    }

protected:
    bool DoExecute() override
    {
        if (!Proto && !CheckOpts()) {
            return false;
        }

        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Reading StopEndpoint request");
        auto request = std::make_shared<NProto::TStopEndpointRequest>();
        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            request->SetUnixSocketPath(UnixSocketPath);
            request->SetDiskId(DiskId);
            request->MutableHeaders()->SetClientId(ClientId);
        }

        STORAGE_DEBUG("Sending StopEndpoint request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->StopEndpoint(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received StopEndpoint response");
        if (Proto) {
            SerializeToTextFormat(result, output);
            return true;
        }

        if (HasError(result)) {
            output << FormatError(result.GetError()) << Endl;
            return false;
        }

        output << "OK" << Endl;
        return true;
    }

private:
    bool CheckOpts() const
    {
        if (!UnixSocketPath) {
            STORAGE_ERROR("Unix socket path is required");
            return false;
        }

        if (static_cast<bool>(DiskId) != static_cast<bool>(ClientId)) {
            const auto* passedArgument = DiskId ? "disk id" : "client id";
            const auto* otherArgument = DiskId ? "client id" : "disk id";
            STORAGE_ERROR(
                "There is no sens to pass " << passedArgument << " without "
                                            << otherArgument);
            return false;
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewStopEndpointCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TStopEndpointCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
