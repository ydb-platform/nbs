#include "endpoint_proxy.h"

#include <cloud/blockstore/libs/endpoint_proxy/client/client.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TStartProxyEndpointCommand final: public TCommand
{
private:
    TString UnixSocketPath;
    TString NbdDeviceFile;
    ui32 BlockSize = 0;
    ui64 BlocksCount = 0;

public:
    explicit TStartProxyEndpointCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("socket", "unix-socket path")
            .RequiredArgument("STR")
            .StoreResult(&UnixSocketPath);

        Opts.AddLongOption("nbd-device", "nbd device file")
            .RequiredArgument("STR")
            .StoreResult(&NbdDeviceFile);

        Opts.AddLongOption("block-size", "nbd options: block size")
            .RequiredArgument("STR")
            .StoreResult(&BlockSize);

        Opts.AddLongOption("blocks-count", "nbd options: blocks count")
            .RequiredArgument("STR")
            .StoreResult(&BlocksCount);
    }

protected:
    bool DoExecute() override
    {
        if (!Proto && !CheckOpts()) {
            return false;
        }

        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Reading StartProxyEndpoint request");
        auto request = std::make_shared<NProto::TStartProxyEndpointRequest>();
        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            request->SetUnixSocketPath(UnixSocketPath);
            request->SetNbdDevice(NbdDeviceFile);
            request->SetBlockSize(BlockSize);
            request->SetBlocksCount(BlocksCount);
        }

        STORAGE_DEBUG("Sending StartProxyEndpoint request");
        auto result = WaitFor(EndpointProxyClient->StartProxyEndpoint(
            std::move(request)));

        STORAGE_DEBUG("Received StartProxyEndpoint response");
        if (Proto) {
            SerializeToTextFormat(result, output);
            return true;
        }

        if (HasError(result)) {
            output << FormatError(result.GetError()) << Endl;
            return false;
        }

        output << result.DebugString() << Endl;
        return true;
    }

private:
    bool CheckOpts() const
    {
        if (!UnixSocketPath) {
            STORAGE_ERROR("UnixSocketPath is required");
            return false;
        }

        if (!NbdDeviceFile) {
            STORAGE_ERROR("NbdDeviceFile is required");
            return false;
        }

        if (!BlockSize) {
            STORAGE_ERROR("BlockSize is required");
            return false;
        }

        if (!BlocksCount) {
            STORAGE_ERROR("BlocksCount is required");
            return false;
        }

        if (!EndpointProxyClient) {
            STORAGE_ERROR("EndpointProxyClient not initialized");
            return false;
        }

        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TStopProxyEndpointCommand final: public TCommand
{
private:
    TString UnixSocketPath;

public:
    explicit TStopProxyEndpointCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("socket", "unix-socket path")
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

        STORAGE_DEBUG("Reading StopProxyEndpoint request");
        auto request = std::make_shared<NProto::TStopProxyEndpointRequest>();
        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            request->SetUnixSocketPath(UnixSocketPath);
        }

        STORAGE_DEBUG("Sending StopProxyEndpoint request");
        auto result = WaitFor(EndpointProxyClient->StopProxyEndpoint(
            std::move(request)));

        STORAGE_DEBUG("Received StopProxyEndpoint response");
        if (Proto) {
            SerializeToTextFormat(result, output);
            return true;
        }

        if (HasError(result)) {
            output << FormatError(result.GetError()) << Endl;
            return false;
        }

        output << result.DebugString() << Endl;
        return true;
    }

private:
    bool CheckOpts() const
    {
        if (!UnixSocketPath) {
            STORAGE_ERROR("UnixSocketPath is required");
            return false;
        }

        if (!EndpointProxyClient) {
            STORAGE_ERROR("EndpointProxyClient not initialized");
            return false;
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewStartProxyEndpointCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TStartProxyEndpointCommand>(std::move(client));
}

TCommandPtr NewStopProxyEndpointCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TStopProxyEndpointCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
