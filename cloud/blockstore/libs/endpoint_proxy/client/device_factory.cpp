#include "device_factory.h"

#include "client.h"

#include <cloud/blockstore/public/api/protos/endpoints.pb.h>

#include <cloud/blockstore/libs/nbd/device.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString Addr2String(const TNetworkAddress& addr)
{
    const addrinfo* ai = &*addr.Begin();
    TStringBuilder sb;

    for (int i = 0; ai; ++i, ai = ai->ai_next) {
        if (i > 0) {
            ythrow yexception() << "unexpected address";
        }

        sb << static_cast<const NAddr::IRemoteAddr&>(NAddr::TAddrInfo(ai));
    }

    return sb;
}

////////////////////////////////////////////////////////////////////////////////

struct TProxyConnection: NBD::IDeviceConnection
{
    const TProxyDeviceConnectionFactoryConfig Config;
    const IEndpointProxyClientPtr Client;
    const TString AddressString;
    const TString DeviceName;
    const ui64 BlockCount;
    const ui32 BlockSize;

    TProxyConnection(
            TProxyDeviceConnectionFactoryConfig config,
            IEndpointProxyClientPtr client,
            const TNetworkAddress& connectAddress,
            TString deviceName,
            ui64 blockCount,
            ui32 blockSize)
        : Config(config)
        , Client(std::move(client))
        , AddressString(Addr2String(connectAddress))
        , DeviceName(std::move(deviceName))
        , BlockCount(blockCount)
        , BlockSize(blockSize)
    {}

    void Start() override
    {
        auto request = std::make_shared<NProto::TStartProxyEndpointRequest>();
        request->SetUnixSocketPath(AddressString);
        request->SetNbdDevice(DeviceName);
        if (Config.DefaultSectorSize) {
            request->SetBlocksCount(
                BlockCount * BlockSize / Config.DefaultSectorSize);
            request->SetBlockSize(Config.DefaultSectorSize);
        } else {
            request->SetBlocksCount(BlockCount);
            request->SetBlockSize(BlockSize);
        }
        // XXX bad signature - can't return a future, sync wait is bad as well
        Client->StartProxyEndpoint(std::move(request));
    }

    void Stop() override
    {
        auto request = std::make_shared<NProto::TStopProxyEndpointRequest>();
        request->SetUnixSocketPath(AddressString);
        // XXX bad signature - can't return a future, sync wait is bad as well
        Client->StopProxyEndpoint(std::move(request));
    }
};

struct TProxyFactory: NBD::IDeviceConnectionFactory
{
    const TProxyDeviceConnectionFactoryConfig Config;
    const IEndpointProxyClientPtr Client;

    explicit TProxyFactory(
            TProxyDeviceConnectionFactoryConfig config,
            IEndpointProxyClientPtr client)
        : Config(config)
        , Client(std::move(client))
    {}

    NBD::IDeviceConnectionPtr Create(
        const TNetworkAddress& connectAddress,
        TString deviceName,
        ui64 blockCount,
        ui32 blockSize) override
    {
        return std::make_shared<TProxyConnection>(
            Config,
            Client,
            connectAddress,
            std::move(deviceName),
            blockCount,
            blockSize);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NBD::IDeviceConnectionFactoryPtr CreateProxyDeviceConnectionFactory(
    TProxyDeviceConnectionFactoryConfig config,
    IEndpointProxyClientPtr client)
{
    return std::make_shared<TProxyFactory>(config, std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
