#include "device_factory.h"

#include "client.h"

#include <cloud/blockstore/public/api/protos/endpoints.pb.h>

#include <cloud/blockstore/libs/nbd/device.h>
#include <cloud/blockstore/libs/nbd/utils.h>

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

struct TProxyDevice: NBD::IDevice
{
    const TProxyDeviceFactoryConfig Config;
    const IEndpointProxyClientPtr Client;
    const TString AddressString;
    TString DevicePath;
    const TString DevicePrefix;
    const ui64 BlockCount;
    const ui32 BlockSize;

    TProxyDevice(
            TProxyDeviceFactoryConfig config,
            IEndpointProxyClientPtr client,
            const TNetworkAddress& connectAddress,
            TString devicePath,
            TString devicePrefix,
            ui64 blockCount,
            ui32 blockSize)
        : Config(config)
        , Client(std::move(client))
        , AddressString(Addr2String(connectAddress))
        , DevicePath(std::move(devicePath))
        , DevicePrefix(std::move(devicePrefix))
        , BlockCount(blockCount)
        , BlockSize(blockSize)
    {}

    NThreading::TFuture<NProto::TError> Start() override
    {
        if (!DevicePath) {
            DevicePath = NBD::FindFreeNbdDevice().replace(
                0,
                NBD::DEFAULT_DEVICE_PREFIX.size(),
                DevicePrefix);

            if (DevicePath == DevicePrefix) {
                return NThreading::MakeFuture(MakeError(
                    E_FAIL,
                    TStringBuilder()
                        << "unable to find free nbd device with prefix "
                        << DevicePrefix.Quote()));
            }
        }
        auto request = std::make_shared<NProto::TStartProxyEndpointRequest>();
        request->SetUnixSocketPath(AddressString);
        request->SetNbdDevice(DevicePath);
        if (Config.DefaultSectorSize) {
            request->SetBlocksCount(
                BlockCount * BlockSize / Config.DefaultSectorSize);
            request->SetBlockSize(Config.DefaultSectorSize);
        } else {
            request->SetBlocksCount(BlockCount);
            request->SetBlockSize(BlockSize);
        }
        request->SetMaxZeroBlocksSubRequestSize(
            Config.MaxZeroBlocksSubRequestSize);
        return Client->StartProxyEndpoint(std::move(request)).Apply(
            [] (const auto& f) {
                return f.GetValue().GetError();
            });
    }

    NThreading::TFuture<NProto::TError> Stop(bool deleteDevice) override
    {
        if (!deleteDevice) {
            return NThreading::MakeFuture(MakeError(S_OK));
        }

        auto request = std::make_shared<NProto::TStopProxyEndpointRequest>();
        request->SetUnixSocketPath(AddressString);
        return Client->StopProxyEndpoint(std::move(request)).Apply(
            [] (const auto& f) {
                return f.GetValue().GetError();
            });
    }

    NThreading::TFuture<NProto::TError> Resize(ui64 deviceSizeInBytes) override
    {
        auto request = std::make_shared<NProto::TResizeProxyDeviceRequest>();
        request->SetUnixSocketPath(AddressString);
        request->SetDeviceSizeInBytes(deviceSizeInBytes);
        return Client->ResizeProxyDevice(std::move(request))
            .Apply([](const auto& f) { return f.GetValue().GetError(); });
    }

    TString GetPath() const override
    {
        return DevicePath;
    }
};

struct TProxyFactory: NBD::IDeviceFactory
{
    const TProxyDeviceFactoryConfig Config;
    const IEndpointProxyClientPtr Client;

    explicit TProxyFactory(
            TProxyDeviceFactoryConfig config,
            IEndpointProxyClientPtr client)
        : Config(config)
        , Client(std::move(client))
    {}

    NBD::IDevicePtr Create(
        const TNetworkAddress& connectAddress,
        TString devicePath,
        ui64 blockCount,
        ui32 blockSize) override
    {
        return std::make_shared<TProxyDevice>(
            Config,
            Client,
            connectAddress,
            std::move(devicePath),
            "",
            blockCount,
            blockSize);
    }

    NBD::IDevicePtr CreateFree(
        const TNetworkAddress& connectAddress,
        TString devicePrefix,
        ui64 blockCount,
        ui32 blockSize) override
    {
        return std::make_shared<TProxyDevice>(
            Config,
            Client,
            connectAddress,
            "",
            std::move(devicePrefix),
            blockCount,
            blockSize);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NBD::IDeviceFactoryPtr CreateProxyDeviceFactory(
    TProxyDeviceFactoryConfig config,
    IEndpointProxyClientPtr client)
{
    return std::make_shared<TProxyFactory>(config, std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
