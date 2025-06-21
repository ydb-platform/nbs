#include "client_handler.h"
#include "netlink_device.h"
#include "utils.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/netlink/socket.h>

#include <linux/nbd-netlink.h>

#include <util/generic/scope.h>
#include <util/stream/mem.h>

namespace NCloud::NBlockStore::NBD {

namespace {

using namespace NThreading;
using namespace NNetlink;

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf NBD_DEVICE_PREFIX = "/dev/nbd";

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, NLMSG_ALIGNTO)

using TNbdStatusRequest = TNetlinkRequest<
    TNetlinkAttribute<NBD_ATTR_INDEX, ui32>>;

struct TNbdStatusResponse {
    TNetlinkHeader Headers;
    ::nlattr NbdDeviceListAttr;
    ::nlattr NbdDeviceItemAttr;
    ::nlattr NbdDeviceIndex;
    ui32 Index;
    ::nlattr NbdDeviceConnectedAttr;
    ui8 Connected;

    void Validate()
    {
        NNetlink::ValidateAttribute(NbdDeviceListAttr, NBD_ATTR_DEVICE_LIST);
        NNetlink::ValidateAttribute(NbdDeviceItemAttr, NBD_DEVICE_ITEM);
        NNetlink::ValidateAttribute(NbdDeviceIndex, NBD_DEVICE_INDEX);
    }
};

using TNbdConfigureRequest = TNetlinkRequest<
    TNetlinkAttribute<NBD_ATTR_INDEX, ui32>,
    TNetlinkAttribute<NBD_ATTR_SIZE_BYTES, ui64>,
    TNetlinkAttribute<NBD_ATTR_BLOCK_SIZE_BYTES, ui64>,
    TNetlinkAttribute<NBD_ATTR_SERVER_FLAGS, ui64>,
    TNetlinkAttribute<NBD_ATTR_TIMEOUT, ui64>,
    TNetlinkAttribute<NBD_ATTR_DEAD_CONN_TIMEOUT, ui64>,
    TNetlinkAttribute<NBD_ATTR_SOCKETS,
        TNetlinkAttribute<NBD_SOCK_ITEM,
            TNetlinkAttribute<NBD_SOCK_FD, ui32>>>>;

using TNbdConfigureFreeRequest = TNetlinkRequest<
    TNetlinkAttribute<NBD_ATTR_SIZE_BYTES, ui64>,
    TNetlinkAttribute<NBD_ATTR_BLOCK_SIZE_BYTES, ui64>,
    TNetlinkAttribute<NBD_ATTR_SERVER_FLAGS, ui64>,
    TNetlinkAttribute<NBD_ATTR_TIMEOUT, ui64>,
    TNetlinkAttribute<NBD_ATTR_DEAD_CONN_TIMEOUT, ui64>,
    TNetlinkAttribute<NBD_ATTR_SOCKETS,
        TNetlinkAttribute<NBD_SOCK_ITEM,
            TNetlinkAttribute<NBD_SOCK_FD, ui32>>>>;

struct TNbdConfigureResponse {
    TNetlinkHeader Header;
    ::nlattr IndexAttr;
    ui32 Index;

    void Validate()
    {
        NNetlink::ValidateAttribute(IndexAttr, NBD_ATTR_INDEX);
    }
};

using TNbdDisconnectRequest = TNetlinkRequest<
    TNetlinkAttribute<NBD_ATTR_INDEX, ui32>>;

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

class TNetlinkDevice final
    : public IDevice
{
private:
    const ui16 FamilyId;
    const ILoggingServicePtr Logging;
    const TNetworkAddress ConnectAddress;
    const TString DevicePath;
    const TString DevicePrefix;
    const TDuration RequestTimeout;
    const TDuration ConnectionTimeout;

    TLog Log;
    IClientHandlerPtr Handler;
    TSocket Socket;
    std::optional<ui32> DeviceIndex;

    TPromise<NProto::TError> StartResult;
    TPromise<NProto::TError> StopResult;

public:
    TNetlinkDevice(
        ILoggingServicePtr logging,
        TNetworkAddress connectAddress,
        TString devicePath,
        TString devicePrefix,
        TDuration requestTimeout,
        TDuration connectionTimeout);

    ~TNetlinkDevice();

    TFuture<NProto::TError> Start() override;
    TFuture<NProto::TError> Stop(bool deleteDevice) override;
    TFuture<NProto::TError> Resize(ui64 deviceSizeInBytes) override;
    TString GetPath() const override;

private:
    TString GetDevice() const;

    void ParseIndex();

    void ConnectSocket();
    void DisconnectSocket();

    void Configure();
    void ConfigureFree();
    void Disconnect();
};

////////////////////////////////////////////////////////////////////////////////

TNetlinkDevice::TNetlinkDevice(
        ILoggingServicePtr logging,
        TNetworkAddress connectAddress,
        TString devicePath,
        TString devicePrefix,
        TDuration requestTimeout,
        TDuration connectionTimeout)
    : FamilyId(NNetlink::GetFamilyId(NBD_GENL_FAMILY_NAME))
    , Logging(std::move(logging))
    , ConnectAddress(std::move(connectAddress))
    , DevicePath(std::move(devicePath))
    , DevicePrefix(std::move(devicePrefix))
    , RequestTimeout(requestTimeout)
    , ConnectionTimeout(connectionTimeout)
{
    Log = Logging->CreateLog("BLOCKSTORE_NBD");
}

TNetlinkDevice::~TNetlinkDevice()
{
    Stop(false).GetValueSync();
}

TFuture<NProto::TError> TNetlinkDevice::Start()
{
    if (StartResult.Initialized()) {
        return StartResult.GetFuture();
    }
    StartResult = NewPromise<NProto::TError>();

    try {
        ConnectSocket();
        if (DevicePath) {
            ParseIndex();
            Configure();
        } else {
            ConfigureFree();
        }

    } catch (const std::exception& e) {
        StartResult.SetValue(MakeError(
            E_FAIL,
            TStringBuilder()
                << "unable to configure " << GetDevice() << ": " << e.what()));
    }

    return StartResult.GetFuture();
}

TFuture<NProto::TError> TNetlinkDevice::Stop(bool deleteDevice)
{
    if (StopResult.Initialized()) {
        return StopResult.GetFuture();
    }
    StopResult = NewPromise<NProto::TError>();

    try {
        DisconnectSocket();

        if (deleteDevice) {
            Disconnect();
        } else {
            StopResult.SetValue(MakeError(S_OK));
        }

    } catch (const TServiceError& e) {
        StopResult.SetValue(MakeError(
            e.GetCode(),
            TStringBuilder()
                << "unable to disconnect " << GetDevice() << ": " << e.what()));
    }

    return StopResult.GetFuture();
}

TString TNetlinkDevice::GetDevice() const
{
    if (DeviceIndex) {
        return TStringBuilder() << "nbd" << *DeviceIndex;
    }
    return "nbd device";
}

void TNetlinkDevice::ParseIndex()
{
    // accept dev/nbd* devices with prefix other than /
    TStringBuf l, r;
    TStringBuf(DevicePath).RSplit(NBD_DEVICE_PREFIX, l, r);

    ui32 index;
    if (!TryFromString(r, index)) {
        throw TServiceError(E_ARGUMENT) << "unable to parse device index";
    }
    DeviceIndex = index;
}

void TNetlinkDevice::ConnectSocket()
{
    STORAGE_DEBUG("connect socket");

    TSocket socket(ConnectAddress);
    if (IsTcpAddress(ConnectAddress)) {
        socket.SetNoDelay(true);
    }

    TSocketInput in(socket);
    TSocketOutput out(socket);

    Handler = CreateClientHandler(Logging);
    Y_ENSURE(Handler->NegotiateClient(in, out));

    Socket = socket;
}

void TNetlinkDevice::DisconnectSocket()
{
    STORAGE_DEBUG("disconnect socket");

    Socket.Close();
}

// query device status and connect or reconfigure it
void TNetlinkDevice::Configure()
{
    NNetlink::TNetlinkSocket socket;

    TNbdStatusRequest request(FamilyId, NBD_CMD_STATUS, *DeviceIndex);
    socket.Send(request);
    NNetlink::TNetlinkResponse<TNbdStatusResponse> status;
    socket.Receive(status);
    STORAGE_INFO("query " << GetDevice());

    const auto& info = Handler->GetExportInfo();
    socket.Send(
        TNbdConfigureRequest(
            FamilyId,
            status.Msg.Connected ? NBD_CMD_RECONFIGURE : NBD_CMD_CONNECT,
            *DeviceIndex,
            static_cast<ui64>(info.Size),
            static_cast<ui64>(info.MinBlockSize),
            static_cast<ui64>(info.Flags),
            RequestTimeout.Seconds(),
            ConnectionTimeout.Seconds(),
            TNetlinkAttribute<
                NBD_SOCK_ITEM,
                TNetlinkAttribute<
                    NBD_SOCK_FD,
                    ui32>>(static_cast<ui32>(Socket))));

    NNetlink::TNetlinkResponse<> configure;
    socket.Receive(configure);

    StartResult.SetValue(MakeError(S_OK));
    STORAGE_INFO("configure " << GetDevice());
}

// connect any free device
void TNetlinkDevice::ConfigureFree()
{
    NNetlink::TNetlinkSocket socket;

    const auto& info = Handler->GetExportInfo();
    socket.Send(
        TNbdConfigureFreeRequest(
            FamilyId,
            NBD_CMD_CONNECT,
            static_cast<ui64>(info.Size),
            static_cast<ui64>(info.MinBlockSize),
            static_cast<ui64>(info.Flags),
            RequestTimeout.Seconds(),
            ConnectionTimeout.Seconds(),
            TNetlinkAttribute<
                NBD_SOCK_ITEM,
                TNetlinkAttribute<
                    NBD_SOCK_FD,
                    ui32>>(static_cast<ui32>(Socket))));

    NNetlink::TNetlinkResponse<TNbdConfigureResponse> configure;
    socket.Receive(configure);
    DeviceIndex = configure.Msg.Index;

    StartResult.SetValue(MakeError(S_OK));
    STORAGE_INFO("configure " << GetDevice())
}

void TNetlinkDevice::Disconnect()
{
    try {
        NNetlink::TNetlinkSocket socket;
        TNbdDisconnectRequest request(
            FamilyId,
            NBD_CMD_DISCONNECT,
            *DeviceIndex);
        socket.Send(request);
        NNetlink::TNetlinkResponse<> response;
        socket.Receive(response);
        StopResult.SetValue(MakeError(S_OK));
        STORAGE_INFO("disconnect " << GetDevice());
    } catch (const TServiceError& e) {
        StartResult.SetValue(MakeError(
            e.GetCode(),
            TStringBuilder()
                << "unable to disconnect " << GetDevice() << ": " << e.what()));
    }
}

TFuture<NProto::TError> TNetlinkDevice::Resize(ui64 deviceSizeInBytes)
{
    try {
        const auto& info = Handler->GetExportInfo();
        NNetlink::TNetlinkSocket socket;
        socket.Send(
            TNbdConfigureRequest(
                FamilyId,
                NBD_CMD_RECONFIGURE,
                *DeviceIndex,
                deviceSizeInBytes,
                static_cast<ui64>(info.MinBlockSize),
                static_cast<ui64>(info.Flags),
                RequestTimeout.Seconds(),
                ConnectionTimeout.Seconds(),
                TNetlinkAttribute<
                    NBD_SOCK_ITEM,
                    TNetlinkAttribute<
                        NBD_SOCK_FD,
                        ui32>>(static_cast<ui32>(Socket))));
        NNetlink::TNetlinkResponse<> response;
        socket.Receive(response);
        STORAGE_INFO("resize " << GetDevice());
    } catch (const TServiceError& e) {
        return MakeFuture(MakeError(
            e.GetCode(),
            TStringBuilder()
                << "unable to resize " << GetDevice() << ": " << e.what()));
    }

    return MakeFuture(MakeError(S_OK));
}

TString TNetlinkDevice::GetPath() const
{
    if (DevicePath) {
        return DevicePath;
    }
    if (DeviceIndex) {
        return TStringBuilder() << DevicePrefix << *DeviceIndex;
    }
    return "nbd device";
}

////////////////////////////////////////////////////////////////////////////////

class TNetlinkDeviceFactory final
    : public IDeviceFactory
{
private:
    const ILoggingServicePtr Logging;
    const TDuration RequestTimeout;
    const TDuration ConnectionTimeout;

public:
    TNetlinkDeviceFactory(
            ILoggingServicePtr logging,
            TDuration requestTimeout,
            TDuration connectionTimeout)
        : Logging(std::move(logging))
        , RequestTimeout(requestTimeout)
        , ConnectionTimeout(connectionTimeout)
    {}

    IDevicePtr Create(
        const TNetworkAddress& connectAddress,
        TString devicePath,
        ui64 blockCount,
        ui32 blockSize) override
    {
        Y_UNUSED(blockCount);
        Y_UNUSED(blockSize);

        return std::make_shared<TNetlinkDevice>(
            Logging,
            connectAddress,
            std::move(devicePath),
            "",
            RequestTimeout,
            ConnectionTimeout);
    }

    IDevicePtr CreateFree(
        const TNetworkAddress& connectAddress,
        TString devicePrefix,
        ui64 blockCount,
        ui32 blockSize) override
    {
        Y_UNUSED(blockCount);
        Y_UNUSED(blockSize);

        return std::make_shared<TNetlinkDevice>(
            Logging,
            connectAddress,
            "",
            std::move(devicePrefix),
            RequestTimeout,
            ConnectionTimeout);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDevicePtr CreateNetlinkDevice(
    ILoggingServicePtr logging,
    TNetworkAddress connectAddress,
    TString devicePath,
    TDuration requestTimeout,
    TDuration connectionTimeout)
{
    return std::make_shared<TNetlinkDevice>(
        std::move(logging),
        std::move(connectAddress),
        std::move(devicePath),
        "",
        requestTimeout,
        connectionTimeout);
}

IDevicePtr CreateFreeNetlinkDevice(
    ILoggingServicePtr logging,
    TNetworkAddress connectAddress,
    TString devicePrefix,
    TDuration requestTimeout,
    TDuration connectionTimeout)
{
    return std::make_shared<TNetlinkDevice>(
        std::move(logging),
        std::move(connectAddress),
        "",
        std::move(devicePrefix),
        requestTimeout,
        connectionTimeout);
}

IDeviceFactoryPtr CreateNetlinkDeviceFactory(
    ILoggingServicePtr logging,
    TDuration requestTimeout,
    TDuration connectionTimeout)
{
    return std::make_shared<TNetlinkDeviceFactory>(
        std::move(logging),
        requestTimeout,
        connectionTimeout);
}

}   // namespace NCloud::NBlockStore::NBD
