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

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf NBD_DEVICE_SUFFIX = "/dev/nbd";

////////////////////////////////////////////////////////////////////////////////

using NNetlink::TNetlinkHeader;

#pragma pack(push, NLMSG_ALIGNTO)

struct TNbdStatusRequest
{
    TNetlinkHeader Headers;
    ::nlattr DeviceIndexAttr;
    ui32 DeviceIndex;

    TNbdStatusRequest(ui16 familyId, ui32 deviceIndex)
        : Headers{sizeof(TNbdStatusRequest), familyId, NBD_CMD_STATUS}
        , DeviceIndexAttr{sizeof(DeviceIndex) + NLA_HDRLEN, NBD_ATTR_INDEX}
        , DeviceIndex(deviceIndex)
    {}
};

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

struct TNbdConfigureDeviceRequest
{
    TNetlinkHeader Headers;
    ::nlattr DeviceIndexAttr;
    ui32 DeviceIndex;
    ::nlattr DeviceSizeAttr;
    ui64 DeviceSizeInBytes;
    ::nlattr BlockSizeAttr;
    ui64 BlockSizeInBytes;
    ::nlattr ServerFlagsAttr;
    ui64 ServerFlags;
    ::nlattr RequestTimeoutAttr;
    ui64 RequestTimeout;
    ::nlattr ConnectionTimeoutAttr;
    ui64 ConnectionTimeout;
    ::nlattr SocketsAttr;
    ::nlattr SocketItemAttr;
    ::nlattr SocketFdAttr;
    ui32 SocketFd;

    TNbdConfigureDeviceRequest(
            ui16 familyId,
            bool connected,
            ui32 deviceIndex,
            ui64 deviceSizeInBytes,
            ui64 blockSizeInBytes,
            ui64 serverFlags,
            ui64 requestTimeout,
            ui64 connectionTimeout,
            ui32 socketFd)
        : Headers{sizeof(TNbdConfigureDeviceRequest), familyId, static_cast<ui8>(connected ? NBD_CMD_RECONFIGURE : NBD_CMD_CONNECT)}
        , DeviceIndexAttr{sizeof(DeviceIndex) + NLA_HDRLEN, NBD_ATTR_INDEX}
        , DeviceIndex(deviceIndex)
        , DeviceSizeAttr{sizeof(DeviceSizeInBytes) + NLA_HDRLEN, NBD_ATTR_SIZE_BYTES}
        , DeviceSizeInBytes(deviceSizeInBytes)
        , BlockSizeAttr{sizeof(BlockSizeInBytes) + NLA_HDRLEN, NBD_ATTR_BLOCK_SIZE_BYTES}
        , BlockSizeInBytes(blockSizeInBytes)
        , ServerFlagsAttr{sizeof(ServerFlags) + NLA_HDRLEN, NBD_ATTR_SERVER_FLAGS}
        , ServerFlags(serverFlags)
        , RequestTimeoutAttr{sizeof(RequestTimeout) + NLA_HDRLEN, NBD_ATTR_TIMEOUT}
        , RequestTimeout(requestTimeout)
        , ConnectionTimeoutAttr{sizeof(ConnectionTimeout) + NLA_HDRLEN, NBD_ATTR_DEAD_CONN_TIMEOUT}
        , ConnectionTimeout(connectionTimeout)
        // attribute length is calculated as size of payload + number of nested
        // attributes * attribute header length
        , SocketsAttr{sizeof(SocketFd) + 3 * NLA_HDRLEN, NBD_ATTR_SOCKETS}
        , SocketItemAttr{sizeof(SocketFd) + 2 * NLA_HDRLEN, NBD_SOCK_ITEM}
        , SocketFdAttr{sizeof(SocketFd) + NLA_HDRLEN, NBD_SOCK_FD}
        , SocketFd(socketFd)
    {}
};

struct TNbdDisconnectRequest
{
    TNetlinkHeader Headers;
    ::nlattr DeviceIndexAttr;
    ui32 DeviceIndex;

    TNbdDisconnectRequest(ui16 familyId, ui32 deviceIndex)
        : Headers{sizeof(TNbdStatusRequest), familyId, NBD_CMD_DISCONNECT}
        , DeviceIndexAttr{sizeof(DeviceIndex) + NLA_HDRLEN, NBD_ATTR_INDEX}
        , DeviceIndex(deviceIndex)
    {}
};

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

class TNetlinkDevice final
    : public IDevice
    , public std::enable_shared_from_this<TNetlinkDevice>
{
private:
    const ILoggingServicePtr Logging;
    const TNetworkAddress ConnectAddress;
    const TString DeviceName;
    const TDuration RequestTimeout;
    const TDuration ConnectionTimeout;
    const bool Reconfigure;

    TLog Log;
    IClientHandlerPtr Handler;
    TSocket Socket;
    ui32 DeviceIndex;

    TPromise<NProto::TError> StartResult;
    TPromise<NProto::TError> StopResult;

public:
    TNetlinkDevice(
        ILoggingServicePtr logging,
        TNetworkAddress connectAddress,
        TString deviceName,
        TDuration requestTimeout,
        TDuration connectionTimeout,
        bool reconfigure);

    ~TNetlinkDevice();

    TFuture<NProto::TError> Start() override;
    TFuture<NProto::TError> Stop(bool deleteDevice) override;
    TFuture<NProto::TError> Resize(ui64 deviceSizeInBytes) override;

private:
    void ParseIndex();

    void ConnectSocket();
    void DisconnectSocket();

    void Connect();
    void Disconnect();
    void DoConnect(bool connected);

    ui16 GetFamilyId();
};

////////////////////////////////////////////////////////////////////////////////

TNetlinkDevice::TNetlinkDevice(
        ILoggingServicePtr logging,
        TNetworkAddress connectAddress,
        TString deviceName,
        TDuration requestTimeout,
        TDuration connectionTimeout,
        bool reconfigure)
    : Logging(std::move(logging))
    , ConnectAddress(std::move(connectAddress))
    , DeviceName(std::move(deviceName))
    , RequestTimeout(requestTimeout)
    , ConnectionTimeout(connectionTimeout)
    , Reconfigure(reconfigure)
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
        ParseIndex();
        ConnectSocket();
        Connect();

    } catch (const std::exception& e) {
        StartResult.SetValue(MakeError(
            E_FAIL,
            TStringBuilder()
                << "unable to configure " << DeviceName << ": " << e.what()));
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
                << "unable to disconnect " << DeviceName << ": " << e.what()));
    }

    return StopResult.GetFuture();
}

TFuture<NProto::TError> TNetlinkDevice::Resize(ui64 deviceSizeInBytes)
{
    try {
        const auto& info = Handler->GetExportInfo();
        NNetlink::TNetlinkSocket socket;
        TNbdConfigureDeviceRequest request(
            GetFamilyId(),
            true,
            DeviceIndex,
            deviceSizeInBytes,
            static_cast<ui64>(info.MinBlockSize),
            static_cast<ui64>(info.Flags),
            RequestTimeout.Seconds(),
            ConnectionTimeout.Seconds(),
            static_cast<ui32>(Socket));
        socket.Send(request);
        NNetlink::TNetlinkResponse<> response;
        socket.Receive(response);
    } catch (const TServiceError& e) {
        return MakeFuture(MakeError(
            e.GetCode(),
            TStringBuilder()
                << "unable to resize " << DeviceName << ": " << e.what()));
    }

    return MakeFuture(MakeError(S_OK));
}

void TNetlinkDevice::ParseIndex()
{
    // accept dev/nbd* devices with prefix other than /
    TStringBuf l, r;
    TStringBuf(DeviceName).RSplit(NBD_DEVICE_SUFFIX, l, r);

    if (!TryFromString(r, DeviceIndex)) {
        throw TServiceError(E_ARGUMENT) << "unable to parse device index";
    }
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

// queries device status and configure/reconfigure device
void TNetlinkDevice::Connect()
{
    try {
        NNetlink::TNetlinkSocket socket;
        TNbdStatusRequest request(GetFamilyId(), DeviceIndex);
        socket.Send(request);
        NNetlink::TNetlinkResponse<TNbdStatusResponse> response;
        socket.Receive(response);
        DoConnect(response.Msg.Connected);
    } catch (const TServiceError& e) {
        StartResult.SetValue(MakeError(
            e.GetCode(),
            TStringBuilder()
                << "unable to configure " << DeviceName << ": " << e.what()));
    }
}

void TNetlinkDevice::Disconnect()
{
    STORAGE_INFO("disconnect " << DeviceName);
    try {
        NNetlink::TNetlinkSocket socket;
        TNbdDisconnectRequest request(GetFamilyId(), DeviceIndex);
        socket.Send(request);
        NNetlink::TNetlinkResponse<> response;
        socket.Receive(response);
        StopResult.SetValue(MakeError(S_OK));
    } catch (const TServiceError& e) {
        StopResult.SetValue(MakeError(
            e.GetCode(),
            TStringBuilder()
                << "unable to disconnect " << DeviceName << ": " << e.what()));
    }
}

void TNetlinkDevice::DoConnect(bool connected)
{
    try {
        if (connected) {
            if (!Reconfigure) {
                throw TServiceError(E_FAIL) << "device is already in use";
            }
            STORAGE_INFO("reconfigure " << DeviceName);
        } else {
            STORAGE_INFO("connect " << DeviceName);
        }
        const auto& info = Handler->GetExportInfo();
        NNetlink::TNetlinkSocket socket;
        TNbdConfigureDeviceRequest request(
            GetFamilyId(),
            connected,
            DeviceIndex,
            static_cast<ui64>(info.Size),
            static_cast<ui64>(info.MinBlockSize),
            static_cast<ui64>(info.Flags),
            RequestTimeout.Seconds(),
            ConnectionTimeout.Seconds(),
            static_cast<ui32>(Socket));
        socket.Send(request);
        NNetlink::TNetlinkResponse<> response;
        socket.Receive(response);
        StartResult.SetValue(MakeError(S_OK));
    } catch (const TServiceError& e) {
        StartResult.SetValue(MakeError(
            e.GetCode(),
            TStringBuilder()
                << "unable to configure " << DeviceName << ": " << e.what()));
    }
}

ui16 TNetlinkDevice::GetFamilyId()
{
    static ui16 familyId = NNetlink::GetFamilyId(NBD_GENL_FAMILY_NAME);
    return familyId;
}

////////////////////////////////////////////////////////////////////////////////

class TNetlinkDeviceFactory final
    : public IDeviceFactory
{
private:
    const ILoggingServicePtr Logging;
    const TDuration RequestTimeout;
    const TDuration ConnectionTimeout;
    const bool Reconfigure;

public:
    TNetlinkDeviceFactory(
            ILoggingServicePtr logging,
            TDuration requestTimeout,
            TDuration connectionTimeout,
            bool reconfigure)
        : Logging(std::move(logging))
        , RequestTimeout(requestTimeout)
        , ConnectionTimeout(connectionTimeout)
        , Reconfigure(reconfigure)
    {}

    IDevicePtr Create(
        const TNetworkAddress& connectAddress,
        TString deviceName,
        ui64 blockCount,
        ui32 blockSize) override
    {
        Y_UNUSED(blockCount);
        Y_UNUSED(blockSize);

        return CreateNetlinkDevice(
            Logging,
            connectAddress,
            std::move(deviceName),
            RequestTimeout,
            ConnectionTimeout,
            Reconfigure);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDevicePtr CreateNetlinkDevice(
    ILoggingServicePtr logging,
    TNetworkAddress connectAddress,
    TString deviceName,
    TDuration requestTimeout,
    TDuration connectionTimeout,
    bool reconfigure)
{
    return std::make_shared<TNetlinkDevice>(
        std::move(logging),
        std::move(connectAddress),
        std::move(deviceName),
        requestTimeout,
        connectionTimeout,
        reconfigure);
}

IDeviceFactoryPtr CreateNetlinkDeviceFactory(
    ILoggingServicePtr logging,
    TDuration requestTimeout,
    TDuration connectionTimeout,
    bool reconfigure)
{
    return std::make_shared<TNetlinkDeviceFactory>(
        std::move(logging),
        requestTimeout,
        connectionTimeout,
        reconfigure);
}

}   // namespace NCloud::NBlockStore::NBD
