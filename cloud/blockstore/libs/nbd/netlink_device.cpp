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

////////////////////////////////////////////////////////////////////////////////

void ValidateAttribute(const ::nlattr& attribute, ui16 expectedAttribute)
{
    if (attribute.nla_type != expectedAttribute) {
        throw yexception() << "Invalid attribute type: " << attribute.nla_type
                           << " Expected attribute type: " << expectedAttribute;
    }
}

////////////////////////////////////////////////////////////////////////////////

// Documentation:
// https://github.com/torvalds/linux/blob/master/Documentation/accounting/taskstats.rst

#pragma pack(push, NLMSG_ALIGNTO)

struct TNbdFamilyIdRequest
{
    ::nlmsghdr MessageHeader =
        {sizeof(TNbdFamilyIdRequest), GENL_ID_CTRL, NLM_F_REQUEST, 0, 0};
    ::genlmsghdr GenericHeader = {CTRL_CMD_GETFAMILY, 1, 0};
    ::nlattr FamilyNameAttr = {
        sizeof(FamilyName) + NLA_HDRLEN,
        CTRL_ATTR_FAMILY_NAME};
    const char FamilyName[sizeof(NBD_GENL_FAMILY_NAME)] = NBD_GENL_FAMILY_NAME;
};

struct TNbdFamilyIdResponse
{
    ::nlmsghdr MessageHeader;
    ::genlmsghdr GenericHeader;
    ::nlattr FamilyNameAttr;
    char FamilyName[sizeof(NBD_GENL_FAMILY_NAME)];
    alignas(NLMSG_ALIGNTO)::nlattr FamilyIdAttr;
    ui16 FamilyId;

    void Validate()
    {
        ValidateAttribute(FamilyNameAttr, CTRL_ATTR_FAMILY_NAME);
        ValidateAttribute(FamilyIdAttr, CTRL_ATTR_FAMILY_ID);
    }
};

struct TNbdStatusRequest
{
    ::nlmsghdr MessageHeader;
    ::genlmsghdr GenericHeader;
    ::nlattr DeviceIndexAttr;
    ui32 DeviceIndex;

    TNbdStatusRequest(ui16 familyId, ui32 deviceIndex)
        : MessageHeader{sizeof(TNbdStatusRequest), familyId, NLM_F_REQUEST, 0, 0}
        , GenericHeader{NBD_CMD_STATUS, 1, 0}
        , DeviceIndexAttr{sizeof(DeviceIndex) + NLA_HDRLEN, NBD_ATTR_INDEX}
        , DeviceIndex(deviceIndex)
    {}
};

struct TNbdStatusResponse {
    ::nlmsghdr MessageHeader;
    ::genlmsghdr GenericHeader;

    ::nlattr NbdDeviceListAttr; //NBD_ATTR_DEVICE_LIST
    ::nlattr NbdDeviceItemAttr; // NBD_DEVICE_ITEM
    ::nlattr NbdDeviceIndex;
    ui32 Index;
    ::nlattr NbdDeviceConnectedAttr; // NBD_DEVICE_CONNECTED
    ui8 Connected;
};

struct TNbdConfigureDeviceRequest
{
    ::nlmsghdr MessageHeader;
    ::genlmsghdr GenericHeader;
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
        : MessageHeader{sizeof(TNbdConfigureDeviceRequest), familyId, NLM_F_REQUEST, 0, 0}
        , GenericHeader{static_cast<ui8>(connected ? NBD_CMD_RECONFIGURE : NBD_CMD_CONNECT), 1, 0}
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
        , SocketsAttr{sizeof(SocketFd) + 3 * NLA_HDRLEN, NBD_ATTR_SOCKETS}
        , SocketItemAttr{sizeof(SocketFd) + 2 * NLA_HDRLEN, NBD_SOCK_ITEM}
        , SocketFdAttr{sizeof(SocketFd) + NLA_HDRLEN, NBD_SOCK_FD}
        , SocketFd(socketFd)
    {}
};

struct TNbdConfigureDeviceResponse
{
    ::nlmsghdr MessageHeader;
    ::genlmsghdr GenericHeader;
};

struct TNetlinkError {
    ::nlmsghdr MessageHeader;
    ::nlmsgerr Error;
};

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

template <typename T, size_t MaxMsgSize = 1024>
union TNetlinkResponse {
    T Msg;
    TNetlinkError Error;
    ui8 Buffer[MaxMsgSize];

    TNetlinkResponse() {
        static_assert(sizeof(T) < MaxMsgSize);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TNtlnkSocket
{
private:
    TSocket Socket;
    ui32 SocketTimeoutMs = 100;

public:
    TNtlnkSocket(ui32 socketTimeoutMs = 100)
        : Socket(::socket(PF_NETLINK, SOCK_RAW, NETLINK_GENERIC))
        , SocketTimeoutMs(socketTimeoutMs)
    {
        if (Socket < 0) {
            throw yexception() << "Failed to create netlink socket";
        }
        Socket.SetSocketTimeout(0, SocketTimeoutMs);
    }

    template <typename TNetlinkMessage>
    void Send(const TNetlinkMessage& msg)
    {
        auto ret = Socket.Send(&msg, sizeof(msg));
        if (ret == -1) {
            throw yexception()
                << "Failed to send netlink message: " << strerror(errno);
        }
    }

    template <typename T>
    void Receive(TNetlinkResponse<T>& response)
    {
        auto ret = Socket.Recv(&response, sizeof(response));
        if (ret < 0) {
            throw yexception()
                << "Failed to receive netlink message: " << strerror(errno);
        }

        if (response.Error.MessageHeader.nlmsg_type == NLMSG_ERROR) {
            throw yexception()
                << "Failed to receive netlink message: kernel returned error: "
                << response.Error.Error.error << " message size: " << ret;
        }

        if (!NLMSG_OK(&response.Msg.MessageHeader, ret)) {
            throw yexception()
                << "Failed to parse netlink message: incorrect format";
        }
        return;
    }
};

ui16 GetFamilyId()
{
    TNtlnkSocket socket;
    socket.Send(TNbdFamilyIdRequest());
    TNetlinkResponse<TNbdFamilyIdResponse> response;
    socket.Receive(response);
    response.Msg.Validate();
    return response.Msg.FamilyId;
}

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf NBD_DEVICE_SUFFIX = "/dev/nbd";

////////////////////////////////////////////////////////////////////////////////

class TNetlinkDevice final
    : public IDevice
    , public std::enable_shared_from_this<TNetlinkDevice>
{
    using TNetlinkMessage = NCloud::NNetlink::TMessage;
    using TNetlinkSocket = NCloud::NNetlink::TSocket;
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

    int StatusHandler(nl_msg* nlmsg);
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
        TNtlnkSocket socket;
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
        TNetlinkResponse<TNbdConfigureDeviceRequest> response;
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

// queries device status eand registers callback that will connect
// or reconfigure (if Reconfigure == true) specified device
void TNetlinkDevice::Connect()
{
    try {
        TNtlnkSocket socket;
        TNbdStatusRequest request(GetFamilyId(), DeviceIndex);
        socket.Send(request);
        TNetlinkResponse<TNbdStatusResponse> response;
        socket.Receive(response);
        DoConnect(response.Msg.Connected);
    } catch (const TServiceError& e) {
        StartResult.SetValue(MakeError(
            e.GetCode(),
            TStringBuilder()
                << "unable to configure " << DeviceName << ": " << e.what()));
    } catch (std::exception& e) {
        StartResult.SetValue(MakeError(
            E_FAIL,
            TStringBuilder()
                << "unable to configure " << DeviceName << ": " << e.what()));
    }
}

void TNetlinkDevice::Disconnect()
{
    STORAGE_INFO("disconnect " << DeviceName);

    TNetlinkSocket socket("nbd");
    TNetlinkMessage message(socket.GetFamily(), NBD_CMD_DISCONNECT);
    message.Put(NBD_ATTR_INDEX, DeviceIndex);
    socket.Send(message);
    StopResult.SetValue(MakeError(S_OK));
}

void TNetlinkDevice::DoConnect(bool connected)
{
    try {
        if (connected && !Reconfigure) {
            throw TServiceError(E_FAIL) << "device is already in use";
        }
        const auto& info = Handler->GetExportInfo();
        TNtlnkSocket socket;
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
        TNetlinkResponse<TNbdConfigureDeviceResponse> response;
        socket.Receive(response);
        StartResult.SetValue(MakeError(S_OK));

    } catch (const TServiceError& e) {
        STORAGE_ERROR("unable to configure device: " << e.what());
        StartResult.SetValue(MakeError(
            e.GetCode(),
            TStringBuilder()
                << "unable to configure " << DeviceName << ": " << e.what()));
    }
}

int TNetlinkDevice::StatusHandler(nl_msg* nlmsg)
{
    auto header = static_cast<genlmsghdr*>(nlmsg_data(nlmsg_hdr(nlmsg)));
    nlattr* attr[NBD_ATTR_MAX + 1] = {};
    nlattr* deviceItem[NBD_DEVICE_ITEM_MAX + 1] = {};
    nlattr* device[NBD_DEVICE_ATTR_MAX + 1] = {};

    nla_policy deviceItemPolicy[NBD_DEVICE_ITEM_MAX + 1] = {};
    deviceItemPolicy[NBD_DEVICE_ITEM].type = NLA_NESTED;

    nla_policy devicePolicy[NBD_DEVICE_ATTR_MAX + 1] = {};
    devicePolicy[NBD_DEVICE_INDEX].type = NLA_U32;
    devicePolicy[NBD_DEVICE_CONNECTED].type = NLA_U8;

    if (int err = nla_parse(
            attr,
            NBD_ATTR_MAX,
            genlmsg_attrdata(header, 0),
            genlmsg_attrlen(header, 0),
            NULL))
    {
        StartResult.SetValue(MakeError(
            E_FAIL,
            TStringBuilder() << "unable to parse NBD_CMD_STATUS response: "
                             << nl_geterror(err)));
        return NL_STOP;
    }

    if (!attr[NBD_ATTR_DEVICE_LIST]) {
        StartResult.SetValue(MakeError(
            E_FAIL,
            "did not receive NBD_ATTR_DEVICE_LIST"));
        return NL_STOP;
    }

    if (int err = nla_parse_nested(
            deviceItem,
            NBD_DEVICE_ITEM_MAX,
            attr[NBD_ATTR_DEVICE_LIST],
            deviceItemPolicy))
    {
        StartResult.SetValue(MakeError(
            E_FAIL,
            TStringBuilder() << "unable to parse NBD_ATTR_DEVICE_LIST: "
                             << nl_geterror(err)));
        return NL_STOP;
    }

    if (!deviceItem[NBD_DEVICE_ITEM]) {
        StartResult.SetValue(MakeError(
            E_FAIL,
            "did not receive NBD_DEVICE_ITEM"));
        return NL_STOP;
    }

    if (int err = nla_parse_nested(
            device,
            NBD_DEVICE_ATTR_MAX,
            deviceItem[NBD_DEVICE_ITEM],
            devicePolicy))
    {
        StartResult.SetValue(MakeError(
            E_FAIL,
            TStringBuilder() << "unable to parse NBD_DEVICE_ITEM: "
                             << nl_geterror(err)));
        return NL_STOP;
    }

    if (!device[NBD_DEVICE_CONNECTED]) {
        StartResult.SetValue(MakeError(
            E_FAIL,
            "did not receive NBD_DEVICE_CONNECTED"));
        return NL_STOP;
    }

    DoConnect(nla_get_u8(device[NBD_DEVICE_CONNECTED]));
    return NL_OK;
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
