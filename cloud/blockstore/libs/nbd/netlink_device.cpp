#include "client_handler.h"
#include "netlink_device.h"
#include "utils.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <linux/nbd-netlink.h>

#include <netlink/genl/ctrl.h>
#include <netlink/genl/genl.h>
#include <netlink/netlink.h>

#include <util/generic/scope.h>
#include <util/stream/mem.h>

namespace NCloud::NBlockStore::NBD {

namespace {

using namespace NThreading;

using TResponseHandler = std::function<int(genlmsghdr*)>;

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf NBD_DEVICE_SUFFIX = "/dev/nbd";

////////////////////////////////////////////////////////////////////////////////

class TNetlinkSocket
{
private:
    nl_sock* Socket;
    int Family;

public:
    TNetlinkSocket()
    {
        Socket = nl_socket_alloc();

        if (Socket == nullptr) {
            throw TServiceError(E_FAIL) << "unable to allocate netlink socket";
        }

        if (int err = genl_connect(Socket)) {
            nl_socket_free(Socket);
            throw TServiceError(E_FAIL)
                << "unable to connect to generic netlink socket: "
                << nl_geterror(err);
        }

        Family = genl_ctrl_resolve(Socket, "nbd");

        if (Family < 0) {
            nl_socket_free(Socket);
            throw TServiceError(E_FAIL)
                << "unable to resolve nbd netlink family: "
                << nl_geterror(Family);
        }
    }

    ~TNetlinkSocket()
    {
        nl_socket_free(Socket);
    }

    int GetFamily() const
    {
        return Family;
    }

    template <typename F>
    void SetCallback(nl_cb_type type, F func)
    {
        auto arg = std::make_unique<TResponseHandler>(std::move(func));

        if (int err = nl_socket_modify_cb(
                Socket,
                type,
                NL_CB_CUSTOM,
                TNetlinkSocket::ResponseHandler,
                arg.get()))
        {
            throw TServiceError(E_FAIL)
                << "unable to set socket callback: " << nl_geterror(err);
        }
        arg.release();
    }

    static int ResponseHandler(nl_msg* msg, void* arg)
    {
        auto func = std::unique_ptr<TResponseHandler>(
            static_cast<TResponseHandler*>(arg));

        return (*func)(static_cast<genlmsghdr*>(nlmsg_data(nlmsg_hdr(msg))));
    }

    void Send(nl_msg* message)
    {
        if (int err = nl_send_auto(Socket, message); err < 0) {
            throw TServiceError(E_FAIL)
                << "send error: " << nl_geterror(err);
        }
        if (int err = nl_wait_for_ack(Socket)) {
            // this is either recv error, or an actual error message received
            // from the kernel
            throw TServiceError(E_FAIL)
                << "recv error: " << nl_geterror(err);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TNestedAttribute
{
private:
    nl_msg* Message;
    nlattr* Attribute;

public:
    TNestedAttribute(nl_msg* message, int attribute)
        : Message(message)
    {
        Attribute = nla_nest_start(message, attribute);
        if (!Attribute) {
            throw TServiceError(E_FAIL) << "unable to nest attribute";
        }
    }

    ~TNestedAttribute()
    {
        nla_nest_end(Message, Attribute);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TNetlinkMessage
{
private:
    nl_msg* Message;

public:
    TNetlinkMessage(int family, int command)
    {
        Message = nlmsg_alloc();
        if (Message == nullptr) {
            throw TServiceError(E_FAIL) << "unable to allocate message";
        }
        genlmsg_put(
            Message,
            NL_AUTO_PORT,
            NL_AUTO_SEQ,
            family,
            0,          // hdrlen
            0,          // flags
            command,
            0);         // version
    }

    ~TNetlinkMessage()
    {
        nlmsg_free(Message);
    }

    operator nl_msg*() const
    {
        return Message;
    }

    template <typename T>
    void Put(int attribute, T data)
    {
        if (int err = nla_put(Message, attribute, sizeof(T), &data)) {
            throw TServiceError(E_FAIL) << "unable to put attribute "
                << attribute << ": " << nl_geterror(err);
        }
    }

    TNestedAttribute Nest(int attribute)
    {
        return TNestedAttribute(Message, attribute);
    }
};

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

    int StatusHandler(genlmsghdr* header);
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

    } catch (const TServiceError& e) {
        StartResult.SetValue(MakeError(
            e.GetCode(),
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
        deleteDevice ? Disconnect() : StopResult.SetValue(MakeError(S_OK));

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
        TNetlinkSocket socket;
        TNetlinkMessage message(socket.GetFamily(), NBD_CMD_RECONFIGURE);

        message.Put(NBD_ATTR_INDEX, DeviceIndex);
        message.Put(NBD_ATTR_SIZE_BYTES, deviceSizeInBytes);

        {
            auto attr = message.Nest(NBD_ATTR_SOCKETS);
            auto item = message.Nest(NBD_SOCK_ITEM);
            message.Put(NBD_SOCK_FD, static_cast<ui32>(Socket));
        }

        socket.Send(message);

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
    TNetlinkSocket socket;
    socket.SetCallback(
        NL_CB_VALID,
        [device = shared_from_this()] (auto* header) {
            return device->StatusHandler(header);
        });

    TNetlinkMessage message(socket.GetFamily(), NBD_CMD_STATUS);
    message.Put(NBD_ATTR_INDEX, DeviceIndex);
    socket.Send(message);
}

void TNetlinkDevice::Disconnect()
{
    STORAGE_INFO("disconnect " << DeviceName);

    TNetlinkSocket socket;
    TNetlinkMessage message(socket.GetFamily(), NBD_CMD_DISCONNECT);
    message.Put(NBD_ATTR_INDEX, DeviceIndex);
    socket.Send(message);
    StopResult.SetValue(MakeError(S_OK));
}

void TNetlinkDevice::DoConnect(bool connected)
{
    try {
        auto command = NBD_CMD_CONNECT;
        if (connected) {
            if (!Reconfigure) {
                throw TServiceError(E_FAIL) << "device is already in use";
            }
            command = NBD_CMD_RECONFIGURE;
            STORAGE_INFO(DeviceName << " is already in use, reconfigure");
        } else {
            STORAGE_INFO("connect " << DeviceName);
        }

        TNetlinkSocket socket;
        TNetlinkMessage message(socket.GetFamily(), command);

        const auto& info = Handler->GetExportInfo();
        message.Put(NBD_ATTR_INDEX, DeviceIndex);
        message.Put(NBD_ATTR_SIZE_BYTES, static_cast<ui64>(info.Size));
        message.Put(
            NBD_ATTR_BLOCK_SIZE_BYTES,
            static_cast<ui64>(info.MinBlockSize));
        message.Put(NBD_ATTR_SERVER_FLAGS, static_cast<ui64>(info.Flags));

        if (RequestTimeout) {
            message.Put(NBD_ATTR_TIMEOUT, RequestTimeout.Seconds());
        }

        if (ConnectionTimeout) {
            message.Put(
                NBD_ATTR_DEAD_CONN_TIMEOUT,
                ConnectionTimeout.Seconds());
        }

        {
            auto attr = message.Nest(NBD_ATTR_SOCKETS);
            auto item = message.Nest(NBD_SOCK_ITEM);
            message.Put(NBD_SOCK_FD, static_cast<ui32>(Socket));
        }

        socket.Send(message);
        StartResult.SetValue(MakeError(S_OK));

    } catch (const TServiceError& e) {
        StartResult.SetValue(MakeError(
            e.GetCode(),
            TStringBuilder()
                << "unable to configure " << DeviceName << ": " << e.what()));
    }
}

int TNetlinkDevice::StatusHandler(genlmsghdr* header)
{
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
