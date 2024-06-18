#include "client_handler.h"
#include "netlink_device.h"
#include "utils.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <linux/nbd-netlink.h>

#include <netlink/genl/ctrl.h>
#include <netlink/genl/genl.h>
#include <netlink/netlink.h>

#include <util/generic/scope.h>

namespace NCloud::NBlockStore::NBD {

namespace {

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

        if (genl_connect(Socket)) {
            nl_socket_free(Socket);
            throw TServiceError(E_FAIL)
                << "unable to connect to generic netlink socket";
        }

        Family = genl_ctrl_resolve(Socket, "nbd");
        if (Family < 0) {
            nl_socket_free(Socket);
            throw TServiceError(E_FAIL)
                << "unable to resolve nbd netlink "
                   "family, make sure nbd module is loaded";
        }
    }

    ~TNetlinkSocket()
    {
        nl_socket_free(Socket);
    }

    operator nl_sock*() const
    {
        return Socket;
    }

    int GetFamily() const
    {
        return Family;
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
        if (Attribute) {
            nla_nest_end(Message, Attribute);
        }
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
        if (Message) {
            nlmsg_free(Message);
        }
    }

    template <typename T>
    void Put(int attribute, T data)
    {
        if (nla_put(Message, attribute, sizeof(T), &data) < 0) {
            throw TServiceError(E_FAIL) << "unable to put attribute " << attribute;
        }
    }

    TNestedAttribute Nest(int attribute)
    {
        return TNestedAttribute(Message, attribute);
    }

    void Send(nl_sock* socket)
    {
        // send will free message even if it fails
        auto* message = Message;
        Message = nullptr;
        if (nl_send_sync(socket, message) < 0) {
            throw TServiceError(E_FAIL) << "unable to send message";
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TNetlinkDevice final
    : public IDevice
{
private:
    const ILoggingServicePtr Logging;
    const TNetworkAddress ConnectAddress;
    const TString DeviceName;
    const TDuration Timeout;
    const TDuration DeadConnectionTimeout;
    const bool Reconfigure;

    TLog Log;
    IClientHandlerPtr Handler;
    TSocket Socket;
    ui32 DeviceIndex;

    TAtomic ShouldStop = 0;

public:
    TNetlinkDevice(
            ILoggingServicePtr logging,
            TNetworkAddress connectAddress,
            TString deviceName,
            TDuration timeout,
            TDuration deadConnectionTimeout,
            bool reconfigure)
        : Logging(std::move(logging))
        , ConnectAddress(std::move(connectAddress))
        , DeviceName(std::move(deviceName))
        , Timeout(timeout)
        , DeadConnectionTimeout(deadConnectionTimeout)
        , Reconfigure(reconfigure)
    {
        Log = Logging->CreateLog("BLOCKSTORE_NBD");

        if (sscanf(DeviceName.c_str(), "/dev/nbd%u", &DeviceIndex) != 1) {
            throw TServiceError(E_ARGUMENT) << "invalid nbd device target";
        }
    }

    ~TNetlinkDevice()
    {
        Stop(false);
    }

    NThreading::TFuture<NProto::TError> Start() override
    {
        ConnectSocket();
        ConnectDevice();

        return NThreading::MakeFuture(MakeError(S_OK));
    }

    NThreading::TFuture<NProto::TError> Stop(bool deleteDevice) override
    {
        if (AtomicSwap(&ShouldStop, 1) == 1) {
            return NThreading::MakeFuture(MakeError(S_OK));
        }

        if (deleteDevice) {
            DisconnectDevice();
            DisconnectSocket();
        }

        return NThreading::MakeFuture(MakeError(S_OK));
    }

private:
    void ConnectSocket();
    void DisconnectSocket();

    void ConnectDevice();
    void DoConnectDevice(bool connected);
    void DisconnectDevice();

    static int StatusHandler(nl_msg* message, void* argument);
};

////////////////////////////////////////////////////////////////////////////////

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

void TNetlinkDevice::DoConnectDevice(bool connected)
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
        message.Put(NBD_ATTR_BLOCK_SIZE_BYTES, static_cast<ui64>(info.MinBlockSize));
        message.Put(NBD_ATTR_SERVER_FLAGS, static_cast<ui64>(info.Flags));

        if (Timeout) {
            message.Put(NBD_ATTR_TIMEOUT, Timeout.Seconds());
        }

        if (DeadConnectionTimeout) {
            message.Put(NBD_ATTR_DEAD_CONN_TIMEOUT, DeadConnectionTimeout.Seconds());
        }

        {
            auto attr = message.Nest(NBD_ATTR_SOCKETS);
            auto item = message.Nest(NBD_SOCK_ITEM);
            message.Put(NBD_SOCK_FD, static_cast<ui32>(Socket));
        }

        message.Send(socket);
    } catch (const TServiceError& e) {
        STORAGE_ERROR("unable to configure " << DeviceName << ": " << e.what());
    }
}

void TNetlinkDevice::DisconnectDevice()
{
    STORAGE_INFO("disconnect " << DeviceName);

    try {
        TNetlinkSocket socket;
        TNetlinkMessage message(socket.GetFamily(), NBD_CMD_DISCONNECT);
        message.Put(NBD_ATTR_INDEX, DeviceIndex);
        message.Send(socket);
    } catch (const TServiceError& e) {
        STORAGE_ERROR("unable to disconnect " << DeviceName << ": " << e.what());
    }
}

// queries device status and registers callback that will connect
// or reconfigure (if Reconfigure == true) specified device
void TNetlinkDevice::ConnectDevice()
{
    try {
        TNetlinkSocket socket;
        nl_socket_modify_cb(
            socket,
            NL_CB_VALID,
            NL_CB_CUSTOM,
            TNetlinkDevice::StatusHandler,
            this);

        // TODO: use proper context containing device pointer and a socket
        // send_sync waits for the response and invokes callback immediately
        // even before returning, so it's technically okay to pass 'this' as
        // an argument, but it still looks flimsy

        TNetlinkMessage message(socket.GetFamily(), NBD_CMD_STATUS);
        message.Put(NBD_ATTR_INDEX, DeviceIndex);
        message.Send(socket);
    } catch (const TServiceError& e) {
        throw TServiceError(e.GetCode())
            << "unable to configure " << DeviceName << ": " << e.what();
    }
}

int TNetlinkDevice::StatusHandler(nl_msg* message, void* argument)
{
    auto* header = static_cast<genlmsghdr*>(nlmsg_data(nlmsg_hdr(message)));
    auto* conn = static_cast<TNetlinkDevice*>(argument);
    auto Log = conn->Log;
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
        STORAGE_ERROR("unable to parse NBD_CMD_STATUS response: " << err);
        return NL_STOP;
    }

    if (!attr[NBD_ATTR_DEVICE_LIST]) {
        STORAGE_ERROR("did not receive NBD_ATTR_DEVICE_LIST");
        return NL_STOP;
    }

    if (int err = nla_parse_nested(
            deviceItem,
            NBD_DEVICE_ITEM_MAX,
            attr[NBD_ATTR_DEVICE_LIST],
            deviceItemPolicy))
    {
        STORAGE_ERROR("unable to parse NBD_ATTR_DEVICE_LIST: " << err);
        return NL_STOP;
    }

    if (!deviceItem[NBD_DEVICE_ITEM]) {
        STORAGE_ERROR("did not receive NBD_DEVICE_ITEM");
        return NL_STOP;
    }

    if (int err = nla_parse_nested(
            device,
            NBD_DEVICE_ATTR_MAX,
            deviceItem[NBD_DEVICE_ITEM],
            devicePolicy))
    {
        STORAGE_ERROR("unable to parse NBD_DEVICE_ITEM: " << err);
        return NL_STOP;
    }

    if (!device[NBD_DEVICE_CONNECTED]) {
        STORAGE_ERROR("did not receive NBD_DEVICE_CONNECTED");
        return NL_STOP;
    }

    conn->DoConnectDevice(nla_get_u8(device[NBD_DEVICE_CONNECTED]));

    return NL_OK;
}

////////////////////////////////////////////////////////////////////////////////

class TNetlinkDeviceFactory final
    : public IDeviceFactory
{
private:
    const ILoggingServicePtr Logging;
    const TDuration Timeout;
    const TDuration DeadConnectionTimeout;
    const bool Reconfigure;

public:
    TNetlinkDeviceFactory(
            ILoggingServicePtr logging,
            TDuration timeout,
            TDuration deadConnectionTimeout,
            bool reconfigure)
        : Logging(std::move(logging))
        , Timeout(std::move(timeout))
        , DeadConnectionTimeout(std::move(deadConnectionTimeout))
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
            std::move(connectAddress),
            std::move(deviceName),
            Timeout,
            DeadConnectionTimeout,
            Reconfigure);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDevicePtr CreateNetlinkDevice(
    ILoggingServicePtr logging,
    TNetworkAddress connectAddress,
    TString deviceName,
    TDuration timeout,
    TDuration deadConnectionTimeout,
    bool reconfigure)
{
    return std::make_shared<TNetlinkDevice>(
        std::move(logging),
        std::move(connectAddress),
        std::move(deviceName),
        timeout,
        deadConnectionTimeout,
        reconfigure);
}

IDeviceFactoryPtr CreateNetlinkDeviceFactory(
    ILoggingServicePtr logging,
    TDuration timeout,
    TDuration deadConnectionTimeout,
    bool reconfigure)
{
    return std::make_shared<TNetlinkDeviceFactory>(
        std::move(logging),
        std::move(timeout),
        std::move(deadConnectionTimeout),
        reconfigure);
}

}   // namespace NCloud::NBlockStore::NBD
