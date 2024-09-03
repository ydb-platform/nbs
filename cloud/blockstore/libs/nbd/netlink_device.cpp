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

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf NBD_DEVICE_SUFFIX = "/dev/nbd";

////////////////////////////////////////////////////////////////////////////////

inline TString ErrorText(int err) {
    char buf[64];
    strerror_r(err, buf, sizeof(buf));
    return TStringBuilder() << buf << " (" << err << ")";
}

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

    template <typename A>
    void SetCallback(
        nl_cb_type type,
        nl_recvmsg_msg_cb_t func,
        std::unique_ptr<A> arg)
    {
        if (int err = nl_socket_modify_cb(
                Socket,
                type,
                NL_CB_CUSTOM,
                func,
                arg.get()))
        {
            throw TServiceError(E_FAIL)
                << "unable to set socket callback: " << nl_geterror(err);
        }
        arg.release();
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
        if (int err = nla_put(Message, attribute, sizeof(T), &data)) {
            throw TServiceError(E_FAIL) << "unable to put attribute "
                << attribute << ": " << nl_geterror(err);
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
        if (int err = nl_send_sync(socket, message)) {
            throw TServiceError(E_FAIL)
                << "unable to send message: " << nl_geterror(err);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TNetlinkDevice final
    : public IDevice
    , public std::enable_shared_from_this<TNetlinkDevice>
{
private:
    struct THandlerContext
    {
        std::shared_ptr<TNetlinkDevice> Device;

        THandlerContext(std::shared_ptr<TNetlinkDevice> device)
            : Device(std::move(device))
        {}
    };

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

    TPromise<NProto::TError> StartResult = NewPromise<NProto::TError>();
    TPromise<NProto::TError> StopResult = NewPromise<NProto::TError>();
    TPromise<NProto::TError> ResizeResult;

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
    }

    ~TNetlinkDevice()
    {
        Stop(false).GetValueSync();
    }

    TFuture<NProto::TError> Start() override;
    TFuture<NProto::TError> Stop(bool deleteDevice) override;
    TFuture<NProto::TError> Resize(ui64 deviceSizeInBytes) override;

private:
    void ParseIndex();

    void ConnectSocket();
    void DisconnectSocket();

    void ConnectDevice();
    void DoConnectDevice(bool connected);
    void DisconnectDevice();

    static int StatusHandler(nl_msg* message, void* argument);
    static int ResizeHandler(nl_msg* message, void* argument);
    static int ConnectHandler(nl_msg* message, void* argment);
    static int DisconnectHandler(nl_msg* message, void* argument);
};

////////////////////////////////////////////////////////////////////////////////

TFuture<NProto::TError> TNetlinkDevice::Start()
{
    try {
        ParseIndex();
        ConnectSocket();
        ConnectDevice();
    } catch (const TServiceError& e) {
        StartResult.SetValue(MakeError(
            e.GetCode(),
            TStringBuilder() << "unable to configure " << DeviceName
                             << ": " << e.what()));
    }

    // will be set asynchronously in
    // Connect > StatusHandler > DoConnect > ConnectHandler
    return StartResult.GetFuture();
}

TFuture<NProto::TError> TNetlinkDevice::Stop(bool deleteDevice)
{
    if (AtomicSwap(&ShouldStop, 1) == 1) {
        return StopResult.GetFuture();
    }

    if (!deleteDevice) {
        StopResult.SetValue(MakeError(S_OK));
        return StopResult.GetFuture();
    }

    try {
        DisconnectDevice();
        DisconnectSocket();
        StopResult.SetValue(MakeError(S_OK));
    } catch (const TServiceError& e) {
        StopResult.SetValue(MakeError(
            e.GetCode(),
            TStringBuilder() << "unable to disconnect " << DeviceName << ": "
                             << e.what()));
    }

    return StopResult.GetFuture();
}

NThreading::TFuture<NProto::TError> TNetlinkDevice::Resize(
    ui64 deviceSizeInBytes)
{
    if (ResizeResult.Initialized()) {
        return MakeFuture(MakeError(
            E_TRY_AGAIN,
            TStringBuilder() << "unable to resize " << DeviceName << ": "
                             << ErrorText(EINPROGRESS)));
    }
    ResizeResult = NewPromise<NProto::TError>();

    try {
        TNetlinkSocket socket;
        socket.SetCallback(
            NL_CB_MSG_IN,
            TNetlinkDevice::ResizeHandler,
            std::make_unique<THandlerContext>(shared_from_this()));

        TNetlinkMessage message(socket.GetFamily(), NBD_CMD_RECONFIGURE);

        message.Put(NBD_ATTR_INDEX, DeviceIndex);
        message.Put(NBD_ATTR_SIZE_BYTES, deviceSizeInBytes);

        {
            auto attr = message.Nest(NBD_ATTR_SOCKETS);
            auto item = message.Nest(NBD_SOCK_ITEM);
            message.Put(NBD_SOCK_FD, static_cast<ui32>(Socket));
        }

        message.Send(socket);

    } catch (const TServiceError& e) {
        auto resizeResult = std::move(ResizeResult);

        return MakeFuture(MakeError(
            e.GetCode(),
            TStringBuilder() << "unable to resize "
                << DeviceName << ": " << e.what()));
    }

    return ResizeResult.GetFuture();
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

// queries device status and registers callback that will connect
// or reconfigure (if Reconfigure == true) specified device
void TNetlinkDevice::ConnectDevice()
{
    TNetlinkSocket socket;
    socket.SetCallback(
        NL_CB_MSG_IN,
        TNetlinkDevice::StatusHandler,
        std::make_unique<THandlerContext>(shared_from_this()));

    TNetlinkMessage message(socket.GetFamily(), NBD_CMD_STATUS);
    message.Put(NBD_ATTR_INDEX, DeviceIndex);
    message.Send(socket);
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
        socket.SetCallback(
            NL_CB_MSG_IN,
            TNetlinkDevice::ConnectHandler,
            std::make_unique<THandlerContext>(shared_from_this()));

        TNetlinkMessage message(socket.GetFamily(), command);

        const auto& info = Handler->GetExportInfo();
        message.Put(NBD_ATTR_INDEX, DeviceIndex);
        message.Put(NBD_ATTR_SIZE_BYTES, static_cast<ui64>(info.Size));
        message.Put(
            NBD_ATTR_BLOCK_SIZE_BYTES,
            static_cast<ui64>(info.MinBlockSize));
        message.Put(NBD_ATTR_SERVER_FLAGS, static_cast<ui64>(info.Flags));

        if (Timeout) {
            message.Put(NBD_ATTR_TIMEOUT, Timeout.Seconds());
        }

        if (DeadConnectionTimeout) {
            message.Put(
                NBD_ATTR_DEAD_CONN_TIMEOUT,
                DeadConnectionTimeout.Seconds());
        }

        {
            auto attr = message.Nest(NBD_ATTR_SOCKETS);
            auto item = message.Nest(NBD_SOCK_ITEM);
            message.Put(NBD_SOCK_FD, static_cast<ui32>(Socket));
        }

        message.Send(socket);

    } catch (const TServiceError& e) {
        StartResult.SetValue(MakeError(
            e.GetCode(),
            TStringBuilder() << "unable to configure " << DeviceName
                             << ": " << e.what()));
    }
}

void TNetlinkDevice::DisconnectDevice()
{
    STORAGE_INFO("disconnect " << DeviceName);

    TNetlinkSocket socket;
    socket.SetCallback(
        NL_CB_MSG_IN,
        TNetlinkDevice::DisconnectHandler,
        std::make_unique<THandlerContext>(shared_from_this()));

    TNetlinkMessage message(socket.GetFamily(), NBD_CMD_DISCONNECT);
    message.Put(NBD_ATTR_INDEX, DeviceIndex);
    message.Send(socket);
}

int TNetlinkDevice::StatusHandler(nl_msg* message, void* argument)
{
    auto* header = static_cast<genlmsghdr*>(nlmsg_data(nlmsg_hdr(message)));
    auto context = std::unique_ptr<THandlerContext>(
        static_cast<THandlerContext*>(argument));

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
        context->Device->StartResult.SetValue(MakeError(
            E_FAIL,
            TStringBuilder() << "unable to parse NBD_CMD_STATUS response: "
                             << nl_geterror(err)));
        return NL_STOP;
    }

    if (!attr[NBD_ATTR_DEVICE_LIST]) {
        context->Device->StartResult.SetValue(MakeError(
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
        context->Device->StartResult.SetValue(MakeError(
            E_FAIL,
            TStringBuilder() << "unable to parse NBD_ATTR_DEVICE_LIST: "
                             << nl_geterror(err)));
        return NL_STOP;
    }

    if (!deviceItem[NBD_DEVICE_ITEM]) {
        context->Device->StartResult.SetValue(MakeError(
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
        context->Device->StartResult.SetValue(MakeError(
            E_FAIL,
            TStringBuilder() << "unable to parse NBD_DEVICE_ITEM: "
                             << nl_geterror(err)));
        return NL_STOP;
    }

    if (!device[NBD_DEVICE_CONNECTED]) {
        context->Device->StartResult.SetValue(MakeError(
            E_FAIL,
            "did not receive NBD_DEVICE_CONNECTED"));
        return NL_STOP;
    }

    context->Device->DoConnectDevice(nla_get_u8(device[NBD_DEVICE_CONNECTED]));

    return NL_OK;
}

int TNetlinkDevice::ConnectHandler(nl_msg* message, void* argument)
{
    nlmsghdr* header = nlmsg_hdr(message);
    auto context = std::unique_ptr<THandlerContext>(
        static_cast<THandlerContext*>(argument));

    NProto::TError result = MakeError(S_OK);

    // since NL_F_ACK is set by default, NLMSG_ERROR will be sent even if there
    // is no error
    if (header->nlmsg_type != NLMSG_ERROR) {
        return NL_OK;
    }
    nlmsgerr *error = static_cast<nlmsgerr*>(nlmsg_data(header));

    if (error->error) {
        result = MakeError(
            E_FAIL,
            TStringBuilder() << "unable to connect "
                             << context->Device->DeviceName << ": "
                             << ErrorText(error->error));
    }
    context->Device->StartResult.SetValue(std::move(result));

    return NL_OK;
}

int TNetlinkDevice::DisconnectHandler(nl_msg* message, void* argument)
{
    nlmsghdr* header = nlmsg_hdr(message);
    auto context = std::unique_ptr<THandlerContext>(
        static_cast<THandlerContext*>(argument));

    // since NL_F_ACK is set by default, NLMSG_ERROR will be sent even if there
    // is no error
    if (header->nlmsg_type != NLMSG_ERROR) {
        return NL_OK;
    }
    nlmsgerr *error = static_cast<nlmsgerr*>(nlmsg_data(header));

    if (error->error) {
        auto Log = context->Device->Log;
        STORAGE_ERROR(
            TStringBuilder() << "unable to disconnect "
                             << context->Device->DeviceName
                             << ": " << ErrorText(error->error));
    }
    return NL_OK;
}

int TNetlinkDevice::ResizeHandler(nl_msg* message, void* argument)
{
    nlmsghdr* header = nlmsg_hdr(message);
    auto context = std::unique_ptr<THandlerContext>(
        static_cast<THandlerContext*>(argument));

    NProto::TError result = MakeError(S_OK);

    // since NL_F_ACK is set by default, NLMSG_ERROR will be sent even if there
    // is no error
    if (header->nlmsg_type != NLMSG_ERROR) {
        return NL_OK;
    }
    nlmsgerr *error = static_cast<nlmsgerr*>(nlmsg_data(header));

    if (error->error) {
        result = MakeError(
            E_FAIL,
            TStringBuilder() << "unable to resize "
                             << context->Device->DeviceName
                             << ": " << ErrorText(error->error));
    }

    auto resizeResult = std::move(context->Device->ResizeResult);
    resizeResult.SetValue(std::move(result));

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
            connectAddress,
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
