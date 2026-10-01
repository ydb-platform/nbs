#include "client_handler.h"
#include "netlink_device.h"
#include "utils.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/netlink/socket.h>

#include <linux/nbd-netlink.h>

#include <exception>

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

using TNbdResizeRequest = TNetlinkRequest<
    TNetlinkAttribute<NBD_ATTR_INDEX, ui32>,
    TNetlinkAttribute<NBD_ATTR_SIZE_BYTES, ui64>,
    TNetlinkAttribute<NBD_ATTR_BLOCK_SIZE_BYTES, ui64>>;

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
    , public std::enable_shared_from_this<TNetlinkDevice>
{
private:
    const ITaskQueuePtr Executor;
    const ILoggingServicePtr Logging;
    const TNetworkAddress ConnectAddress;
    const TString DevicePath;
    const TString DevicePrefix;
    const TDuration RequestTimeout;
    const TDuration ConnectionTimeout;

    ui16 FamilyId = 0;
    TLog Log;
    IClientHandlerPtr Handler;
    TSocket Socket;
    std::optional<ui32> DeviceIndex;

    TFuture<NProto::TError> StartResult;
    TFuture<NProto::TError> StopResult;
    bool Disconnected = false;

public:
    TNetlinkDevice(
        ILoggingServicePtr logging,
        TNetworkAddress connectAddress,
        TString devicePath,
        TString devicePrefix,
        TDuration requestTimeout,
        TDuration connectionTimeout,
        ITaskQueuePtr executor);

    ~TNetlinkDevice();

    TFuture<NProto::TError> Start() override;
    TFuture<NProto::TError> Stop(bool deleteDevice) override;
    TFuture<NProto::TError> Resize(ui64 deviceSizeInBytes) override;

    TString GetPath() const override;

private:
    TFuture<NProto::TError> Configure();
    TFuture<NProto::TError> ConfigureFree();
    TFuture<NProto::TError> Disconnect();

    TString GetDevice() const;

    void ParseIndex();
    void ConnectSocket();
    void DisconnectSocket();

    NProto::TError MakeNetlinkError(
        TStringBuf operation,
        const std::exception& e) const;
};

////////////////////////////////////////////////////////////////////////////////

TNetlinkDevice::TNetlinkDevice(
        ILoggingServicePtr logging,
        TNetworkAddress connectAddress,
        TString devicePath,
        TString devicePrefix,
        TDuration requestTimeout,
        TDuration connectionTimeout,
        ITaskQueuePtr executor)
    : Executor(std::move(executor))
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
        return StartResult;
    }

    StartResult = NNetlink::GetFamilyId(Executor, NBD_GENL_FAMILY_NAME)
        .Apply([self = shared_from_this()](const auto& result) {
            self->FamilyId = result.GetValue();
            self->ConnectSocket();
            if (self->DevicePath) {
                self->ParseIndex();
                return self->Configure();
            }
            return self->ConfigureFree();
        }).Apply([self = shared_from_this()](const auto& result) {
            try {
                return result.GetValue();
            } catch (const std::exception& e) {
                return self->MakeNetlinkError("configure", e);
            }
        });

    return StartResult;
}

TFuture<NProto::TError> TNetlinkDevice::Stop(bool deleteDevice)
{
    if (!StopResult.Initialized()) {
        DisconnectSocket();
        StopResult = MakeFuture(MakeError(S_OK));
    }

    if (deleteDevice && DeviceIndex && !Disconnected) {
        auto disconnect = Disconnect();
        Disconnected = true;
        StopResult = std::move(disconnect);
    }

    return StopResult;
}

// query device status and connect or reconfigure it
TFuture<NProto::TError> TNetlinkDevice::Configure()
{
    return
        NNetlink::Send<TNbdStatusResponse>(
            Executor,
            TNbdStatusRequest(
                FamilyId,
                NBD_CMD_STATUS,
                *DeviceIndex))
        .Apply([self = shared_from_this()](const auto& result) {
            const auto& status = result.GetValue();
            const auto& info = self->Handler->GetExportInfo();
            auto& Log = self->Log;
            STORAGE_INFO("query " << self->GetDevice());
            return NNetlink::Send(
                self->Executor,
                TNbdConfigureRequest(
                    self->FamilyId,
                    status.Msg.Connected ? NBD_CMD_RECONFIGURE
                                         : NBD_CMD_CONNECT,
                    *self->DeviceIndex,
                    static_cast<ui64>(info.Size),
                    static_cast<ui64>(info.MinBlockSize),
                    static_cast<ui64>(info.Flags),
                    self->RequestTimeout.Seconds(),
                    self->ConnectionTimeout.Seconds(),
                    TNetlinkAttribute<
                        NBD_SOCK_ITEM,
                        TNetlinkAttribute<NBD_SOCK_FD, ui32>>(
                        static_cast<ui32>(self->Socket))));
        }).Apply([self = shared_from_this()](const auto& result) {
            result.TryRethrow();
            auto& Log = self->Log;
            STORAGE_INFO("configure " << self->GetDevice());
            return MakeError(S_OK);
        });
}

// connect any free device
TFuture<NProto::TError> TNetlinkDevice::ConfigureFree()
{
    const auto& info = Handler->GetExportInfo();

    return
        NNetlink::Send<TNbdConfigureResponse>(
            Executor,
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
                    TNetlinkAttribute<NBD_SOCK_FD, ui32>>(
                    static_cast<ui32>(Socket))))
        .Apply([self = shared_from_this()](const auto& result) {
            self->DeviceIndex = result.GetValue().Msg.Index;
            auto& Log = self->Log;
            STORAGE_INFO("configure " << self->GetDevice());
            return MakeError(S_OK);
        });
}

TFuture<NProto::TError> TNetlinkDevice::Disconnect()
{
    return
        NNetlink::Send(
            Executor,
            TNbdDisconnectRequest(
                FamilyId,
                NBD_CMD_DISCONNECT,
                *DeviceIndex))
        .Apply([self = shared_from_this()](const auto& result) {
            try {
                result.TryRethrow();
                auto& Log = self->Log;
                STORAGE_INFO("disconnect " << self->GetDevice());
                return MakeError(S_OK);
            } catch (const TServiceError& e) {
                return self->MakeNetlinkError("disconnect", e);
            }
        });
}

TFuture<NProto::TError> TNetlinkDevice::Resize(ui64 deviceSizeInBytes)
{
    const auto& info = Handler->GetExportInfo();

    return
        NNetlink::Send(
            Executor,
            TNbdResizeRequest(
                FamilyId,
                NBD_CMD_RECONFIGURE,
                *DeviceIndex,
                deviceSizeInBytes,
                static_cast<ui64>(info.MinBlockSize)))
        .Apply([self = shared_from_this()](const auto& result) {
            try {
                result.TryRethrow();
                auto& Log = self->Log;
                STORAGE_INFO("resize " << self->GetDevice());
                return MakeError(S_OK);
            } catch (const TServiceError& e) {
                return self->MakeNetlinkError("resize", e);
            }
        });
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
        STORAGE_THROW_SERVICE_ERROR(E_ARGUMENT)
            << "unable to parse device index";
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

NProto::TError TNetlinkDevice::MakeNetlinkError(
    TStringBuf operation,
    const std::exception& e) const
{
    const auto* serviceError = dynamic_cast<const TServiceError*>(&e);
    return MakeError(
        serviceError ? serviceError->GetCode() : E_FAIL,
        TStringBuilder()
            << "unable to " << operation << " " << GetDevice()
            << ": " << e.what());
}

////////////////////////////////////////////////////////////////////////////////

class TNetlinkDeviceFactory final
    : public IDeviceFactory
{
private:
    const ILoggingServicePtr Logging;
    const TDuration RequestTimeout;
    const TDuration ConnectionTimeout;
    const ITaskQueuePtr Executor;

public:
    TNetlinkDeviceFactory(
            ILoggingServicePtr logging,
            TDuration requestTimeout,
            TDuration connectionTimeout,
            ITaskQueuePtr executor)
        : Logging(std::move(logging))
        , RequestTimeout(requestTimeout)
        , ConnectionTimeout(connectionTimeout)
        , Executor(std::move(executor))
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
            ConnectionTimeout,
            Executor);
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
            ConnectionTimeout,
            Executor);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDevicePtr CreateNetlinkDevice(
    ILoggingServicePtr logging,
    TNetworkAddress connectAddress,
    TString devicePath,
    TDuration requestTimeout,
    TDuration connectionTimeout,
    ITaskQueuePtr executor)
{
    return std::make_shared<TNetlinkDevice>(
        std::move(logging),
        std::move(connectAddress),
        std::move(devicePath),
        "",
        requestTimeout,
        connectionTimeout,
        std::move(executor));
}

IDevicePtr CreateFreeNetlinkDevice(
    ILoggingServicePtr logging,
    TNetworkAddress connectAddress,
    TString devicePrefix,
    TDuration requestTimeout,
    TDuration connectionTimeout,
    ITaskQueuePtr executor)
{
    return std::make_shared<TNetlinkDevice>(
        std::move(logging),
        std::move(connectAddress),
        "",
        std::move(devicePrefix),
        requestTimeout,
        connectionTimeout,
        std::move(executor));
}

IDeviceFactoryPtr CreateNetlinkDeviceFactory(
    ILoggingServicePtr logging,
    TDuration requestTimeout,
    TDuration connectionTimeout,
    ITaskQueuePtr executor)
{
    return std::make_shared<TNetlinkDeviceFactory>(
        std::move(logging),
        requestTimeout,
        connectionTimeout,
        std::move(executor));
}

}   // namespace NCloud::NBlockStore::NBD
