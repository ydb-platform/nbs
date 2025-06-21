#include "device.h"

#include "client_handler.h"
#include "utils.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/system/file.h>
#include <util/system/thread.h>

#include <sys/ioctl.h>

namespace NCloud::NBlockStore::NBD {

namespace {

////////////////////////////////////////////////////////////////////////////////

enum {
    NBD_SET_SOCK                = _IO(0xAB, 0),
    NBD_SET_BLKSIZE             = _IO(0xAB, 1),
    NBD_SET_SIZE                = _IO(0xAB, 2),
    NBD_DO_IT                   = _IO(0xAB, 3),
    NBD_CLEAR_SOCK              = _IO(0xAB, 4),
    NBD_CLEAR_QUE               = _IO(0xAB, 5),
    NBD_PRINT_DEBUG             = _IO(0xAB, 6),
    NBD_SET_SIZE_BLOCKS         = _IO(0xAB, 7),
    NBD_DISCONNECT              = _IO(0xAB, 8),
    NBD_SET_TIMEOUT             = _IO(0xAB, 9),
    NBD_SET_FLAGS               = _IO(0xAB, 10),
};

////////////////////////////////////////////////////////////////////////////////

class TDevice final
    : public ISimpleThread
    , public IDevice
{
private:
    const ILoggingServicePtr Logging;
    const TNetworkAddress ConnectAddress;
    TString DevicePath;
    const TString DevicePrefix;
    const ui64 BlockCount;
    const ui32 BlockSize;
    const TDuration Timeout;

    TLog Log;
    IClientHandlerPtr Handler;
    TSocket Socket;
    TFileHandle Device;

    TAtomic ShouldStop = 0;

public:
    TDevice(
            ILoggingServicePtr logging,
            const TNetworkAddress& connectAddress,
            TString devicePath,
            TString devicePrefix,
            ui64 blockCount,
            ui32 blockSize,
            TDuration timeout)
        : Logging(std::move(logging))
        , ConnectAddress(connectAddress)
        , DevicePath(std::move(devicePath))
        , DevicePrefix(std::move(devicePrefix))
        , BlockCount(blockCount)
        , BlockSize(blockSize)
        , Timeout(timeout)
    {
        Log = Logging->CreateLog("BLOCKSTORE_NBD");
    }

    ~TDevice() override
    {
        Stop(true);
    }

    NThreading::TFuture<NProto::TError> Start() override
    {
        if (!DevicePath) {
            DevicePath = FindFreeNbdDevice().replace(
                0,
                DEFAULT_DEVICE_PREFIX.size(),
                DevicePrefix);

            if (DevicePath == DevicePrefix) {
                return NThreading::MakeFuture(MakeError(
                    E_FAIL,
                    TStringBuilder()
                        << "unable to find free nbd device with prefix "
                        << DevicePrefix.Quote()));
            }
        }
        try {
            ConnectSocket();
            ConnectDevice();

            ISimpleThread::Start();
        } catch (...) {
            return NThreading::MakeFuture(MakeError(
                E_FAIL,
                CurrentExceptionMessage()));
        }

        return NThreading::MakeFuture(MakeError(S_OK));
    }

    NThreading::TFuture<NProto::TError> Stop(bool deleteDevice) override
    {
        // device configured via ioctl interface is bound to the process, there
        // is no point keeping it
        Y_UNUSED(deleteDevice);

        if (AtomicSwap(&ShouldStop, 1) == 1) {
            return NThreading::MakeFuture(MakeError(S_OK));
        }

        try {
            DisconnectDevice();

            ISimpleThread::Join();

            Device.Close();
            DisconnectSocket();
        } catch (...) {
            return NThreading::MakeFuture(MakeError(
                E_FAIL,
                CurrentExceptionMessage()));
        }

        return NThreading::MakeFuture(MakeError(S_OK));
    }

    NThreading::TFuture<NProto::TError> Resize(ui64 deviceSizeInBytes) override
    {
        if (!Device.IsOpen()) {
            return NThreading::MakeFuture(
                MakeError(E_FAIL, "Device is not open"));
        }

        STORAGE_DEBUG("NBD_SET_SIZE " << deviceSizeInBytes);
        auto ret = ioctl(Device, NBD_SET_SIZE, deviceSizeInBytes);
        if (ret < 0) {
            return NThreading::MakeFuture(
                MakeError(E_FAIL, "Could not setup device size"));
        }

        return NThreading::MakeFuture(MakeError(S_OK));
    }

    TString GetPath() const override
    {
        return DevicePath;
    }

private:
    void* ThreadProc() override
    {
        HandleIO();
        return nullptr;
    }

    void ConnectSocket();
    void DisconnectSocket();

    void ConnectDevice();
    void HandleIO();
    void DisconnectDevice();
};

////////////////////////////////////////////////////////////////////////////////

void TDevice::ConnectSocket()
{
    STORAGE_DEBUG("Connect socket");

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

void TDevice::DisconnectSocket()
{
    STORAGE_DEBUG("Disconnect socket");

    // TODO: graceful disconnect

    Socket.Close();
}

void TDevice::ConnectDevice()
{
    STORAGE_DEBUG("Connect device");

    TFileHandle device(DevicePath, OpenExisting | RdWr);

    if (!device.IsOpen()) {
         ythrow TFileError() << "cannot open " << DevicePath;
    }

    ioctl(device, NBD_CLEAR_SOCK);

    const auto& exportInfo = Handler->GetExportInfo();
    ui32 sectorSize = exportInfo.MinBlockSize;
    ui64 sectors = exportInfo.Size / sectorSize;

    const auto expectedSize = BlockCount * BlockSize;
    if (expectedSize && expectedSize != exportInfo.Size) {
        ythrow TFileError() << "unexpected export info, size mismatch: "
            << expectedSize << " != " << exportInfo.Size << ", sectorSize: "
            << exportInfo.MinBlockSize;
    }

    STORAGE_DEBUG("NBD_SET_BLKSIZE " << sectorSize);
    int ret = ioctl(device, NBD_SET_BLKSIZE, sectorSize);
    if (ret < 0) {
        ythrow TFileError() << "could not setup device block size: " << ret;
    }

    STORAGE_DEBUG("NBD_SET_SIZE_BLOCKS " << sectors);
    ret = ioctl(device, NBD_SET_SIZE_BLOCKS, sectors);
    if (ret < 0) {
        ythrow TFileError() << "could not setup device blocks count: " << ret;
    }

    STORAGE_DEBUG("NBD_SET_FLAGS " << exportInfo.Flags);
    ret = ioctl(device, NBD_SET_FLAGS, exportInfo.Flags);
    if (ret < 0) {
        ythrow TFileError() << "could not setup device flags: " << ret;
    }

    STORAGE_DEBUG("NBD_SET_SOCK " << static_cast<SOCKET>(Socket));
    ret = ioctl(device, NBD_SET_SOCK, static_cast<SOCKET>(Socket));
    if (ret < 0) {
        ythrow TFileError() << "could not bind socket to device: " << ret;
    }

    if (Timeout) {
        STORAGE_DEBUG("NBD_SET_TIMEOUT " << Timeout);
        ret = ioctl(device, NBD_SET_TIMEOUT, Timeout.Seconds());
        if (ret < 0) {
            ythrow TFileError() << "could not set timeout to device: " << ret;
        }
    }

    Device.Swap(device);
}

void TDevice::HandleIO()
{
    STORAGE_DEBUG("Start IO for device");

    int ret = ioctl(Device, NBD_DO_IT);
    if (ret < 0 && LastSystemError() != EPIPE) {
        STORAGE_ERROR("Error handling IO for device: " << LastSystemError());
    }

    STORAGE_DEBUG("IO for device stopped");

    ioctl(Device, NBD_CLEAR_QUE);
    ioctl(Device, NBD_CLEAR_SOCK);
}

void TDevice::DisconnectDevice()
{
    STORAGE_DEBUG("Disconnect device");

    ioctl(Device, NBD_CLEAR_QUE);
    ioctl(Device, NBD_DISCONNECT);
    ioctl(Device, NBD_CLEAR_SOCK);

    // device will be closed when thread exit
}

////////////////////////////////////////////////////////////////////////////////

class TDeviceStub final
    : public IDevice
{
public:
    NThreading::TFuture<NProto::TError> Start() override
    {
        return NThreading::MakeFuture(MakeError(S_OK));
    }

    NThreading::TFuture<NProto::TError> Stop(bool deleteDevice) override
    {
        Y_UNUSED(deleteDevice);

        return NThreading::MakeFuture(MakeError(S_OK));
    }

    NThreading::TFuture<NProto::TError> Resize(ui64 deviceSizeInBytes) override
    {
        Y_UNUSED(deviceSizeInBytes);
        return NThreading::MakeFuture(MakeError(S_OK));
    }

    TString GetPath() const override
    {
        return "";
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDeviceFactory final
    : public IDeviceFactory
{
private:
    const ILoggingServicePtr Logging;
    const TDuration Timeout;

public:
    TDeviceFactory(ILoggingServicePtr logging, TDuration timeout)
        : Logging(std::move(logging))
        , Timeout(timeout)
    {}

    IDevicePtr Create(
        const TNetworkAddress& connectAddress,
        TString devicePath,
        ui64 blockCount,
        ui32 blockSize) override
    {
        return std::make_shared<TDevice>(
            Logging,
            connectAddress,
            std::move(devicePath),
            "",
            blockCount,
            blockSize,
            Timeout);
    }

    IDevicePtr CreateFree(
        const TNetworkAddress& connectAddress,
        TString devicePrefix,
        ui64 blockCount,
        ui32 blockSize) override
    {
        return std::make_shared<TDevice>(
            Logging,
            connectAddress,
            "",
            std::move(devicePrefix),
            blockCount,
            blockSize,
            Timeout);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDevicePtr CreateDevice(
    ILoggingServicePtr logging,
    const TNetworkAddress& connectAddress,
    TString devicePath,
    TDuration timeout)
{
    return std::make_shared<TDevice>(
        std::move(logging),
        connectAddress,
        std::move(devicePath),
        "",
        0, // blockCount
        0, // blockSize
        timeout);
}

IDevicePtr CreateDeviceStub()
{
    return std::make_shared<TDeviceStub>();
}

IDeviceFactoryPtr CreateDeviceFactory(
    ILoggingServicePtr logging,
    TDuration timeout)
{
    return std::make_shared<TDeviceFactory>(
        std::move(logging),
        timeout);
}

}   // namespace NCloud::NBlockStore::NBD
