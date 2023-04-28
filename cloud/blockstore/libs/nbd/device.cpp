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

class TDeviceConnection final
    : public ISimpleThread
    , public IDeviceConnection
{
private:
    const ILoggingServicePtr Logging;
    const TNetworkAddress ConnectAddress;
    const TString DeviceName;
    const TDuration Timeout;

    TLog Log;
    IClientHandlerPtr Handler;
    TSocket Socket;
    TFileHandle Device;

public:
    TDeviceConnection(
            ILoggingServicePtr logging,
            const TNetworkAddress& connectAddress,
            TString deviceName,
            TDuration timeout)
        : Logging(std::move(logging))
        , ConnectAddress(connectAddress)
        , DeviceName(std::move(deviceName))
        , Timeout(timeout)
    {}

    void Start() override
    {
        Log = Logging->CreateLog("BLOCKSTORE_NBD");

        ConnectSocket();
        ConnectDevice();

        ISimpleThread::Start();
    }

    void Stop() override
    {
        DisconnectDevice();

        ISimpleThread::Join();

        Device.Close();
        DisconnectSocket();
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

void TDeviceConnection::ConnectSocket()
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

void TDeviceConnection::DisconnectSocket()
{
    STORAGE_DEBUG("Disconnect socket");

    // TODO: graceful disconnect

    Socket.Close();
}

void TDeviceConnection::ConnectDevice()
{
    STORAGE_DEBUG("Connect device");

    TFileHandle device(DeviceName, OpenExisting | RdWr);
    ioctl(device, NBD_CLEAR_SOCK);

    const auto& exportInfo = Handler->GetExportInfo();
    ui32 sectorSize = exportInfo.MinBlockSize;
    ui64 sectors = exportInfo.Size / sectorSize;

    STORAGE_DEBUG("NBD_SET_BLKSIZE " << sectorSize);
    int ret = ioctl(device, NBD_SET_BLKSIZE, sectorSize);
    if (ret < 0) {
        ythrow TFileError() << "could not setup device block size";
    }

    STORAGE_DEBUG("NBD_SET_SIZE_BLOCKS " << sectors);
    ret = ioctl(device, NBD_SET_SIZE_BLOCKS, sectors);
    if (ret < 0) {
        ythrow TFileError() << "could not setup device blocks count";
    }

    STORAGE_DEBUG("NBD_SET_FLAGS " << exportInfo.Flags);
    ret = ioctl(device, NBD_SET_FLAGS, exportInfo.Flags);
    if (ret < 0) {
        ythrow TFileError() << "could not setup device flags";
    }

    STORAGE_DEBUG("NBD_SET_SOCK " << static_cast<SOCKET>(Socket));
    ret = ioctl(device, NBD_SET_SOCK, static_cast<SOCKET>(Socket));
    if (ret < 0) {
        ythrow TFileError() << "could not bind socket to device";
    }

    if (Timeout) {
        STORAGE_DEBUG("NBD_SET_TIMEOUT " << Timeout);
        ret = ioctl(device, NBD_SET_TIMEOUT, Timeout.Seconds());
        if (ret < 0) {
            ythrow TFileError() << "could not set timeout to device";
        }
    }

    Device.Swap(device);
}

void TDeviceConnection::HandleIO()
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

void TDeviceConnection::DisconnectDevice()
{
    STORAGE_DEBUG("Disconnect device");

    ioctl(Device, NBD_CLEAR_QUE);
    ioctl(Device, NBD_DISCONNECT);
    ioctl(Device, NBD_CLEAR_SOCK);

    // device will be closed when thread exit
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDeviceConnectionPtr CreateDeviceConnection(
    ILoggingServicePtr logging,
    TNetworkAddress connectAddress,
    TString deviceName,
    TDuration timeout)
{
    return std::make_shared<TDeviceConnection>(
        std::move(logging),
        std::move(connectAddress),
        std::move(deviceName),
        timeout);
}

}   // namespace NCloud::NBlockStore::NBD
