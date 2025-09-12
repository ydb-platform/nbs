#include "backend_io_uring.h"

#include "backend.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/io_uring/context.h>

namespace NCloud::NBlockStore::NVHostServer {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TIoUringBackend final: public IBackend
{
private:
    const ILoggingServicePtr Logging;
    TLog Log;

    IEncryptorPtr Encryptor;

    ui32 BlockSize = 0;
    ui32 BatchSize = 0;

    // TODO

public:
    TIoUringBackend(IEncryptorPtr encryptor, ILoggingServicePtr logging);

    vhd_bdev_info Init(const TOptions& options) final;
    void Start() final;
    void Stop() final;
    void ProcessQueue(
        ui32 queueIndex,
        vhd_request_queue* queue,
        TSimpleStats& queueStats) final;
    std::optional<TSimpleStats> GetCompletionStats(TDuration timeout) final;
};

////////////////////////////////////////////////////////////////////////////////

TIoUringBackend::TIoUringBackend(
        IEncryptorPtr encryptor,
        ILoggingServicePtr logging)
    : Logging(std::move(logging))
    , Encryptor(std::move(encryptor))
{
    Y_UNUSED(BlockSize);
}

vhd_bdev_info TIoUringBackend::Init(const TOptions& options)
{
    STORAGE_INFO("Initializing io_uring backend");
    if (Encryptor) {
        STORAGE_INFO("Encryption enabled");
    }

    BatchSize = options.BatchSize;

    // TODO

    return {};
}

void TIoUringBackend::Start()
{
    STORAGE_INFO("Starting io_uring backend");

    // TODO
}

void TIoUringBackend::Stop()
{
    STORAGE_INFO("Stopping io_uring backend");

    // TODO
}

void TIoUringBackend::ProcessQueue(
    ui32 queueIndex,
    vhd_request_queue* queue,
    TSimpleStats& queueStats)
{
    Y_UNUSED(queueIndex);
    Y_UNUSED(queue);
    Y_UNUSED(queueStats);
}

std::optional<TSimpleStats> TIoUringBackend::GetCompletionStats(TDuration timeout)
{
    Y_UNUSED(timeout);

    // TODO

    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBackendPtr CreateIoUringBackend(
    IEncryptorPtr encryptor,
    ILoggingServicePtr logging)
{
    return std::make_shared<TIoUringBackend>(
        std::move(encryptor),
        std::move(logging));
}

}   // namespace NCloud::NBlockStore::NVHostServer
