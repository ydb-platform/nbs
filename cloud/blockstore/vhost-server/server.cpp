#include "server.h"

#include "backend.h"

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <cloud/contrib/vhost/include/vhost/blockdev.h>
#include <cloud/contrib/vhost/include/vhost/server.h>
#include <cloud/contrib/vhost/include/vhost/types.h>
#include <cloud/contrib/vhost/platform.h>

#include <library/cpp/logger/log.h>
#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/generic/iterator_range.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/stream/output.h>
#include <util/stream/printf.h>
#include <util/system/datetime.h>
#include <util/system/file.h>

#include <atomic>
#include <span>
#include <thread>

using namespace NThreading;

namespace NCloud::NBlockStore::NVHostServer {

namespace {

////////////////////////////////////////////////////////////////////////////////

TLog Log;

constexpr TDuration COMPLETION_STATS_WAIT_DURATION = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

void LogStderr(LogLevel level, const char* fmt, ...)
{
    static ELogPriority levelToPriority[] {
        TLOG_ERR,
        TLOG_WARNING,
        TLOG_INFO,
        TLOG_DEBUG
    };

    auto priority = levelToPriority[level];

    va_list args;
    va_start(args, fmt);

    if (Log.IsOpen() && priority <= Log.FiltrationLevel()) {
        auto elem = Log << priority;
        Printf(elem, fmt, args);
    }

    va_end(args);
}

void DeleteSocket(const TString& path)
{
    if (unlink(path.c_str()) != -1) {
        return;
    }

    if (errno == ENOENT) {
        return;
    }

    const int err = errno;
    STORAGE_WARN(
        "Can't delete socket file %s: %s (%d)",
        path.c_str(),
        strerror(err),
        err);
}

////////////////////////////////////////////////////////////////////////////////

class TServer final
    : public IServer
{
private:
    const ILoggingServicePtr Logging;
    IBackendPtr Backend;

    TString SocketPath;

    vhd_vdev* Handler = nullptr;
    vhd_bdev_info Info = {};

    TVector<vhd_request_queue*> Queues;
    std::unique_ptr<TAtomicStats[]> QueueStats;

    TVector<std::thread> QueueThreads;

public:
    explicit TServer(ILoggingServicePtr logging, IBackendPtr backend);

    void Start(const TOptions& options) override;
    void Stop() override;

    TSimpleStats GetStats(const TSimpleStats& prevStats) override;

private:
    void QueueThreadFunc(ui32 queueIndex);

    void SyncQueueStats(ui32 queueIndex, const TSimpleStats& queueStats);
};

////////////////////////////////////////////////////////////////////////////////

TServer::TServer(ILoggingServicePtr logging, IBackendPtr backend)
    : Logging{std::move(logging)}
    , Backend(backend)
{}

void TServer::Start(const TOptions& options)
{
    Log = Logging->CreateLog("SERVER");
    STORAGE_INFO("Starting the server");

    SocketPath = options.SocketPath;

    Info = Backend->Init(options);

    for (ui32 i = 0; i != options.QueueCount; ++i) {
        auto* queue = vhd_create_request_queue();
        Y_ABORT_UNLESS(queue, "vhd_create_request_queue failed");
        Queues.push_back(queue);
    }

    QueueStats = std::make_unique<TAtomicStats[]>(options.QueueCount);

    Y_ABORT_UNLESS(
        vhd_start_vhost_server(LogStderr) >= 0,
        "vhd_start_vhost_server failed");

    DeleteSocket(SocketPath);

    // Register backend for existing queue and add backend to it.
    Handler = vhd_register_blockdev(
        &Info,
        Queues.data(),
        Queues.size(),
        Backend.get());
    Y_ABORT_UNLESS(Handler, "vhd_register_blockdev: Can't register device");

    if (!options.NoChmod) {
        if (Chmod(
                SocketPath.c_str(),
                static_cast<int>(options.SocketAccessMode)) != 0)
        {
            Y_ABORT(
                "failed to chmod socket %s: %s",
                SocketPath.c_str(),
                strerror(errno));
        }
    }

    Backend->Start();

    // start libvhost request queue runner thread
    QueueThreads.reserve(options.QueueCount);

    for (ui32 i = 0; i != options.QueueCount; ++i) {
        QueueThreads.emplace_back([this, i] { QueueThreadFunc(i); });
    }

    STORAGE_INFO("Server started");
}

void TServer::Stop()
{
    STORAGE_INFO("Stopping the server");

    auto promise = NewPromise();
    vhd_unregister_blockdev(Handler, [] (void* opaque) {
        static_cast<TPromise<void>*>(opaque)->SetValue();
    }, &promise);

    promise.GetFuture().Wait();

    // 2. Stop request queues. For each do:
    // 2.1 Stop a request queue
    for (vhd_request_queue* queue: Queues) {
        vhd_stop_queue(queue);
    }

    // 2.2 Wait for queue's thread to join
    for (auto& queueThread: QueueThreads) {
        queueThread.join();
    }

    // 3. Stop the completion thread
    Backend->Stop();

    // 4. Release request queues and stop the vhost server in any order
    for (vhd_request_queue* queue: Queues) {
        vhd_release_request_queue(queue);
    }
    vhd_stop_vhost_server();

    // 5. Release related resources

    DeleteSocket(SocketPath);

    STORAGE_INFO("Server has been stopped.");
}

TSimpleStats TServer::GetStats(const TSimpleStats& prevStats)
{
    auto completionStats =
        Backend->GetCompletionStats(COMPLETION_STATS_WAIT_DURATION);
    if (!completionStats) {
        return prevStats;
    }

    for (ui32 i = 0; i != Queues.size(); ++i) {
        *completionStats += QueueStats[i];
    }

    return *completionStats;
}

void TServer::QueueThreadFunc(ui32 queueIndex)
{
    NCloud::SetCurrentThreadName("VHOST");

    vhd_request_queue* queue = Queues[queueIndex];

    TSimpleStats queueStats;

    for (;;) {
        int ret = vhd_run_queue(queue);

        if (ret != -EAGAIN) {
            if (ret < 0) {
                STORAGE_ERROR("vhd_run_queue error: %d", ret);
            }
            break;
        }

        Backend->ProcessQueue(queueIndex, queue, queueStats);

        SyncQueueStats(queueIndex, queueStats);
    }
}

void TServer::SyncQueueStats(ui32 queueIndex, const TSimpleStats& queueStats)
{
    auto& stats = QueueStats[queueIndex];

    stats.Dequeued = queueStats.Dequeued;
    stats.Submitted = queueStats.Submitted;
    stats.SubFailed = queueStats.SubFailed;
    stats.EncryptorErrors = queueStats.EncryptorErrors;

    stats.Requests[0] = queueStats.Requests[VHD_BDEV_READ];
    stats.Requests[1] = queueStats.Requests[VHD_BDEV_WRITE];
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<IServer> CreateServer(
    ILoggingServicePtr logging,
    IBackendPtr backend)
{
    return std::make_shared<TServer>(std::move(logging), std::move(backend));
}

}   // namespace NCloud::NBlockStore::NVHostServer
