#include "options.h"

#include <cloud/storage/core/libs/aio/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/thread.h>

#include <contrib/libs/liburing/src/include/liburing.h>

#include <util/datetime/base.h>
#include <util/generic/deque.h>
#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/file.h>
#include <util/thread/lfstack.h>

#include <libaio.h>

#include <chrono>
#include <format>
#include <latch>
#include <new>
#include <random>
#include <span>
#include <thread>

namespace {

using namespace std::chrono_literals;
using namespace NCloud;

////////////////////////////////////////////////////////////////////////////////

i64 GetFileLength(const TString& path)
{
    TFileHandle file(path, EOpenModeFlag::RdOnly | EOpenModeFlag::OpenExisting);
    Y_ABORT_UNLESS(file.IsOpen());

    const i64 size = file.Seek(0, sEnd);
    Y_ABORT_UNLESS(size);

    return size;
}

////////////////////////////////////////////////////////////////////////////////

struct TRandomOffset
{
    const ui32 BlockSize;
    std::random_device Device;
    std::uniform_int_distribution<i64> Dist;

    TRandomOffset(ui32 blockSize, i64 blockCount)
        : BlockSize(blockSize)
        , Dist(0, blockCount - 1)
    {}

    i64 operator () ()
    {
        return Dist(Device) * BlockSize;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TCompletion: NCloud::TFileIOCompletion
{
    ui32 Index = 0;
    TLockFreeStack<ui32>* Responses = nullptr;

    static void Process(
        TFileIOCompletion* obj,
        const NProto::TError& error,
        ui32 bytesTransferred)
    {
        Y_ABORT_UNLESS(obj);
        Y_ABORT_UNLESS(bytesTransferred);
        Y_ABORT_IF(HasError(error));

        auto* self = static_cast<TCompletion*>(obj);
        Y_ABORT_UNLESS(self->Responses);

        self->Responses->Enqueue(self->Index);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TJobStats
{
    std::atomic<ui64> Completed = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TTest
{
private:
    const TOptions Options;
    TFileHandle File;

    const i64 FileSize;
    const i64 BlockCount;

    TInstant TestStartTime;
    std::atomic_flag ShouldStop = false;

public:
    explicit TTest(const TOptions& options)
        : Options(options)
        , File{options.Path, OpenExisting | RdOnly | DirectAligned | Sync}
        , FileSize(Min<i64>(GetFileLength(Options.Path), 1_GB))
        , BlockCount(FileSize / Options.BlockSize)
    {
        Y_ABORT_UNLESS(File.IsOpen());
        Y_ABORT_UNLESS(FileSize);
        Y_ABORT_UNLESS(BlockCount);
    }

    void Run()
    {
        auto apiFunc = GetApiFunc();

        TVector<TJobStats> stats(Options.Threads);
        TVector<std::thread> threads;

        std::latch latch{1 + Options.Threads};

        for (ui32 i = 0; i != Options.Threads; ++i) {
            threads.emplace_back([&, stats = &stats[i]] {
                latch.arrive_and_wait();
                std::invoke(apiFunc, this, *stats);
            });
        }

        latch.arrive_and_wait();

        auto completed = [&] {
            ui64 total = 0;
            for (auto& s: stats) {
                total += s.Completed.load();
            }

            return total;
        };

        TestStartTime = TInstant::Now();
        const auto testDeadline = Options.Duration.ToDeadLine(TestStartTime);
        const TDuration updateDelay = 1s;

        for (;;) {
            const auto now = TInstant::Now();
            const auto dt = Min<TDuration>(updateDelay, testDeadline - now);
            if (!dt) {
                break;
            }

            const ui64 prev = completed();
            Sleep(dt);
            const ui64 cur = completed();

            if (!Options.Quiet) {
                ShowProgress(cur - prev, TInstant::Now() - now);
            }
        }

        ShouldStop.test_and_set();
        for (auto& thread: threads) {
            thread.join();
        }

        // print the summary
        if (!Options.Quiet) {
            const auto now = TInstant::Now();
            Cout << "\n============================\n";
            ShowProgress(completed(), now - TestStartTime);
            Cout << Endl;
        }
    }

private:
    auto GetApiFunc() const -> void (TTest::*)(TJobStats&)
    {
        switch (Options.Api) {
            case EApi::IoUring:
                return &TTest::RingThread;
            case EApi::LinuxAio:
                return &TTest::AioThread;
            case EApi::NbsAio:
                return &TTest::NbsAioThread;
            default:
                return nullptr;
        }
    }

    void ShowProgress(ui64 requests, TDuration dt)
    {
        const double iops = requests / dt.SecondsFloat();
        const ui64 bw = static_cast<ui64>(iops * Options.BlockSize);
        const auto now = TInstant::Now();

        Cout << std::format(
            "\r[{}] IOPS: {:8.2f} BW: {}/s {} {:4.1f}%",
            ToString(Options.Api).c_str(),
            iops,
            FormatByteSize(bw).c_str(),
            FormatDuration(now - TestStartTime).c_str(),
            (now - TestStartTime).MillisecondsFloat() * 100.f /
                Options.Duration.MillisecondsFloat());
        Cout.Flush();
    }

    auto CreateBuffers()
        -> std::pair<TVector<std::span<char>>, std::shared_ptr<char>>
    {
        char* mem = static_cast<char*>(std::aligned_alloc(
            Options.BlockSize,
            Options.BlockSize * Options.IoDepth));

        std::shared_ptr<char> storage(mem, std::free);

        TVector<std::span<char>> buffers(Options.IoDepth);

        for (std::span<char>& buf: buffers) {
            buf = std::span(mem, Options.BlockSize);
            mem += Options.BlockSize;
        }

        return {std::move(buffers), storage};
    }

    void RingThread(TJobStats& stats)
    {
        SetCurrentThreadName("RNG");

        auto [buffers, storage] = CreateBuffers();

        io_uring ring {};

        // init ring
        {
            io_uring_params params{
                .flags = IORING_SETUP_SINGLE_ISSUER |
                         IORING_SETUP_DEFER_TASKRUN |
                         IORING_SETUP_COOP_TASKRUN};

            if (Options.SqPolling) {
                params.flags = IORING_SETUP_SINGLE_ISSUER |
                               IORING_SETUP_SQPOLL;
                params.sq_thread_idle = Max<ui32>();
            }

            const int ret =
                io_uring_queue_init_params(Options.RingSize, &ring, &params);
            Y_ABORT_UNLESS(ret == 0, "io_uring_queue_init_params: %d", ret);
        }

        // register file
        {
            const int fd = File;
            const int ret = io_uring_register_files(&ring, &fd, 1);
            Y_ABORT_UNLESS(ret == 0, "io_uring_register_files: %d", ret);
        }

        // register storage
        {
            iovec buf{
                .iov_base = storage.get(),
                .iov_len = Options.BlockSize * Options.IoDepth};

            const int ret = io_uring_register_buffers(&ring, &buf, 1);
            Y_ABORT_UNLESS(ret == 0, "io_uring_register_buffers: %d", ret);
        }

        TRandomOffset offset(Options.BlockSize, BlockCount);

        TDeque<ui32> requests(Options.IoDepth);
        std::iota(requests.begin(), requests.end(), 0u);

        ui64 submitted = 0;
        ui64 completed = 0;

        std::array<io_uring_cqe*, 64> cqes{};

        while (!ShouldStop.test()) {
            while (!requests.empty()) {
                io_uring_sqe* sqe = io_uring_get_sqe(&ring);
                if (!sqe) {
                    break;
                }
                const ui32 i = requests.front();
                requests.pop_front();

                std::span<char> buf = buffers[i];
                io_uring_prep_read_fixed(
                    sqe,
                    0,   // fd
                    buf.data(),
                    buf.size(),
                    offset(),
                    0);   // buf_index
                io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
                io_uring_sqe_set_data64(sqe, i);

                ++submitted;
            }

            if (Options.SqPolling) {
                const int ret = io_uring_submit(&ring);
                Y_ABORT_IF(ret < 0);
            } else {
                const int ret = io_uring_submit_and_get_events(&ring);
                Y_ABORT_IF(ret < 0);
            }

            const ui32 m =
                io_uring_peek_batch_cqe(&ring, cqes.data(), cqes.size());

            for (ui32 i = 0; i != m; ++i) {
                io_uring_cqe* cqe = cqes[i];
                Y_ABORT_IF(cqe->res < 0);
                requests.push_back(io_uring_cqe_get_data64(cqe));
            }

            io_uring_cq_advance(&ring, m);
            completed += m;

            stats.Completed = completed;
        }

        Y_ABORT_UNLESS(submitted >= completed);

        // drain
        if (const ui64 remains = submitted - completed) {
            const int ret = io_uring_wait_cqe_nr(&ring, cqes.data(), remains);
            Y_ABORT_UNLESS(
                ret == 0,
                "io_uring_wait_cqe_nr: %d remains: %lu",
                -ret,
                remains);
            completed += remains;
        }

        Y_ABORT_UNLESS(
            submitted == completed,
            "submitted: %lu completed: %lu",
            submitted,
            completed);

        stats.Completed = completed;

        io_uring_queue_exit(&ring);
    }

    void AioThread(TJobStats& stats)
    {
        SetCurrentThreadName("AIO");

        auto [buffers, storage] = CreateBuffers();

        TRandomOffset offset(Options.BlockSize, BlockCount);

        TVector<iocb> iocbs(Options.IoDepth);
        for (ui64 i = 0; i != Options.IoDepth; ++i) {
            iocb* cb = &iocbs[i];
            std::span<char> buf = buffers[i];
            io_prep_pread(cb, File, buf.data(), buf.size(), offset());
        }

        TVector<iocb*> requests(Options.IoDepth);
        for (ui32 i = 0; i != Options.IoDepth; ++i) {
            requests[i] = &iocbs[i];
        }

        io_context_t ctx = {};
        const int err = io_setup(Options.RingSize, &ctx);
        Y_ABORT_UNLESS(!err, "io_setup: %d", err);

        ui64 submitted = 0;
        ui64 completed = 0;

        std::array<io_event, 64> events{};

        while (!ShouldStop.test()) {
            while (!requests.empty()) {
                const int ret =
                    io_submit(ctx, requests.size(), requests.data());
                Y_ABORT_UNLESS(ret >= 0, "io_submit: %d", -ret);

                if (!ret) {
                    break;
                }

                submitted += ret;
                requests.erase(requests.begin(), requests.begin() + ret);
            }

            const int ret =
                io_getevents(ctx, 1, events.size(), events.data(), nullptr);
            Y_ABORT_UNLESS(ret >= 0, "io_getevents: %d", ret);

            for (int i = 0; i != ret; ++i) {
                iocb* cb = events[i].obj;
                cb->u.c.offset = offset();
                requests.push_back(cb);
            }

            completed += ret;
            stats.Completed = completed;
        }

        Y_ABORT_UNLESS(submitted >= completed);

        // drain
        if (const ui64 remains = submitted - completed) {
            const int ret =
                io_getevents(ctx, remains, remains, events.data(), nullptr);
            Y_ABORT_UNLESS(ret >= 0, "io_getevents: %d", ret);
            completed += ret;
        }

        Y_ABORT_UNLESS(
            submitted == completed,
            "submitted: %lu completed: %lu",
            submitted,
            completed);

        stats.Completed = completed;

        io_destroy(ctx);
    }

    void NbsAioThread(TJobStats& stats)
    {
        auto service = Options.NbsAioThreads == 0
            ? CreateAIOService(Options.RingSize)
            : CreateThreadedAIOService(Options.NbsAioThreads, Options.RingSize);

        service->Start();
        NbsAioThreadImpl(stats, *service);
        service->Stop();
    }

    void NbsAioThreadImpl(TJobStats& stats, IFileIOService& service)
    {
        SetCurrentThreadName("nAIO");

        auto [buffers, storage] = CreateBuffers();

        TRandomOffset offset(Options.BlockSize, BlockCount);

        TLockFreeStack<ui32> responses;
        TVector<TCompletion> completions(Options.IoDepth);
        for (ui32 i = 0; i != Options.IoDepth; ++i) {
            completions[i].Index = i;
            completions[i].Responses = &responses;
            completions[i].Func = &TCompletion::Process;
        }

        TDeque<ui32> requests(Options.IoDepth);
        std::iota(requests.begin(), requests.end(), 0u);

        ui64 submitted = 0;
        ui64 completed = 0;

        while (!ShouldStop.test()) {
            while (!requests.empty()) {
                const ui32 i = requests.front();
                requests.pop_front();

                auto* completion = &completions[i];
                service.AsyncRead(File, offset(), buffers[i], completion);

                ++submitted;
            }

            const size_t m = requests.size();
            responses.DequeueAll(&requests);
            completed += requests.size() - m;
            stats.Completed = completed;
        }

        Y_ABORT_UNLESS(submitted >= completed);

        // drain
        while (submitted != completed) {
            const size_t m = requests.size();
            responses.DequeueAll(&requests);
            completed += requests.size() - m;
        }

        stats.Completed = completed;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    TOptions options(argc, argv);

    TVector<EApi> testCases{options.Api};

    if (options.Api == EApi::TestAll) {
        testCases = {EApi::LinuxAio, EApi::IoUring, EApi::NbsAio};
    }

    for (auto api: testCases) {
        options.Api = api;
        TTest aio(options);
        aio.Run();
    }

    return 0;
}
