#include "service.h"

#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/common/file_io_stats.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <fcntl.h>
#include <util/folder/dirut.h>
#include <util/folder/tempdir.h>
#include <util/generic/array_ref.h>
#include <util/generic/scope.h>
#include <util/generic/size_literals.h>
#include <util/random/random.h>
#include <util/stream/file.h>
#include <util/string/strip.h>
#include <util/system/file.h>

#include <atomic>
#include <chrono>

namespace NCloud {

using namespace NThreading;
using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 BlockSize = 4_KB;
constexpr const ui64 BlockCount = 1024;
constexpr const ui32 SubmissionQueueSize = 32;
constexpr const auto Timeout = 15s;

////////////////////////////////////////////////////////////////////////////////

TFsPath TryGetRamDrivePath()
{
    auto p = GetRamDrivePath();
    return !p ? GetSystemTempDir() : p;
}

[[nodiscard]] std::shared_ptr<char> AllocMem(ui64 size)
{
    return {static_cast<char*>(std::aligned_alloc(BlockSize, size)), std::free};
}

////////////////////////////////////////////////////////////////////////////////

// Keep the caller's affinity intact even when SetUp or the test throws.
struct TAffinityGuard
{
    cpu_set_t Original;

    TAffinityGuard()
    {
        CPU_ZERO(&Original);
        UNIT_ASSERT_VALUES_EQUAL(
            sched_getaffinity(0, sizeof(Original), &Original),
            0);
    }

    ~TAffinityGuard()
    {
        Y_ABORT_UNLESS(
            sched_setaffinity(0, sizeof(Original), &Original) == 0,
            "Failed to restore test thread affinity");
    }
};

struct TFixture: public NUnitTest::TBaseFixture
{
    static constexpr ui32 ServicesCount = 2;

    TAffinityGuard AffinityGuard;
    TTempDir TempDir = TTempDir::NewTempDir(TryGetRamDrivePath().GetPath());
    TFileHandle FileData;
    TVector<IFileIOServicePtr> Services;
    cpu_set_t IoWqThreadCpuset;

    void SetUp(NUnitTest::TTestContext& context) final
    {
        Y_UNUSED(context);

        const TFsPath filePath = TempDir.Path() / "test";

        FileData = TFileHandle(
            filePath,
            OpenAlways | RdWr | DirectAligned | Sync);
        FileData.Resize(BlockCount * BlockSize);

        auto factory = CreateIoUringServiceFactory({
            .SubmissionQueueEntries = SubmissionQueueSize,
            .MaxKernelWorkersCount = 1,
            .ShareKernelWorkers = true,
            .ForceAsyncIO = true,
            .PropagateAffinityToKernelWorkers = true,
            .SQKernelPollingEnabled = false,
        });

        SelectIoWqThreadAffinity();
        Services.reserve(ServicesCount);
        for (ui32 i = 0; i != ServicesCount; ++i) {
            Services.push_back(factory->CreateFileIOService());
        }

        for (const auto& service: Services) {
            service->Start();
        }
    }

    void TearDown(NUnitTest::TTestContext& context) final
    {
        Y_UNUSED(context);

        ValidateIoWqThreadAffinity();

        for (const auto& service: Services) {
            service->Stop();
        }
    }

    void SelectIoWqThreadAffinity()
    {
        TVector<int> allowedCores;
        for (int cpu = 0; cpu != CPU_SETSIZE; ++cpu) {
            if (CPU_ISSET(cpu, &AffinityGuard.Original)) {
                allowedCores.push_back(cpu);
            }
        }
        UNIT_ASSERT(!allowedCores.empty());
        const int selectedCore =
            allowedCores[RandomNumber<ui32>(allowedCores.size())];

        CPU_ZERO(&IoWqThreadCpuset);
        CPU_SET(selectedCore, &IoWqThreadCpuset);

        int res = sched_setaffinity(0, sizeof(IoWqThreadCpuset), &IoWqThreadCpuset);
        UNIT_ASSERT(res == 0);
    }

    void ValidateIoWqThreadAffinity()
    {
        TFsPath taskDir("/proc/self/task");

        TVector<TString> threadIds;
        taskDir.ListNames(threadIds);

        for (const auto& threadId : threadIds) {
            auto threadDir = taskDir / threadId;
            auto threadName = TFileInput(threadDir / "comm").ReadAll();
            StripInPlace(threadName);
            if (!threadName.StartsWith("iou-wrk")) {
                continue;
            }

            cpu_set_t cpuset;
            pid_t tid = FromString(threadId);
            int res = sched_getaffinity(tid, sizeof(cpu_set_t), &cpuset);
            UNIT_ASSERT_C(res == 0, "sched_getaffinity failed: " << res);

            res = std::memcmp(&cpuset, &IoWqThreadCpuset, sizeof(cpuset));
            if (res != 0 ) {
                auto dumpCpuset = [](auto& out, auto &cpuset) {
                    for (int i = 0; i < CPU_SETSIZE; ++i) {
                        if (CPU_ISSET(i, &cpuset)) {
                            out << i << " ";
                        }
                    }
                };

                TStringBuilder out;
                out << "Thread ID: " << threadId << ", Comm: " << threadName;
                out << ", Expected affinity: ";
                dumpCpuset(out, IoWqThreadCpuset);
                out << ", Observed affinity: ";
                dumpCpuset(out, cpuset);

                UNIT_ASSERT_C(false, out);
            }

            Cerr << Endl;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFixtureNull: public NUnitTest::TBaseFixture
{
    IFileIOServicePtr IoUring;

    void SetUp(NUnitTest::TTestContext& context) final
    {
        Y_UNUSED(context);

        auto factory = CreateIoUringServiceNullFactory(
            {.SubmissionQueueEntries = SubmissionQueueSize,
             .MaxKernelWorkersCount = 1});

        IoUring = factory->CreateFileIOService();
        IoUring->Start();
    }

    void TearDown(NUnitTest::TTestContext& context) final
    {
        Y_UNUSED(context);

        IoUring->Stop();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIoUringTest)
{
    Y_UNIT_TEST_F(ShouldReadWrite, TFixture)
    {
        const ui64 requestStartIndex = 20;
        const ui64 requestBlockCount = 200;
        const ui64 length = requestBlockCount * BlockSize;
        const i64 offset = requestStartIndex * BlockSize;

        std::shared_ptr<char> memory = AllocMem(length);

        TArrayRef<char> buffer {memory.get(), length};

        for (int i = 0; i != ServicesCount; ++i) {
            auto& service = *Services[i];

            const int expectedData = 'A' + i;
            std::memset(buffer.data(), expectedData, buffer.size());

            {
                auto result = service.AsyncWrite(FileData, offset, buffer);
                UNIT_ASSERT_VALUES_EQUAL(buffer.size(), result.GetValue(Timeout));
            }

            std::memset(buffer.data(), 0, buffer.size());

            {
                auto result = service.AsyncRead(FileData, offset, buffer);

                UNIT_ASSERT_VALUES_EQUAL(buffer.size(), result.GetValue(Timeout));
            }

            for (char val: buffer) {
                UNIT_ASSERT(expectedData == val);
            }
        }
    }

    Y_UNIT_TEST_F(ShouldReadWriteV, TFixture)
    {
        const ui64 requestStartIndex = 20;
        const ui64 requestBlockCount = 200;
        const ui64 length = requestBlockCount * BlockSize;
        const i64 offset = requestStartIndex * BlockSize;

        std::shared_ptr<char> memory = AllocMem(length);

        TVector<TArrayRef<char>> buffers{
            {memory.get(), 20 * BlockSize},
            {memory.get() + 20 * BlockSize, 80 * BlockSize},
            {memory.get() + 100 * BlockSize, 40 * BlockSize},
            {memory.get() + 140 * BlockSize, 60 * BlockSize}};

        TVector<TArrayRef<const char>> constBuffers;
        for (auto& buffer: buffers) {
            constBuffers.emplace_back(buffer.data(), buffer.size());
        }

        for (int i = 0; i != ServicesCount; ++i) {
            auto& service = *Services[i];

            const int expectedData = 'A' + i;

            for (auto& buffer: buffers) {
                std::memset(buffer.data(), expectedData, buffer.size());
            }

            {
                auto result =
                    service.AsyncWriteV(FileData, offset, constBuffers);
                UNIT_ASSERT_VALUES_EQUAL(length, result.GetValue(Timeout));
            }

            for (auto& buffer: buffers) {
                std::memset(buffer.data(), 0, buffer.size());
            }

            {
                auto result = service.AsyncReadV(FileData, offset, buffers);
                UNIT_ASSERT_VALUES_EQUAL(length, result.GetValue(Timeout));
            }

            for (auto& buffer: buffers) {
                for (char val: buffer) {
                    UNIT_ASSERT(expectedData == val);
                }
            }
        }
    }

    Y_UNIT_TEST_F(ShouldStop, TFixture)
    {
        for (const auto& service: Services) {
            service->Stop();
        }
    }

    Y_UNIT_TEST_F(ShouldReadWriteWithSyncFlags, TFixture)
    {
        const TFsPath filePath = TempDir.Path() / "test_sync_flags";
        TFileHandle fileData(filePath, OpenAlways | RdWr | DirectAligned);
        fileData.Resize(BlockCount * BlockSize);

        const ui64 requestStartIndex = 20;
        const ui64 requestBlockCount = 16;
        const ui64 length = requestBlockCount * BlockSize;
        const i64 offset = requestStartIndex * BlockSize;

        std::shared_ptr<char> memory = AllocMem(length);
        TArrayRef<char> buffer {memory.get(), length};

        for (int i = 0; i != ServicesCount; ++i) {
            auto& service = *Services[i];
            const int syncData = 'A' + i;
            const int dsyncData = 'a' + i;
            std::memset(buffer.data(), syncData, buffer.size());

            {
                auto result = service.AsyncWrite(fileData, offset, buffer, O_SYNC);
                UNIT_ASSERT_VALUES_EQUAL(buffer.size(), result.GetValue(Timeout));
            }

            std::memset(buffer.data(), 0, buffer.size());
            {
                auto result = service.AsyncRead(fileData, offset, buffer);
                UNIT_ASSERT_VALUES_EQUAL(buffer.size(), result.GetValue(Timeout));
            }

            for (char val: buffer) {
                UNIT_ASSERT_VALUES_EQUAL(syncData, val);
            }

            std::memset(buffer.data(), dsyncData, buffer.size());
            {
                auto result = service.AsyncWrite(fileData, offset, buffer, O_DSYNC);
                UNIT_ASSERT_VALUES_EQUAL(buffer.size(), result.GetValue(Timeout));
            }

            std::memset(buffer.data(), 0, buffer.size());
            {
                auto result = service.AsyncRead(fileData, offset, buffer);
                UNIT_ASSERT_VALUES_EQUAL(buffer.size(), result.GetValue(Timeout));
            }

            for (char val: buffer) {
                UNIT_ASSERT_VALUES_EQUAL(dsyncData, val);
            }
        }
    }

    Y_UNIT_TEST_F(ShouldReadWriteVWithSyncFlags, TFixture)
    {
        const TFsPath filePath = TempDir.Path() / "test_sync_flags_v";
        TFileHandle fileData(filePath, OpenAlways | RdWr | DirectAligned);
        fileData.Resize(BlockCount * BlockSize);

        const ui64 requestStartIndex = 20;
        const ui64 requestBlockCount = 16;
        const ui64 length = requestBlockCount * BlockSize;
        const i64 offset = requestStartIndex * BlockSize;

        std::shared_ptr<char> memory = AllocMem(length);

        TVector<TArrayRef<char>> buffers{
            {memory.get(), 4 * BlockSize},
            {memory.get() + 4 * BlockSize, 6 * BlockSize},
            {memory.get() + 10 * BlockSize, 6 * BlockSize}};

        TVector<TArrayRef<const char>> constBuffers;
        for (auto& buffer: buffers) {
            constBuffers.emplace_back(buffer.data(), buffer.size());
        }

        for (int i = 0; i != ServicesCount; ++i) {
            auto& service = *Services[i];
            const int syncData = 'A' + i;
            const int dsyncData = 'a' + i;

            for (auto& buffer: buffers) {
                std::memset(buffer.data(), syncData, buffer.size());
            }
            {
                auto result =
                    service.AsyncWriteV(fileData, offset, constBuffers, O_SYNC);
                UNIT_ASSERT_VALUES_EQUAL(length, result.GetValue(Timeout));
            }

            for (auto& buffer: buffers) {
                std::memset(buffer.data(), 0, buffer.size());
            }
            {
                auto result = service.AsyncReadV(fileData, offset, buffers);
                UNIT_ASSERT_VALUES_EQUAL(length, result.GetValue(Timeout));
            }
            for (auto& buffer: buffers) {
                for (char val: buffer) {
                    UNIT_ASSERT_VALUES_EQUAL(syncData, val);
                }
            }

            for (auto& buffer: buffers) {
                std::memset(buffer.data(), dsyncData, buffer.size());
            }
            {
                auto result =
                    service.AsyncWriteV(fileData, offset, constBuffers, O_DSYNC);
                UNIT_ASSERT_VALUES_EQUAL(length, result.GetValue(Timeout));
            }

            for (auto& buffer: buffers) {
                std::memset(buffer.data(), 0, buffer.size());
            }
            {
                auto result = service.AsyncReadV(fileData, offset, buffers);
                UNIT_ASSERT_VALUES_EQUAL(length, result.GetValue(Timeout));
            }

            for (auto& buffer: buffers) {
                for (char val: buffer) {
                    UNIT_ASSERT_VALUES_EQUAL(dsyncData, val);
                }
            }
        }
    }

    Y_UNIT_TEST(ShouldCollectStats)
    {
        const TFsPath filePath = TryGetRamDrivePath() / "test";
        TFileHandle fileData(
            filePath,
            OpenAlways | RdWr | DirectAligned | Sync);
        fileData.Resize(BlockCount * BlockSize);

        auto registry = std::make_shared<TFileIOStatsRegistry>();
        auto service = CreateIoUringServiceFactory(
                           {.SubmissionQueueEntries = SubmissionQueueSize},
                           registry)
                           ->CreateFileIOService();
        service->Start();

        const auto& stats = *registry->GetEntries()[0].Stats;

        const ui64 length = 2 * BlockSize;
        std::shared_ptr<char> memory = AllocMem(length);
        TArrayRef<char> buffer{memory.get(), length};

        TVector<TArrayRef<char>> buffers{
            {memory.get(), BlockSize},
            {memory.get() + BlockSize, BlockSize}};

        TVector<TArrayRef<const char>> constBuffers{
            {memory.get(), BlockSize},
            {memory.get() + BlockSize, BlockSize}};

        UNIT_ASSERT_VALUES_EQUAL(
            length,
            service->AsyncWrite(fileData, 0, buffer).GetValue(Timeout));
        UNIT_ASSERT_VALUES_EQUAL(
            length,
            service->AsyncWriteV(fileData, 0, constBuffers).GetValue(Timeout));
        UNIT_ASSERT_VALUES_EQUAL(
            length,
            service->AsyncRead(fileData, 0, buffer).GetValue(Timeout));
        UNIT_ASSERT_VALUES_EQUAL(
            length,
            service->AsyncReadV(fileData, 0, buffers).GetValue(Timeout));

        // a partial read at the end of the file is a successful operation
        UNIT_ASSERT_VALUES_EQUAL(
            BlockSize,
            service
                ->AsyncRead(fileData, (BlockCount - 1) * BlockSize, buffer)
                .GetValue(Timeout));

        // failed operation
        {
            TFileHandle invalid;
            UNIT_ASSERT_EXCEPTION(
                service->AsyncWrite(invalid, 0, buffer).GetValue(Timeout),
                TServiceError);
        }

        const auto reads = stats.GetStats(EFileIORequest::Read);
        UNIT_ASSERT_VALUES_EQUAL(3, reads.Count);
        UNIT_ASSERT_VALUES_EQUAL(0, reads.Errors);
        UNIT_ASSERT_VALUES_EQUAL(3 * length, reads.RequestBytes);
        UNIT_ASSERT_VALUES_EQUAL(0, reads.InProgress);

        const auto writes = stats.GetStats(EFileIORequest::Write);
        UNIT_ASSERT_VALUES_EQUAL(2, writes.Count);
        UNIT_ASSERT_VALUES_EQUAL(1, writes.Errors);
        UNIT_ASSERT_VALUES_EQUAL(3 * length, writes.RequestBytes);
        UNIT_ASSERT_VALUES_EQUAL(0, writes.InProgress);

        service->Stop();

        // the stop signal is not accounted
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            stats.GetStats(EFileIORequest::Read).Count);
    }

    Y_UNIT_TEST(ShouldRegisterStats)
    {
        auto registry = std::make_shared<TFileIOStatsRegistry>();
        auto factory = CreateIoUringServiceFactory(
            {.SubmissionQueueEntries = SubmissionQueueSize},
            registry);

        auto service1 = factory->CreateFileIOService();
        auto service2 = factory->CreateFileIOService();

        const auto entries = registry->GetEntries();
        UNIT_ASSERT_VALUES_EQUAL(2, entries.size());
        UNIT_ASSERT_VALUES_EQUAL("io_uring", entries[0].Backend);
        UNIT_ASSERT_VALUES_EQUAL("0", entries[0].ServiceId);
        UNIT_ASSERT_VALUES_EQUAL("io_uring", entries[1].Backend);
        UNIT_ASSERT_VALUES_EQUAL("1", entries[1].ServiceId);
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIoUringNullTest)
{
    Y_UNIT_TEST_F(ShouldInvokeCompletions, TFixtureNull)
    {
        TFileHandle file;
        const ui32 requests = 32;
        const ui32 length = 1024;

        TVector<TFuture<ui32>> futures;
        TArrayRef<char> buffer{nullptr, length};

        for (ui32 i = 0; i != requests; ++i) {
            futures.push_back(IoUring->AsyncRead(file, 0, buffer));
            futures.push_back(IoUring->AsyncReadV(file, 0, {{buffer}}));

            futures.push_back(IoUring->AsyncWrite(file, 0, buffer));
            futures.push_back(IoUring->AsyncWriteV(file, 0, {{buffer}}));
        }

        for (auto& future: futures) {
            try {
                const ui32 len = future.GetValue(Timeout);
                UNIT_ASSERT_VALUES_EQUAL(length, len);
            } catch (const TServiceError& e) {
                // EINVAL is expected if the linux kernel is older than 6.0
                UNIT_ASSERT_VALUES_EQUAL_C(
                    MAKE_SYSTEM_ERROR(EINVAL),
                    e.GetCode(),
                    e.GetMessage());
            }
        }
    }
}

}   // namespace NCloud
