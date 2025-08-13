#include "write_back_cache.h"

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore_test.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/random/random.h>
#include <util/system/tempfile.h>

#include <latch>
#include <memory>
#include <mutex>
#include <thread>

namespace NCloud::NFileStore::NFuse {

using namespace std::chrono_literals;

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 CacheCapacityBytes = 1024*1024 + 1024;

////////////////////////////////////////////////////////////////////////////////

void SleepForRandomDurationMs(ui32 maxDurationMs)
{
    const auto durationMs = RandomNumber(maxDurationMs);
    if (durationMs != 0) {
        std::this_thread::sleep_for(durationMs*1ms);
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TInFlightRequestTracker
{
    TVector<ui32> InFlightRequests;
    std::mutex Mutex;

    ui32 Count(ui64 offset, ui64 length)
    {
        ui32 res = 0;

        {
            std::unique_lock lock(Mutex);

            const auto end = offset + length;

            for (auto i = offset; i < Min(end, InFlightRequests.size()); i++) {
                res += InFlightRequests[i];
            }
        }

        return res;
    }

    // returns previous in-flight request count in range
    ui32 Add(ui64 offset, ui64 length)
    {
        ui32 res = 0;

        {
            std::unique_lock lock(Mutex);

            const auto end = offset + length;
            // append zeroes if needed
            const auto newSize = Max(InFlightRequests.size(), end);
            InFlightRequests.resize(newSize, 0);

            for (auto i = offset; i < end; i++) {
                res += InFlightRequests[i];
                InFlightRequests[i]++;
            }
        }

        return res;
    }

    void Remove(ui64 offset, ui64 length)
    {
        {
            std::unique_lock lock(Mutex);

            const auto end = offset + length;

            for (auto i = offset; i < end; i++) {
                Y_ABORT_UNLESS(InFlightRequests[i]);
                InFlightRequests[i]--;
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
{
    ILoggingServicePtr Logging;
    TLog Log;

    std::shared_ptr<TFileStoreTest> Session;
    ITimerPtr Timer;
    ISchedulerPtr Scheduler;
    TDuration CacheAutomaticFlushPeriod;
    TTempFileHandle TempFileHandle;
    TWriteBackCache Cache;

    TCallContextPtr CallContext;

    // maps handle to data
    THashMap<ui64, TString> ExpectedData;
    std::mutex ExpectedDataMutex;

    // maps handle to data
    THashMap<ui64, TString> UnflushedData;
    std::mutex UnflushedDataMutex;

    // maps handle to data
    THashMap<ui64, TString> FlushedData;
    std::mutex FlushedDataMutex;

    // ensures that the data is not flushed twice, does not work well with cache
    // recreation because after recreation, the data may be flushed again
    bool EraseExpectedUnflushedDataAfterFirstUse = false;

    THashMap<ui64, TInFlightRequestTracker> InFlightReadRequestTracker;
    THashMap<ui64, TInFlightRequestTracker> InFlightWriteRequestTracker;

    std::atomic<int> SessionWriteDataHandlerCalled;

    TBootstrap(TDuration cacheAutomaticFlushPeriod = {})
    {
        CacheAutomaticFlushPeriod = cacheAutomaticFlushPeriod;

        Logging = CreateLoggingService("console", TLogSettings{});
        Logging->Start();
        Log = Logging->CreateLog("WRITE_BACK_CACHE");

        Timer = CreateWallClockTimer();
        Scheduler = CreateScheduler(Timer);
        Scheduler->Start();

        Session = std::make_shared<TFileStoreTest>();

        Session->ReadDataHandler = [&] (auto, auto request) {
            const auto handle = request->GetHandle();
            const auto offset = request->GetOffset();
            const auto length = request->GetLength();

            // overlapping write requests are not allowed
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                InFlightWriteRequestTracker[handle].Count(offset, length));

            InFlightReadRequestTracker[handle].Add(offset, length);
            Y_DEFER {
                InFlightReadRequestTracker[handle].Remove(offset, length);
            };

            std::unique_lock lock(FlushedDataMutex);

            NProto::TReadDataResponse response;

            if (!FlushedData.contains(request->GetHandle())) {
                return MakeFuture(response);
            }

            auto data = FlushedData[request->GetHandle()];
            // append zeroes if needed
            auto newSize = Max(
                data.size(),
                request->GetOffset() + request->GetLength());
            data.resize(newSize, 0);

            data = TStringBuf(data).SubString(
                request->GetOffset(),
                request->GetLength());

            auto responseOffset = RandomNumber(10u);
            auto responseBuffer = TString(data.size() + responseOffset, 0);
            data.copy(responseBuffer.begin() + responseOffset, data.size());

            response.SetBuffer(std::move(responseBuffer));
            response.SetBufferOffset(responseOffset);

            return MakeFuture(response);
        };

        Session->WriteDataHandler = [&] (auto, auto request) {
            const auto handle = request->GetHandle();
            const auto offset = request->GetOffset();
            const auto length = request->GetBuffer().length();

            // overlapping read requests are not allowed
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                InFlightReadRequestTracker[handle].Count(offset, length));

            // overlapping write requests are not allowed
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                InFlightWriteRequestTracker[handle].Add(offset, length));
            Y_DEFER {
                InFlightWriteRequestTracker[handle].Remove(offset, length);
            };

            std::unique_lock lock1(UnflushedDataMutex);
            std::unique_lock lock2(FlushedDataMutex);

            STORAGE_INFO("Flushing " << request->GetBuffer().Quote()
                << " to @" << request->GetHandle()
                << " at offset " << request->GetOffset());

            UNIT_ASSERT(UnflushedData.contains(request->GetHandle()));

            const auto unflushed = UnflushedData[request->GetHandle()];
            UNIT_ASSERT_LE(
                request->GetOffset() + request->GetBuffer().length(),
                unflushed.length());

            auto from = TStringBuf(unflushed).SubString(
                request->GetOffset(),
                request->GetBuffer().length());
            UNIT_ASSERT_VALUES_EQUAL(from, request->GetBuffer());

            auto& to = FlushedData[request->GetHandle()];
            // append zeroes if needed
            auto newSize = Max(to.size(), request->GetOffset() + from.size());
            to.resize(newSize, 0);
            to.replace(request->GetOffset(), from.size(), from);

            if (EraseExpectedUnflushedDataAfterFirstUse) {
                memset(const_cast<char*>(from.data()), char(0), from.length());
            }

            SessionWriteDataHandlerCalled++;

            NProto::TWriteDataResponse response;
            return MakeFuture(response);
        };

        RecreateCache();

        CallContext = MakeIntrusive<TCallContext>();
    }

    ~TBootstrap()
    {
        Scheduler->Stop();
    }

    void RecreateCache()
    {
        STORAGE_INFO("Recreating cache");

        Cache = TWriteBackCache(
            Session,
            Scheduler,
            Timer,
            TempFileHandle.GetName(),
            CacheCapacityBytes,
            CacheAutomaticFlushPeriod);
    }

    TFuture<NProto::TReadDataResponse> ReadFromCache(
        ui64 handle,
        ui64 offset,
        ui64 length)
    {
        auto request = std::make_shared<NProto::TReadDataRequest>();
        request->SetHandle(handle);
        request->SetOffset(offset);
        request->SetLength(length);

        return Cache.ReadData(CallContext, move(request));
    }

    void ValidateCache(ui64 handle, ui64 offset, TString expected)
    {
        auto future = ReadFromCache(handle, offset, expected.length());
        auto response = future.GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(
            expected,
            response.GetBuffer().substr(response.GetBufferOffset()),
            TStringBuilder() << " while validating @" << handle
            << " at offset " << offset
            << " and length " << expected.length());
    }

    void ValidateCache(ui64 handle, ui64 offset, size_t length)
    {
        auto future = ReadFromCache(handle, offset, length);
        auto response = future.GetValueSync();

        // In concurrent tests, new data may be written at any moment.
        // We need take the most recent ExpectedData each time.
        //
        // Also, a race condition is still possible here:
        // ExpectedData is not fully synchonized with the internal state.
        // The following scenario may happen:
        // 1. WriteData has written new data.
        // 2. ReadFromCache reads new data.
        // 3. The callback updates ExpectedData.
        TString expected;
        {
            std::unique_lock lock(ExpectedDataMutex);
            expected =
                TStringBuf(ExpectedData[handle]).SubString(offset, length);
        }

        UNIT_ASSERT_VALUES_EQUAL_C(
            expected,
            response.GetBuffer().substr(response.GetBufferOffset()),
            TStringBuilder() << " while validating @" << handle
            << " at offset " << offset
            << " and length " << length);
    }

    void ValidateCache(ui64 handle)
    {
        size_t dataLength = 0;
        {
            std::unique_lock lock(ExpectedDataMutex);
            dataLength = ExpectedData[handle].length();
        }

        for (size_t offset = 0; offset < dataLength; offset++) {
            for (size_t len = 1; len + offset < dataLength; len++) {
                // TODO(svartmetal): validate with 'out of bounds'
                // requests also
                ValidateCache(handle, offset, len);
            }
        }
    }

    void ValidateCache()
    {
        for (const auto& [handle, _]: ExpectedData) {
            ValidateCache(handle);
        }
    }

    TFuture<NProto::TWriteDataResponse> WriteToCache(
        ui64 handle,
        ui64 offset,
        TString buffer)
    {
        auto request = std::make_shared<NProto::TWriteDataRequest>();
        request->SetHandle(handle);
        request->SetOffset(offset);
        request->SetBuffer(buffer);

        auto future = Cache.WriteData(CallContext, std::move(request));

        future.Subscribe([&, handle, offset, buffer] (auto) {
            STORAGE_INFO("Written " << buffer.Quote()
                << " to @" << handle
                << " at offset " << offset);

            auto write = [=] (auto* data) {
                // append zeroes if needed
                auto newSize = Max(data->size(), offset + buffer.size());
                data->resize(newSize, 0);
                data->replace(offset, buffer.size(), buffer);
            };

            {
                std::unique_lock lock1(ExpectedDataMutex);
                std::unique_lock lock2(UnflushedDataMutex);

                write(&ExpectedData[handle]);
                write(&UnflushedData[handle]);
            }
        });

        return future;
    }

    void WriteToCacheSync(ui64 handle, ui64 offset, TString buffer)
    {
        WriteToCache(handle, offset, buffer).GetValueSync();
    }

    void FlushCache(ui64 handle)
    {
        STORAGE_INFO("Flushing @" << handle);

        Cache.FlushData(handle).GetValueSync();

        {
            std::unique_lock lock1(ExpectedDataMutex);
            std::unique_lock lock2(UnflushedDataMutex);
            std::unique_lock lock3(FlushedDataMutex);

            UNIT_ASSERT_VALUES_EQUAL(ExpectedData[handle], FlushedData[handle]);

            if (EraseExpectedUnflushedDataAfterFirstUse) {
                UnflushedData.erase(handle);
            }
        }
    }

    void FlushCache()
    {
        STORAGE_INFO("Flushing all data");

        Cache.FlushAllData().GetValueSync();

        {
            std::unique_lock lock1(ExpectedDataMutex);
            std::unique_lock lock2(UnflushedDataMutex);
            std::unique_lock lock3(FlushedDataMutex);

            UNIT_ASSERT_VALUES_EQUAL(ExpectedData, FlushedData);

            UnflushedData.clear();
        }
    }

    void ValidateCacheIsFlushed()
    {
        std::unique_lock lock1(ExpectedDataMutex);
        std::unique_lock lock3(FlushedDataMutex);

        UNIT_ASSERT_VALUES_EQUAL(ExpectedData, FlushedData);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TWriteBackCacheTest)
{
    Y_UNIT_TEST(ShouldReadEmptyCache)
    {
        TBootstrap b;

        auto readPromise = NewPromise<NProto::TReadDataResponse>();
        b.Session->ReadDataHandler = [&] (auto, auto) {
            return readPromise.GetFuture();
        };

        NProto::TReadDataResponse response;
        // return empty buffer in response
        readPromise.SetValue(response);

        auto readFuture = b.ReadFromCache(1, 0, 1);
        UNIT_ASSERT(readFuture.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            TString(1, 0),
            readFuture.GetValue().GetBuffer());
    }

    Y_UNIT_TEST(ShouldReadAndFlushEmptyCache)
    {
        TBootstrap b;

        b.ValidateCache(1, 0, "\0\0\0");
        b.ValidateCache(2, 10, "\0\0\0\0");

        b.FlushCache();
        b.ValidateCache(1, 5, "\0\0\0");

        b.FlushCache(1);
        b.ValidateCache(1, 100, "\0\0\0\0\0\0");
    }

    Y_UNIT_TEST(ShouldMergeRequestsWhenFlushing)
    {
        TBootstrap b;

        b.WriteToCacheSync(1, 0, "aa");
        b.WriteToCacheSync(1, 1, "bbb");
        b.WriteToCacheSync(1, 2, "cccc");
        b.WriteToCacheSync(1, 3, "ddddd");
        b.WriteToCacheSync(1, 4, "eeeeee");

        bool flushed = false;

        b.Session->WriteDataHandler = [&] (auto, auto request) {
            UNIT_ASSERT(!flushed);
            flushed = true;

            UNIT_ASSERT_VALUES_EQUAL(1, request->GetHandle());
            UNIT_ASSERT_VALUES_EQUAL(0, request->GetOffset());
            UNIT_ASSERT_VALUES_EQUAL("abcdeeeeee", request->GetBuffer());

            NProto::TWriteDataResponse response;
            return MakeFuture(response);
        };

        b.Cache.FlushData(1).GetValueSync();
    }

    Y_UNIT_TEST(ShouldSequenceReadAndWriteRequestsAvoidingConflicts)
    {
        // WriteBackCache implementation ensures than Flush can execute
        // as fast as it can - when an entry is cached, nothing prevents it
        // from being flushed.
        //
        // The synchronization point is transition of pending request to cache.
        //
        // It is not allowed:
        // 1. To read data that overlaps with any pending write requests.
        // 2. To write data that overlaps with any in-flight read operation.
        //
        // It is allowed to simultaneously read the same data or have
        // multiple overlapping pending write requests.

        TBootstrap b;
        b.Session->WriteDataHandler = [&] (auto, auto) {
            return MakeFuture<NProto::TWriteDataResponse>({});
        };

        TVector<TPromise<NProto::TReadDataResponse>> readPromises;

        b.Session->ReadDataHandler = [&] (auto, auto) {
            readPromises.push_back(NewPromise<NProto::TReadDataResponse>());
            return readPromises.back().GetFuture();
        };

        auto readFuture1 = b.ReadFromCache(1, 0, 10);
        UNIT_ASSERT_VALUES_EQUAL(1, readPromises.size());
        UNIT_ASSERT(!readFuture1.HasValue());

        // Multiple read requests do not block each other
        auto readFuture2 = b.ReadFromCache(1, 0, 10);
        UNIT_ASSERT_VALUES_EQUAL(2, readPromises.size());
        UNIT_ASSERT(!readFuture2.HasValue());

        // It is not allowed to write data that is being read
        auto writeFuture1 = b.WriteToCache(1, 0, "abcdefghij");
        UNIT_ASSERT(!writeFuture1.HasValue());

        // Write requests has priority over read requests
        // Read request will not proceed because of pending write requests
        auto readFuture3 = b.ReadFromCache(1, 0, 10);
        UNIT_ASSERT_VALUES_EQUAL(2, readPromises.size());
        UNIT_ASSERT(!readFuture3.HasValue());

        // Can read and write other handle
        auto writeFuture2 = b.WriteToCache(2, 0, "abcdefghij");
        UNIT_ASSERT(writeFuture2.HasValue());
        auto readFuture4 = b.ReadFromCache(2, 5, 15);
        UNIT_ASSERT_VALUES_EQUAL(3, readPromises.size());
        UNIT_ASSERT(!readFuture4.HasValue());

        // Cannot write to [5, 15) because there is an in-flight read request
        // at [0, 10)
        auto writeFuture3 = b.WriteToCache(1, 5, "0123456789");
        UNIT_ASSERT(!writeFuture3.HasValue());

        // It is possible to write to [10, 20) because there are no in-flight
        // read requests. There are write request but they don't interfere.
        auto writeFuture4 = b.WriteToCache(1, 10, "ABCDEFGHIJ");
        UNIT_ASSERT(writeFuture4.HasValue());

        // Cannot read [10, 25) because of pending write request at [5, 15)
        auto readFuture5 = b.ReadFromCache(1, 10, 15);
        UNIT_ASSERT_VALUES_EQUAL(3, readPromises.size());
        UNIT_ASSERT(!readFuture5.HasValue());

        // It is still possible to write to [10, 20) because there are only
        // pending read requests but no in-flight requests
        auto writeFuture5 = b.WriteToCache(1, 10, "ABCDEFGHIJ");
        UNIT_ASSERT(writeFuture5.HasValue());

        // Proceed with readFuture1
        readPromises[0].SetValue({});
        UNIT_ASSERT_VALUES_EQUAL(3, readPromises.size());
        UNIT_ASSERT(readFuture1.HasValue());
        UNIT_ASSERT(!readFuture2.HasValue());
        UNIT_ASSERT(!readFuture3.HasValue());
        UNIT_ASSERT(!readFuture5.HasValue());
        UNIT_ASSERT(!writeFuture1.HasValue());
        UNIT_ASSERT(!writeFuture3.HasValue());

        // Proceed with readFuture2
        // All write requests will be completed
        // The remaining read requests ([0, 10) or [10, 25)) will proceed
        // Read request [0, 10) is fullfilled by cache - it will not pass
        // request to Session
        readPromises[1].SetValue({});
        UNIT_ASSERT_VALUES_EQUAL(4, readPromises.size());
        UNIT_ASSERT(readFuture2.HasValue());
        UNIT_ASSERT(readFuture3.HasValue());
        UNIT_ASSERT(!readFuture5.HasValue());
        UNIT_ASSERT(writeFuture1.HasValue());
        UNIT_ASSERT(writeFuture3.HasValue());

        // Complete all read requests
        readPromises[3].SetValue({});
        UNIT_ASSERT_VALUES_EQUAL(4, readPromises.size());
        UNIT_ASSERT(readFuture5.HasValue());
    }

    Y_UNIT_TEST(ShouldReadAfterWrite)
    {
        TBootstrap b;

        b.WriteToCacheSync(1, 0, "abc");
        // additional check for test correctness
        UNIT_ASSERT_VALUES_EQUAL("abc", b.ExpectedData[1]);
        b.ValidateCache();

        b.WriteToCacheSync(2, 2, "bcde");
        // additional check for test correctness
        UNIT_ASSERT_VALUES_EQUAL(TString("\0\0bcde", 6), b.ExpectedData[2]);
        b.ValidateCache();

        b.WriteToCacheSync(1, 11, "abcde");
        b.ValidateCache();

        b.WriteToCacheSync(1, 7, "bcdefgh");
        b.WriteToCacheSync(2, 0, "cdefghijkl");
        b.WriteToCacheSync(1, 3, "defghijklmn");
        b.ValidateCache();

        b.RecreateCache();
        b.ValidateCache();

        b.WriteToCacheSync(1, 0, "defgh");
        // additional check for test correctness
        UNIT_ASSERT_VALUES_EQUAL("defghfghijklmnde", b.ExpectedData[1]);
        b.ValidateCache();

        b.FlushCache();
        b.ValidateCache();

        b.WriteToCacheSync(1, 1, "ijklmn");
        b.RecreateCache();
        b.ValidateCache();
    }

    Y_UNIT_TEST(ShouldFlushAutomatically)
    {
        const auto automaticFlushPeriod = TDuration::MilliSeconds(1);
        TBootstrap b(automaticFlushPeriod);

        int retryCount = 0;

        auto waitForFlush = [&] (int attempt) {
            while (true) {
                b.Timer->Sleep(TDuration::MilliSeconds(1));

                if (b.SessionWriteDataHandlerCalled.load() < attempt) {
                    // log every 10 seconds
                    if (retryCount % 10000 == 0) {
                        auto& Log = b.Log;
                        STORAGE_INFO("Waiting for flush attempt " << attempt);
                    }

                    retryCount++;
                    continue;
                }

                UNIT_ASSERT_VALUES_EQUAL(
                    attempt,
                    b.SessionWriteDataHandlerCalled.load());
                b.ValidateCacheIsFlushed();
                return;
            }
        };

        b.WriteToCacheSync(1, 11, "abcde");
        waitForFlush(1);
        b.WriteToCacheSync(1, 22, "efghij");
        waitForFlush(2);
    }

    void TestShouldReadAfterWriteRandomized(bool withRecreation = false) {
        TBootstrap b;
        // ensures that the data is not flushed twice, does not work well with
        // cache recreation because after recreation, the data may be flushed
        // again
        b.EraseExpectedUnflushedDataAfterFirstUse = !withRecreation;

        const TString alphabet = "abcdefghijklmnopqrstuvwxyz";

        int flushesRemaining = 10;
        int writesRemaining = 333;

        while (writesRemaining--) {
            const ui64 offset = RandomNumber(alphabet.length());
            const ui64 length = Max(
                1ul,
                RandomNumber(alphabet.length() - offset));

            auto data = TStringBuf(alphabet).SubString(offset, length);

            b.WriteToCacheSync(
                RandomNumber(3u),
                offset + RandomNumber(11u),
                TString(data));

            if (RandomNumber(10u) == 0 && flushesRemaining > 0) {
                if (auto handle = RandomNumber(4u)) {
                    if (handle == 3) {
                        b.FlushCache();
                    } else {
                        b.FlushCache(handle);
                    }
                }
                flushesRemaining--;
            }

            if (withRecreation && RandomNumber(20u) == 0) {
                b.RecreateCache();
            }

            b.ValidateCache();
        }

        if (withRecreation) {
            b.RecreateCache();
        }

        b.ValidateCache();
    }

    Y_UNIT_TEST(ShouldReadAfterWriteRandomized)
    {
        TestShouldReadAfterWriteRandomized();
    }

    Y_UNIT_TEST(ShouldReadAfterWriteRandomizedWithRecreation)
    {
        TestShouldReadAfterWriteRandomized(true /* withRecreation */);
    }

    Y_UNIT_TEST(ShouldWriteAndFlushConcurrently)
    {
        TBootstrap b;

        b.WriteToCacheSync(1, 0, "cdefghijklm");
        b.WriteToCacheSync(2, 11, "defghijklmn");

        std::latch start{3};

        TVector<std::thread> threads;

        threads.emplace_back([&] {
            start.arrive_and_wait();
            b.Cache.FlushData(1).GetValueSync();
        });

        threads.emplace_back([&] {
            start.arrive_and_wait();
            b.Cache.FlushData(2).GetValueSync();
        });

        start.arrive_and_wait();
        b.Cache.FlushAllData().GetValueSync();

        for (auto& t: threads) {
            t.join();
        }

        b.RecreateCache();
        b.ValidateCache();
    }

    void TestShouldReadAfterWriteConcurrently(
        bool withManualFlush = false,
        bool withAutomaticFlush = false)
    {
        const auto automaticFlushPeriod =
            withAutomaticFlush ? TDuration::MilliSeconds(1) : TDuration();
        TBootstrap b(automaticFlushPeriod);

        const TString alphabet = "abcdefghijklmnopqrstuvwxyz";

        const ui32 rwThreadCount = 3;
        const ui32 roThreadCount = 2;
        const auto threadCount = rwThreadCount + roThreadCount;

        std::latch start{threadCount};

        TVector<std::thread> threads;

        for (ui32 i = 0; i < rwThreadCount; i++) {
            threads.emplace_back([&, handle = i] {
                start.arrive_and_wait();

                int flushesRemaining = 10;
                int writesRemaining = 333;

                while (writesRemaining--) {
                    SleepForRandomDurationMs(10);

                    const ui64 offset = RandomNumber(alphabet.length());
                    const ui64 length = Max(
                        1ul,
                        RandomNumber(alphabet.length() - offset));

                    auto data = TStringBuf(alphabet).SubString(offset, length);

                    b.WriteToCacheSync(
                        handle,
                        offset + RandomNumber(11u),
                        TString(data));

                    if (withManualFlush) {
                        if (RandomNumber(10u) == 0 && flushesRemaining > 0) {
                            b.FlushCache(handle);
                            flushesRemaining--;
                        }
                    }

                    b.ValidateCache(handle);
                }
            });
        }

        // read-only threads for "smoke" testing
        for (ui32 i = 0; i < roThreadCount; i++) {
            threads.emplace_back([&] {
                start.arrive_and_wait();

                int readsRemaining = 111;
                while (readsRemaining--) {
                    SleepForRandomDurationMs(10);

                    auto request = std::make_shared<NProto::TReadDataRequest>();
                    request->SetHandle(0);
                    request->SetOffset(0);
                    request->SetLength(333);

                    auto future = b.Cache.ReadData(
                        b.CallContext,
                        move(request));
                    auto response = future.GetValueSync();
                }
            });
        }

        for (auto& t: threads) {
            t.join();
        }

        b.RecreateCache();
        b.ValidateCache();
    }

    Y_UNIT_TEST(ShouldReadAfterWriteConcurrently)
    {
        TestShouldReadAfterWriteConcurrently();
    }

    Y_UNIT_TEST(ShouldReadAfterWriteConcurrentlyWithManualFlush)
    {
        TestShouldReadAfterWriteConcurrently(true /* withManualFlush */);
    }

    Y_UNIT_TEST(ShouldNotMissPendingEntries)
    {
        TBootstrap b;

        int writeRequestsActual = 0;
        int writeRequestsExpected = 0;
        int pendingWriteRequests = 0;
        ui64 nextOffset = 0;

        auto promise = NewPromise<NProto::TWriteDataResponse>();

        b.Session->WriteDataHandler = [&] (auto, auto) {
            writeRequestsActual++;
            return promise.GetFuture();
        };

        while (pendingWriteRequests < 32) {
            auto future = b.WriteToCache(1, nextOffset, "a");
            nextOffset += 10;
            writeRequestsExpected++;
            if (!future.HasValue()) {
                pendingWriteRequests++;
            }
        }

        promise.SetValue({});

        b.Cache.FlushData(1);

        UNIT_ASSERT_VALUES_EQUAL(writeRequestsExpected, writeRequestsActual);
    }

    /* TODO(svartmetal): fix tests with automatic flush
    Y_UNIT_TEST(ShouldReadAfterWriteConcurrentlyWithAutomaticFlush)
    {
        TestShouldReadAfterWriteConcurrently(
            false,  // withManualFlush
            true);  // withAutomaticFlush
    }

    Y_UNIT_TEST(ShouldReadAfterWriteConcurrentlyWithManualAndAutomaticFlush)
    {
        TestShouldReadAfterWriteConcurrently(
            true,   // withManualFlush
            true);  // withAutomaticFlush
    }
    */
}

}   // namespace NCloud::NFileStore::NFuse
