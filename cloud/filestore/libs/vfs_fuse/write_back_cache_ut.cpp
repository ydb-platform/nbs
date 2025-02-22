#include "write_back_cache.h"

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore_test.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/random/random.h>
#include <util/system/tempfile.h>

#include <latch>
#include <memory>
#include <mutex>
#include <random>
#include <thread>

namespace NCloud::NFileStore::NFuse {

using namespace std::chrono_literals;

using namespace NCloud::NFileStore::NVFS;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 CacheSize = 1024;

////////////////////////////////////////////////////////////////////////////////

void SleepForRandomDuration(ui32 lowDurationUs, ui32 highDurationUs)
{
    Y_ABORT_UNLESS(lowDurationUs < highDurationUs);

    std::random_device device;
    std::mt19937 generator(device());
    std::uniform_int_distribution<> distribution(lowDurationUs, highDurationUs);

    const auto duration = distribution(generator);
    std::this_thread::sleep_for(duration*1us);
}

////////////////////////////////////////////////////////////////////////////////

struct TInFlightRequestTracker
{
    TVector<ui32> InFlightRequests;
    TMutex Lock;

    ui32 Count(ui64 offset, ui64 length)
    {
        ui32 res = 0;

        with_lock (Lock) {
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

        with_lock (Lock) {
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
        with_lock (Lock) {
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
    TTempFileHandle TempFileHandle;
    TWriteBackCachePtr Cache;

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

    TBootstrap()
    {
        Logging = CreateLoggingService("console", TLogSettings{});
        Logging->Start();
        Log = Logging->CreateLog("WRITE_BACK_CACHE");

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
                response.SetBuffer(TString(request->GetLength(), 0));
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
            response.SetBuffer(data);
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

            NProto::TWriteDataResponse response;
            return MakeFuture(response);
        };

        RecreateCache();

        CallContext = MakeIntrusive<TCallContext>();
    }

    ~TBootstrap() = default;

    void RecreateCache()
    {
        STORAGE_INFO("Recreating cache");

        Cache = CreateWriteBackCache(
            Session,
            TempFileHandle.GetName(),
            CacheSize);
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

        return Cache->ReadData(CallContext, move(request));
    }

    void ValidateCache(ui64 handle, ui64 offset, TString expected)
    {
        auto future = ReadFromCache(handle, offset, expected.length());
        auto response = future.GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(
            expected,
            response.GetBuffer(),
            TStringBuilder() << " while validating @" << handle
            << " at offset " << offset
            << " and length " << expected.length());
    }

    void ValidateCache(ui64 handle)
    {
        TString data;
        {
            std::unique_lock lock(ExpectedDataMutex);
            data = ExpectedData[handle];
        }

        for (size_t offset = 0; offset < data.length(); offset++) {
            for (size_t len = 1; len + offset < data.length(); len++) {
                auto substr = TStringBuf(data).SubString(offset, len);
                // TODO(svartmetal): validate with 'out of bounds'
                // requests also
                ValidateCache(handle, offset, TString(substr));
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

        auto future = Cache->WriteData(CallContext, std::move(request));

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

        Cache->FlushData(handle).GetValueSync();

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

        Cache->FlushAllData().GetValueSync();

        {
            std::unique_lock lock1(ExpectedDataMutex);
            std::unique_lock lock2(UnflushedDataMutex);
            std::unique_lock lock3(FlushedDataMutex);

            UNIT_ASSERT_VALUES_EQUAL(ExpectedData, FlushedData);

            UnflushedData.clear();
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TWriteBackCacheTest)
{
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

        b.Cache->FlushData(1).GetValueSync();
    }

    Y_UNIT_TEST(ShouldSequenceReadAndWriteRequestsAvoidingConflicts)
    {
        TBootstrap b;

        ui32 readAttempts = 0;

        auto readPromise1 = NewPromise<NProto::TReadDataResponse>();
        b.Session->ReadDataHandler = [&] (auto, auto) {
            readAttempts++;
            return readPromise1.GetFuture();
        };

        auto readFuture1 = b.ReadFromCache(1, 0, 10);
        UNIT_ASSERT_VALUES_EQUAL(1, readAttempts);
        UNIT_ASSERT(!readFuture1.HasValue());

        auto readPromise2 = NewPromise<NProto::TReadDataResponse>();
        b.Session->ReadDataHandler = [&] (auto, auto) {
            readAttempts++;
            return readPromise2.GetFuture();
        };

        auto readFuture2 = b.ReadFromCache(1, 0, 10);
        // it is allowed to read same data twice
        UNIT_ASSERT_VALUES_EQUAL(2, readAttempts);
        UNIT_ASSERT(!readFuture2.HasValue());

        auto flushPromise1 = NewPromise<NProto::TWriteDataResponse>();

        ui32 flushAttempts = 0;
        b.Session->WriteDataHandler = [&] (auto, auto) {
            flushAttempts++;
            return flushPromise1.GetFuture();
        };

        auto writeFuture1 = b.WriteToCache(1, 0, "abcdefghij");
        // data is stored in cache, flush should not happen
        UNIT_ASSERT_VALUES_EQUAL(0, flushAttempts);
        UNIT_ASSERT(writeFuture1.HasValue());

        auto flushFuture1 = b.Cache->FlushData(1);
        // it is not allowed to flush because read requests are in progress
        UNIT_ASSERT_VALUES_EQUAL(0, flushAttempts);
        UNIT_ASSERT(!flushFuture1.HasValue());

        NProto::TReadDataResponse response;
        response.SetBuffer("abcdefghij");

        readPromise1.SetValue(response);
        // still it is not allowed to flush, because one read request is left
        UNIT_ASSERT_VALUES_EQUAL(0, flushAttempts);
        UNIT_ASSERT(!flushFuture1.HasValue());

        UNIT_ASSERT_VALUES_EQUAL(2, readAttempts);
        UNIT_ASSERT(!readFuture2.HasValue());

        readPromise2.SetValue(response);
        // finally it is allowed to flush
        UNIT_ASSERT_VALUES_EQUAL(1, flushAttempts);
        UNIT_ASSERT(!flushFuture1.HasValue());

        auto readPromise3 = NewPromise<NProto::TReadDataResponse>();
        b.Session->ReadDataHandler = [&] (auto, auto) {
            readAttempts++;
            return readPromise3.GetFuture();
        };

        auto readFuture3 = b.ReadFromCache(1, 10, 10);
        // and it is allowed to read other (non-overlapping) range
        UNIT_ASSERT_VALUES_EQUAL(3, readAttempts);
        UNIT_ASSERT(!readFuture3.HasValue());

        auto readPromise4 = NewPromise<NProto::TReadDataResponse>();
        b.Session->ReadDataHandler = [&] (auto, auto) {
            readAttempts++;
            return readPromise4.GetFuture();
        };

        auto readFuture4 = b.ReadFromCache(1, 0, 11);
        // but it is not allowed to read range that is not flushed yet
        UNIT_ASSERT_VALUES_EQUAL(3, readAttempts);
        UNIT_ASSERT(!readFuture4.HasValue());

        // finish writing (flushing) the data
        flushPromise1.SetValue({});
        UNIT_ASSERT(flushFuture1.HasValue());
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
            b.Cache->FlushData(1).GetValueSync();
        });

        threads.emplace_back([&] {
            start.arrive_and_wait();
            b.Cache->FlushData(2).GetValueSync();
        });

        start.arrive_and_wait();
        b.Cache->FlushAllData().GetValueSync();

        for (auto& t: threads) {
            t.join();
        }

        b.RecreateCache();
        b.ValidateCache();
    }

    void TestShouldReadAfterWriteConcurrently(bool withManualFlush = false)
    {
        TBootstrap b;

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
                    SleepForRandomDuration(0, 10);

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
                    SleepForRandomDuration(0, 10);

                    auto request = std::make_shared<NProto::TReadDataRequest>();
                    request->SetHandle(0);
                    request->SetOffset(0);
                    request->SetLength(333);

                    auto future = b.Cache->ReadData(
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

    Y_UNIT_TEST(ShouldReadAfterWriteAndFlushConcurrently)
    {
        TestShouldReadAfterWriteConcurrently(true /* withManualFlush */);
    }
}

}   // namespace NCloud::NFileStore::NFuse
