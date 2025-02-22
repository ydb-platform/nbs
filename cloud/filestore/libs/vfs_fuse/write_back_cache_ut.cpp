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
#include <thread>

namespace NCloud::NFileStore::NFuse {

using namespace NCloud::NFileStore::NVFS;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 CacheSize = 1024;

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

    TBootstrap()
    {
        Logging = CreateLoggingService("console", TLogSettings{});
        Logging->Start();
        Log = Logging->CreateLog("WRITE_BACK_CACHE");

        Session = std::make_shared<TFileStoreTest>();

        Session->ReadDataHandler = [&] (auto, auto request) {
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

            // TODO(svartmetal): zero unflushed data as it is no longer needed
            // memset(const_cast<char*>(from.data()), char(0), from.length());

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

    void ValidateCache(ui64 handle, ui64 offset, TString expected)
    {
        auto request = std::make_shared<NProto::TReadDataRequest>();
        request->SetHandle(handle);
        request->SetOffset(offset);
        request->SetLength(expected.length());

        auto future = Cache->ReadData(CallContext, move(request));
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

    void WriteToCache(ui64 handle, ui64 offset, TString buffer)
    {
        auto request = std::make_shared<NProto::TWriteDataRequest>();
        request->SetHandle(handle);
        request->SetOffset(offset);
        request->SetBuffer(buffer);

        Cache->WriteData(CallContext, std::move(request)).GetValueSync();

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
    }

    void FlushCache(ui64 handle)
    {
        STORAGE_INFO("Flushing @" << handle);

        Cache->FlushData(handle).GetValueSync();

        {
            std::unique_lock lock1(ExpectedDataMutex);
            std::unique_lock lock2(FlushedDataMutex);

            UNIT_ASSERT_VALUES_EQUAL(ExpectedData[handle], FlushedData[handle]);

            // TODO(svartmetal): erase unflushed data
            // UnflushedData.erase(handle);
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

    Y_UNIT_TEST(ShouldWriteAndFlushCacheConcurrently)
    {
        TBootstrap b;

        b.WriteToCache(1, 0, "cdefghijklm");
        b.WriteToCache(2, 11, "defghijklmn");

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

    Y_UNIT_TEST(ShouldReadAfterWrite)
    {
        TBootstrap b;

        b.WriteToCache(1, 0, "abc");
        // additional check for test correctness
        UNIT_ASSERT_VALUES_EQUAL("abc", b.ExpectedData[1]);
        b.ValidateCache();

        b.WriteToCache(2, 2, "bcde");
        // additional check for test correctness
        UNIT_ASSERT_VALUES_EQUAL(TString("\0\0bcde", 6), b.ExpectedData[2]);
        b.ValidateCache();

        b.WriteToCache(1, 11, "abcde");
        b.ValidateCache();

        b.WriteToCache(1, 7, "bcdefgh");
        b.WriteToCache(2, 0, "cdefghijkl");
        b.WriteToCache(1, 3, "defghijklmn");
        b.ValidateCache();

        b.RecreateCache();
        b.ValidateCache();

        b.WriteToCache(1, 0, "defgh");
        // additional check for test correctness
        UNIT_ASSERT_VALUES_EQUAL("defghfghijklmnde", b.ExpectedData[1]);
        b.ValidateCache();

        b.FlushCache();
        b.ValidateCache();

        b.WriteToCache(1, 1, "ijklmn");
        b.RecreateCache();
        b.ValidateCache();
    }

    Y_UNIT_TEST(ShouldReadAfterWriteRandomized)
    {
        TBootstrap b;

        const TString alphabet = "abcdefghijklmnopqrstuvwxyz";

        int flushesRemaining = 10;
        int writesRemaining = 333;

        while (writesRemaining--) {
            const ui32 offset = RandomNumber(alphabet.length());
            const ui32 length = Max(
                1ul,
                RandomNumber(alphabet.length() - offset));

            auto data = TStringBuf(alphabet).SubString(offset, length);

            b.WriteToCache(
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

            if (RandomNumber(20u) == 0) {
                b.RecreateCache();
            }

            b.ValidateCache();
        }

        b.RecreateCache();
        b.ValidateCache();
    }

    void TestShouldReadAfterWriteConcurrently(bool withFlush)
    {
        TBootstrap b;

        const TString alphabet = "abcdefghijklmnopqrstuvwxyz";

        const ui32 threadCount = 3;
        std::latch start{threadCount};

        TVector<std::thread> threads;

        for (ui32 i = 0; i < threadCount; i++) {
            threads.emplace_back([withFlush, &b, alphabet, &start, handle = i] {
                start.arrive_and_wait();

                int flushesRemaining = 10;
                int writesRemaining = 333;

                while (writesRemaining--) {
                    const ui32 offset = RandomNumber(alphabet.length());
                    const ui32 length = Max(
                        1ul,
                        RandomNumber(alphabet.length() - offset));

                    auto data = TStringBuf(alphabet).SubString(offset, length);

                    b.WriteToCache(
                        handle,
                        offset + RandomNumber(11u),
                        TString(data));

                    if (withFlush) {
                        if (RandomNumber(10u) == 0 && flushesRemaining > 0) {
                            b.FlushCache(handle);
                            flushesRemaining--;
                        }
                    }

                    b.ValidateCache(handle);
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
        TestShouldReadAfterWriteConcurrently(false /* withFlush */);
    }

    Y_UNIT_TEST(ShouldReadAfterWriteWithFlushConcurrently)
    {
        TestShouldReadAfterWriteConcurrently(true /* withFlush */);
    }
}

}   // namespace NCloud::NFileStore::NFuse
