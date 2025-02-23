#include "write_back_cache.h"

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore_test.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/random/random.h>
#include <util/system/tempfile.h>

#include <memory>

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
    THashMap<ui64, TString> UnflushedData;
    THashMap<ui64, TString> FlushedData;

    TBootstrap()
    {
        Logging = CreateLoggingService("console", TLogSettings{});
        Logging->Start();
        Log = Logging->CreateLog("WRITE_BACK_CACHE");

        Session = std::make_shared<TFileStoreTest>();

        Session->ReadDataHandler = [&] (auto, auto request) {
            NProto::TReadDataResponse response;

            // TODO(svartmetal): fail on attempt to read not flushed data
            if (!FlushedData.contains(request->GetHandle())) {
                response.SetBuffer(TString(request->GetLength(), 0));
                return MakeFuture(response);
            }

            auto data = FlushedData[request->GetHandle()];
            // TODO(svartmetal): fail on attempt to read out of bounds
            // instead of appending zeroes
            // UNIT_ASSERT_LE(
            //    request->GetOffset() + request->GetLength(),
            //    data.length());
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
            STORAGE_INFO("Flushing " << request->GetBuffer()
                << " to @" << request->GetHandle()
                << " at offset " << request->GetOffset());

            UNIT_ASSERT(UnflushedData.contains(request->GetHandle()));

            const auto unflushed = UnflushedData[request->GetHandle()];
            UNIT_ASSERT_LE(
                request->GetOffset() + request->GetBuffer().length(),
                unflushed.length());

            auto from = request->GetBuffer();
            // TODO(svartmetal): check that request buffer matches unflushed
            // data
            // auto from = TStringBuf(unflushed).SubString(
            //    request->GetOffset(),
            //    request->GetBuffer().length());
            // UNIT_ASSERT_VALUES_EQUAL(from, request->GetBuffer());

            auto& to = FlushedData[request->GetHandle()];
            // append zeroes if needed
            auto newSize = Max(to.size(), request->GetOffset() + from.size());
            to.resize(newSize, 0);
            to.replace(request->GetOffset(), from.size(), from);

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

    void ValidateCache(ui64 handle, ui64 offset, ui32 length)
    {
        auto request = std::make_shared<NProto::TReadDataRequest>();
        request->SetHandle(handle);
        request->SetOffset(offset);
        request->SetLength(length);

        auto future = Cache->ReadData(CallContext, move(request));
        auto response = future.GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(
            TStringBuf(ExpectedData[handle]).SubString(offset, length),
            response.GetBuffer(),
            TStringBuilder() << " while validating @" << handle
            << " at offset " << offset
            << " and length " << length);
    }

    void ValidateCache()
    {
        for (const auto& [handle, data]: ExpectedData) {
            for (size_t offset = 0; offset < data.length(); offset++) {
                for (size_t len = 1; len + offset < data.length(); len++) {
                    ValidateCache(handle, offset, len);
                }
            }
        }
    }

    bool WriteToCache(ui64 handle, ui64 offset, TString buffer)
    {
        auto request = std::make_shared<NProto::TWriteDataRequest>();
        request->SetHandle(handle);
        request->SetOffset(offset);
        request->SetBuffer(buffer);

        if (!Cache->AddWriteDataRequest(std::move(request))) {
            return false;
        }

        STORAGE_INFO("Written " << buffer.Quote()
            << " to @" << handle
            << " at offset " << offset);

        auto write = [=] (auto* data) {
            // append zeroes if needed
            auto newSize = Max(data->size(), offset + buffer.size());
            data->resize(newSize, 0);
            data->replace(offset, buffer.size(), buffer);
        };
        write(&ExpectedData[handle]);
        write(&UnflushedData[handle]);

        return true;
    }

    void FlushCache()
    {
        Cache->FlushAllData().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(ExpectedData, FlushedData);

        UnflushedData.clear();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TWriteBackCacheTest)
{
    Y_UNIT_TEST(ShouldReadAfterWrite)
    {
        TBootstrap b;

        // ok to flush empty cache
        b.FlushCache();

        UNIT_ASSERT(b.WriteToCache(1, 0, "abc"));
        // additional check for test correctness
        UNIT_ASSERT_VALUES_EQUAL("abc", b.ExpectedData[1]);
        b.ValidateCache();

        UNIT_ASSERT(b.WriteToCache(2, 2, "bcde"));
        // additional check for test correctness
        UNIT_ASSERT_VALUES_EQUAL(TString("\0\0bcde", 6), b.ExpectedData[2]);
        b.ValidateCache();

        UNIT_ASSERT(b.WriteToCache(1, 11, "abcde"));
        b.ValidateCache();

        UNIT_ASSERT(b.WriteToCache(1, 7, "bcdefgh"));
        UNIT_ASSERT(b.WriteToCache(2, 0, "cdefghijkl"));
        UNIT_ASSERT(b.WriteToCache(1, 3, "defghijklmn"));
        b.ValidateCache();

        b.RecreateCache();
        b.ValidateCache();

        UNIT_ASSERT(b.WriteToCache(1, 0, "defgh"));
        // additional check for test correctness
        UNIT_ASSERT_VALUES_EQUAL("defghfghijklmnde", b.ExpectedData[1]);
        b.ValidateCache();

        b.FlushCache();
        b.ValidateCache();

        UNIT_ASSERT(b.WriteToCache(1, 1, "ijklmn"));
        b.RecreateCache();
        b.ValidateCache();
    }

    Y_UNIT_TEST(ShouldReadAfterWriteRandomized)
    {
        TBootstrap b;

        const TString alphabet = "abcdefghijklmnopqrstuvwxyz";

        int flushesRemaining = 10;

        bool possibleToAdd = true;
        while (possibleToAdd) {
            const ui32 offset = RandomNumber(alphabet.length());
            const ui32 length = Max(
                1ul,
                RandomNumber(alphabet.length() - offset));

            auto data = TStringBuf(alphabet).SubString(offset, length);

            possibleToAdd = b.WriteToCache(
                RandomNumber(3u),
                offset + RandomNumber(11u),
                TString(data));

            if (RandomNumber(10u) == 0 && flushesRemaining > 0) {
                b.FlushCache();
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
}

}   // namespace NCloud::NFileStore::NFuse
