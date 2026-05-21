#include "persistent_storage.h"

#include "persistent_storage_stats.h"

#include <cloud/filestore/libs/diagnostics/metrics/metric.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/filemap.h>
#include <util/system/tempfile.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 DefaultCapacity = 256;

struct TBootstrap
{
    ILoggingServicePtr Logging;
    TLog Log;

    TTempFileHandle TempFile;
    IPersistentStorageStatsPtr Stats;
    IPersistentStoragePtr Storage;
    TPersistentStorageMetrics Metrics;

    TBootstrap()
        : Stats(CreatePersistentStorageStats())
        , Metrics(Stats->CreateMetrics())
    {
        Logging = CreateLoggingService("console", TLogSettings{});
        Logging->Start();
        Log = Logging->CreateLog("WRITE_BACK_CACHE");
    }

    NProto::TError Initialize()
    {
        auto res = CreateFileRingBufferPersistentStorage(
            Stats,
            {.FilePath = TempFile.GetName(),
             .DataCapacity = DefaultCapacity,
             .MetadataCapacity = 0,
             .EnableChecksumValidation = true},
            Log,
            "[tag]");

        if (HasError(res)) {
            return res.GetError();
        }

        Storage = res.ExtractResult();
        return {};
    }

    void Deinitialize()
    {
        Storage = {};
    }

    NProto::TError Recreate()
    {
        Deinitialize();
        return Initialize();
    }

    const char* Alloc(const TString& data) const
    {
        auto allocationResult = Storage->Alloc(data.size());
        if (HasError(allocationResult)) {
            return nullptr;
        }

        char* ptr = allocationResult.GetResult();
        if (ptr != nullptr) {
            data.copy(ptr, data.size());
            Storage->Commit();
        }
        return ptr;
    }

    bool AllocHasError(const TString& data) const
    {
        auto allocationResult = Storage->Alloc(data.size());
        return HasError(allocationResult);
    }

    void Free(const void* ptr) const
    {
        Storage->Free(ptr);
    }

    TString Dump() const
    {
        TString res;
        Storage->Visit(
            [&res](TStringBuf entry)
            {
                if (!res.empty()) {
                    res += ",";
                }
                res.append(entry);
            });

        return res;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPersistentStorageTest)
{
    Y_UNIT_TEST(Simple)
    {
        TBootstrap b;
        auto& stats = b.Metrics.Storage;

        UNIT_ASSERT(!HasError(b.Initialize()));
        UNIT_ASSERT(b.Storage->Empty());

        const auto* ptr1 = b.Alloc("1234");
        UNIT_ASSERT(ptr1);
        UNIT_ASSERT_VALUES_EQUAL(1, stats.EntryCount->Get());

        const auto* ptr2 = b.Alloc("567890");
        UNIT_ASSERT(ptr2);
        UNIT_ASSERT_VALUES_EQUAL(2, stats.EntryCount->Get());

        UNIT_ASSERT(!b.Alloc(TString(DefaultCapacity, 'x')));
        UNIT_ASSERT_VALUES_EQUAL(2, stats.EntryCount->Get());

        const auto* ptr3 = b.Alloc("abc");
        UNIT_ASSERT(ptr3);
        UNIT_ASSERT_VALUES_EQUAL(3, stats.EntryCount->Get());

        UNIT_ASSERT(!b.Storage->Empty());
        UNIT_ASSERT_STRINGS_EQUAL("1234,567890,abc", b.Dump());

        b.Free(ptr2);
        UNIT_ASSERT_STRINGS_EQUAL("1234,abc", b.Dump());
        UNIT_ASSERT_VALUES_EQUAL(2, stats.EntryCount->Get());

        b.Free(ptr1);
        UNIT_ASSERT_STRINGS_EQUAL("abc", b.Dump());
        UNIT_ASSERT_VALUES_EQUAL(1, stats.EntryCount->Get());

        b.Free(ptr3);
        UNIT_ASSERT_STRINGS_EQUAL("", b.Dump());
        UNIT_ASSERT(b.Storage->Empty());
        UNIT_ASSERT_VALUES_EQUAL(0, stats.EntryCount->Get());
    }

    Y_UNIT_TEST(ShouldThrowOnDoubleFree)
    {
        TBootstrap b;

        UNIT_ASSERT(!HasError(b.Initialize()));
        UNIT_ASSERT(b.Storage->Empty());

        const auto* ptr1 = b.Alloc("1234");
        UNIT_ASSERT(ptr1);

        const auto* ptr2 = b.Alloc("567890");
        UNIT_ASSERT(ptr2);

        b.Free(ptr2);
        UNIT_ASSERT_EXCEPTION(b.Free(ptr2), yexception);

        b.Free(ptr1);
        UNIT_ASSERT_EXCEPTION(b.Free(ptr1), yexception);
    }

    Y_UNIT_TEST(ShouldValidateAllocationSize)
    {
        TBootstrap b;

        UNIT_ASSERT(!HasError(b.Initialize()));
        UNIT_ASSERT(b.Storage->Empty());

        UNIT_ASSERT(b.AllocHasError(""));
    }

    Y_UNIT_TEST(ShouldRecreateStorage)
    {
        TBootstrap b;

        UNIT_ASSERT(!HasError(b.Initialize()));
        UNIT_ASSERT(b.Storage->Empty());

        const auto* ptr1 = b.Alloc("1234");
        UNIT_ASSERT(ptr1);

        const auto* ptr2 = b.Alloc("567890");
        UNIT_ASSERT(ptr2);

        UNIT_ASSERT(!HasError(b.Recreate()));

        UNIT_ASSERT_STRINGS_EQUAL("1234,567890", b.Dump());
    }

    Y_UNIT_TEST(ShouldDetectCorruption)
    {
        TBootstrap b;
        auto& stats = b.Metrics.Storage;

        UNIT_ASSERT(!HasError(b.Initialize()));
        UNIT_ASSERT(b.Storage->Empty());

        const auto* ptr1 = b.Alloc("1234");
        UNIT_ASSERT(ptr1);

        const auto* ptr2 = b.Alloc("567890");
        UNIT_ASSERT(ptr2);

        b.Deinitialize();

        {
            // Simulate data corruption
            TFileMap m(b.TempFile.GetName(), TMemoryMapCommon::oRdWr);
            const ui64 len = b.TempFile.GetLength();
            UNIT_ASSERT(DefaultCapacity < len);
            m.Map(0, len);
            char* data = static_cast<char*>(m.Ptr());
            for (auto ofs = len - DefaultCapacity; ofs < len; ++ofs) {
                data[ofs] ^= 0xFF;
            }
        }

        UNIT_ASSERT(HasError(b.Recreate()));
        UNIT_ASSERT(stats.Corrupted->Get());
    }

    Y_UNIT_TEST(ShouldReportAndUpdateStats)
    {
        TBootstrap b;
        auto& stats = b.Metrics.Storage;

        UNIT_ASSERT(!HasError(b.Initialize()));
        UNIT_ASSERT(b.Storage->Empty());

        const auto* ptr1 = b.Alloc("1234");
        UNIT_ASSERT(ptr1);

        const auto* ptr2 = b.Alloc("567890");
        UNIT_ASSERT(ptr2);

        auto maxByteCount = stats.RawUsedByteMaxCount->Get();

        UNIT_ASSERT_VALUES_EQUAL(2, stats.EntryCount->Get());
        UNIT_ASSERT_VALUES_EQUAL(2, stats.EntryMaxCount->Get());
        UNIT_ASSERT_LT(0, stats.RawUsedByteCount->Get());
        UNIT_ASSERT_VALUES_EQUAL(maxByteCount, stats.RawUsedByteCount->Get());
        UNIT_ASSERT_VALUES_EQUAL(
            DefaultCapacity,
            stats.RawCapacityByteCount->Get());

        b.Free(ptr1);

        UNIT_ASSERT_VALUES_EQUAL(1, stats.EntryCount->Get());
        UNIT_ASSERT_VALUES_EQUAL(2, stats.EntryMaxCount->Get());
        UNIT_ASSERT_LT(0, stats.RawUsedByteCount->Get());
        UNIT_ASSERT_GT(maxByteCount, stats.RawUsedByteCount->Get());
        UNIT_ASSERT_VALUES_EQUAL(
            maxByteCount,
            stats.RawUsedByteMaxCount->Get());

        for (int i = 0; i <= 15; ++i) {
            b.Storage->UpdateStats();
        }

        UNIT_ASSERT_VALUES_EQUAL(1, stats.EntryCount->Get());
        UNIT_ASSERT_VALUES_EQUAL(1, stats.EntryMaxCount->Get());
        UNIT_ASSERT_LT(0, stats.RawUsedByteCount->Get());
        UNIT_ASSERT_GT(maxByteCount, stats.RawUsedByteCount->Get());
        UNIT_ASSERT_VALUES_EQUAL(
            stats.RawUsedByteCount->Get(),
            stats.RawUsedByteMaxCount->Get());
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
