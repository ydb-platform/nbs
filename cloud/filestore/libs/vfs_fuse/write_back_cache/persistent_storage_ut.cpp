#include "persistent_storage.h"

#include <cloud/filestore/libs/vfs_fuse/write_back_cache/test/test_write_back_cache_stats.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/filemap.h>
#include <util/system/tempfile.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 DefaultCapacity = 256;

struct TBootstrap
{
    TTempFileHandle TempFile;
    std::shared_ptr<TTestWriteBackCacheStats> Stats;
    IPersistentStoragePtr Storage;

    TBootstrap()
        : Stats(std::make_shared<TTestWriteBackCacheStats>())
    {}

    NProto::TError Initialize()
    {
        auto res = CreateFileRingBufferPersistentStorage(
            Stats,
            {.FilePath = TempFile.GetName(),
             .DataCapacity = DefaultCapacity,
             .MetadataCapacity = 0,
             .EnableChecksumValidation = true});

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
        UNIT_ASSERT(!HasError(allocationResult));

        char* ptr = allocationResult.GetResult();
        if (ptr != nullptr) {
            data.copy(ptr, data.size());
            UNIT_ASSERT(Storage->Commit());
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
        auto& stats = b.Stats->StorageStats;

        UNIT_ASSERT(!HasError(b.Initialize()));
        UNIT_ASSERT(b.Storage->Empty());

        const auto* ptr1 = b.Alloc("1234");
        UNIT_ASSERT(ptr1);
        UNIT_ASSERT_VALUES_EQUAL(1, stats.EntryCount);

        const auto* ptr2 = b.Alloc("567890");
        UNIT_ASSERT(ptr2);
        UNIT_ASSERT_VALUES_EQUAL(2, stats.EntryCount);

        UNIT_ASSERT(!b.Alloc(TString(DefaultCapacity, 'x')));
        UNIT_ASSERT_VALUES_EQUAL(2, stats.EntryCount);

        const auto* ptr3 = b.Alloc("abc");
        UNIT_ASSERT(ptr3);
        UNIT_ASSERT_VALUES_EQUAL(3, stats.EntryCount);

        UNIT_ASSERT(!b.Storage->Empty());
        UNIT_ASSERT_STRINGS_EQUAL("1234,567890,abc", b.Dump());

        b.Free(ptr2);
        UNIT_ASSERT_STRINGS_EQUAL("1234,abc", b.Dump());
        UNIT_ASSERT_VALUES_EQUAL(2, stats.EntryCount);

        b.Free(ptr1);
        UNIT_ASSERT_STRINGS_EQUAL("abc", b.Dump());
        UNIT_ASSERT_VALUES_EQUAL(1, stats.EntryCount);

        b.Free(ptr3);
        UNIT_ASSERT_STRINGS_EQUAL("", b.Dump());
        UNIT_ASSERT(b.Storage->Empty());
        UNIT_ASSERT_VALUES_EQUAL(0, stats.EntryCount);
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
        auto& stats = b.Stats->StorageStats;

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
        UNIT_ASSERT(stats.IsCorrupted);
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
