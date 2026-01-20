#include "persistent_storage_impl.h"

#include <cloud/filestore/libs/vfs_fuse/write_back_cache/test/write_back_cache_stats_test.h>

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
    TTestWriteBackCacheStats Stats;
    TFileRingBufferStorage Storage;

    NProto::TError Initialize()
    {
        return Storage.Init(
            Stats,
            {.FilePath = TempFile.GetName(),
             .DataCapacity = DefaultCapacity,
             .MetadataCapacity = 0,
             .EnableChecksumCalculation = true,
             .EnableChecksumValidation = true});
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

    const void* Alloc(const TString& data)
    {
        return Storage.Alloc(
            [&data](char* ptr, size_t size)
            {
                UNIT_ASSERT(size == data.size());
                data.copy(ptr, size);
            },
            data.size());
    }

    void Free(const void* ptr)
    {
        Storage.Free(ptr);
    }

    TString Dump()
    {
        TString res;
        Storage.Visit(
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

        UNIT_ASSERT(!HasError(b.Initialize()));
        UNIT_ASSERT(b.Storage.Empty());

        const auto* ptr1 = b.Alloc("1234");
        UNIT_ASSERT(ptr1);

        const auto* ptr2 = b.Alloc("567890");
        UNIT_ASSERT(ptr2);

        UNIT_ASSERT(!b.Alloc(TString(DefaultCapacity, 'x')));

        const auto* ptr3 = b.Alloc("abc");
        UNIT_ASSERT(ptr3);

        UNIT_ASSERT(!b.Storage.Empty());
        UNIT_ASSERT_STRINGS_EQUAL("1234,567890,abc", b.Dump());

        b.Free(ptr2);
        UNIT_ASSERT_STRINGS_EQUAL("1234,abc", b.Dump());

        b.Free(ptr1);
        UNIT_ASSERT_STRINGS_EQUAL("abc", b.Dump());

        b.Free(ptr3);
        UNIT_ASSERT_STRINGS_EQUAL("", b.Dump());
        UNIT_ASSERT(b.Storage.Empty());
    }

    Y_UNIT_TEST(ShouldThrowOnDoubleFree)
    {
        TBootstrap b;

        UNIT_ASSERT(!HasError(b.Initialize()));
        UNIT_ASSERT(b.Storage.Empty());

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
        UNIT_ASSERT(b.Storage.Empty());

        UNIT_ASSERT_EXCEPTION_C(
            b.Alloc(""),
            yexception,
            "The requested allocation size is zero");

        UNIT_ASSERT_EXCEPTION_C(
            b.Alloc(TString(DefaultCapacity + 1, 'x')),
            yexception,
            "The requested allocation size exceeds the storage capacity");
    }

    Y_UNIT_TEST(ShouldRecreateStorage)
    {
        TBootstrap b;

        UNIT_ASSERT(!HasError(b.Initialize()));
        UNIT_ASSERT(b.Storage.Empty());

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

        UNIT_ASSERT(!HasError(b.Initialize()));
        UNIT_ASSERT(b.Storage.Empty());

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
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
