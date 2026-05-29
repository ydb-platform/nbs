#include "directory_handle_cache.h"

#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/buffer.h>
#include <util/generic/size_literals.h>

#include <algorithm>

namespace NCloud::NFileStore::NFuse {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestFileMapMemoryLimiter final: public IFileMapMemoryLimiter
{
    bool CanIncreaseResult = true;
    ui64 Current = 0;

    bool CanIncrease(ui64 value) const override
    {
        Y_UNUSED(value);
        return CanIncreaseResult;
    }

    void Increase(ui64 value) override
    {
        Current += value;
    }

    void Decrease(ui64 value) override
    {
        UNIT_ASSERT(Current >= value);
        Current -= value;
    }
};

TBufferPtr CreateContent(size_t size, char value)
{
    auto content = std::make_shared<TBuffer>();
    content->Resize(size);
    std::fill(content->Data(), content->Data() + content->Size(), value);
    return content;
}

struct TDirectoryHandleCacheTestFixture: public NUnitTest::TBaseFixture
{
    ILoggingServicePtr Logging = CreateLoggingService("console");
    TLog Log = Logging->CreateLog("DIR_HANDLE_CACHE_TEST");

    std::shared_ptr<TTestFileMapMemoryLimiter> Limiter =
        std::make_shared<TTestFileMapMemoryLimiter>();
    TTempDir TempDir;
    TString StoragePath = TempDir.Path() / "directory_handles";

    TDirectoryHandleStoragePtr CreateStorage(
        ui64 persistentHandleMaxSize = 2_GB)
    {
        return CreateDirectoryHandleStorage(
            {.Log = Log,
             .FileMapMemoryLimiter = Limiter,
             .FilePath = StoragePath,
             .MaxRecords = 32,
             .InitialDataAreaSize = 128,
             .MaxDataAreaStepSize = 128,
             .InitialDataMoveBufferSize = 64,
             .PersistentHandleMaxSize = persistentHandleMaxSize});
    }

    TDirectoryHandleCache CreateCache(
        TDirectoryHandleStoragePtr storage = nullptr)
    {
        return TDirectoryHandleCache(
            Log,
            CreateDirectoryHandleStats(CreateWallClockTimer()),
            std::move(storage));
    }

    TDirectoryHandleCache CreateStoredCache()
    {
        return CreateCache(CreateStorage());
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE_F(TDirectoryHandleCacheTest, TDirectoryHandleCacheTestFixture)
{
    Y_UNIT_TEST(ShouldBypassStorageAfterUpdateFailure)
    {
        const ui64 handleId = [&]
        {
            auto cache = CreateStoredCache();

            const ui64 id = cache.CreateHandle(42);
            auto handle = cache.FindHandle(id);
            UNIT_ASSERT(handle);

            Limiter->CanIncreaseResult = false;
            auto chunk = handle->UpdateContent(
                1024,
                0,
                CreateContent(1024, 'x'),
                1,
                "next");
            cache.AppendChunk(id, handle, chunk);

            Limiter->CanIncreaseResult = true;
            chunk = handle->UpdateContent(
                1024,
                1024,
                CreateContent(1024, 'y'),
                2,
                {});
            cache.AppendChunk(id, handle, chunk);

            return id;
        }();

        auto storage = CreateStorage();

        TDirectoryHandleMap handles;
        storage->LoadHandles(handles);

        UNIT_ASSERT_VALUES_EQUAL(0, handles.size());
        UNIT_ASSERT(!handles.contains(handleId));
    }

    Y_UNIT_TEST(ShouldReturnChunkForClosedHandleContentUpdate)
    {
        auto cache = CreateCache();

        const ui64 id = cache.CreateHandle(42);
        auto handle = cache.FindHandle(id);
        UNIT_ASSERT(handle);

        const auto [serializedSize, chunkCount] = handle->GetMetrics();

        cache.RemoveHandle(id);

        auto chunk =
            handle->UpdateContent(1024, 0, CreateContent(1024, 'x'), 1, {});

        UNIT_ASSERT_VALUES_EQUAL(1024, chunk.DirectoryContent.GetSize());
        const auto [updatedSerializedSize, updatedChunkCount] =
            handle->GetMetrics();
        UNIT_ASSERT_GT(updatedSerializedSize, serializedSize);
        UNIT_ASSERT_VALUES_EQUAL(chunkCount + 1, updatedChunkCount);
    }

    Y_UNIT_TEST(ShouldNotStoreClosedHandleContentUpdate)
    {
        ui64 id = 0;
        {
            auto cache = CreateStoredCache();

            id = cache.CreateHandle(42);
            auto handle = cache.FindHandle(id);
            UNIT_ASSERT(handle);

            cache.RemoveHandle(id);

            auto chunk =
                handle->UpdateContent(1024, 0, CreateContent(1024, 'x'), 1, {});
            cache.AppendChunk(id, handle, chunk);

            UNIT_ASSERT_VALUES_EQUAL(1024, chunk.DirectoryContent.GetSize());
        }

        auto storage = CreateStorage();

        TDirectoryHandleMap handles;
        storage->LoadHandles(handles);

        UNIT_ASSERT(!handles.contains(id));
        UNIT_ASSERT_VALUES_EQUAL(0, handles.size());
    }

    Y_UNIT_TEST(ShouldBypassStorageAfterPersistentHandleSizeLimitExceeded)
    {
        const ui64 handleId = [&]
        {
            auto cache = CreateCache(CreateStorage(512));

            const ui64 id = cache.CreateHandle(42);
            auto handle = cache.FindHandle(id);
            UNIT_ASSERT(handle);

            auto chunk =
                handle->UpdateContent(1024, 0, CreateContent(1024, 'x'), 1, {});
            cache.AppendChunk(id, handle, chunk);

            chunk = handle->UpdateContent(
                1024,
                1024,
                CreateContent(1024, 'y'),
                2,
                {});
            cache.AppendChunk(id, handle, chunk);

            return id;
        }();

        auto storage = CreateStorage();

        TDirectoryHandleMap handles;
        storage->LoadHandles(handles);

        UNIT_ASSERT_VALUES_EQUAL(0, handles.size());
        UNIT_ASSERT(!handles.contains(handleId));
    }

    Y_UNIT_TEST(ShouldDropLoadedHandleAbovePersistentHandleSizeLimit)
    {
        const ui64 handleId = 42;
        {
            auto storage = CreateStorage();
            TDirectoryHandle handle(100);
            storage->StoreHandle(
                handleId,
                handle,
                TDirectoryHandleChunk{.Index = 100});

            auto chunk =
                handle.UpdateContent(1024, 0, CreateContent(1024, 'x'), 1, {});
            storage->UpdateHandle(handleId, handle, chunk);
        }

        auto storage = CreateStorage(512);

        TDirectoryHandleMap handles;
        storage->LoadHandles(handles);

        UNIT_ASSERT_VALUES_EQUAL(0, handles.size());
        UNIT_ASSERT(!handles.contains(handleId));
    }

    Y_UNIT_TEST(ShouldDropStoredHandleWithMissingOrDuplicatedUpdateVersion)
    {
        const ui64 handleId = 42;
        {
            auto storage = CreateStorage();
            TDirectoryHandle handle(100);

            storage->StoreHandle(
                handleId,
                handle,
                TDirectoryHandleChunk{.Index = 100});

            TDirectoryHandleChunk chunk1{
                .Key = 1024,
                .UpdateVersion = 2,
                .Index = 100,
                .DirectoryContent = {CreateContent(1024, 'x'), 0, 1024}};
            storage->UpdateHandle(handleId, handle, chunk1);

            TDirectoryHandleChunk chunk2{
                .Key = 2048,
                .UpdateVersion = 2,
                .Index = 100,
                .DirectoryContent = {CreateContent(1024, 'y'), 0, 1024}};
            storage->UpdateHandle(handleId, handle, chunk2);
        }

        auto storage = CreateStorage();

        TDirectoryHandleMap handles;
        storage->LoadHandles(handles);

        UNIT_ASSERT(!handles.contains(handleId));
        UNIT_ASSERT_VALUES_EQUAL(0, handles.size());
    }
}

}   // namespace NCloud::NFileStore::NFuse
