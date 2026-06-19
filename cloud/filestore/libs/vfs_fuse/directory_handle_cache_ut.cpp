#include "directory_handle_cache.h"

#include <cloud/filestore/libs/diagnostics/metrics/registry.h>

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

    IDirectoryHandleStorageStatsPtr StorageStats;

    TDirectoryHandleModuleStatsPtr CreateStats()
    {
        auto timer = CreateWallClockTimer();
        StorageStats = CreateDirectoryHandleStorageStats(timer);
        return CreateDirectoryHandleStats(std::move(timer), StorageStats);
    }

    TDirectoryHandleStoragePtr CreateStorage(
        TDirectoryHandleModuleStatsPtr stats,
        ui64 persistentHandleMaxSize = 2_GB)
    {
        Y_UNUSED(stats);
        return CreateDirectoryHandleStorage(
            {.Log = Log,
             .FileMapMemoryLimiter = Limiter,
             .Stats = StorageStats,
             .FilePath = StoragePath,
             .MaxRecords = 32,
             .InitialDataAreaSize = 128,
             .MaxDataAreaStepSize = 128,
             .InitialDataMoveBufferSize = 64,
             .PersistentHandleMaxSize = persistentHandleMaxSize});
    }

    TDirectoryHandleCache CreateStoredCache(ui64 persistentHandleMaxSize = 2_GB)
    {
        auto stats = CreateStats();
        return TDirectoryHandleCache(
            Log,
            stats,
            CreateStorage(stats, persistentHandleMaxSize),
            nullptr);
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

        auto storage = CreateStorage(CreateStats());

        TDirectoryHandleMap handles;
        storage->LoadHandles(handles);

        UNIT_ASSERT_VALUES_EQUAL(0, handles.size());
        UNIT_ASSERT(!handles.contains(handleId));
    }

    Y_UNIT_TEST(ShouldReturnChunkForClosedHandleContentUpdate)
    {
        auto cache = TDirectoryHandleCache(Log, CreateStats(), nullptr, nullptr);

        const ui64 id = cache.CreateHandle(42);
        auto handle = cache.FindHandle(id);
        UNIT_ASSERT(handle);

        const auto [serializedSize, chunkCount] = handle->GetStats();

        cache.RemoveHandle(id);

        auto chunk =
            handle->UpdateContent(1024, 0, CreateContent(1024, 'x'), 1, {});

        UNIT_ASSERT_VALUES_EQUAL(1024, chunk.DirectoryContent.GetSize());
        const auto [updatedSerializedSize, updatedChunkCount] =
            handle->GetStats();
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

        auto storage = CreateStorage(CreateStats());

        TDirectoryHandleMap handles;
        storage->LoadHandles(handles);

        UNIT_ASSERT(!handles.contains(id));
        UNIT_ASSERT_VALUES_EQUAL(0, handles.size());
    }

    Y_UNIT_TEST(ShouldBypassStorageAfterPersistentHandleSizeLimitExceeded)
    {
        const ui64 handleId = [&]
        {
            auto cache = CreateStoredCache(512);

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

        auto storage = CreateStorage(CreateStats());

        TDirectoryHandleMap handles;
        storage->LoadHandles(handles);

        UNIT_ASSERT_VALUES_EQUAL(0, handles.size());
        UNIT_ASSERT(!handles.contains(handleId));
    }

    Y_UNIT_TEST(ShouldDropLoadedHandleAbovePersistentHandleSizeLimit)
    {
        const ui64 handleId = 42;
        {
            auto storage = CreateStorage(CreateStats());
            TDirectoryHandle handle(100);
            storage->StoreHandle(
                handleId,
                handle,
                TDirectoryHandleChunk{.Index = 100});

            auto chunk =
                handle.UpdateContent(1024, 0, CreateContent(1024, 'x'), 1, {});
            storage->UpdateHandle(handleId, handle, chunk);
        }

        auto timer = CreateWallClockTimer();
        StorageStats = CreateDirectoryHandleStorageStats(timer);
        auto stats = CreateDirectoryHandleStats(timer, StorageStats);

        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto metricsRegistry = NMetrics::CreateMetricsRegistry({}, counters);
        auto aggregatableMetricsRegistry =
            NMetrics::CreateMetricsRegistryStub();
        stats->RegisterCounters(metricsRegistry, aggregatableMetricsRegistry);

        auto storage = CreateStorage(stats, 512);

        TDirectoryHandleMap handles;
        storage->LoadHandles(handles);

        UNIT_ASSERT_VALUES_EQUAL(0, handles.size());
        UNIT_ASSERT(!handles.contains(handleId));

        metricsRegistry->Update(timer->Now());

        auto handleSizeLimitRejectionCount =
            counters->FindCounter("Storage_HandleSizeLimitRejectionCount");
        UNIT_ASSERT(handleSizeLimitRejectionCount);
        UNIT_ASSERT_VALUES_EQUAL(1, handleSizeLimitRejectionCount->Val());
    }

    Y_UNIT_TEST(ShouldDropStoredHandleWithMissingOrDuplicatedUpdateVersion)
    {
        const ui64 handleId = 42;
        {
            auto storage = CreateStorage(CreateStats());
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

        auto storage = CreateStorage(CreateStats());

        TDirectoryHandleMap handles;
        storage->LoadHandles(handles);

        UNIT_ASSERT(!handles.contains(handleId));
        UNIT_ASSERT_VALUES_EQUAL(0, handles.size());
    }

    Y_UNIT_TEST(ShouldResetLoadedContentCacheVersionAfterRestart)
    {
        ui64 loadedHandleId = 0;
        {
            auto cache = CreateStoredCache();

            loadedHandleId = cache.CreateHandle(100);
            auto handle = cache.FindHandle(loadedHandleId);
            UNIT_ASSERT(handle);

            auto chunk = handle->UpdateContent(
                1024,
                0,
                CreateContent(1024, 'x'),
                10,
                {});
            cache.AppendChunk(loadedHandleId, handle, chunk);

            auto content = handle->ReadContent(1024, 0, Log);
            UNIT_ASSERT(content);
            UNIT_ASSERT_VALUES_EQUAL(10, content->CacheVersion);
        }

        auto cache = CreateStoredCache();

        auto loadedHandle = cache.FindHandle(loadedHandleId);
        UNIT_ASSERT(loadedHandle);

        auto loadedContent = loadedHandle->ReadContent(1024, 0, Log);
        UNIT_ASSERT(loadedContent);
        UNIT_ASSERT_VALUES_EQUAL(0, loadedContent->CacheVersion);

        const ui64 newHandleId = cache.CreateHandle(200);
        auto newHandle = cache.FindHandle(newHandleId);
        UNIT_ASSERT(newHandle);

        auto chunk = newHandle->UpdateContent(
            1024,
            0,
            CreateContent(1024, 'y'),
            20,
            {});
        cache.AppendChunk(newHandleId, newHandle, chunk);

        auto newContent = newHandle->ReadContent(1024, 0, Log);
        UNIT_ASSERT(newContent);
        UNIT_ASSERT_VALUES_EQUAL(20, newContent->CacheVersion);

        loadedContent = loadedHandle->ReadContent(1024, 0, Log);
        UNIT_ASSERT(loadedContent);
        UNIT_ASSERT_VALUES_EQUAL(0, loadedContent->CacheVersion);
    }

    Y_UNIT_TEST(ShouldRegisterMetrics)
    {
        auto timer = CreateWallClockTimer();
        StorageStats = CreateDirectoryHandleStorageStats(timer);
        auto stats = CreateDirectoryHandleStats(timer, StorageStats);

        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto metricsRegistry = NMetrics::CreateMetricsRegistry({}, counters);
        auto aggregatableMetricsRegistry =
            NMetrics::CreateMetricsRegistryStub();
        stats->RegisterCounters(metricsRegistry, aggregatableMetricsRegistry);

        auto entryVersionCache =
            std::make_shared<TDirectoryEntryVersionCache>(1, stats);
        auto cache = TDirectoryHandleCache(
            Log,
            stats,
            CreateStorage(stats),
            entryVersionCache);

        const ui64 id = cache.CreateHandle(42);
        auto handle = cache.FindHandle(id);
        UNIT_ASSERT(handle);

        stats->UpdateStats(timer->Now());
        metricsRegistry->Update(timer->Now());

        UNIT_ASSERT(counters->FindCounter("MaxCacheSize"));
        UNIT_ASSERT(counters->FindCounter("MaxChunkCount"));

        auto maxOpenHandleCount = counters->FindCounter("MaxOpenHandleCount");
        UNIT_ASSERT(maxOpenHandleCount);
        UNIT_ASSERT_VALUES_EQUAL(1, maxOpenHandleCount->Val());

        auto maxEntryVersionCacheEntryCount =
            counters->FindCounter("MaxEntryVersionCacheEntryCount");
        UNIT_ASSERT(maxEntryVersionCacheEntryCount);
        UNIT_ASSERT_VALUES_EQUAL(0, maxEntryVersionCacheEntryCount->Val());

        entryVersionCache->AdvanceVersion(42, "child", 10);
        entryVersionCache->AdvanceVersion(42, "child", 20);
        entryVersionCache->AdvanceVersion(42, "other", 30);

        stats->UpdateStats(timer->Now());
        metricsRegistry->Update(timer->Now());
        UNIT_ASSERT_VALUES_EQUAL(2, maxEntryVersionCacheEntryCount->Val());

        auto rawCapacityByteMaxCount =
            counters->FindCounter("Storage_RawCapacityByteMaxCount");
        UNIT_ASSERT(rawCapacityByteMaxCount);
        UNIT_ASSERT_GT(rawCapacityByteMaxCount->Val(), 0);

        auto rawUsedByteMaxCount =
            counters->FindCounter("Storage_RawUsedByteMaxCount");
        UNIT_ASSERT(rawUsedByteMaxCount);
        UNIT_ASSERT_GT(rawUsedByteMaxCount->Val(), 0);

        auto memoryLimiterRejectionCount =
            counters->FindCounter("Storage_MemoryLimiterRejectionCount");
        UNIT_ASSERT(memoryLimiterRejectionCount);
        UNIT_ASSERT_VALUES_EQUAL(0, memoryLimiterRejectionCount->Val());

        auto handleSizeLimitRejectionCount =
            counters->FindCounter("Storage_HandleSizeLimitRejectionCount");
        UNIT_ASSERT(handleSizeLimitRejectionCount);
        UNIT_ASSERT_VALUES_EQUAL(0, handleSizeLimitRejectionCount->Val());

        Limiter->CanIncreaseResult = false;

        auto chunk = handle->UpdateContent(
            64 * 1024,
            0,
            CreateContent(64 * 1024, 'z'),
            1,
            "next");
        cache.AppendChunk(id, handle, chunk);

        stats->UpdateStats(timer->Now());
        metricsRegistry->Update(timer->Now());

        UNIT_ASSERT_GT(memoryLimiterRejectionCount->Val(), 0);

        cache.RemoveHandle(id);
        cache.CreateHandle(43);
        entryVersionCache->AdvanceVersion(43, "child", 40);

        stats->UpdateStats(timer->Now());
        metricsRegistry->Update(timer->Now());
        UNIT_ASSERT_VALUES_EQUAL(2, maxEntryVersionCacheEntryCount->Val());
    }

    Y_UNIT_TEST(ShouldRegisterEntryVersionCacheForCreatedHandle)
    {
        auto entryVersionCache =
            std::make_shared<TDirectoryEntryVersionCache>(
                1,
                CreateDirectoryHandleStatsStub());
        auto cache = TDirectoryHandleCache(
            Log,
            CreateStats(),
            nullptr,
            entryVersionCache);

        const ui64 id = cache.CreateHandle(42);

        entryVersionCache->AdvanceVersion(42, "child", 10);
        UNIT_ASSERT_VALUES_EQUAL(
            10,
            entryVersionCache->GetVersion(42, "child"));

        UNIT_ASSERT(cache.RemoveHandle(id, 42));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            entryVersionCache->GetVersion(42, "child"));
    }

    Y_UNIT_TEST(ShouldKeepEntryVersionCacheUntilLastHandleIsRemoved)
    {
        auto entryVersionCache =
            std::make_shared<TDirectoryEntryVersionCache>(
                1,
                CreateDirectoryHandleStatsStub());
        auto cache = TDirectoryHandleCache(
            Log,
            CreateStats(),
            nullptr,
            entryVersionCache);

        const ui64 firstId = cache.CreateHandle(42);
        const ui64 secondId = cache.CreateHandle(42);

        entryVersionCache->AdvanceVersion(42, "child", 10);
        UNIT_ASSERT_VALUES_EQUAL(
            10,
            entryVersionCache->GetVersion(42, "child"));

        UNIT_ASSERT(cache.RemoveHandle(firstId, 42));
        UNIT_ASSERT_VALUES_EQUAL(
            10,
            entryVersionCache->GetVersion(42, "child"));

        UNIT_ASSERT(cache.RemoveHandle(secondId, 42));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            entryVersionCache->GetVersion(42, "child"));
    }

    Y_UNIT_TEST(ShouldUnregisterEntryVersionCacheOnClear)
    {
        auto entryVersionCache =
            std::make_shared<TDirectoryEntryVersionCache>(
                1,
                CreateDirectoryHandleStatsStub());
        auto cache = TDirectoryHandleCache(
            Log,
            CreateStats(),
            nullptr,
            entryVersionCache);

        cache.CreateHandle(42);
        cache.CreateHandle(43);

        entryVersionCache->AdvanceVersion(42, "child1", 10);
        entryVersionCache->AdvanceVersion(43, "child2", 20);

        cache.Clear();

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            entryVersionCache->GetVersion(42, "child1"));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            entryVersionCache->GetVersion(43, "child2"));
    }

    Y_UNIT_TEST(ShouldUnregisterEntryVersionCacheOnReset)
    {
        auto entryVersionCache =
            std::make_shared<TDirectoryEntryVersionCache>(
                1,
                CreateDirectoryHandleStatsStub());
        auto cache = TDirectoryHandleCache(
            Log,
            CreateStats(),
            nullptr,
            entryVersionCache);

        cache.CreateHandle(42);
        cache.CreateHandle(43);

        entryVersionCache->AdvanceVersion(42, "child1", 10);
        entryVersionCache->AdvanceVersion(43, "child2", 20);

        cache.Reset();

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            entryVersionCache->GetVersion(42, "child1"));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            entryVersionCache->GetVersion(43, "child2"));
    }

    Y_UNIT_TEST(ShouldNotRegisterEntryVersionCacheForLoadedHandles)
    {
        const ui64 handleId = 42;
        {
            auto storage = CreateStorage(CreateStats());
            TDirectoryHandle handle(100);
            storage->StoreHandle(
                handleId,
                handle,
                TDirectoryHandleChunk{.Index = 100});
        }

        auto entryVersionCache =
            std::make_shared<TDirectoryEntryVersionCache>(
                1,
                CreateDirectoryHandleStatsStub());
        auto stats = CreateStats();
        auto cache = TDirectoryHandleCache(
            Log,
            stats,
            CreateStorage(stats),
            entryVersionCache);

        UNIT_ASSERT(cache.FindHandle(handleId));

        entryVersionCache->AdvanceVersion(100, "child", 10);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            entryVersionCache->GetVersion(100, "child"));
    }

}

}   // namespace NCloud::NFileStore::NFuse
