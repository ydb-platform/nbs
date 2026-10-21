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

using EStatus = TReadContentResult::EStatus;

TReadContentResult
ReadContent(TDirectoryHandle& handle, size_t size, size_t offset)
{
    auto result = handle.ReadContent(size, offset);
    UNIT_ASSERT_C(!HasError(result), FormatError(result.GetError()));
    return result.ExtractResult();
}

void ReadAndRecord(
    TDirectoryHandleCache& cache,
    TDirectoryHandle& handle,
    size_t size,
    size_t offset)
{
    const auto result = ReadContent(handle, size, offset);
    UNIT_ASSERT_EQUAL(EStatus::Hit, result.Status);
    cache.RecordRead(result);
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

    const ui64 ReadStatsSizeThreshold = 0;

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
            nullptr /* directoryEntryVersionCache */,
            ReadStatsSizeThreshold);
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
        auto cache = TDirectoryHandleCache(
            Log,
            CreateStats(),
            nullptr /* storage */,
            nullptr /* directoryEntryVersionCache */,
            ReadStatsSizeThreshold);

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

            auto result = ReadContent(*handle, 1024, 0);
            UNIT_ASSERT_EQUAL(EStatus::Hit, result.Status);
            UNIT_ASSERT_VALUES_EQUAL(10, result.DirectoryContent.CacheVersion);
        }

        auto cache = CreateStoredCache();

        auto loadedHandle = cache.FindHandle(loadedHandleId);
        UNIT_ASSERT(loadedHandle);

        auto loadedResult = ReadContent(*loadedHandle, 1024, 0);
        UNIT_ASSERT_EQUAL(EStatus::Hit, loadedResult.Status);
        UNIT_ASSERT_VALUES_EQUAL(0, loadedResult.DirectoryContent.CacheVersion);

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

        auto newResult = ReadContent(*newHandle, 1024, 0);
        UNIT_ASSERT_EQUAL(EStatus::Hit, newResult.Status);
        UNIT_ASSERT_VALUES_EQUAL(20, newResult.DirectoryContent.CacheVersion);

        loadedResult = ReadContent(*loadedHandle, 1024, 0);
        UNIT_ASSERT_EQUAL(EStatus::Hit, loadedResult.Status);
        UNIT_ASSERT_VALUES_EQUAL(0, loadedResult.DirectoryContent.CacheVersion);
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
            entryVersionCache,
            ReadStatsSizeThreshold);

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
            nullptr /* storage */,
            entryVersionCache,
            ReadStatsSizeThreshold);

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
            nullptr /* storage */,
            entryVersionCache,
            ReadStatsSizeThreshold);

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
            nullptr /* storage */,
            entryVersionCache,
            ReadStatsSizeThreshold);

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
            nullptr /* storage */,
            entryVersionCache,
            ReadStatsSizeThreshold);

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

    Y_UNIT_TEST(ShouldRegisterEntryVersionCacheForLoadedHandles)
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
            entryVersionCache,
            ReadStatsSizeThreshold);

        auto loadedHandle = cache.FindHandle(handleId);
        UNIT_ASSERT(loadedHandle);

        auto chunk = loadedHandle->UpdateContent(
            1024,
            0,
            CreateContent(1024, 'x'),
            10,
            {});
        cache.AppendChunk(handleId, loadedHandle, chunk);

        entryVersionCache->AdvanceVersion(100, "child", 10);
        UNIT_ASSERT_VALUES_EQUAL(
            10,
            entryVersionCache->GetVersion(100, "child"));
    }

    Y_UNIT_TEST(ShouldRestoreLastChunkAfterEmptyFinalPage)
    {
        const ui64 handleId = [&]
        {
            auto cache = CreateStoredCache();

            const ui64 otherId = cache.CreateHandle(43);
            const ui64 id = cache.CreateHandle(42);
            auto handle = cache.FindHandle(id);
            UNIT_ASSERT(handle);

            auto chunk = handle->UpdateContent(
                1024,
                0,
                CreateContent(1024, 'x'),
                1,
                "next");
            cache.AppendChunk(id, handle, chunk);

            // frees the first storage record, the empty chunk reuses it and
            // is loaded before the chunk with the same key
            cache.RemoveHandle(otherId);

            chunk = handle->UpdateContent(
                1024,
                1024,
                std::make_shared<TBuffer>(),
                2,
                {});
            cache.AppendChunk(id, handle, chunk);

            auto result = ReadContent(*handle, 1024, 512);
            UNIT_ASSERT_EQUAL(EStatus::Hit, result.Status);
            UNIT_ASSERT_VALUES_EQUAL(512, result.DirectoryContent.GetSize());

            return id;
        }();

        auto cache = CreateStoredCache();
        auto handle = cache.FindHandle(handleId);
        UNIT_ASSERT(handle);

        auto result = ReadContent(*handle, 1024, 512);
        UNIT_ASSERT_EQUAL(EStatus::Hit, result.Status);
        UNIT_ASSERT_VALUES_EQUAL(512, result.DirectoryContent.GetSize());
    }

    Y_UNIT_TEST(ShouldSkipEmptyChunkWhenRestoringHandle)
    {
        TDirectoryHandle handle(42);

        TDirectoryHandleChunk emptyChunk{
            .Key = 1024,
            .UpdateVersion = 2,
            .Index = 42,
        };
        handle.ConsumeChunk(emptyChunk, Log);

        TDirectoryHandleChunk chunk{
            .Key = 1024,
            .UpdateVersion = 1,
            .Index = 42,
            .Cookie = "next",
            .DirectoryContent = {.Content = CreateContent(1024, 'x')},
        };
        handle.ConsumeChunk(chunk, Log);

        auto result = ReadContent(handle, 1024, 512);
        UNIT_ASSERT_EQUAL(EStatus::Hit, result.Status);
        UNIT_ASSERT_VALUES_EQUAL(512, result.DirectoryContent.GetSize());
    }

    Y_UNIT_TEST(ShouldRejectReadOutsideCachedContent)
    {
        TDirectoryHandle handle(42);

        handle.UpdateContent(1024, 0, CreateContent(1024, 'x'), 1, "c1");

        auto result = handle.ReadContent(1024, 5120);
        UNIT_ASSERT(HasError(result));
        UNIT_ASSERT_VALUES_EQUAL(E_FS_INVAL, result.GetError().GetCode());

        UNIT_ASSERT_EQUAL(
            EStatus::Miss,
            ReadContent(handle, 1024, 1024).Status);

        auto content = ReadContent(handle, 1024, 512);
        UNIT_ASSERT_EQUAL(EStatus::Hit, content.Status);
        UNIT_ASSERT_VALUES_EQUAL('x', *content.DirectoryContent.GetData());

        handle.UpdateContent(1024, 1024, CreateContent(1024, 'y'), 2, {});

        content = ReadContent(handle, 1024, 5120);
        UNIT_ASSERT_EQUAL(EStatus::Eof, content.Status);
        UNIT_ASSERT_VALUES_EQUAL(0, content.DirectoryContent.GetSize());
    }

    Y_UNIT_TEST(ShouldCountChunkSwitches)
    {
        auto timer = CreateWallClockTimer();
        auto stats =
            CreateDirectoryHandleStats(timer, nullptr /* storageStats */);

        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto metricsRegistry = NMetrics::CreateMetricsRegistry({}, counters);
        stats->RegisterCounters(
            metricsRegistry,
            NMetrics::CreateMetricsRegistryStub());

        auto cache = TDirectoryHandleCache(
            Log,
            stats,
            nullptr /* storage */,
            nullptr /* directoryEntryVersionCache */,
            ReadStatsSizeThreshold);

        const ui64 id = cache.CreateHandle(42);
        auto handle = cache.FindHandle(id);
        UNIT_ASSERT(handle);

        for (ui64 i = 0; i < 3; ++i) {
            handle->UpdateContent(
                1024,
                i * 1024,
                CreateContent(1024, 'a' + i),
                i + 1,
                {});
        }

        metricsRegistry->Update(timer->Now());
        auto chunkSwitchCount = counters->FindCounter("ChunkSwitchCount");
        UNIT_ASSERT(chunkSwitchCount);
        UNIT_ASSERT_VALUES_EQUAL(0, chunkSwitchCount->Val());

        ReadAndRecord(cache, *handle, 1024, 2048);
        ReadAndRecord(cache, *handle, 1024, 2560);
        metricsRegistry->Update(timer->Now());
        UNIT_ASSERT_VALUES_EQUAL(0, chunkSwitchCount->Val());

        ReadAndRecord(cache, *handle, 1024, 256);
        ReadAndRecord(cache, *handle, 1024, 512);
        metricsRegistry->Update(timer->Now());
        UNIT_ASSERT_VALUES_EQUAL(1, chunkSwitchCount->Val());

        ReadAndRecord(cache, *handle, 1024, 1024);
        ReadAndRecord(cache, *handle, 1024, 2048);
        metricsRegistry->Update(timer->Now());
        UNIT_ASSERT_VALUES_EQUAL(2, chunkSwitchCount->Val());

        ReadAndRecord(cache, *handle, 1024, 256);
        metricsRegistry->Update(timer->Now());
        UNIT_ASSERT_VALUES_EQUAL(3, chunkSwitchCount->Val());

        ReadAndRecord(cache, *handle, 1024, 2048);
        metricsRegistry->Update(timer->Now());
        UNIT_ASSERT_VALUES_EQUAL(3, chunkSwitchCount->Val());

        handle->ResetContent();
        handle->UpdateContent(1024, 0, CreateContent(1024, 'x'), 1, {});
        ReadAndRecord(cache, *handle, 1024, 256);
        metricsRegistry->Update(timer->Now());
        UNIT_ASSERT_VALUES_EQUAL(3, chunkSwitchCount->Val());
    }

    Y_UNIT_TEST(ShouldNotReportChunkSwitchOnFirstReadAfterRestore)
    {
        TDirectoryHandle handle(42);
        for (ui64 i = 0; i < 3; ++i) {
            TDirectoryHandleChunk chunk{
                .Key = (i + 1) * 1024,
                .UpdateVersion = i,
                .Index = 42,
                .DirectoryContent = {.Content = CreateContent(1024, 'a' + i)},
            };
            handle.ConsumeChunk(chunk, Log);
        }

        UNIT_ASSERT(!ReadContent(handle, 1024, 256).ChunkSwitched);
        UNIT_ASSERT(ReadContent(handle, 1024, 1024).ChunkSwitched);
        UNIT_ASSERT(!ReadContent(handle, 1024, 2048).ChunkSwitched);
        UNIT_ASSERT(ReadContent(handle, 1024, 256).ChunkSwitched);
    }

    Y_UNIT_TEST(ShouldRecordReadStatsOnlyForHandlesOverThreshold)
    {
        auto timer = CreateWallClockTimer();
        auto stats =
            CreateDirectoryHandleStats(timer, nullptr /* storageStats */);

        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto metricsRegistry = NMetrics::CreateMetricsRegistry({}, counters);
        stats->RegisterCounters(
            metricsRegistry,
            NMetrics::CreateMetricsRegistryStub());

        const ui64 readStatsSizeThreshold = 4_KB;
        auto cache = TDirectoryHandleCache(
            Log,
            stats,
            nullptr /* storage */,
            nullptr /* directoryEntryVersionCache */,
            readStatsSizeThreshold);

        const ui64 id = cache.CreateHandle(42);
        auto handle = cache.FindHandle(id);
        UNIT_ASSERT(handle);

        for (ui64 i = 0; i < 2; ++i) {
            handle->UpdateContent(
                1024,
                i * 1024,
                CreateContent(1024, 'a' + i),
                i + 1,
                {});
        }
        UNIT_ASSERT_LT(
            handle->GetStats().SerializedSize,
            readStatsSizeThreshold);

        metricsRegistry->Update(timer->Now());
        auto chunkSwitchCount = counters->FindCounter("ChunkSwitchCount");
        UNIT_ASSERT(chunkSwitchCount);

        ReadAndRecord(cache, *handle, 1024, 256);
        metricsRegistry->Update(timer->Now());
        UNIT_ASSERT_VALUES_EQUAL(0, chunkSwitchCount->Val());

        for (ui64 i = 2; i < 4; ++i) {
            handle->UpdateContent(
                1024,
                i * 1024,
                CreateContent(1024, 'a' + i),
                i + 1,
                {});
        }
        UNIT_ASSERT_GT(
            handle->GetStats().SerializedSize,
            readStatsSizeThreshold);

        ReadAndRecord(cache, *handle, 1024, 256);
        metricsRegistry->Update(timer->Now());
        UNIT_ASSERT_VALUES_EQUAL(1, chunkSwitchCount->Val());
    }
}

}   // namespace NCloud::NFileStore::NFuse
