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

    TDirectoryHandleStatsPtr CreateStats()
    {
        auto timer = CreateWallClockTimer();
        StorageStats = CreateDirectoryHandleStorageStats(timer);
        return CreateDirectoryHandleStats(std::move(timer), StorageStats);
    }

    TDirectoryHandleStoragePtr CreateStorage(
        TDirectoryHandleStatsPtr stats,
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
            CreateStorage(stats, persistentHandleMaxSize));
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
        auto cache = TDirectoryHandleCache(Log, CreateStats(), nullptr);

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

        auto storage = CreateStorage(CreateStats(), 512);

        TDirectoryHandleMap handles;
        storage->LoadHandles(handles);

        UNIT_ASSERT_VALUES_EQUAL(0, handles.size());
        UNIT_ASSERT(!handles.contains(handleId));
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

        auto cache = TDirectoryHandleCache(Log, stats, CreateStorage(stats));

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
    }
}

}   // namespace NCloud::NFileStore::NFuse
