#include "directory_handle_cache.h"

#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/buffer.h>

#include <algorithm>

namespace NCloud::NFileStore::NFuse {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestMemoryController final: public IMemoryController
{
    bool CanIncreaseResult = true;
    ui64 Current = 0;

    bool CanIncreaseFileMapUsage(ui64 value) const override
    {
        Y_UNUSED(value);
        return CanIncreaseResult;
    }

    void IncreaseFileMapUsage(ui64 value) override
    {
        Current += value;
    }

    void DecreaseFileMapUsage(ui64 value) override
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDirectoryHandleCacheTest)
{
    Y_UNIT_TEST(ShouldBypassStorageAfterUpdateFailure)
    {
        auto logging = CreateLoggingService("console");
        auto log = logging->CreateLog("DIR_HANDLE_CACHE_TEST");
        auto controller = std::make_shared<TTestMemoryController>();

        TTempDir tempDir;
        const TString storagePath = tempDir.Path() / "directory_handles";

        const ui64 handleId = [&]
        {
            auto storage = CreateDirectoryHandleStorage(
                log,
                storagePath,
                32,
                128,
                128,
                64,
                controller);

            TDirectoryHandleCache cache(
                log,
                CreateDirectoryHandleStats(CreateWallClockTimer()),
                std::move(storage));

            const ui64 id = cache.CreateHandle(42);
            auto handle = cache.FindHandle(id);
            UNIT_ASSERT(handle);

            controller->CanIncreaseResult = false;
            auto chunk = handle->UpdateContent(
                1024,
                0,
                CreateContent(1024, 'x'),
                1,
                "next");
            cache.AppendChunk(id, chunk);

            controller->CanIncreaseResult = true;
            chunk = handle->UpdateContent(
                1024,
                1024,
                CreateContent(1024, 'y'),
                2,
                {});
            cache.AppendChunk(id, chunk);

            return id;
        }();

        auto storage = CreateDirectoryHandleStorage(
            log,
            storagePath,
            32,
            128,
            128,
            64,
            controller);

        TDirectoryHandleMap handles;
        storage->LoadHandles(handles);

        UNIT_ASSERT_VALUES_EQUAL(0, handles.size());
        UNIT_ASSERT(!handles.contains(handleId));
    }

    Y_UNIT_TEST(ShouldReturnChunkForClosedHandleContentUpdate)
    {
        auto logging = CreateLoggingService("console");
        auto log = logging->CreateLog("DIR_HANDLE_CACHE_TEST");

        TDirectoryHandleCache cache(
            log,
            CreateDirectoryHandleStats(CreateWallClockTimer()),
            nullptr);

        const ui64 id = cache.CreateHandle(42);
        auto handle = cache.FindHandle(id);
        UNIT_ASSERT(handle);

        const auto serializedSize = handle->GetSerializedSize();
        const auto chunkCount = handle->GetChunkCount();

        cache.RemoveHandle(id);

        auto chunk =
            handle->UpdateContent(1024, 0, CreateContent(1024, 'x'), 1, {});

        UNIT_ASSERT_VALUES_EQUAL(1024, chunk.DirectoryContent.GetSize());
        UNIT_ASSERT_GT(handle->GetSerializedSize(), serializedSize);
        UNIT_ASSERT_VALUES_EQUAL(chunkCount + 1, handle->GetChunkCount());
    }

    Y_UNIT_TEST(ShouldNotStoreClosedHandleContentUpdate)
    {
        auto logging = CreateLoggingService("console");
        auto log = logging->CreateLog("DIR_HANDLE_CACHE_TEST");
        auto controller = std::make_shared<TTestMemoryController>();

        TTempDir tempDir;
        const TString storagePath = tempDir.Path() / "directory_handles";

        ui64 id = 0;
        {
            auto storage = CreateDirectoryHandleStorage(
                log,
                storagePath,
                32,
                128,
                128,
                64,
                controller);

            TDirectoryHandleCache cache(
                log,
                CreateDirectoryHandleStats(CreateWallClockTimer()),
                std::move(storage));

            id = cache.CreateHandle(42);
            auto handle = cache.FindHandle(id);
            UNIT_ASSERT(handle);

            cache.RemoveHandle(id);

            auto chunk =
                handle->UpdateContent(1024, 0, CreateContent(1024, 'x'), 1, {});
            cache.AppendChunk(id, chunk);

            UNIT_ASSERT_VALUES_EQUAL(1024, chunk.DirectoryContent.GetSize());
        }

        auto storage = CreateDirectoryHandleStorage(
            log,
            storagePath,
            32,
            128,
            128,
            64,
            controller);

        TDirectoryHandleMap handles;
        storage->LoadHandles(handles);

        UNIT_ASSERT(!handles.contains(id));
        UNIT_ASSERT_VALUES_EQUAL(0, handles.size());
    }
}

}   // namespace NCloud::NFileStore::NFuse
