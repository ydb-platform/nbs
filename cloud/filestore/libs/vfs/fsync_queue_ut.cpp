#include "fsync_queue.h"

#include <library/cpp/testing/unittest/registar.h>

#include <array>
#include <random>

namespace NCloud::NFileStore::NVFS {

namespace {

////////////////////////////////////////////////////////////////////////////////

std::mt19937_64 CreateRandomEngine()
{
    std::random_device rd;
    std::array<ui32, std::mt19937_64::state_size> randomData;
    std::generate(std::begin(randomData), std::end(randomData), std::ref(rd));
    std::seed_seq seeds(std::begin(randomData), std::end(randomData));
    return std::mt19937_64(seeds);
}

////////////////////////////////////////////////////////////////////////////////

struct TEnvironment: public NUnitTest::TBaseFixture
{
    const TString FileSystemId = "nfs_test";
    const ILoggingServicePtr Logging =
        CreateLoggingService("console", {TLOG_RESOURCES});

    TFSyncQueue Queue = TFSyncQueue(FileSystemId, Logging);

    TFSyncCache::TRequestId CurrentRequestId = 1ul;
    std::underlying_type_t<TNodeId> CurrentNodeId = 1ul;

    std::mt19937_64 eng = CreateRandomEngine();
    THashSet<THandle> UsedHandles;

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {}

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {}

    TFSyncCache::TRequestId GetNextRequestId()
    {
        return CurrentRequestId++;
    }

    TNodeId GetNextNodeId()
    {
        return TNodeId{CurrentNodeId++};
    }

    THandle GetRandomHandle()
    {
        using H = std::underlying_type_t<THandle>;

        std::uniform_int_distribution<H> dist(1, std::numeric_limits<H>::max());

        for (;;) {
            THandle handle{dist(eng)};

            if (UsedHandles.emplace(handle).second) {
                return handle;
            }
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFSyncQueueTest)
{
    Y_UNIT_TEST_F(Smoke, TEnvironment)
    {
        const auto reqId = GetNextRequestId();
        const auto nodeId = GetNextNodeId();

        Queue.Enqueue(reqId, nodeId);

        const auto future = Queue.WaitForRequests(GetNextRequestId(), nodeId);
        UNIT_ASSERT(!future.HasValue());

        Queue.Dequeue(reqId, {}, nodeId);

        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT(!HasError(future.GetValueSync()));
    }

    Y_UNIT_TEST_F(
        ShouldReturnImmediatelyIfThereAreNoRequestsInQueue,
        TEnvironment)
    {
        const auto nodeId = GetNextNodeId();

        {
            const auto future = Queue.WaitForRequests(
                GetNextRequestId(),
                nodeId);   // Local meta
            UNIT_ASSERT(future.HasValue());
            UNIT_ASSERT(!HasError(future.GetValueSync()));
        }

        {
            const auto future =
                Queue.WaitForRequests(GetNextRequestId());   // Global meta
            UNIT_ASSERT(future.HasValue());
            UNIT_ASSERT(!HasError(future.GetValueSync()));
        }

        {
            const auto future = Queue.WaitForDataRequests(
                GetNextRequestId(),
                nodeId,
                GetRandomHandle());   // Local data
            UNIT_ASSERT(future.HasValue());
            UNIT_ASSERT(!HasError(future.GetValueSync()));
        }

        {
            const auto future =
                Queue.WaitForDataRequests(GetNextRequestId());   // Global data
            UNIT_ASSERT(future.HasValue());
            UNIT_ASSERT(!HasError(future.GetValueSync()));
        }
    }

    Y_UNIT_TEST_F(ShouldWorkWithLocalMetaRequests, TEnvironment)
    {
        const auto firstNodeId = GetNextNodeId();
        const auto secondNodeId = GetNextNodeId();

        const auto firstMetaReqId = GetNextRequestId();

        const auto firstDataReqId = GetNextRequestId();
        const auto firstHandle = GetRandomHandle();

        const auto secondMetaReqId = GetNextRequestId();

        const auto fsyncReqId = GetNextRequestId();

        Queue.Enqueue(firstMetaReqId, firstNodeId);                // Meta
        Queue.Enqueue(firstDataReqId, firstNodeId, firstHandle);   // Data
        Queue.Enqueue(secondMetaReqId, secondNodeId);              // Meta

        const auto future =
            Queue.WaitForRequests(fsyncReqId, firstNodeId);   // Local meta
        UNIT_ASSERT(!future.HasValue());

        Queue.Dequeue(firstDataReqId, {}, firstNodeId, firstHandle);
        UNIT_ASSERT(!future.HasValue());

        Queue.Dequeue(firstMetaReqId, {}, firstNodeId);
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT(!HasError(future.GetValueSync()));

        Queue.Dequeue(secondMetaReqId, {}, secondNodeId);
    }

    Y_UNIT_TEST_F(ShouldWorkWithGlobalMetaRequests, TEnvironment)
    {
        const auto firstNodeId = GetNextNodeId();
        const auto secondNodeId = GetNextNodeId();
        const auto thirdNodeId = GetNextNodeId();

        const auto firstMetaReqId = GetNextRequestId();

        const auto secondDataReqId = GetNextRequestId();
        const auto secondHandle = GetRandomHandle();

        const auto fsyncReqId = GetNextRequestId();

        const auto thirdMetaReqId = GetNextRequestId();

        Queue.Enqueue(firstMetaReqId, firstNodeId);                   // Meta
        Queue.Enqueue(secondDataReqId, secondNodeId, secondHandle);   // Data

        const auto future = Queue.WaitForRequests(fsyncReqId);   // Global meta
        UNIT_ASSERT(!future.HasValue());

        Queue.Enqueue(thirdMetaReqId, thirdNodeId);   // Meta unused

        Queue.Dequeue(firstMetaReqId, {}, firstNodeId);
        UNIT_ASSERT(!future.HasValue());

        Queue.Dequeue(secondDataReqId, {}, secondNodeId, secondHandle);
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT(!HasError(future.GetValueSync()));

        Queue.Dequeue(thirdMetaReqId, {}, thirdNodeId);
    }

    Y_UNIT_TEST_F(ShouldWorkWithLocalDataRequests, TEnvironment)
    {
        const auto firstNodeId = GetNextNodeId();
        const auto secondNodeId = GetNextNodeId();

        const auto firstMetaReqId = GetNextRequestId();

        const auto firstDataReqId = GetNextRequestId();
        const auto firstHandle = GetRandomHandle();

        const auto secondMetaReqId = GetNextRequestId();

        const auto fsyncReqId = GetNextRequestId();

        Queue.Enqueue(firstMetaReqId, firstNodeId);                // Meta
        Queue.Enqueue(firstDataReqId, firstNodeId, firstHandle);   // Data
        Queue.Enqueue(secondMetaReqId, secondNodeId);              // Meta

        const auto future = Queue.WaitForDataRequests(
            fsyncReqId,
            firstNodeId,
            firstHandle);   // Local data
        UNIT_ASSERT(!future.HasValue());

        Queue.Dequeue(firstDataReqId, {}, firstNodeId, firstHandle);
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT(!HasError(future.GetValueSync()));

        Queue.Dequeue(secondMetaReqId, {}, secondNodeId);
        Queue.Dequeue(firstMetaReqId, {}, firstNodeId);
    }

    Y_UNIT_TEST_F(ShouldWorkWithGlobalDataRequests, TEnvironment)
    {
        const auto firstNodeId = GetNextNodeId();
        const auto secondNodeId = GetNextNodeId();
        const auto thirdNodeId = GetNextNodeId();

        const auto firstMetaReqId = GetNextRequestId();

        const auto firstDataReqId = GetNextRequestId();
        const auto firstHandle = GetRandomHandle();

        const auto secondDataReqId = GetNextRequestId();
        const auto secondHandle = GetRandomHandle();

        const auto fsyncReqId = GetNextRequestId();

        const auto thirdDataReqId = GetNextRequestId();
        const auto thirdHandle = GetRandomHandle();

        Queue.Enqueue(firstMetaReqId, firstNodeId);                   // Meta
        Queue.Enqueue(firstDataReqId, firstNodeId, firstHandle);      // Data
        Queue.Enqueue(secondDataReqId, secondNodeId, secondHandle);   // Data

        const auto future =
            Queue.WaitForDataRequests(fsyncReqId);   // Global data
        UNIT_ASSERT(!future.HasValue());

        Queue.Enqueue(
            thirdDataReqId,
            thirdNodeId,
            thirdHandle);   // Data unused

        Queue.Dequeue(secondDataReqId, {}, secondNodeId, secondHandle);
        UNIT_ASSERT(!future.HasValue());

        Queue.Dequeue(firstDataReqId, {}, firstNodeId, firstHandle);
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT(!HasError(future.GetValueSync()));

        Queue.Dequeue(thirdDataReqId, {}, thirdNodeId, thirdHandle);
        Queue.Dequeue(firstMetaReqId, {}, firstNodeId);
    }

    Y_UNIT_TEST_F(ShouldWorkWithSimpleOrder, TEnvironment)
    {
        const auto firstNodeId = GetNextNodeId();
        const auto secondNodeId = GetNextNodeId();

        {
            const auto firstDataReqId = GetNextRequestId();
            const auto firstHandle = GetRandomHandle();
            Queue.Enqueue(firstDataReqId, firstNodeId, firstHandle);

            const auto secondDataReqId = GetNextRequestId();
            const auto secondHandle = GetRandomHandle();
            Queue.Enqueue(secondDataReqId, secondNodeId, secondHandle);

            const auto firstFsync = Queue.WaitForDataRequests(
                GetNextRequestId(),
                firstNodeId,
                firstHandle);
            UNIT_ASSERT(!firstFsync.HasValue());

            const auto thirdDataReqId = GetNextRequestId();
            const auto thirdHandle = GetRandomHandle();

            const auto secondFsync = Queue.WaitForDataRequests(
                GetNextRequestId(),
                secondNodeId,
                secondHandle);
            UNIT_ASSERT(!firstFsync.HasValue());
            UNIT_ASSERT(!secondFsync.HasValue());

            Queue.Enqueue(thirdDataReqId, firstNodeId, thirdHandle);
            UNIT_ASSERT(!firstFsync.HasValue());
            UNIT_ASSERT(!secondFsync.HasValue());

            Queue.Dequeue(firstDataReqId, {}, firstNodeId, firstHandle);
            UNIT_ASSERT(firstFsync.HasValue());
            UNIT_ASSERT(!HasError(firstFsync.GetValueSync()));
            UNIT_ASSERT(!secondFsync.HasValue());

            Queue.Dequeue(thirdDataReqId, {}, firstNodeId, thirdHandle);
            UNIT_ASSERT(!secondFsync.HasValue());

            Queue.Dequeue(secondDataReqId, {}, secondNodeId, secondHandle);
            UNIT_ASSERT(secondFsync.HasValue());
            UNIT_ASSERT(!HasError(secondFsync.GetValueSync()));
        }

        {
            const auto firstDataReqId = GetNextRequestId();
            const auto firstHandle = GetRandomHandle();
            Queue.Enqueue(firstDataReqId, firstNodeId, firstHandle);

            const auto secondMetaReqId = GetNextRequestId();
            Queue.Enqueue(secondMetaReqId, secondNodeId);

            const auto secondDataReqId = GetNextRequestId();
            const auto secondHandle = GetRandomHandle();
            Queue.Enqueue(secondDataReqId, secondNodeId, secondHandle);

            const auto dataFsync =
                Queue.WaitForDataRequests(GetNextRequestId());
            UNIT_ASSERT(!dataFsync.HasValue());

            const auto metaFsync =
                Queue.WaitForRequests(GetNextRequestId(), secondNodeId);
            UNIT_ASSERT(!dataFsync.HasValue());
            UNIT_ASSERT(!metaFsync.HasValue());

            Queue.Dequeue(secondDataReqId, {}, secondNodeId, secondHandle);
            UNIT_ASSERT(!dataFsync.HasValue());
            UNIT_ASSERT(!metaFsync.HasValue());

            Queue.Dequeue(firstDataReqId, {}, firstNodeId, firstHandle);
            UNIT_ASSERT(dataFsync.HasValue());
            UNIT_ASSERT(!HasError(dataFsync.GetValueSync()));
            UNIT_ASSERT(!metaFsync.HasValue());

            Queue.Dequeue(secondMetaReqId, {}, secondNodeId);
            UNIT_ASSERT(metaFsync.HasValue());
            UNIT_ASSERT(!HasError(metaFsync.GetValueSync()));
        }

        {
            const auto firstMetaReqId = GetNextRequestId();
            Queue.Enqueue(firstMetaReqId, firstNodeId);

            const auto secondDataReqId = GetNextRequestId();
            const auto secondHandle = GetRandomHandle();
            Queue.Enqueue(secondDataReqId, secondNodeId, secondHandle);

            const auto secondMetaReqId = GetNextRequestId();
            Queue.Enqueue(secondMetaReqId, secondNodeId);

            const auto firstDataReqId = GetNextRequestId();
            const auto firstHandle = GetRandomHandle();
            Queue.Enqueue(firstDataReqId, firstNodeId, firstHandle);

            const auto metaFsync = Queue.WaitForRequests(GetNextRequestId());
            UNIT_ASSERT(!metaFsync.HasValue());

            const auto dataFsync =
                Queue.WaitForDataRequests(GetNextRequestId());
            UNIT_ASSERT(!metaFsync.HasValue());
            UNIT_ASSERT(!dataFsync.HasValue());

            Queue.Dequeue(firstDataReqId, {}, firstNodeId, firstHandle);
            UNIT_ASSERT(!metaFsync.HasValue());
            UNIT_ASSERT(!dataFsync.HasValue());

            Queue.Dequeue(firstMetaReqId, {}, firstNodeId);
            UNIT_ASSERT(!metaFsync.HasValue());
            UNIT_ASSERT(!dataFsync.HasValue());

            Queue.Dequeue(secondDataReqId, {}, secondNodeId, secondHandle);
            UNIT_ASSERT(!metaFsync.HasValue());
            UNIT_ASSERT(dataFsync.HasValue());
            UNIT_ASSERT(!HasError(dataFsync.GetValueSync()));

            Queue.Dequeue(secondMetaReqId, {}, secondNodeId);
            UNIT_ASSERT(metaFsync.HasValue());
            UNIT_ASSERT(!HasError(metaFsync.GetValueSync()));
        }
    }
}

}   // namespace NCloud::NFileStore::NVFS
