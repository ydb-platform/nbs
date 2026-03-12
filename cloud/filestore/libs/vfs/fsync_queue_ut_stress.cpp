#include "fsync_queue.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/condvar.h>
#include <util/system/mutex.h>
#include <util/thread/factory.h>

#include <array>
#include <atomic>
#include <random>
#include <thread>

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

class TScopedTasks
{
private:
    using TThread = THolder<IThreadFactory::IThread>;

    TVector<TThread> Workers;

    bool Started = false;
    TCondVar StartedCV;
    TMutex Mutex;

public:
    ~TScopedTasks()
    {
        UNIT_ASSERT(Workers.empty());
    }

    void Start()
    {
        with_lock (Mutex) {
            Started = true;
        }
        StartedCV.BroadCast();
    }

    void Stop()
    {
        for (auto& worker: Workers) {
            worker->Join();
        }
        Workers.clear();
    }

    template <typename TFunction>
    void Add(TFunction&& function)
    {
        Workers.push_back(SystemThreadFactory()->Run(
            [this, function = std::forward<TFunction>(function)] {
                with_lock (Mutex) {
                    StartedCV.Wait(Mutex, [&] { return Started; });
                }
                function();
            }
        ));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TEnvironment
    : public NUnitTest::TBaseFixture
{
    const TString FileSystemId = "nfs_test";
    const ILoggingServicePtr Logging =
        CreateLoggingService("console", { TLOG_RESOURCES });

    TFSyncQueue Queue = TFSyncQueue(FileSystemId, Logging);

    std::atomic<TFSyncCache::TRequestId> CurrentRequestId = 1ul;
    std::atomic<TNodeId::TUnderlyingType> CurrentNodeId = 1ul;

    TMutex HandleMutex;
    std::mt19937_64 eng = CreateRandomEngine();
    THashSet<THandle> UsedHandles;

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {}

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {}

    TFSyncCache::TRequestId GetNextRequestId()
    {
        return CurrentRequestId.fetch_add(1ul);
    }

    TNodeId GetNextNodeId()
    {
        return TNodeId {CurrentNodeId.fetch_add(1ul)};
    }

    THandle GetRandomHandle()
    {
        using H = THandle::TUnderlyingType;

        std::uniform_int_distribution<H> dist(1, std::numeric_limits<H>::max());

        with_lock (HandleMutex) {
            for (;;) {
                THandle handle {dist(eng)};

                if (UsedHandles.emplace(handle).second) {
                    return handle;
                }
            }
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFSyncQueueStressTest)
{
    Y_UNIT_TEST_F(Stress, TEnvironment)
    {
        constexpr ui64 fsyncCount = 5'000ul;

        std::atomic<ui64> fsyncStarted = 0ul;
        std::atomic<ui64> fsyncCompleted = 0ul;

        std::atomic<TFSyncCache::TRequestId> metaFsyncGlobal = 0ul;
        std::atomic<TFSyncCache::TRequestId> dataFsyncGlobal = 0ul;

        TMutex metaFsyncMutex;
        TMap<TNodeId, TFSyncCache::TRequestId> metaFsyncMap;

        TMutex dataFsyncMutex;
        TMap<
            std::pair<TNodeId, THandle>,
            TFSyncCache::TRequestId> dataFsyncMap;

        TMutex metaMutex;
        TMap<TFSyncCache::TRequestId, TNodeId> metaMap;

        TMutex dataMutex;
        TMap<
            TFSyncCache::TRequestId,
            std::pair<TNodeId, THandle>> dataMap;

        std::atomic_flag shouldStop(false);

        const auto getRandomMetaRequest = [&] {
            static auto localEng = CreateRandomEngine();

            with_lock (metaMutex) {
                const auto size = metaMap.size();
                if (size == 0) {
                    return TNodeId {};
                }
                std::uniform_int_distribution<ui64> dist(0, size - 1);
                return TNodeId {std::next(metaMap.begin(), dist(localEng))->second};
            }
        };

        const auto isEmptyMetaMap = [&] {
            with_lock (metaMutex) {
                return metaMap.empty();
            }
        };

        const auto addMetaRequest = [&] (TNodeId nodeId) {
            const auto reqId = GetNextRequestId();
            Queue.Enqueue(reqId, nodeId);
            with_lock (metaMutex) {
                metaMap.emplace(reqId, nodeId);
            }
        };

        const auto removeRandomMetaRequest = [&] {
            static auto localEng = CreateRandomEngine();

            TFSyncCache::TRequestId reqId = 0ul;
            TNodeId nodeId;
            with_lock (metaMutex) {
                const auto size = metaMap.size();
                if (size == 0) {
                    return;
                }

                std::uniform_int_distribution<ui64> dist(0, size - 1);
                auto it = std::next(metaMap.begin(), dist(localEng));
                reqId = it->first;
                nodeId = it->second;
                metaMap.erase(it);
            }
            Queue.Dequeue(reqId, {}, nodeId);
        };

        const auto getRandomDataRequest = [&] {
            static auto localEng = CreateRandomEngine();

            with_lock (dataMutex) {
                const auto size = dataMap.size();
                if (size == 0) {
                    return std::make_pair(TNodeId {}, THandle {});
                }
                std::uniform_int_distribution<ui64> dist(0, size - 1);
                return std::next(dataMap.begin(), dist(localEng))->second;
            }
        };

        const auto isEmptyDataMap = [&] {
            with_lock (dataMutex) {
                return dataMap.empty();
            }
        };

        const auto addDataRequest = [&] (
                TNodeId nodeId,
                THandle handle)
            {
                const auto reqId = GetNextRequestId();
                Queue.Enqueue(reqId, nodeId, handle);
                with_lock (dataMutex) {
                    dataMap.emplace(reqId, std::make_pair(nodeId, handle));
                }
            };

        const auto removeRandomDataRequest = [&] {
            static auto localEng = CreateRandomEngine();

            TFSyncCache::TRequestId reqId = 0ul;
            TNodeId nodeId;
            THandle handle;
            with_lock (dataMutex) {
                const auto size = dataMap.size();
                if (size == 0) {
                    return;
                }

                std::uniform_int_distribution<ui64> dist(0, size - 1);
                auto it = std::next(dataMap.begin(), dist(localEng));
                reqId = it->first;
                nodeId = it->second.first;
                handle = it->second.second;
                dataMap.erase(it);
            }
            Queue.Dequeue(reqId, {}, nodeId, handle);
        };

        TScopedTasks tasks;

        // Producer
        tasks.Add([&, this] {
            auto eng = CreateRandomEngine();

            std::uniform_int_distribution<ui64> requestDist(0ul, 2ul);
            std::uniform_int_distribution<ui64> shouldPerformFsyncDist(0ul, 255ul);

            while (fsyncStarted.load() < fsyncCount) {
                switch (requestDist(eng)) {
                    case 0ul: {
                        switch (requestDist(eng)) {
                            case 0ul: {
                                const auto nodeId = getRandomMetaRequest();
                                if (!nodeId) {
                                    break;
                                }
                                addMetaRequest(nodeId);
                                break;
                            }
                            case 1ul: {
                                const auto [nodeId, handle] = getRandomDataRequest();
                                if (!nodeId && !handle) {
                                    break;
                                }
                                addMetaRequest(nodeId);
                                break;
                            }
                            case 2ul: {
                                addMetaRequest(GetNextNodeId());
                                break;
                            }
                            default: {
                                // unreacheable code
                                UNIT_ASSERT(false);
                            }
                        }
                        break;
                    }
                    case 1ul: {
                        switch (requestDist(eng)) {
                            case 0ul: {
                                const auto nodeId = getRandomMetaRequest();
                                if (!nodeId) {
                                    break;
                                }
                                addDataRequest(nodeId, GetRandomHandle());
                                break;
                            }
                            case 1ul: {
                                const auto [nodeId, handle] = getRandomDataRequest();
                                if (!nodeId && !handle) {
                                    break;
                                }
                                addDataRequest(nodeId, handle);
                                break;
                            }
                            case 2ul: {
                                addDataRequest(GetNextNodeId(), GetRandomHandle());
                                break;
                            }
                            default: {
                                // unreacheable code
                                UNIT_ASSERT(false);
                            }
                        }
                        break;
                    }
                    default: {
                        break;
                    }
                }

                const auto fsyncVal = shouldPerformFsyncDist(eng);
                if (fsyncVal < 64ul) {
                    const auto nodeId = getRandomMetaRequest();
                    if (nodeId) {
                        fsyncStarted.fetch_add(1ul);
                        const auto reqId = GetNextRequestId();
                        Queue.WaitForRequests(reqId, nodeId)
                            .Subscribe([&, nodeId, reqId] (const auto& future) {
                                Y_UNUSED(future);
                                with_lock (metaFsyncMutex) {
                                    UNIT_ASSERT(std::exchange(metaFsyncMap[nodeId], reqId) < reqId);
                                }
                                fsyncCompleted.fetch_add(1ul);
                            });
                    }
                } else if (fsyncVal < 128ul) {
                    const auto dataReq = getRandomDataRequest();
                    if (dataReq.first && dataReq.second) {
                        fsyncStarted.fetch_add(1ul);
                        const auto reqId = GetNextRequestId();
                        Queue.WaitForDataRequests(reqId, dataReq.first, dataReq.second)
                            .Subscribe([&, key = dataReq, reqId] (const auto& future) {
                                Y_UNUSED(future);
                                with_lock (dataFsyncMutex) {
                                    UNIT_ASSERT(std::exchange(dataFsyncMap[key], reqId) < reqId);
                                }
                                fsyncCompleted.fetch_add(1ul);
                            });
                    }
                } else if (fsyncVal < 129ul) {
                    fsyncStarted.fetch_add(1ul);
                    const auto reqId = GetNextRequestId();
                    Queue.WaitForRequests(reqId)
                        .Subscribe([&, reqId] (const auto& future) {
                            Y_UNUSED(future);
                            UNIT_ASSERT(metaFsyncGlobal.exchange(reqId) < reqId);
                            fsyncCompleted.fetch_add(1ul);
                        });
                } else if (fsyncVal < 130ul) {
                    fsyncStarted.fetch_add(1ul);
                    const auto reqId = GetNextRequestId();
                    Queue.WaitForDataRequests(reqId)
                        .Subscribe([&, reqId] (const auto& future) {
                            Y_UNUSED(future);
                            UNIT_ASSERT(dataFsyncGlobal.exchange(reqId) < reqId);
                            fsyncCompleted.fetch_add(1ul);
                        });
                }
            }

            shouldStop.test_and_set();
        });

        // Consumers
        const auto threads = std::max(8u, std::thread::hardware_concurrency()) - 1ul;
        for (ui32 i = 0; i < threads; ++i) {
            tasks.Add([&, index = i] {
                auto eng = CreateRandomEngine();
                std::uniform_int_distribution<ui64> dist(0, threads);
                while (!shouldStop.test() ||
                           fsyncCompleted.load() < fsyncStarted.load() ||
                           !isEmptyMetaMap() ||
                           !isEmptyDataMap()) {
                    if (dist(eng) == 0ul) {
                        if (index % 2 == 0) {
                            removeRandomMetaRequest();
                        } else {
                            removeRandomDataRequest();
                        }
                    }
                }
            });
        }

        tasks.Start();
        tasks.Stop();

        UNIT_ASSERT_VALUES_EQUAL(fsyncCount, fsyncStarted.load());
        UNIT_ASSERT_VALUES_EQUAL(fsyncCount, fsyncStarted.load());
        UNIT_ASSERT_VALUES_EQUAL(0u, metaMap.size());
        UNIT_ASSERT_VALUES_EQUAL(0u, dataMap.size());
    }
}

}   // namespace NCloud::NFileStore::NVFS
