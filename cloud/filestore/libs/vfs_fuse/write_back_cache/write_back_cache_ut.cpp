#include "write_back_cache.h"

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore_test.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/random/random.h>
#include <util/system/tempfile.h>

#include <latch>
#include <memory>
#include <mutex>
#include <thread>

namespace NCloud::NFileStore::NFuse {

using namespace std::chrono_literals;

using namespace NThreading;

using EWriteDataRequestStatus = TWriteBackCache::EWriteDataRequestStatus;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 CacheCapacityBytes = 1024 * 1024 + 1024;

constexpr ui32 DefaultMaxWriteRequestSize = 1_MB;
constexpr ui32 DefaultMaxWriteRequestsCount = 64;
constexpr ui32 DefaultMaxSumWriteRequestsSize = 32_MB;

constexpr ui64 NodeToHandleOffset = 1000;

////////////////////////////////////////////////////////////////////////////////

void SleepForRandomDurationMs(ui32 maxDurationMs)
{
    const auto durationMs = RandomNumber(maxDurationMs);
    if (durationMs != 0) {
        std::this_thread::sleep_for(durationMs*1ms);
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TInFlightRequestTracker
{
    TVector<ui32> InFlightRequests;
    std::mutex Mutex;

    ui32 Count(ui64 offset, ui64 length)
    {
        ui32 res = 0;

        {
            std::unique_lock lock(Mutex);

            const auto end = offset + length;

            for (auto i = offset; i < Min(end, InFlightRequests.size()); i++) {
                res += InFlightRequests[i];
            }
        }

        return res;
    }

    // Returns previous in-flight request count in range
    ui32 Add(ui64 offset, ui64 length)
    {
        ui32 res = 0;

        {
            std::unique_lock lock(Mutex);

            const auto end = offset + length;
            // Append zeroes if needed
            const auto newSize = Max(InFlightRequests.size(), end);
            InFlightRequests.resize(newSize, 0);

            for (auto i = offset; i < end; i++) {
                res += InFlightRequests[i];
                InFlightRequests[i]++;
            }
        }

        return res;
    }

    void Remove(ui64 offset, ui64 length)
    {
        {
            std::unique_lock lock(Mutex);

            const auto end = offset + length;

            for (auto i = offset; i < end; i++) {
                Y_ABORT_UNLESS(InFlightRequests[i]);
                InFlightRequests[i]--;
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteDataRequestStats
{
    ui64 InProgressCount = 0;
    TInstant MinTime = TInstant::Zero();
    TVector<TDuration> Data;
    ui64 Count = 0;

    void ResetNonDerivativeCounters()
    {
        InProgressCount = 0;
        MinTime = TInstant::Zero();
    }
};

struct TReadDataRequestStats
{
    TVector<TDuration> Data;
    ui64 CacheMissCount = 0;
    ui64 CachePartialHitCount = 0;
    ui64 CacheFullHitCount = 0;
};

struct TWriteBackCacheStats
    : public IWriteBackCacheStats
{
    ui64 InProgressFlushCount = 0;
    ui64 CompletedFlushCount = 0;
    ui64 FailedFlushCount = 0;

    ui64 NodeCount = 0;

    TWriteDataRequestStats PendingStats;
    TWriteDataRequestStats CachedStats;
    TWriteDataRequestStats FlushRequestedStats;
    TWriteDataRequestStats FlushingStats;
    TWriteDataRequestStats FlushedStats;

    TReadDataRequestStats ReadStats;

    TWriteBackCache::TPersistentQueueStats PersistentQueueStats;

    // Do not store more than the specified amount of elements in the following
    // vectors in order to prevent OOM for large tests
    ui64 MaxItems = 1000000;

    void ResetNonDerivativeCounters() override
    {
        InProgressFlushCount = 0;
        NodeCount = 0;

        PendingStats.ResetNonDerivativeCounters();
        CachedStats.ResetNonDerivativeCounters();
        FlushRequestedStats.ResetNonDerivativeCounters();
        FlushingStats.ResetNonDerivativeCounters();
        FlushedStats.ResetNonDerivativeCounters();
    }

    void FlushStarted() override
    {
        InProgressFlushCount++;
    }

    void FlushCompleted() override
    {
        InProgressFlushCount--;
        CompletedFlushCount++;
    }

    void FlushFailed() override
    {
        FailedFlushCount++;
    }

    void IncrementNodeCount() override
    {
        NodeCount++;
    }

    void DecrementNodeCount() override
    {
        NodeCount--;
    }

    TWriteDataRequestStats& GetWriteStats(EWriteDataRequestStatus status)
    {
        switch (status) {
            case EWriteDataRequestStatus::Pending:
                return PendingStats;
            case EWriteDataRequestStatus::Cached:
                return CachedStats;
            case EWriteDataRequestStatus::FlushRequested:
                return FlushRequestedStats;
            case EWriteDataRequestStatus::Flushing:
                return FlushingStats;
            case EWriteDataRequestStatus::Flushed:
                return FlushedStats;
            default:
                Y_ABORT("Unknown EWriteDataRequestStatus value");
        }
    }

    void WriteDataRequestEnteredStatus(
        TWriteBackCache::EWriteDataRequestStatus status) override
    {
        auto& stats = GetWriteStats(status);
        stats.InProgressCount++;
    }

    void WriteDataRequestExitedStatus(
        TWriteBackCache::EWriteDataRequestStatus status,
        TDuration duration) override
    {
        auto& stats = GetWriteStats(status);
        stats.Count++;
        stats.InProgressCount--;
        if (stats.Data.size() < MaxItems) {
            stats.Data.push_back(duration);
        }
    }

    void WriteDataRequestUpdateMinTime(
        TWriteBackCache::EWriteDataRequestStatus status,
        TInstant minTime) override
    {
        auto& stats = GetWriteStats(status);
        stats.MinTime = minTime;
    }

    void AddReadDataStats(
        IWriteBackCacheStats::EReadDataRequestCacheStatus status,
        TDuration pendingDuration) override
    {
        if (ReadStats.Data.size() < MaxItems) {
            ReadStats.Data.push_back(pendingDuration);
        }
        switch (status) {
            case EReadDataRequestCacheStatus::Miss:
                ReadStats.CacheMissCount++;
                break;
            case EReadDataRequestCacheStatus::PartialHit:
                ReadStats.CachePartialHitCount++;
                break;
            case EReadDataRequestCacheStatus::FullHit:
                ReadStats.CacheFullHitCount++;
                break;
            default:
                Y_ABORT("Unknown EReadDataRequestCacheState value");
        }
    }

    void UpdatePersistentQueueStats(
        const TWriteBackCache::TPersistentQueueStats& stats) override
    {
        PersistentQueueStats = stats;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestTimer
    : public ITimer
{
    TInstant Current = ::Now();

    TInstant Now() override
    {
        return Current;
    }

    void Sleep(TDuration duration) override
    {
        Current += duration;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
{
    ILoggingServicePtr Logging;
    TLog Log;

    std::shared_ptr<TFileStoreTest> Session;
    std::shared_ptr<TTestTimer> Timer;
    std::shared_ptr<TTestScheduler> Scheduler;
    std::shared_ptr<TWriteBackCacheStats> Stats;
    TDuration CacheAutomaticFlushPeriod;
    TDuration CacheFlushRetryPeriod;
    TTempFileHandle TempFileHandle;
    TWriteBackCache Cache;

    ui32 MaxWriteRequestSize = 0;
    ui32 MaxWriteRequestsCount = 0;
    ui32 MaxSumWriteRequestsSize = 0;

    TCallContextPtr CallContext;

    // Maps nodeId to data
    THashMap<ui64, TString> ExpectedData;
    std::mutex ExpectedDataMutex;

    // Maps nodeId to data
    THashMap<ui64, TString> UnflushedData;
    std::mutex UnflushedDataMutex;

    // Maps nodeId to data
    THashMap<ui64, TString> FlushedData;
    std::mutex FlushedDataMutex;

    // Ensures that the data is not flushed twice, does not work well with cache
    // recreation because after recreation, the data may be flushed again
    bool EraseExpectedUnflushedDataAfterFirstUse = false;

    THashMap<ui64, TInFlightRequestTracker> InFlightReadRequestTracker;
    THashMap<ui64, TInFlightRequestTracker> InFlightWriteRequestTracker;

    std::atomic<int> SessionWriteDataHandlerCalled;

    TBootstrap(
            TDuration cacheAutomaticFlushPeriod = {},
            ui32 maxWriteRequestSize = 0,
            ui32 maxWriteRequestsCount = 0,
            ui32 maxSumWriteRequestsSize = 0)
        : MaxWriteRequestSize(maxWriteRequestSize > 0
            ? maxWriteRequestSize
            : DefaultMaxWriteRequestSize)
        , MaxWriteRequestsCount(maxWriteRequestsCount > 0
            ? maxWriteRequestsCount
            : DefaultMaxWriteRequestsCount)
        , MaxSumWriteRequestsSize(maxSumWriteRequestsSize > 0
            ? maxSumWriteRequestsSize
            : DefaultMaxSumWriteRequestsSize)
    {
        CacheAutomaticFlushPeriod = cacheAutomaticFlushPeriod;
        CacheFlushRetryPeriod = TDuration::MilliSeconds(100);

        Logging = CreateLoggingService("console", TLogSettings{});
        Logging->Start();
        Log = Logging->CreateLog("WRITE_BACK_CACHE");

        Timer = std::make_shared<TTestTimer>();
        Scheduler = std::make_shared<TTestScheduler>();
        Scheduler->Start();

        Stats = std::make_shared<TWriteBackCacheStats>();

        Session = std::make_shared<TFileStoreTest>();

        Session->ReadDataHandler = [&] (auto, auto request) {
            const auto nodeId = request->GetNodeId();
            const auto offset = request->GetOffset();
            const auto length = request->GetLength();

            // Overlapping write requests are not allowed
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                InFlightWriteRequestTracker[nodeId].Count(offset, length));

            InFlightReadRequestTracker[nodeId].Add(offset, length);
            Y_DEFER {
                InFlightReadRequestTracker[nodeId].Remove(offset, length);
            };

            std::unique_lock lock(FlushedDataMutex);

            NProto::TReadDataResponse response;

            if (!FlushedData.contains(nodeId)) {
                return MakeFuture(response);
            }

            auto data = FlushedData[nodeId];
            // Append zeroes if needed
            auto newSize = Max(
                data.size(),
                request->GetOffset() + request->GetLength());
            data.resize(newSize, 0);

            data = TStringBuf(data).SubString(
                request->GetOffset(),
                request->GetLength());

            auto responseOffset = RandomNumber(10u);
            auto responseBuffer = TString(data.size() + responseOffset, 0);
            data.copy(responseBuffer.begin() + responseOffset, data.size());

            response.SetBuffer(std::move(responseBuffer));
            response.SetBufferOffset(responseOffset);

            return MakeFuture(response);
        };

        Session->WriteDataHandler = [&] (auto, auto request) {
            const auto nodeId = request->GetNodeId();
            const auto offset = request->GetOffset();
            const auto length = request->GetBuffer().length();

            // Overlapping read requests are not allowed
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                InFlightReadRequestTracker[nodeId].Count(offset, length));

            // Overlapping write requests are not allowed
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                InFlightWriteRequestTracker[nodeId].Add(offset, length));
            Y_DEFER {
                InFlightWriteRequestTracker[nodeId].Remove(offset, length);
            };

            std::unique_lock lock1(UnflushedDataMutex);
            std::unique_lock lock2(FlushedDataMutex);

            STORAGE_INFO("Flushing " << request->GetBuffer().Quote()
                << " to @" << request->GetNodeId()
                << " at offset " << request->GetOffset());

            UNIT_ASSERT(UnflushedData.contains(nodeId));

            const auto unflushed = UnflushedData[nodeId];
            UNIT_ASSERT_LE(
                request->GetOffset() + request->GetBuffer().length(),
                unflushed.length());

            auto from = TStringBuf(unflushed).SubString(
                request->GetOffset(),
                request->GetBuffer().length());
            UNIT_ASSERT_VALUES_EQUAL(from, request->GetBuffer());

            auto& to = FlushedData[nodeId];
            // Append zeroes if needed
            auto newSize = Max(to.size(), request->GetOffset() + from.size());
            to.resize(newSize, 0);
            to.replace(request->GetOffset(), from.size(), from);

            if (EraseExpectedUnflushedDataAfterFirstUse) {
                memset(const_cast<char*>(from.data()), char(0), from.length());
            }

            SessionWriteDataHandlerCalled++;

            NProto::TWriteDataResponse response;
            return MakeFuture(response);
        };

        RecreateCache();

        CallContext = MakeIntrusive<TCallContext>();
    }

    ~TBootstrap()
    {
        Scheduler->Stop();
    }

    void RecreateCache()
    {
        STORAGE_INFO("Recreating cache");

        Cache = TWriteBackCache(
            Session,
            Scheduler,
            Timer,
            Stats,
            TempFileHandle.GetName(),
            CacheCapacityBytes,
            CacheAutomaticFlushPeriod,
            CacheFlushRetryPeriod,
            MaxWriteRequestSize,
            MaxWriteRequestsCount,
            MaxSumWriteRequestsSize);
    }

    TFuture<NProto::TReadDataResponse> ReadFromCache(
        ui64 nodeId,
        ui64 handle,
        ui64 offset,
        ui64 length)
    {
        auto request = std::make_shared<NProto::TReadDataRequest>();
        request->SetNodeId(nodeId);
        request->SetHandle(handle);
        request->SetOffset(offset);
        request->SetLength(length);

        return Cache.ReadData(CallContext, move(request));
    }

    TFuture<NProto::TReadDataResponse> ReadFromCache(
        ui64 nodeId,
        ui64 offset,
        ui64 length)
    {
        auto handle = nodeId + NodeToHandleOffset;
        return ReadFromCache(nodeId, handle, offset, length);
    }

    void ValidateCache(ui64 nodeId, ui64 offset, TString expected)
    {
        auto future = ReadFromCache(nodeId, offset, expected.length());
        auto response = future.GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(
            expected,
            response.GetBuffer().substr(response.GetBufferOffset()),
            TStringBuilder() << " while validating @" << nodeId
            << " at offset " << offset
            << " and length " << expected.length());
    }

    void ValidateCache(ui64 nodeId, ui64 offset, size_t length)
    {
        auto future = ReadFromCache(nodeId, offset, length);
        auto response = future.GetValueSync();

        // In concurrent tests, new data may be written at any moment.
        // We need take the most recent ExpectedData each time.
        //
        // Also, a race condition is still possible here:
        // ExpectedData is not fully synchonized with the internal state.
        // The following scenario may happen:
        // 1. WriteData has written new data.
        // 2. ReadFromCache reads new data.
        // 3. The callback updates ExpectedData.
        TString expected;
        {
            std::unique_lock lock(ExpectedDataMutex);
            expected =
                TStringBuf(ExpectedData[nodeId]).SubString(offset, length);
        }

        UNIT_ASSERT_VALUES_EQUAL_C(
            expected,
            response.GetBuffer().substr(response.GetBufferOffset()),
            TStringBuilder() << " while validating @" << nodeId
            << " at offset " << offset
            << " and length " << length);
    }

    void ValidateCache(ui64 nodeId)
    {
        size_t dataLength = 0;
        {
            std::unique_lock lock(ExpectedDataMutex);
            dataLength = ExpectedData[nodeId].length();
        }

        for (size_t offset = 0; offset < dataLength; offset++) {
            for (size_t len = 1; len + offset < dataLength; len++) {
                // TODO(svartmetal): validate with 'out of bounds'
                // requests also
                ValidateCache(nodeId, offset, len);
            }
        }
    }

    void ValidateCache()
    {
        for (const auto& [nodeId, _]: ExpectedData) {
            ValidateCache(nodeId);
        }
    }

    TFuture<NProto::TWriteDataResponse> WriteToCache(
        ui64 nodeId,
        ui64 handle,
        ui64 offset,
        TString buffer)
    {
        auto request = std::make_shared<NProto::TWriteDataRequest>();
        request->SetNodeId(nodeId);
        request->SetHandle(handle);
        request->SetOffset(offset);
        request->SetBuffer(buffer);

        auto future = Cache.WriteData(CallContext, std::move(request));

        future.Subscribe([&, nodeId, offset, buffer] (auto) {
            STORAGE_INFO("Written " << buffer.Quote()
                << " to @" << nodeId
                << " at offset " << offset);

            auto write = [=] (auto* data) {
                // append zeroes if needed
                auto newSize = Max(data->size(), offset + buffer.size());
                data->resize(newSize, 0);
                data->replace(offset, buffer.size(), buffer);
            };

            {
                std::unique_lock lock1(ExpectedDataMutex);
                std::unique_lock lock2(UnflushedDataMutex);

                write(&ExpectedData[nodeId]);
                write(&UnflushedData[nodeId]);
            }
        });

        return future;
    }

    TFuture<NProto::TWriteDataResponse> WriteToCache(
        ui64 nodeId,
        ui64 offset,
        TString buffer)
    {
        auto handle = nodeId + NodeToHandleOffset;
        return WriteToCache(nodeId, handle, offset, std::move(buffer));
    }

    void WriteToCacheSync(ui64 nodeId, ui64 handle, ui64 offset, TString buffer)
    {
        WriteToCache(nodeId, handle, offset, std::move(buffer)).GetValueSync();
    }

    void WriteToCacheSync(ui64 nodeId, ui64 offset, TString buffer)
    {
        WriteToCache(nodeId, offset, std::move(buffer)).GetValueSync();
    }

    void FlushCache(ui64 nodeId)
    {
        STORAGE_INFO("Flushing @" << nodeId);

        Cache.FlushNodeData(nodeId).GetValueSync();

        {
            std::unique_lock lock1(ExpectedDataMutex);
            std::unique_lock lock2(UnflushedDataMutex);
            std::unique_lock lock3(FlushedDataMutex);

            UNIT_ASSERT_VALUES_EQUAL(ExpectedData[nodeId], FlushedData[nodeId]);

            if (EraseExpectedUnflushedDataAfterFirstUse) {
                UnflushedData.erase(nodeId);
            }
        }
    }

    void FlushCache()
    {
        STORAGE_INFO("Flushing all data");

        Cache.FlushAllData().GetValueSync();

        {
            std::unique_lock lock1(ExpectedDataMutex);
            std::unique_lock lock2(UnflushedDataMutex);
            std::unique_lock lock3(FlushedDataMutex);

            UNIT_ASSERT_VALUES_EQUAL(ExpectedData, FlushedData);

            UnflushedData.clear();
        }
    }

    void ValidateCacheIsFlushed()
    {
        std::unique_lock lock1(ExpectedDataMutex);
        std::unique_lock lock3(FlushedDataMutex);

        UNIT_ASSERT_VALUES_EQUAL(ExpectedData, FlushedData);
    }

    void CheckStatsAreEmpty() const
    {
        UNIT_ASSERT_EQUAL(0, Stats->InProgressFlushCount);
        UNIT_ASSERT_EQUAL(0, Stats->NodeCount);
        UNIT_ASSERT_EQUAL(0, Stats->PersistentQueueStats.RawUsedBytesCount);
        UNIT_ASSERT_EQUAL(TInstant::Zero(), Stats->PendingStats.MinTime);
        UNIT_ASSERT_EQUAL(TInstant::Zero(), Stats->CachedStats.MinTime);
        UNIT_ASSERT_EQUAL(TInstant::Zero(), Stats->FlushRequestedStats.MinTime);
        UNIT_ASSERT_EQUAL(TInstant::Zero(), Stats->FlushingStats.MinTime);
        UNIT_ASSERT_EQUAL(TInstant::Zero(), Stats->FlushedStats.MinTime);
        UNIT_ASSERT_EQUAL(0, Stats->PendingStats.InProgressCount);
        UNIT_ASSERT_EQUAL(0, Stats->CachedStats.InProgressCount);
        UNIT_ASSERT_EQUAL(0, Stats->FlushRequestedStats.InProgressCount);
        UNIT_ASSERT_EQUAL(0, Stats->FlushingStats.InProgressCount);
        UNIT_ASSERT_EQUAL(0, Stats->FlushedStats.InProgressCount);
    }

    void CheckWriteDataRequestStats(
        const TWriteDataRequestStats& stats,
        const TString& name,
        ui64 expectedInProgressCount,
        ui64 expectedCount,
        TInstant expectedMinTime)
    {
        UNIT_ASSERT_EQUAL_C(
            expectedInProgressCount,
            stats.InProgressCount,
            name << "Stats.InProgressCount: expected = "
                 << expectedInProgressCount
                 << ", actual = " << stats.InProgressCount);

        UNIT_ASSERT_EQUAL_C(
            expectedCount,
            stats.Count,
            name << "Stats.Count: expected = " << expectedCount
                 << ", actual = " << stats.Count);

        UNIT_ASSERT_EQUAL_C(
            expectedMinTime,
            stats.MinTime,
            name << "Stats.MinTime: expected = " << expectedMinTime
                 << ", actual = " << stats.MinTime);
    }

    void CheckPendingWriteDataRequestStats(
        ui64 expectedInProgressCount,
        ui64 expectedCount,
        TInstant expectedMinTime)
    {
        CheckWriteDataRequestStats(
            Stats->PendingStats,
            "Pending",
            expectedInProgressCount,
            expectedCount,
            expectedMinTime);
    }

    void CheckCachedWriteDataRequestStats(
        ui64 expectedInProgressCount,
        ui64 expectedCount,
        TInstant expectedMinTime)
    {
        CheckWriteDataRequestStats(
            Stats->CachedStats,
            "Cached",
            expectedInProgressCount,
            expectedCount,
            expectedMinTime);
    }

    void CheckFlushRequestedWriteDataRequestStats(
        ui64 expectedInProgressCount,
        ui64 expectedCount,
        TInstant expectedMinTime)
    {
        CheckWriteDataRequestStats(
            Stats->FlushRequestedStats,
            "FlushRequested",
            expectedInProgressCount,
            expectedCount,
            expectedMinTime);
    }

    void CheckFlushingWriteDataRequestStats(
        ui64 expectedInProgressCount,
        ui64 expectedCount,
        TInstant expectedMinTime)
    {
        CheckWriteDataRequestStats(
            Stats->FlushingStats,
            "Flushing",
            expectedInProgressCount,
            expectedCount,
            expectedMinTime);
    }

    void CheckFlushedWriteDataRequestStats(
        ui64 expectedInProgressCount,
        ui64 expectedCount,
        TInstant expectedMinTime)
    {
        CheckWriteDataRequestStats(
            Stats->FlushedStats,
            "Flushed",
            expectedInProgressCount,
            expectedCount,
            expectedMinTime);
    }
};

struct TWriteRequestLogger
{
    struct TRequest
    {
        ui64 NodeId = 0;
        ui64 Offset = 0;
        ui64 Length = 0;
    };

    TVector<TRequest> Requests;

    void Subscribe(TBootstrap& b)
    {
        auto previousHandler = b.Session->WriteDataHandler;
        b.Session->WriteDataHandler =
            [this,
             previousHandler =
                 std::move(previousHandler)](auto context, auto request) mutable
        {
            Requests.push_back(
                {.NodeId = request->GetNodeId(),
                 .Offset = request->GetOffset(),
                 .Length =
                     request->GetBuffer().size() - request->GetBufferOffset()});

            return previousHandler(std::move(context), std::move(request));
        };
    }

    TString GetLog(ui64 nodeId) const
    {
        TStringBuilder sb;
        for (const auto& request : Requests) {
            if (request.NodeId != nodeId) {
                continue;
            }
            if (!sb.empty()) {
                sb << ", ";
            }
            sb << "(" << request.Offset << ", " << request.Length << ")";
        }
        return sb;
    }
};

template <class T>
TFuture<T> CompleteFutureOnTrigger(TFuture<T> future, TFuture<void> trigger)
{
    auto promise = NewPromise<T>();
    auto result = promise.GetFuture();

    trigger.Subscribe(
        [future = std::move(future), promise = std::move(promise)](auto) mutable
        {
            future.Subscribe(
                [promise = std::move(promise)](auto f) mutable
                {
                    promise.SetValue(f.GetValue());
                });
        });

    return result;
}

template <class T, class... TArgs>
class TManualProceedHandlers: TNonCopyable
{
private:
    std::function<T(TArgs...)> PrevHandler;
    TDeque<TPromise<void>> Triggers;

public:
    explicit TManualProceedHandlers(std::function<T(TArgs...)>& handler)
        : PrevHandler(std::move(handler))
    {
        handler = [this](TArgs... args)
        {
            auto promise = NewPromise();

            auto result = CompleteFutureOnTrigger(
                PrevHandler(std::forward<TArgs>(args)...),
                promise.GetFuture());

            Triggers.push_back(std::move(promise));
            return result;
        };
    }

    bool empty() const
    {
        return Triggers.empty();
    }

    size_t size() const
    {
        return Triggers.size();
    }

    void ProceedAll()
    {
        while (!Triggers.empty()) {
            Triggers.front().SetValue();
            Triggers.pop_front();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TStatsCalculator
{
    ui64 WriteDataFlushCount = 0;
    ui64 FlushCount = 0;

    struct TState
    {
        ui32 NodeId = 0;
        bool Flushed = false;
    };

    TDeque<TState> Queue;
    THashMap<ui32, ui64> UnflushedRequestCount;

    void Write(ui32 nodeId)
    {
        Queue.push_back({.NodeId = nodeId, .Flushed = false});
        UnflushedRequestCount[nodeId]++;
    }

    void Flush(ui32 nodeId)
    {
        for (auto& stats: Queue) {
            if (stats.NodeId != nodeId || stats.Flushed) {
                continue;
            }
            stats.Flushed = true;
        }

        auto it = UnflushedRequestCount.find(nodeId);
        if (it != UnflushedRequestCount.end()) {
            WriteDataFlushCount += it->second;
            FlushCount++;
            UnflushedRequestCount.erase(it);
        }

        while (!Queue.empty() && Queue.front().Flushed) {
            Queue.pop_front();
        }
    }

    void FlushAll()
    {
        FlushCount += UnflushedRequestCount.size();
        for (const auto& pair: UnflushedRequestCount) {
            WriteDataFlushCount += pair.second;
        }

        Queue.clear();
        UnflushedRequestCount.clear();
    }

    void Unflush()
    {
        for (auto& stats: Queue) {
            if (stats.Flushed) {
                stats.Flushed = false;
                UnflushedRequestCount[stats.NodeId]++;
            }
        }
    }

    ui64 GetCachedQueueRequestCount() const
    {
        return Queue.size();
    }

    ui64 GetNodeCount() const
    {
        return UnflushedRequestCount.size();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TWriteBackCacheTest)
{
    Y_UNIT_TEST(ShouldReadEmptyCache)
    {
        TBootstrap b;

        auto readPromise = NewPromise<NProto::TReadDataResponse>();
        b.Session->ReadDataHandler = [&] (auto, auto) {
            return readPromise.GetFuture();
        };

        NProto::TReadDataResponse response;
        // Return empty buffer in response
        readPromise.SetValue(response);

        auto readFuture = b.ReadFromCache(1, 0, 1);
        UNIT_ASSERT(readFuture.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            TString(1, 0),
            readFuture.GetValue().GetBuffer());
    }

    Y_UNIT_TEST(ShouldReadAndFlushEmptyCache)
    {
        TBootstrap b;

        b.ValidateCache(1, 0, "\0\0\0");
        b.ValidateCache(2, 10, "\0\0\0\0");

        b.FlushCache();
        b.ValidateCache(1, 5, "\0\0\0");

        b.FlushCache(1);
        b.ValidateCache(1, 100, "\0\0\0\0\0\0");
    }

    Y_UNIT_TEST(ShouldMergeRequestsWhenFlushing)
    {
        TBootstrap b;

        b.WriteToCacheSync(1, 0, "aa");
        b.WriteToCacheSync(1, 1, "bbb");
        b.WriteToCacheSync(1, 2, "cccc");
        b.WriteToCacheSync(1, 3, "ddddd");
        b.WriteToCacheSync(1, 4, "eeeeee");

        bool flushed = false;

        b.Session->WriteDataHandler = [&] (auto, auto request) {
            UNIT_ASSERT(!flushed);
            flushed = true;

            UNIT_ASSERT_VALUES_EQUAL(1, request->GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL(0, request->GetOffset());
            UNIT_ASSERT_VALUES_EQUAL("abcdeeeeee", request->GetBuffer());

            NProto::TWriteDataResponse response;
            return MakeFuture(response);
        };

        b.Cache.FlushNodeData(1).GetValueSync();
    }

    Y_UNIT_TEST(ShouldSequenceReadAndWriteRequestsAvoidingConflicts)
    {
        // WriteBackCache implementation ensures than Flush can execute
        // as fast as it can - when an entry is cached, nothing prevents it
        // from being flushed.
        //
        // The synchronization point is transition of pending request to cache.
        //
        // It is not allowed:
        // 1. To read data that overlaps with any pending write requests.
        // 2. To write data that overlaps with any in-flight read operation.
        //
        // It is allowed to simultaneously read the same data or have
        // multiple overlapping pending write requests.

        TBootstrap b;
        b.Session->WriteDataHandler = [&] (auto, auto) {
            return MakeFuture<NProto::TWriteDataResponse>({});
        };

        TVector<TPromise<NProto::TReadDataResponse>> readPromises;

        b.Session->ReadDataHandler = [&] (auto, auto) {
            readPromises.push_back(NewPromise<NProto::TReadDataResponse>());
            return readPromises.back().GetFuture();
        };

        auto readFuture1 = b.ReadFromCache(1, 0, 10);
        UNIT_ASSERT_VALUES_EQUAL(1, readPromises.size());
        UNIT_ASSERT(!readFuture1.HasValue());

        // Multiple read requests do not block each other
        auto readFuture2 = b.ReadFromCache(1, 0, 10);
        UNIT_ASSERT_VALUES_EQUAL(2, readPromises.size());
        UNIT_ASSERT(!readFuture2.HasValue());

        // It is not allowed to write data that is being read
        auto writeFuture1 = b.WriteToCache(1, 0, "abcdefghij");
        UNIT_ASSERT(!writeFuture1.HasValue());

        // Write requests has priority over read requests
        // Read request will not proceed because of pending write requests
        auto readFuture3 = b.ReadFromCache(1, 0, 10);
        UNIT_ASSERT_VALUES_EQUAL(2, readPromises.size());
        UNIT_ASSERT(!readFuture3.HasValue());

        // Can read and write other handle
        auto writeFuture2 = b.WriteToCache(2, 0, "abcdefghij");
        UNIT_ASSERT(writeFuture2.HasValue());
        auto readFuture4 = b.ReadFromCache(2, 5, 15);
        UNIT_ASSERT_VALUES_EQUAL(3, readPromises.size());
        UNIT_ASSERT(!readFuture4.HasValue());

        // Cannot write to [5, 15) because there is an in-flight read request
        // at [0, 10)
        auto writeFuture3 = b.WriteToCache(1, 5, "0123456789");
        UNIT_ASSERT(!writeFuture3.HasValue());

        // It is possible to write to [10, 20) because there are no in-flight
        // read requests. There are write request but they don't interfere.
        auto writeFuture4 = b.WriteToCache(1, 10, "ABCDEFGHIJ");
        UNIT_ASSERT(writeFuture4.HasValue());

        // Cannot read [10, 25) because of pending write request at [5, 15)
        auto readFuture5 = b.ReadFromCache(1, 10, 15);
        UNIT_ASSERT_VALUES_EQUAL(3, readPromises.size());
        UNIT_ASSERT(!readFuture5.HasValue());

        // It is still possible to write to [10, 20) because there are only
        // pending read requests but no in-flight requests
        auto writeFuture5 = b.WriteToCache(1, 10, "ABCDEFGHIJ");
        UNIT_ASSERT(writeFuture5.HasValue());

        // Proceed with readFuture1
        readPromises[0].SetValue({});
        UNIT_ASSERT_VALUES_EQUAL(3, readPromises.size());
        UNIT_ASSERT(readFuture1.HasValue());
        UNIT_ASSERT(!readFuture2.HasValue());
        UNIT_ASSERT(!readFuture3.HasValue());
        UNIT_ASSERT(!readFuture5.HasValue());
        UNIT_ASSERT(!writeFuture1.HasValue());
        UNIT_ASSERT(!writeFuture3.HasValue());

        // Proceed with readFuture2
        // All write requests will be completed
        // The remaining read requests ([0, 10) or [10, 25)) will proceed
        // Read request [0, 10) is fullfilled by cache - it will not pass
        // request to Session
        readPromises[1].SetValue({});
        UNIT_ASSERT_VALUES_EQUAL(4, readPromises.size());
        UNIT_ASSERT(readFuture2.HasValue());
        UNIT_ASSERT(readFuture3.HasValue());
        UNIT_ASSERT(!readFuture5.HasValue());
        UNIT_ASSERT(writeFuture1.HasValue());
        UNIT_ASSERT(writeFuture3.HasValue());

        // Complete all read requests
        readPromises[3].SetValue({});
        UNIT_ASSERT_VALUES_EQUAL(4, readPromises.size());
        UNIT_ASSERT(readFuture5.HasValue());
    }

    Y_UNIT_TEST(ShouldReadAfterWrite)
    {
        TBootstrap b;

        b.WriteToCacheSync(1, 0, "abc");
        // Additional check for test correctness
        UNIT_ASSERT_VALUES_EQUAL("abc", b.ExpectedData[1]);
        b.ValidateCache();

        b.WriteToCacheSync(2, 2, "bcde");
        // Additional check for test correctness
        UNIT_ASSERT_VALUES_EQUAL(TString("\0\0bcde", 6), b.ExpectedData[2]);
        b.ValidateCache();

        b.WriteToCacheSync(1, 11, "abcde");
        b.ValidateCache();

        b.WriteToCacheSync(1, 7, "bcdefgh");
        b.WriteToCacheSync(2, 0, "cdefghijkl");
        b.WriteToCacheSync(1, 3, "defghijklmn");
        b.ValidateCache();

        b.RecreateCache();
        b.ValidateCache();

        b.WriteToCacheSync(1, 0, "defgh");
        // Additional check for test correctness
        UNIT_ASSERT_VALUES_EQUAL("defghfghijklmnde", b.ExpectedData[1]);
        b.ValidateCache();

        b.FlushCache();
        b.ValidateCache();

        b.WriteToCacheSync(1, 1, "ijklmn");
        b.RecreateCache();
        b.ValidateCache();
    }

    Y_UNIT_TEST(ShouldFlushAutomatically)
    {
        const auto automaticFlushPeriod = TDuration::MilliSeconds(1);
        TBootstrap b(automaticFlushPeriod);

        auto checkFlush = [&](int attempt)
        {
            UNIT_ASSERT_VALUES_EQUAL(
                attempt,
                b.SessionWriteDataHandlerCalled.load());
            b.ValidateCacheIsFlushed();
        };

        b.WriteToCacheSync(1, 11, "abcde");
        b.Scheduler->RunAllScheduledTasks();
        checkFlush(1);

        b.WriteToCacheSync(1, 22, "efghij");
        b.Scheduler->RunAllScheduledTasks();
        checkFlush(2);
    }

    Y_UNIT_TEST(ShouldFlushAutomaticallyForDifferentNodes)
    {
        const auto automaticFlushPeriod = TDuration::MilliSeconds(1);
        TBootstrap b(automaticFlushPeriod);

        // Prevent write requests initiated by Flush from completing immediately
        TManualProceedHandlers writeRequests(b.Session->WriteDataHandler);

        b.WriteToCacheSync(1, 11, "abcde");
        b.Scheduler->RunAllScheduledTasks();
        UNIT_ASSERT_EQUAL(1, writeRequests.size());

        // Stuck at flushing for one node should not affect automatic flushing
        // for another node
        b.WriteToCacheSync(2, 22, "efghij");
        b.Scheduler->RunAllScheduledTasks();
        UNIT_ASSERT_EQUAL(2, writeRequests.size());
    }

    void TestShouldReadAfterWriteRandomized(bool withRecreation = false) {
        TBootstrap b;
        // Ensures that the data is not flushed twice, does not work well with
        // cache recreation because after recreation, the data may be flushed
        // again
        b.EraseExpectedUnflushedDataAfterFirstUse = !withRecreation;

        const TString alphabet = "abcdefghijklmnopqrstuvwxyz";

        int flushesRemaining = 10;
        int writesRemaining = 333;

        TStatsCalculator stats;

        while (writesRemaining--) {
            const ui64 offset = RandomNumber(alphabet.length());
            const ui64 length = Max(
                1ul,
                RandomNumber(alphabet.length() - offset));

            auto data = TStringBuf(alphabet).SubString(offset, length);

            ui32 nodeId = RandomNumber(3u);

            b.WriteToCacheSync(
                nodeId,
                offset + RandomNumber(11u),
                TString(data));

            stats.Write(nodeId);

            if (RandomNumber(10u) == 0 && flushesRemaining > 0) {
                if (auto nodeId = RandomNumber(4u)) {
                    if (nodeId == 3) {
                        b.FlushCache();
                        stats.FlushAll();
                    } else {
                        b.FlushCache(nodeId);
                        stats.Flush(nodeId);
                    }
                }
                flushesRemaining--;
            }

            if (withRecreation && RandomNumber(20u) == 0) {
                b.RecreateCache();
                stats.Unflush();
            }

            b.ValidateCache();

            UNIT_ASSERT_EQUAL(0, b.Stats->PendingStats.InProgressCount);
            UNIT_ASSERT_EQUAL(
                stats.GetCachedQueueRequestCount(),
                b.Stats->CachedStats.InProgressCount +
                    b.Stats->FlushRequestedStats.InProgressCount +
                    b.Stats->FlushingStats.InProgressCount +
                    b.Stats->FlushedStats.InProgressCount);
            UNIT_ASSERT_EQUAL(stats.GetNodeCount(), b.Stats->NodeCount);
            UNIT_ASSERT_EQUAL(stats.FlushCount, b.Stats->CompletedFlushCount);
        }

        if (withRecreation) {
            b.RecreateCache();
        }

        b.ValidateCache();

        b.FlushCache();
        b.CheckStatsAreEmpty();
    }

    Y_UNIT_TEST(ShouldReadAfterWriteRandomized)
    {
        TestShouldReadAfterWriteRandomized();
    }

    Y_UNIT_TEST(ShouldReadAfterWriteRandomizedWithRecreation)
    {
        TestShouldReadAfterWriteRandomized(true /* withRecreation */);
    }

    Y_UNIT_TEST(ShouldWriteAndFlushConcurrently)
    {
        TBootstrap b;

        b.WriteToCacheSync(1, 0, "cdefghijklm");
        b.WriteToCacheSync(2, 11, "defghijklmn");

        std::latch start{3};

        TVector<std::thread> threads;

        threads.emplace_back([&] {
            start.arrive_and_wait();
            b.Cache.FlushNodeData(1).GetValueSync();
        });

        threads.emplace_back([&] {
            start.arrive_and_wait();
            b.Cache.FlushNodeData(2).GetValueSync();
        });

        start.arrive_and_wait();
        b.Cache.FlushAllData().GetValueSync();

        for (auto& t: threads) {
            t.join();
        }

        b.RecreateCache();
        b.ValidateCache();
    }

    void TestShouldReadAfterWriteConcurrently(
        bool withManualFlush = false,
        bool withAutomaticFlush = false)
    {
        const auto automaticFlushPeriod =
            withAutomaticFlush ? TDuration::MilliSeconds(1) : TDuration();
        TBootstrap b(automaticFlushPeriod);

        const TString alphabet = "abcdefghijklmnopqrstuvwxyz";

        const ui32 rwThreadCount = 3;
        const ui32 roThreadCount = 2;
        const auto threadCount = rwThreadCount + roThreadCount;

        std::latch start{threadCount};

        TVector<std::thread> threads;

        for (ui32 i = 0; i < rwThreadCount; i++) {
            threads.emplace_back([&, nodeId = i] {
                start.arrive_and_wait();

                int flushesRemaining = 10;
                int writesRemaining = 333;

                while (writesRemaining--) {
                    SleepForRandomDurationMs(10);

                    const ui64 offset = RandomNumber(alphabet.length());
                    const ui64 length = Max(
                        1ul,
                        RandomNumber(alphabet.length() - offset));

                    auto data = TStringBuf(alphabet).SubString(offset, length);

                    b.WriteToCacheSync(
                        nodeId,
                        offset + RandomNumber(11u),
                        TString(data));

                    if (withManualFlush) {
                        if (RandomNumber(10u) == 0 && flushesRemaining > 0) {
                            b.FlushCache(nodeId);
                            flushesRemaining--;
                        }
                    }

                    b.ValidateCache(nodeId);
                }
            });
        }

        // Read-only threads for "smoke" testing
        for (ui32 i = 0; i < roThreadCount; i++) {
            threads.emplace_back([&] {
                start.arrive_and_wait();

                int readsRemaining = 111;
                while (readsRemaining--) {
                    SleepForRandomDurationMs(10);

                    auto request = std::make_shared<NProto::TReadDataRequest>();
                    request->SetNodeId(0);
                    request->SetOffset(0);
                    request->SetLength(333);

                    auto future = b.Cache.ReadData(
                        b.CallContext,
                        move(request));
                    auto response = future.GetValueSync();
                }
            });
        }

        for (auto& t: threads) {
            t.join();
        }

        b.RecreateCache();
        b.ValidateCache();

        b.FlushCache();
        b.CheckStatsAreEmpty();
    }

    Y_UNIT_TEST(ShouldReadAfterWriteConcurrently)
    {
        TestShouldReadAfterWriteConcurrently();
    }

    Y_UNIT_TEST(ShouldReadAfterWriteConcurrentlyWithManualFlush)
    {
        TestShouldReadAfterWriteConcurrently(true /* withManualFlush */);
    }

    Y_UNIT_TEST(ShouldNotMissPendingEntries)
    {
        TBootstrap b;

        int writeRequestsActual = 0;
        int writeRequestsExpected = 0;
        int pendingWriteRequests = 0;
        ui64 nextOffset = 0;

        auto promise = NewPromise<NProto::TWriteDataResponse>();

        b.Session->WriteDataHandler = [&] (auto, auto) {
            writeRequestsActual++;
            return promise.GetFuture();
        };

        while (pendingWriteRequests < 32) {
            auto future = b.WriteToCache(1, nextOffset, "a");
            nextOffset += 10;
            writeRequestsExpected++;
            if (!future.HasValue()) {
                pendingWriteRequests++;
            }
        }

        promise.SetValue({});

        b.Cache.FlushNodeData(1);

        UNIT_ASSERT_VALUES_EQUAL(writeRequestsExpected, writeRequestsActual);
    }

    Y_UNIT_TEST(ShouldRetryFlushOnFailure)
    {
        constexpr int WriteAttemptsThreshold = 3;

        TBootstrap b;

        std::atomic_int writeAttempts = 0;

        auto prevWriteDataHandler = std::move(b.Session->WriteDataHandler);
        b.Session->WriteDataHandler = [&](auto context, auto request) {
            writeAttempts++;
            if (writeAttempts < WriteAttemptsThreshold) {
                NProto::TWriteDataResponse response;
                *response.MutableError() = MakeError(E_REJECTED);
                return MakeFuture(std::move(response));
            }
            return prevWriteDataHandler(std::move(context), std::move(request));
        };

        b.WriteToCacheSync(1, 12, "hello");
        auto flushFuture = b.Cache.FlushNodeData(1);

        // Flush starts synchronously in FlushData call and makes an attempt
        // to write data but fails
        UNIT_ASSERT_GE(writeAttempts, 0);
        UNIT_ASSERT(!flushFuture.HasValue());

        // WriteData request from Flush succeeds after WriteAttemptsThreshold
        // attempts.
        for (int i = 1; i < WriteAttemptsThreshold; i++) {
            b.Scheduler->RunAllScheduledTasks();
        }

        UNIT_ASSERT(flushFuture.HasValue());
        UNIT_ASSERT_EQUAL(writeAttempts, WriteAttemptsThreshold);
    }

    Y_UNIT_TEST(ShouldSplitLargeWriteRequestsAtFlush)
    {
        // Set MaxWriteRequestSize to 2 bytes
        TBootstrap b({}, 2, 0, 0);

        TWriteRequestLogger logger;
        logger.Subscribe(b);

        b.WriteToCacheSync(1, 0, "aa");
        b.WriteToCacheSync(1, 2, "bb");
        b.WriteToCacheSync(1, 4, "cc");
        b.WriteToCacheSync(1, 6, "dd");
        b.WriteToCacheSync(1, 8, "ee");

        b.Cache.FlushNodeData(1).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(
            "(0, 2), (2, 2), (4, 2), (6, 2), (8, 2)",
            logger.GetLog(1));
    }

    Y_UNIT_TEST(ShouldLimitTotalWriteRequestSizeAtFlush)
    {
        // Set MaxSumWriteRequestsSize to 7 bytes
        TBootstrap b({}, 0, 0, 7);

        TWriteRequestLogger logger;
        logger.Subscribe(b);

        b.WriteToCacheSync(1, 0, "aa");
        b.WriteToCacheSync(1, 2, "bb");
        b.WriteToCacheSync(1, 4, "cc");
        b.WriteToCacheSync(1, 6, "dd");
        b.WriteToCacheSync(1, 8, "ee");

        b.Cache.FlushNodeData(1).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(
            "(0, 6), (6, 4)",
            logger.GetLog(1));
    }

    Y_UNIT_TEST(ShouldLimitWriteRequestsCountAtFlush)
    {
        // Set MaxWriteRequestsCount to 3
        TBootstrap b({}, 0, 3, 0);

        TWriteRequestLogger logger;
        logger.Subscribe(b);

        b.WriteToCacheSync(1, 8, "a");
        b.WriteToCacheSync(1, 6, "b");
        b.WriteToCacheSync(1, 4, "c");
        b.WriteToCacheSync(1, 2, "d");
        b.WriteToCacheSync(1, 0, "e");

        b.Cache.FlushNodeData(1).GetValueSync();

        // Parts are going in the increased order in each flush operation
        UNIT_ASSERT_VALUES_EQUAL(
            "(4, 1), (6, 1), (8, 1), (0, 1), (2, 1)",
            logger.GetLog(1));
    }

    Y_UNIT_TEST(ShouldTryToFlushWhenRequestSizeIsGreaterThanLimit)
    {
        // Set MaxWriteRequestSize to 2 bytes and MaxWriteRequestsCount to 1
        TBootstrap b({}, 2, 1, 0);

        TWriteRequestLogger logger;
        logger.Subscribe(b);

        b.WriteToCacheSync(1, 0, "a");
        // Flush will have two requests (1, 2) and (3, 2) despite the limit
        b.WriteToCacheSync(1, 1, "bbbb");
        b.WriteToCacheSync(1, 5, "c");
        b.WriteToCacheSync(1, 6, "d");

        b.Cache.FlushNodeData(1).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(
            "(0, 1), (1, 2), (3, 2), (5, 2)",
            logger.GetLog(1));
    }

    Y_UNIT_TEST(ShouldShareCacheBetweenHandles)
    {
        TBootstrap b;

        b.WriteToCacheSync(1, 1, 0, "abc");
        b.WriteToCacheSync(1, 2, 1, "def");
        b.WriteToCacheSync(2, 3, 2, "xyz");

        auto readFuture = b.ReadFromCache(1, 1, 0, 3);
        UNIT_ASSERT(readFuture.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            "ade",
            readFuture.GetValue().GetBuffer());
    }

    Y_UNIT_TEST(ShouldReportPersistentQueueStats)
    {
        TBootstrap b;
        auto& stats = b.Stats->PersistentQueueStats;

        UNIT_ASSERT_EQUAL(CacheCapacityBytes, stats.RawCapacity);
        UNIT_ASSERT_EQUAL(0, stats.RawUsedBytesCount);
        UNIT_ASSERT_LT(0, stats.MaxAllocationBytesCount);
        UNIT_ASSERT_GT(CacheCapacityBytes, stats.MaxAllocationBytesCount);
        UNIT_ASSERT_EQUAL(false, stats.IsCorrupted);

        b.WriteToCacheSync(1, 0, "abc");

        UNIT_ASSERT_EQUAL(CacheCapacityBytes, stats.RawCapacity);
        UNIT_ASSERT_LT(0, stats.RawUsedBytesCount);
        UNIT_ASSERT_LT(0, stats.MaxAllocationBytesCount);
        UNIT_ASSERT_GT(CacheCapacityBytes, stats.MaxAllocationBytesCount);
        UNIT_ASSERT_EQUAL(false, stats.IsCorrupted);

        auto prevUsedBytesCount = stats.RawUsedBytesCount;
        auto prevMaxAllocationSize = stats.MaxAllocationBytesCount;

        b.RecreateCache();

        UNIT_ASSERT_EQUAL(CacheCapacityBytes, stats.RawCapacity);
        UNIT_ASSERT_EQUAL(prevUsedBytesCount, stats.RawUsedBytesCount);
        UNIT_ASSERT_EQUAL(prevMaxAllocationSize, stats.MaxAllocationBytesCount);
        UNIT_ASSERT_GT(CacheCapacityBytes, stats.MaxAllocationBytesCount);
        UNIT_ASSERT_EQUAL(false, stats.IsCorrupted);

        b.FlushCache();

        UNIT_ASSERT_EQUAL(CacheCapacityBytes, stats.RawCapacity);
        UNIT_ASSERT_EQUAL(0, stats.RawUsedBytesCount);
        UNIT_ASSERT_LT(0, stats.MaxAllocationBytesCount);
        UNIT_ASSERT_GT(CacheCapacityBytes, stats.MaxAllocationBytesCount);
        UNIT_ASSERT_EQUAL(false, stats.IsCorrupted);

        b.WriteToCacheSync(1, 0, "abc");

        b.TempFileHandle.Pwrite("Corrupt cache", 13, 41);
        b.TempFileHandle.Flush();

        b.RecreateCache();

        UNIT_ASSERT_EQUAL(true, stats.IsCorrupted);
    }

    Y_UNIT_TEST(ShouldReportFlushStats)
    {
        TBootstrap b;
        auto& stats = *b.Stats;
        stats.MaxItems = 10;

        constexpr int WriteAttemptsThreshold = 3;
        std::atomic_int writeAttempts = 0;

        auto prevWriteDataHandler = std::move(b.Session->WriteDataHandler);
        b.Session->WriteDataHandler = [&](auto context, auto request) {
            writeAttempts++;
            if (writeAttempts < WriteAttemptsThreshold) {
                NProto::TWriteDataResponse response;
                *response.MutableError() = MakeError(E_REJECTED);
                return MakeFuture(std::move(response));
            }
            return prevWriteDataHandler(std::move(context), std::move(request));
        };

        b.WriteToCacheSync(1, 0, "abc");
        b.WriteToCacheSync(2, 0, "def");

        UNIT_ASSERT_EQUAL(0, stats.CompletedFlushCount);
        UNIT_ASSERT_EQUAL(0, stats.FailedFlushCount);
        UNIT_ASSERT_EQUAL(0, stats.InProgressFlushCount);
        UNIT_ASSERT_EQUAL(TInstant::Zero(), stats.FlushingStats.MinTime);

        b.Timer->Sleep(TDuration::Seconds(1));
        auto now = b.Timer->Now();
        auto flushFuture1 = b.Cache.FlushNodeData(1);

        UNIT_ASSERT(!flushFuture1.HasValue());
        UNIT_ASSERT_EQUAL(0, stats.CompletedFlushCount);
        UNIT_ASSERT_EQUAL(1, stats.FailedFlushCount);
        UNIT_ASSERT_EQUAL(1, stats.InProgressFlushCount);
        UNIT_ASSERT_EQUAL(now, stats.FlushingStats.MinTime);

        b.Timer->Sleep(TDuration::Seconds(1));
        b.Scheduler->RunAllScheduledTasks();
        auto flushFuture2 = b.Cache.FlushNodeData(2);

        UNIT_ASSERT(!flushFuture1.HasValue());
        UNIT_ASSERT(flushFuture2.HasValue());
        UNIT_ASSERT_EQUAL(1, stats.CompletedFlushCount);
        UNIT_ASSERT_EQUAL(2, stats.FailedFlushCount);
        UNIT_ASSERT_EQUAL(1, stats.InProgressFlushCount);
        UNIT_ASSERT_EQUAL(now, stats.FlushingStats.MinTime);

        b.Timer->Sleep(TDuration::Seconds(1));
        b.Scheduler->RunAllScheduledTasks();

        UNIT_ASSERT(flushFuture1.HasValue());
        UNIT_ASSERT_EQUAL(2, stats.CompletedFlushCount);
        UNIT_ASSERT_EQUAL(2, stats.FailedFlushCount);
        UNIT_ASSERT_EQUAL(0, stats.InProgressFlushCount);
        UNIT_ASSERT_EQUAL(TInstant::Zero(), stats.FlushingStats.MinTime);
    }

    Y_UNIT_TEST(ShouldReportNodeCount)
    {
        TBootstrap b;
        auto& stats = *b.Stats;

        UNIT_ASSERT_EQUAL(0, stats.NodeCount);

        b.WriteToCacheSync(1, 0, "abc");

        UNIT_ASSERT_EQUAL(1, stats.NodeCount);

        b.WriteToCacheSync(2, 0, "def");
        b.WriteToCacheSync(2, 1, "xyz");

        UNIT_ASSERT_EQUAL(2, stats.NodeCount);

        b.RecreateCache();

        UNIT_ASSERT_EQUAL(2, stats.NodeCount);

        b.FlushCache(1);

        UNIT_ASSERT_EQUAL(1, stats.NodeCount);

        b.FlushCache(2);

        UNIT_ASSERT_EQUAL(0, stats.NodeCount);
    }

    Y_UNIT_TEST(ShouldReportWriteDataRequestStats)
    {
        TBootstrap b;

        const auto zero = TInstant::Zero();
        const auto now = b.Timer->Now();
        const auto t1 = now + TDuration::Seconds(1);
        const auto t3 = now + TDuration::Seconds(7);
        const auto t4 = now + TDuration::Seconds(15);
        const auto t5 = now + TDuration::Seconds(31);
        const auto t6 = now + TDuration::Seconds(63);

        // Reaching the capacity will trigger Flush
        // Need to prevent it from completing immediately
        TManualProceedHandlers writeRequests(b.Session->WriteDataHandler);

        b.CheckStatsAreEmpty();

        // --- T1

        b.Timer->Sleep(TDuration::Seconds(1));
        b.WriteToCacheSync(1, 0, "abc");

        b.CheckPendingWriteDataRequestStats(0, 1, zero);
        b.CheckCachedWriteDataRequestStats(1, 0, t1);
        b.CheckFlushRequestedWriteDataRequestStats(0, 0, zero);
        b.CheckFlushingWriteDataRequestStats(0, 0, zero);
        b.CheckFlushedWriteDataRequestStats(0, 0, zero);

        // --- T2

        b.Timer->Sleep(TDuration::Seconds(2));
        b.WriteToCacheSync(2, 0, "def");
        b.WriteToCacheSync(2, 1, "xyz");

        b.CheckPendingWriteDataRequestStats(0, 3, zero);
        b.CheckCachedWriteDataRequestStats(3, 0, t1);
        b.CheckFlushRequestedWriteDataRequestStats(0, 0, zero);
        b.CheckFlushingWriteDataRequestStats(0, 0, zero);
        b.CheckFlushedWriteDataRequestStats(0, 0, zero);

        // --- T3

        b.Timer->Sleep(TDuration::Seconds(4));
        b.Cache.FlushNodeData(2);
        b.WriteToCacheSync(2, 1, "xyz");
        b.Cache.FlushNodeData(2);

        b.CheckPendingWriteDataRequestStats(0, 4, zero);
        b.CheckCachedWriteDataRequestStats(1, 3, t1);
        b.CheckFlushRequestedWriteDataRequestStats(1, 2, t3);
        b.CheckFlushingWriteDataRequestStats(2, 0, t3);
        b.CheckFlushedWriteDataRequestStats(0, 0, zero);

        writeRequests.ProceedAll();

        // WriteData requests for node 2 are flushed but they cannot be removed
        // from the middle of the queue
        b.CheckPendingWriteDataRequestStats(0, 4, zero);
        b.CheckCachedWriteDataRequestStats(1, 3, t1);
        b.CheckFlushRequestedWriteDataRequestStats(0, 3, zero);
        b.CheckFlushingWriteDataRequestStats(0, 3, zero);
        b.CheckFlushedWriteDataRequestStats(3, 0, t3);

        // --- T4

        b.Timer->Sleep(TDuration::Seconds(8));
        b.Cache.FlushNodeData(1);
        b.RecreateCache();

        // Cache recreation forces the requests stored in the queue to be
        // flushed again
        b.CheckPendingWriteDataRequestStats(0, 4, zero);
        b.CheckCachedWriteDataRequestStats(4, 4, t4);
        b.CheckFlushRequestedWriteDataRequestStats(0, 4, zero);
        b.CheckFlushingWriteDataRequestStats(0, 3, zero);
        b.CheckFlushedWriteDataRequestStats(0, 0, zero);

        // --- T5

        b.Timer->Sleep(TDuration::Seconds(16));
        ui64 count = 0;
        while (true) {
            auto future = b.WriteToCache(3, 0, "01234567");
            if (future.HasValue()) {
                count++;
            } else {
                break;
            }
        }

        // FlushAll should have been triggered by hitting cache capacity
        b.CheckPendingWriteDataRequestStats(1, count + 4, t5);
        b.CheckCachedWriteDataRequestStats(0, count + 8, zero);
        b.CheckFlushRequestedWriteDataRequestStats(0, count + 8, zero);
        b.CheckFlushingWriteDataRequestStats(count + 4, 3, t5);
        b.CheckFlushedWriteDataRequestStats(0, 0, zero);

        // --- T6

        b.Timer->Sleep(TDuration::Seconds(32));
        writeRequests.ProceedAll();

        b.CheckPendingWriteDataRequestStats(0, count + 5, zero);
        b.CheckCachedWriteDataRequestStats(1, count + 8, t6);
        b.CheckFlushRequestedWriteDataRequestStats(0, count + 8, zero);
        b.CheckFlushingWriteDataRequestStats(0, count + 7, zero);
        b.CheckFlushedWriteDataRequestStats(0, count + 4, zero);

        // --- T7

        b.Timer->Sleep(TDuration::Seconds(64));
        b.Cache.FlushAllData();

        writeRequests.ProceedAll();

        b.CheckPendingWriteDataRequestStats(0, count + 5, zero);
        b.CheckCachedWriteDataRequestStats(0, count + 9, zero);
        b.CheckFlushRequestedWriteDataRequestStats(0, count + 9, zero);
        b.CheckFlushingWriteDataRequestStats(0, count + 8, zero);
        b.CheckFlushedWriteDataRequestStats(0, count + 5, zero);

        b.CheckStatsAreEmpty();
    }

    Y_UNIT_TEST(ShouldReportReadDataCounters)
    {
        TBootstrap b;
        b.Stats->MaxItems = 10;
        auto& stats = b.Stats->ReadStats;

        // Prevent Flush from completing immediately
        TManualProceedHandlers writeRequests(b.Session->WriteDataHandler);

        b.WriteToCacheSync(1, 0, "abc");

        UNIT_ASSERT_EQUAL(0, stats.CacheFullHitCount);
        UNIT_ASSERT_EQUAL(0, stats.CachePartialHitCount);
        UNIT_ASSERT_EQUAL(0, stats.CacheMissCount);

        b.ReadFromCache(1, 0, 2).IgnoreResult().Wait();

        UNIT_ASSERT_EQUAL(1, stats.CacheFullHitCount);
        UNIT_ASSERT_EQUAL(0, stats.CachePartialHitCount);
        UNIT_ASSERT_EQUAL(0, stats.CacheMissCount);

        b.ReadFromCache(1, 2, 2).IgnoreResult().Wait();

        UNIT_ASSERT_EQUAL(1, stats.CacheFullHitCount);
        UNIT_ASSERT_EQUAL(1, stats.CachePartialHitCount);
        UNIT_ASSERT_EQUAL(0, stats.CacheMissCount);

        b.ReadFromCache(1, 4, 2).IgnoreResult().Wait();

        UNIT_ASSERT_EQUAL(1, stats.CacheFullHitCount);
        UNIT_ASSERT_EQUAL(1, stats.CachePartialHitCount);
        UNIT_ASSERT_EQUAL(1, stats.CacheMissCount);

        // Fill the cache until the requests become pending
        while (true) {
            auto writeFuture = b.WriteToCache(1, 0, "0123456789");
            if (!writeFuture.HasValue()) {
                break;
            }
        }

        // A pending write request holds read-write-lock that
        // prevents read requests from proceeding
        auto readFuture = b.ReadFromCache(1, 0, 2).IgnoreResult();

        b.Timer->Sleep(TDuration::Seconds(1));
        writeRequests.ProceedAll();

        UNIT_ASSERT_EQUAL(4, stats.Data.size());
        UNIT_ASSERT_EQUAL(TDuration::Zero(), stats.Data[0]);
        UNIT_ASSERT_EQUAL(TDuration::Zero(), stats.Data[1]);
        UNIT_ASSERT_EQUAL(TDuration::Zero(), stats.Data[2]);
        UNIT_ASSERT_EQUAL(TDuration::Seconds(1), stats.Data[3]);
    }

    /* TODO(svartmetal): fix tests with automatic flush
    Y_UNIT_TEST(ShouldReadAfterWriteConcurrentlyWithAutomaticFlush)
    {
        TestShouldReadAfterWriteConcurrently(
            false,  // withManualFlush
            true);  // withAutomaticFlush
    }

    Y_UNIT_TEST(ShouldReadAfterWriteConcurrentlyWithManualAndAutomaticFlush)
    {
        TestShouldReadAfterWriteConcurrently(
            true,   // withManualFlush
            true);  // withAutomaticFlush
    }
    */
}

}   // namespace NCloud::NFileStore::NFuse
