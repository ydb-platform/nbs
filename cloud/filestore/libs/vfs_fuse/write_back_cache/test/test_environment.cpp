#include "test_environment.h"

#include <cloud/filestore/libs/diagnostics/module_stats.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore_test.h>

#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <cloud/filestore/libs/vfs_fuse/write_back_cache/write_back_cache_stats.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>

#include <util/generic/hash.h>
#include <util/stream/output.h>
#include <util/system/mutex.h>
#include <util/system/tempfile.h>
#include <util/thread/pool.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration FlushRetryPeriod = TDuration::MilliSeconds(100);
constexpr TDuration WaitTimeout = TDuration::Seconds(5);

////////////////////////////////////////////////////////////////////////////////

TStringBuf ToStringBuf(const NProto::TIovec& iovec)
{
    return {reinterpret_cast<const char*>(iovec.GetBase()), iovec.GetLength()};
}

TMemoryOutput ToMemoryOutput(const NProto::TIovec& iovec)
{
    return {reinterpret_cast<char*>(iovec.GetBase()), iovec.GetLength()};
}

void Write(TString& data, ui64 offset, TStringBuf buffer)
{
    const auto newSize = Max(data.size(), offset + buffer.size());
    data.resize(newSize, 0);
    data.replace(offset, buffer.size(), buffer);
}

////////////////////////////////////////////////////////////////////////////////

class TTestEnvironment
    : public ITestEnvironment
{
private:
    struct TNodeData
    {
        TAdaptiveLock Lock;
        TString Data;
    };

    const TTestEnvironmentConfig Config;
    const bool Concurrent;

    TThreadPool SubmitThreadPool;
    TThreadPool ExecutorThreadPool;

    ILoggingServicePtr Logging;
    TLog Log;
    std::shared_ptr<TFileStoreTest> Session;
    ITimerPtr Timer;
    ISchedulerPtr Scheduler;
    IWriteBackCacheStatsPtr Stats;
    TTempFileHandle TempFileHandle;
    TWriteBackCache Cache;
    IModuleStatsPtr ModuleStats;
    TCallContextPtr CallContext;

    TAdaptiveLock GlobalLock;
    THashMap<ui64, std::unique_ptr<TNodeData>> Nodes;

public:
    TTestEnvironment(
        const TTestEnvironmentConfig& config,
        bool concurrent)
        : Config(config)
        , Concurrent(concurrent)
    {
        Logging = CreateLoggingService("console", TLogSettings{});
        Logging->Start();
        Log = Logging->CreateLog("WRITE_BACK_CACHE");

        if (Config.UseTestTimerAndScheduler) {
            Timer = std::make_shared<TTestTimer>();
            Scheduler = std::make_shared<TTestScheduler>();
        } else {
            Timer = CreateWallClockTimer();
            Scheduler = CreateScheduler(Timer);
        }
        Scheduler->Start();

        Session = std::make_shared<TFileStoreTest>();
        Session->WriteDataHandler = [this](const auto&, auto request)
        {
            return WriteDataHandler(std::move(request));
        };
        Session->ReadDataHandler = [this](const auto&, auto request)
        {
            return ReadDataHandler(std::move(request));
        };

        CallContext = MakeIntrusive<TCallContext>("FileSystemId");

        if (Concurrent) {
            SubmitThreadPool.Start(Config.SubmitThreadCount);
            ExecutorThreadPool.Start(Config.ExecutorThreadCount);
        }

        RecreateCache();
    }

    ~TTestEnvironment() override
    {
        UNIT_ASSERT(Cache.Drain().Wait(WaitTimeout));
        if (Concurrent) {
            SubmitThreadPool.Stop();
            ExecutorThreadPool.Stop();
        }
        Scheduler->Stop();
    }

    void RecreateCache() override
    {
        Stats = CreateWriteBackCacheStats();
        Cache = TWriteBackCache(
            {.Session = Session,
             .Scheduler = Scheduler,
             .Timer = Timer,
             .Stats = Stats,
             .Log = Log,
             .FileSystemId = "FileSystemId",
             .ClientId = "ClientId",
             .FilePath = TempFileHandle.GetName(),
             .CapacityBytes = Config.CacheCapacityBytes,
             .AutomaticFlushPeriod = Config.AutomaticFlushPeriod,
             .FlushRetryPeriod = FlushRetryPeriod,
             .FlushMaxWriteRequestSize = Config.MaxWriteRequestSize,
             .FlushMaxWriteRequestsCount = Config.MaxWriteRequestsCount,
             .FlushMaxSumWriteRequestsSize = Config.MaxSumWriteRequestsSize,
             .ZeroCopyWriteEnabled = Config.ZeroCopyWriteEnabled,
             .FlushWritesInParallelEnabled =
                 Config.FlushWritesInParallelEnabled});
        ModuleStats = Cache.CreateModuleStats();
    }

    TFuture<NProto::TWriteDataResponse> WriteData(
        std::shared_ptr<NProto::TWriteDataRequest> request) override
    {
        if (!Concurrent) {
            return Cache.WriteData(CallContext, std::move(request));
        }

        return Async(
            [this, request = std::move(request)]() mutable
            {
                return Cache.WriteData(CallContext, std::move(request));
            },
            SubmitThreadPool);
    }

    TFuture<NProto::TReadDataResponse> ReadData(
        std::shared_ptr<NProto::TReadDataRequest> request) override
    {
        if (!Concurrent) {
            return Cache.ReadData(CallContext, std::move(request));
        }

        return Async(
            [this, request = std::move(request)]() mutable
            {
                return Cache.ReadData(CallContext, std::move(request));
            },
            SubmitThreadPool);
    }

    TFuture<NProto::TError> Flush(ui64 nodeId) override
    {
        if (!Concurrent) {
            return Cache.FlushNodeData(nodeId);
        }

        return Async(
            [this, nodeId]
            {
                return Cache.FlushNodeData(nodeId);
            },
            SubmitThreadPool);
    }

private:
    TFuture<NProto::TWriteDataResponse> WriteDataHandler(
        std::shared_ptr<NProto::TWriteDataRequest> request)
    {
        auto* nodeData = GetNodeData(request->GetNodeId());

        if (!Concurrent) {
            return MakeFuture(WriteDataHandlerImpl(nodeData, *request));
        }

        return Async(
            [this, nodeData, request = std::move(request)]
            {
                return WriteDataHandlerImpl(nodeData, *request);
            },
            ExecutorThreadPool);
    }

    NProto::TWriteDataResponse WriteDataHandlerImpl(
        TNodeData* nodeData,
        NProto::TWriteDataRequest& request)
    {
        MoveIovecsToBuffer(request);

        auto guard = Guard(nodeData->Lock);
        Write(nodeData->Data, request.GetOffset(), request.GetBuffer());

        return {};
    }

    TFuture<NProto::TReadDataResponse> ReadDataHandler(
        std::shared_ptr<NProto::TReadDataRequest> request)
    {
        auto* nodeData = GetNodeData(request->GetNodeId());

        if (!Concurrent) {
            return MakeFuture(ReadDataHandlerImpl(nodeData, *request));
        }

        return Async(
            [this, nodeData, request = std::move(request)]
            {
                return ReadDataHandlerImpl(nodeData, *request);
            },
            ExecutorThreadPool);
    }

    NProto::TReadDataResponse ReadDataHandlerImpl(
        TNodeData* nodeData,
        const NProto::TReadDataRequest& request)
    {
        auto guard = Guard(nodeData->Lock);

        auto data = TStringBuf(nodeData->Data);
        data = data.Skip(Min(request.GetOffset(), data.size()));
        data = data.Trunc(Min(request.GetLength(), data.size()));

        NProto::TReadDataResponse response;
        if (request.GetIovecs().empty()) {
            response.SetBuffer(TString(data));
        } else {
            response.SetLength(data.size());
            for (const auto& iovec: request.GetIovecs()) {
                if (data.empty()) {
                    break;
                }

                auto out = ToMemoryOutput(iovec);
                const auto length = Min(data.size(), out.Avail());
                out.Write(data.Head(length));
                data.Skip(length);
            }
        }

        return response;
    }

    void MoveIovecsToBuffer(NProto::TWriteDataRequest& request) const
    {
        if (request.GetIovecs().empty()) {
            return;
        }

        UNIT_ASSERT_C(
            Config.ZeroCopyWriteEnabled,
            "TWriteDataRequest generated by TWriteBackCache may contain "
            "Iovecs only if ZeroCopyWriteEnabled flag is enabled");
        UNIT_ASSERT_C(
            request.GetBuffer().empty(),
            "Buffer should be empty if a request contains Iovecs");
        UNIT_ASSERT_VALUES_EQUAL_C(
            0,
            request.GetBufferOffset(),
            "BufferOffset should be zero if a request contains Iovecs");

        TString buffer;
        for (const auto& iovec: request.GetIovecs()) {
            buffer.append(ToStringBuf(iovec));
        }
        request.SetBuffer(std::move(buffer));
        request.ClearIovecs();
    }

    TNodeData* GetNodeData(ui64 nodeId)
    {
        auto guard = Guard(GlobalLock);
        auto& ptr = Nodes[nodeId];
        if (!ptr) {
            ptr = std::make_unique<TNodeData>();
        }
        return ptr.get();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSynchronousTestEnvironment final
    : public TTestEnvironment
{
public:
    explicit TSynchronousTestEnvironment(
            const TTestEnvironmentConfig& config)
        : TTestEnvironment(config, false)
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TConcurrentTestEnvironment final
    : public TTestEnvironment
{
public:
    explicit TConcurrentTestEnvironment(
            const TTestEnvironmentConfig& config)
        : TTestEnvironment(config, true)
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITestEnvironmentPtr CreateTestEnvironment(
    const TTestEnvironmentConfig& config)
{
    if (config.UseConcurrentTestEnvironment) {
        return std::make_unique<TConcurrentTestEnvironment>(config);
    }

    return std::make_unique<TSynchronousTestEnvironment>(config);
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
