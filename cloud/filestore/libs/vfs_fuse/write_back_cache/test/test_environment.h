#pragma once

#include <cloud/filestore/libs/vfs_fuse/write_back_cache/write_back_cache.h>

#include <memory>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct TTestEnvironmentConfig
{
    // Period between automatic attempts to flush all cached data.
    // By default, no automatic flushing is performed.
    TDuration AutomaticFlushPeriod = {};

    // Maximum size of a consolidated WriteData request during a flush.
    ui32 MaxWriteRequestSize = 1024 * 1024;

    // Maximum number of WriteData requests executed in one flush batch.
    ui32 MaxWriteRequestsCount = 64;

    // Maximum total size of WriteData requests in one flush batch.
    ui32 MaxSumWriteRequestsSize = 32 * 1024 * 1024;

    // Use deterministic test timer and scheduler implementations.
    // Note: AutomaticFlushPeriod is ignored when this option is enabled.
    bool UseTestTimerAndScheduler = true;

    // Generate flush requests using iovecs instead of copying their data.
    bool ZeroCopyWriteEnabled = false;

    // Skip validation of flushed WriteData request contents in TBootstrap.
    bool DoNotCheckWriteDataRequestBuffer = false;

    // Allow WriteData requests in the same flush batch to run in parallel.
    bool FlushWritesInParallelEnabled = true;

    // Capacity of the file-backed persistent cache storage.
    ui64 CacheCapacityBytes = 1024 * 1024 + 1024;

    // Select the concurrent implementation in CreateTestEnvironment.
    bool UseConcurrentTestEnvironment = false;

    // Number of threads submitting requests to TWriteBackCache.
    size_t SubmitThreadCount = 4;

    // Number of threads executing underlying file-store requests.
    size_t ExecutorThreadCount = 4;
};

////////////////////////////////////////////////////////////////////////////////

struct ITestEnvironment
{
    virtual ~ITestEnvironment() = default;

    virtual void RecreateCache() = 0;

    virtual NThreading::TFuture<NProto::TWriteDataResponse> WriteData(
        std::shared_ptr<NProto::TWriteDataRequest> request) = 0;

    virtual NThreading::TFuture<NProto::TReadDataResponse> ReadData(
        std::shared_ptr<NProto::TReadDataRequest> request) = 0;

    virtual NThreading::TFuture<NProto::TError> Flush(ui64 nodeId) = 0;
};

using ITestEnvironmentPtr = std::unique_ptr<ITestEnvironment>;

////////////////////////////////////////////////////////////////////////////////

ITestEnvironmentPtr CreateTestEnvironment(
    const TTestEnvironmentConfig& config);

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
