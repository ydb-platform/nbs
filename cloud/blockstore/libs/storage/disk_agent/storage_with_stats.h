#pragma once

#include <cloud/blockstore/libs/service/public.h>

#include <util/datetime/base.h>
#include <util/generic/ylimits.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/types.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TStorageIoStats
{
    static constexpr TDuration Limits[] = {
        TDuration::MicroSeconds(1),
        TDuration::MicroSeconds(2),
        TDuration::MicroSeconds(5),
        TDuration::MicroSeconds(10),
        TDuration::MicroSeconds(20),
        TDuration::MicroSeconds(50),
        TDuration::MicroSeconds(100),
        TDuration::MicroSeconds(200),
        TDuration::MicroSeconds(500),
        TDuration::MicroSeconds(1000),
        TDuration::MicroSeconds(2000),
        TDuration::MicroSeconds(5000),
        TDuration::MicroSeconds(10000),
        TDuration::MicroSeconds(35000),
        TDuration::Max()
    };
    static constexpr size_t BUCKETS_COUNT = std::size(Limits);

    TAtomic Buckets[BUCKETS_COUNT] = {};

    TAtomic BytesRead = 0;
    TAtomic NumReadOps = 0;
    TAtomic BytesWritten = 0;
    TAtomic NumWriteOps = 0;
    TAtomic NumZeroOps = 0;
    TAtomic BytesZeroed = 0;
    TAtomic NumEraseOps = 0;
    TAtomic Errors  = 0;

    void OnReadStart();
    void OnReadComplete(TDuration duration, ui64 requestBytes);

    void OnWriteStart();
    void OnWriteComplete(TDuration duration, ui64 requestBytes);

    void OnZeroStart();
    void OnZeroComplete(TDuration duration, ui64 requestBytes);

    void OnEraseStart();
    void OnEraseComplete(TDuration duration);

    void OnError();

private:
    void IncrementBucket(TDuration duration);
};

using TStorageIoStatsPtr = std::shared_ptr<TStorageIoStats>;

IStoragePtr CreateStorageWithIoStats(
    IStoragePtr storage,
    TStorageIoStatsPtr stats,
    ui32 blockSize);

}   // namespace NCloud::NBlockStore::NStorage
