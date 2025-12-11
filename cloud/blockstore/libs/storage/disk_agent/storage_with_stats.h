#pragma once

#include <cloud/blockstore/libs/service/public.h>

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/datetime/base.h>
#include <util/generic/ylimits.h>
#include <util/system/types.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TStorageIoStats
{
    static constexpr TDuration Limits[] = {
        TDuration::MicroSeconds(1),   TDuration::MicroSeconds(2),
        TDuration::MicroSeconds(3),   TDuration::MicroSeconds(4),
        TDuration::MicroSeconds(5),   TDuration::MicroSeconds(10),
        TDuration::MicroSeconds(15),  TDuration::MicroSeconds(20),
        TDuration::MicroSeconds(25),  TDuration::MicroSeconds(30),
        TDuration::MicroSeconds(35),  TDuration::MicroSeconds(40),
        TDuration::MicroSeconds(50),  TDuration::MicroSeconds(100),
        TDuration::MicroSeconds(200), TDuration::MicroSeconds(500),
        TDuration::MilliSeconds(1),   TDuration::MilliSeconds(2),
        TDuration::MilliSeconds(5),   TDuration::MilliSeconds(10),
        TDuration::MilliSeconds(35),  TDuration::MilliSeconds(100),
        TDuration::Seconds(1),        TDuration::Seconds(10),
        TDuration::Minutes(1)};
    static constexpr size_t BUCKETS_COUNT = std::size(Limits);

    TAtomic Buckets[BUCKETS_COUNT] = {};

    TAtomic BytesRead = 0;
    TAtomic NumReadOps = 0;
    TAtomic BytesWritten = 0;
    TAtomic NumWriteOps = 0;
    TAtomic NumZeroOps = 0;
    TAtomic BytesZeroed = 0;
    TAtomic NumEraseOps = 0;
    TAtomic Errors = 0;

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
