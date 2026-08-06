#pragma once

#include <util/system/types.h>
#include <util/system/yassert.h>

#include <atomic>
#include <type_traits>

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

template <typename T, ui32 BucketShift = 7>
    requires std::is_same_v<T, ui64> || std::is_same_v<T, std::atomic<ui64>>
class THistogram
{
    static constexpr ui32 BucketLsb = 64 - BucketShift;
    static constexpr ui32 NumBucketsPerRange = 1ULL << BucketShift;
    static constexpr ui32 BucketMask = NumBucketsPerRange - 1;
    static constexpr ui32 NumBucketRanges = BucketLsb + 1;
    static constexpr ui32 NumBuckets = NumBucketsPerRange * NumBucketRanges;

    T Buckets[NumBuckets] = {};

public:
    THistogram() = default;

    template <typename U>
    explicit THistogram(const THistogram<U, BucketShift>& rhs) noexcept
    {
        for (ui64 i = 0; i != NumBuckets; ++i) {
            Buckets[i] = rhs.GetBucketValue(i);
        }
    }

    template <typename U>
    THistogram& operator=(const THistogram<U, BucketShift>& rhs) noexcept
    {
        for (ui64 i = 0; i != NumBuckets; ++i) {
            Buckets[i] = rhs.GetBucketValue(i);
        }

        return *this;
    }

    void Increment(ui64 value, ui64 count = 1) noexcept
    {
        const ui32 range = GetBucketRange(value);
        const ui32 index = GetBucketIndex(value, range);

        Buckets[(range << BucketShift) + index] += count;
    }

    template <typename U>
    THistogram& operator+=(const THistogram<U, BucketShift>& rhs) noexcept
    {
        for (ui64 i = 0; i != THistogram::NumBuckets; ++i) {
            Buckets[i] += rhs.GetBucketValue(i);
        }

        return *this;
    }

    template <typename U>
    THistogram& operator-=(const THistogram<U, BucketShift>& rhs) noexcept
    {
        for (ui64 i = 0; i != THistogram::NumBuckets; ++i) {
            if (GetBucketValue(i)) {
                Buckets[i] -= rhs.GetBucketValue(i);
            }
        }

        return *this;
    }

    [[nodiscard]] ui64 GetBucketValue(size_t index) const noexcept
    {
        if constexpr (std::is_same_v<T, std::atomic<ui64>>) {
            return Buckets[index].load();
        } else {
            return Buckets[index];
        }
    }

    template <typename F>
        requires std::is_invocable_v<F, ui64, ui64, ui64>
    void IterateBuckets(F&& fn) const noexcept
    {
        ui64 bucket = 0;
        for (ui64 i = 0; i < THistogram::NumBucketRanges; ++i) {
            for (ui64 j = 0; j < THistogram::NumBucketsPerRange; ++j) {
                const ui64 count = GetCount(i, j);
                const ui64 lastBucket = bucket;

                bucket = GetBucketStart(i, j);

                if (count) {
                    fn(lastBucket, bucket, count);
                }
            }
        }
    }

    template <typename F>
        requires std::is_invocable_v<F, ui64, ui64, ui64>
    void IterateDiffBuckets(const THistogram& rhs, F&& fn) const noexcept
    {
        ui64 bucket = 0;
        for (ui64 i = 0; i < THistogram::NumBucketRanges; ++i) {
            for (ui64 j = 0; j < THistogram::NumBucketsPerRange; ++j) {
                const ui64 lcount = GetCount(i, j);
                const ui64 rcount = rhs.GetCount(i, j);

                const ui64 lastBucket = bucket;

                bucket = THistogram::GetBucketStart(i, j);

                const ui64 count = lcount >= rcount ? (lcount - rcount) : 0;

                if (count) {
                    fn(lastBucket, bucket, count);
                }
            }
        }
    }

private:
    [[nodiscard]] ui64 GetCount(ui32 range, ui32 index) const noexcept
    {
        return GetBucketValue((range << BucketShift) + index);
    }

    [[nodiscard]] static ui32 GetBucketRange(ui64 value) noexcept
    {
        Y_DEBUG_ABORT_UNLESS(value);

        const ui32 clz = __builtin_clzll(value);

        return (clz <= BucketLsb)
            ? (BucketLsb - clz)
            : 0;
    }

    [[nodiscard]] static ui32 GetBucketIndex(ui64 value, ui32 range) noexcept
    {
        const ui32 shift = range
            ? range - 1
            : 0;

        return (value >> shift) & BucketMask;
    }

    [[nodiscard]] static ui64 GetBucketStart(ui32 range, ui64 index) noexcept
    {
        index += 1;

        if (!range) {
            return index;
        }

        return (1ULL << (range + BucketShift - 1)) + (index << (range - 1));
    }
};

}   // namespace NCloud::NBlockStore::NVHostServer
