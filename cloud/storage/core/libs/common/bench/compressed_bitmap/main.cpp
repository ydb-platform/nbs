#include <cloud/storage/core/libs/common/compressed_bitmap.h>

#include <library/cpp/testing/benchmark/bench.h>

#include <util/generic/singleton.h>
#include <util/generic/string.h>
#include <util/random/fast.h>

////////////////////////////////////////////////////////////////////////////////

using NCloud::TCompressedBitmap;

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 RANGE_COUNT = 100000;

////////////////////////////////////////////////////////////////////////////////

struct TRanges
{
    struct TRange
    {
        ui16 Begin;
        ui16 End;
    };

    struct TRangePair
    {
        TRange SetRange;
        TRange UnsetRange;
    };

    TRangePair RangePairs[RANGE_COUNT];
    TReallyFastRng32 Rand;

    TRange Range()
    {
        TRange r;
        r.Begin = Rand.Uniform(1024);
        if (r.Begin == 1023) {
            r.End = 1024;
        } else {
            r.End = r.Begin + 1 + Rand.Uniform(1024 - r.Begin - 1);
        }
        return r;
    }

    TRanges()
        : Rand(777)
    {
        for (ui32 i = 0; i < RANGE_COUNT; ++i) {
            RangePairs[i] = {Range(), Range()};
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

Y_CPU_BENCHMARK(SetUnset, iface)
{
    TCompressedBitmap bitmap(1);
    auto& ranges = *HugeSingleton<TRanges>();
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        const auto& rp = ranges.RangePairs[i % RANGE_COUNT];
        bitmap.Set(rp.SetRange.Begin, rp.SetRange.End);
        bitmap.Unset(rp.UnsetRange.Begin, rp.UnsetRange.End);
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TSerializedBitmap
{
    const ui64 Size = 2ll * 1024 * 1024 * 1024;
    const int RangeSize = 64;
    const ui32 DensityPercent = 50;

    TReallyFastRng32 Rand = 777;
    TVector<TString> Data;
    TVector<TCompressedBitmap::TSerializedChunk> Chunks;

    TSerializedBitmap()
    {
        TCompressedBitmap bitmap(Size);
        for (ui64 i = 0; i < Size; i += RangeSize) {
            if (Rand.Uniform(100) <= DensityPercent) {
                bitmap.Set(i, i + RangeSize);
            }
        }

        auto serializer = bitmap.RangeSerializer(0, Size);
        TCompressedBitmap::TSerializedChunk chunk;
        while (serializer.Next(&chunk)) {
            Data.push_back(TString(chunk.Data));
            Chunks.push_back({chunk.ChunkIdx, Data.back()});
        }
    }

    void Load(TCompressedBitmap* bitmap)
    {
        for (const auto& chunk: Chunks) {
            bitmap->Update(chunk);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

Y_CPU_BENCHMARK(Load, iface)
{
    auto& serialized = *HugeSingleton<TSerializedBitmap>();
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        TCompressedBitmap m(serialized.Size);
        serialized.Load(&m);
    }
}
