#include "histogram.h"

#include "spdk.h"

namespace NCloud::NBlockStore::NSpdk {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TBucketsCollector
{
    const float ToMcsCoeff;
    TVector<TBucketInfo> Buckets;

    explicit TBucketsCollector(ui64 ticksRate)
        : ToMcsCoeff(ticksRate / 1'000'000.0f)
    {}

    void OnData(
        uint64_t start,
        uint64_t end,
        uint64_t count,
        uint64_t total,
        uint64_t soFar)
    {
        Y_UNUSED(total);
        Y_UNUSED(soFar);

        if (count == 0) {
            return;
        }

        const auto val = static_cast<ui32>(
            (start + (end - start) / 2) / ToMcsCoeff);

        Buckets.emplace_back(val, count);
    }

    static void Callback(
        void* ctx,
        uint64_t start,
        uint64_t end,
        uint64_t count,
        uint64_t total,
        uint64_t soFar)
    {
        static_cast<TBucketsCollector*>(ctx)->OnData(
            start, end, count, total, soFar);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

THistogramPtr CreateHistogram()
{
    auto* h = spdk_histogram_data_alloc();

    return { h, [] (auto* h) { spdk_histogram_data_free(h); }};
}

TVector<TBucketInfo> CollectBuckets(
    spdk_histogram_data& histogram,
    ui64 ticksRate)
{
    TBucketsCollector bc(ticksRate);

    spdk_histogram_data_iterate(
        &histogram,
        TBucketsCollector::Callback,
        &bc);

    return std::move(bc.Buckets);
}

}   // namespace NCloud::NBlockStore::NSpdk
