#include "iovector.h"

#include <library/cpp/testing/gbenchmark/benchmark.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

enum class ETestCase
{
    OnlyCopy,
    TrimOnCopy,
    TrimAfterCopy,
};

enum class ENonZeroPosition
{
    First,
    Last,
    Distributed,
};

static void DoBenchmarkTrimVoidBuffers(
    benchmark::State& state,
    ui32 blockSize,
    ui32 blockCount,
    ui32 voidNth,
    ETestCase testCase,
    ENonZeroPosition nonZeroPosition)
{
    const size_t DataSize = blockSize * blockCount;

    TString srcBuffer(DataSize, 0);
    ui32 nonVoidBlockCount = 0;
    ui32 voidBlockCount = 0;
    for (size_t i = 0; i < blockCount; ++i) {
        if (i % voidNth == 0) {
            ++voidBlockCount;
            continue;
        }
        TBlockDataRef nonVoidBlock{srcBuffer.data() + i * blockSize, blockSize};
        size_t offsetInBlock = 0;
        switch (nonZeroPosition) {
            case ENonZeroPosition::First:
                offsetInBlock = 0;
                break;
            case ENonZeroPosition::Last:
                offsetInBlock = blockSize - 1;
                break;
            case ENonZeroPosition::Distributed:
                offsetInBlock = (i * 128) % blockSize;
                break;
        }
        char* dataInBlock =
            const_cast<char*>(nonVoidBlock.Data() + offsetInBlock);
        *dataInBlock = 'Z';
        ++nonVoidBlockCount;
    }

    if (testCase == ETestCase::OnlyCopy) {
        nonVoidBlockCount = blockCount;
        voidBlockCount = 0;
    }

    for (const auto _: state) {
        NProto::TIOVector ioVector;
        switch (testCase) {
            case ETestCase::OnlyCopy: {
                TSgList sgList =
                    ResizeIOVector(ioVector, blockCount, blockSize);
                size_t bytes = SgListCopy(
                    TBlockDataRef{srcBuffer.data(), srcBuffer.size()},
                    sgList);
                Y_ABORT_UNLESS(bytes == DataSize);
            } break;
            case ETestCase::TrimOnCopy: {
                size_t bytes = CopyAndTrimVoidBuffers(
                    TBlockDataRef{srcBuffer.data(), srcBuffer.size()},
                    blockCount,
                    blockSize,
                    &ioVector);
                Y_ABORT_UNLESS(bytes == DataSize);
            } break;
            case ETestCase::TrimAfterCopy: {
                TSgList sgList =
                    ResizeIOVector(ioVector, blockCount, blockSize);
                size_t bytes = SgListCopy(
                    TBlockDataRef{srcBuffer.data(), srcBuffer.size()},
                    sgList);
                Y_ABORT_UNLESS(bytes == DataSize);
                TrimVoidBuffers(&ioVector);
            } break;
        };

        TVoidBuffersStat stat = CountVoidBuffers(ioVector);
        Y_ABORT_UNLESS(stat.NonVoidBlockCount == nonVoidBlockCount);
        Y_ABORT_UNLESS(stat.VoidBlockCount == voidBlockCount);
    }
}

#define DECLARE_BENCH(blockSize, blockCount, voidNth, test)                                 \
    void                                                                                    \
        BenchmarkVoidBuffers_##blockSize##_##blockCount##_##voidNth##_##test##_First(       \
            benchmark::State& state)                                                        \
    {                                                                                       \
        DoBenchmarkTrimVoidBuffers(                                                         \
            state,                                                                          \
            blockSize,                                                                      \
            blockCount,                                                                     \
            voidNth,                                                                        \
            ETestCase::test,                                                                \
            ENonZeroPosition::First);                                                       \
    }                                                                                       \
    void                                                                                    \
        BenchmarkVoidBuffers_##blockSize##_##blockCount##_##voidNth##_##test##_Last(        \
            benchmark::State& state)                                                        \
    {                                                                                       \
        DoBenchmarkTrimVoidBuffers(                                                         \
            state,                                                                          \
            blockSize,                                                                      \
            blockCount,                                                                     \
            voidNth,                                                                        \
            ETestCase::test,                                                                \
            ENonZeroPosition::First);                                                       \
    }                                                                                       \
    void                                                                                    \
        BenchmarkVoidBuffers_##blockSize##_##blockCount##_##voidNth##_##test##_Distributed( \
            benchmark::State& state)                                                        \
    {                                                                                       \
        DoBenchmarkTrimVoidBuffers(                                                         \
            state,                                                                          \
            blockSize,                                                                      \
            blockCount,                                                                     \
            voidNth,                                                                        \
            ETestCase::test,                                                                \
            ENonZeroPosition::First);                                                       \
    }                                                                                       \
    BENCHMARK(                                                                              \
        BenchmarkVoidBuffers_##blockSize##_##blockCount##_##voidNth##_##test##_First);      \
    BENCHMARK(                                                                              \
        BenchmarkVoidBuffers_##blockSize##_##blockCount##_##voidNth##_##test##_Last);       \
    BENCHMARK(                                                                              \
        BenchmarkVoidBuffers_##blockSize##_##blockCount##_##voidNth##_##test##_Distributed);

DECLARE_BENCH(4096, 1024, 1, OnlyCopy);
DECLARE_BENCH(4096, 1024, 1, TrimOnCopy);
DECLARE_BENCH(4096, 1024, 1, TrimAfterCopy);

DECLARE_BENCH(4096, 1024, 2, OnlyCopy);
DECLARE_BENCH(4096, 1024, 2, TrimOnCopy);
DECLARE_BENCH(4096, 1024, 2, TrimAfterCopy);

DECLARE_BENCH(4096, 1024, 10, OnlyCopy);
DECLARE_BENCH(4096, 1024, 10, TrimOnCopy);
DECLARE_BENCH(4096, 1024, 10, TrimAfterCopy);

DECLARE_BENCH(4096, 1024, 50, OnlyCopy);
DECLARE_BENCH(4096, 1024, 50, TrimOnCopy);
DECLARE_BENCH(4096, 1024, 50, TrimAfterCopy);

}   // namespace NCloud::NBlockStore::NStorage
