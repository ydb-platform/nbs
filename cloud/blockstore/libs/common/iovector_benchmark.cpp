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

static void DoReal(
    benchmark::State& state,
    ui32 blockSize,
    ui32 blockCount,
    ui32 voidNth,
    ETestCase testCase,
    ENonZeroPosition nonZeroPosition)
{
    const size_t DataSize = blockSize * blockCount;

    TString srcBuffer(DataSize, 0);
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
    }

    if (testCase == ETestCase::OnlyCopy) {
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
                TrimVoidBuffers(ioVector);
            } break;
        };

        Y_ABORT_UNLESS(CountVoidBuffers(ioVector) == voidBlockCount);
    }
}

static void DoSynthetic(
    benchmark::State& state,
    ui32 blockSize,
    ui32 blockCount,
    ui32 voidNth,
    ENonZeroPosition nonZeroPosition)
{
    const size_t DataSize = blockSize * blockCount;

    TString srcBuffer(DataSize, 0);
    TSgList sgList;
    sgList.reserve(blockCount);
    ui32 voidBlockCount = 0;
    for (size_t i = 0; i < blockCount; ++i) {
        TBlockDataRef block{srcBuffer.data() + i * blockSize, blockSize};
        sgList.push_back(block);

        if (i % voidNth == 0) {
            ++voidBlockCount;
            continue;
        }
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
        char* dataInBlock = const_cast<char*>(block.Data() + offsetInBlock);
        *dataInBlock = 'Z';
    }

    for (const auto _: state) {
        ui32 voidBuffersCount = 0;
        for (const auto& buff: sgList) {
            if (IsAllZeroes(buff.Data(), buff.Size())) {
                ++voidBuffersCount;
            }
        }

        Y_ABORT_UNLESS(voidBlockCount == voidBuffersCount);
    }
}

#define DECLARE_SYNTHETIC(blockSize, blockCount, voidNth)                \
    void Synthetic_##blockSize##_##blockCount##_##voidNth##_First(       \
        benchmark::State& state)                                         \
    {                                                                    \
        DoSynthetic(                                                     \
            state,                                                       \
            blockSize,                                                   \
            blockCount,                                                  \
            voidNth,                                                     \
            ENonZeroPosition::First);                                    \
    }                                                                    \
    void Synthetic_##blockSize##_##blockCount##_##voidNth##_Last(        \
        benchmark::State& state)                                         \
    {                                                                    \
        DoSynthetic(                                                     \
            state,                                                       \
            blockSize,                                                   \
            blockCount,                                                  \
            voidNth,                                                     \
            ENonZeroPosition::Last);                                     \
    }                                                                    \
    void Synthetic_##blockSize##_##blockCount##_##voidNth##_Distributed( \
        benchmark::State& state)                                         \
    {                                                                    \
        DoSynthetic(                                                     \
            state,                                                       \
            blockSize,                                                   \
            blockCount,                                                  \
            voidNth,                                                     \
            ENonZeroPosition::Distributed);                              \
    }                                                                    \
    BENCHMARK(Synthetic_##blockSize##_##blockCount##_##voidNth##_First); \
    BENCHMARK(Synthetic_##blockSize##_##blockCount##_##voidNth##_Last);  \
    BENCHMARK(Synthetic_##blockSize##_##blockCount##_##voidNth##_Distributed);

#define DECLARE_REAL(blockSize, blockCount, voidNth, test)                   \
    void Real_##blockSize##_##blockCount##_##voidNth##_##test##_First(       \
        benchmark::State& state)                                             \
    {                                                                        \
        DoReal(                                                              \
            state,                                                           \
            blockSize,                                                       \
            blockCount,                                                      \
            voidNth,                                                         \
            ETestCase::test,                                                 \
            ENonZeroPosition::First);                                        \
    }                                                                        \
    void Real_##blockSize##_##blockCount##_##voidNth##_##test##_Last(        \
        benchmark::State& state)                                             \
    {                                                                        \
        DoReal(                                                              \
            state,                                                           \
            blockSize,                                                       \
            blockCount,                                                      \
            voidNth,                                                         \
            ETestCase::test,                                                 \
            ENonZeroPosition::Last);                                         \
    }                                                                        \
    void Real_##blockSize##_##blockCount##_##voidNth##_##test##_Distributed( \
        benchmark::State& state)                                             \
    {                                                                        \
        DoReal(                                                              \
            state,                                                           \
            blockSize,                                                       \
            blockCount,                                                      \
            voidNth,                                                         \
            ETestCase::test,                                                 \
            ENonZeroPosition::Distributed);                                  \
    }                                                                        \
    BENCHMARK(Real_##blockSize##_##blockCount##_##voidNth##_##test##_First); \
    BENCHMARK(Real_##blockSize##_##blockCount##_##voidNth##_##test##_Last);  \
    BENCHMARK(                                                               \
        Real_##blockSize##_##blockCount##_##voidNth##_##test##_Distributed);

DECLARE_SYNTHETIC(4096, 1024, 1);
DECLARE_SYNTHETIC(4096, 1024, 2);
DECLARE_SYNTHETIC(4096, 1024, 10);
DECLARE_SYNTHETIC(4096, 1024, 50);
DECLARE_SYNTHETIC(4096, 1024, 5000);

DECLARE_REAL(4096, 1024, 1, OnlyCopy);

DECLARE_REAL(4096, 1024, 1, TrimOnCopy);
DECLARE_REAL(4096, 1024, 1, TrimAfterCopy);

DECLARE_REAL(4096, 1024, 2, TrimOnCopy);
DECLARE_REAL(4096, 1024, 2, TrimAfterCopy);

DECLARE_REAL(4096, 1024, 10, TrimOnCopy);
DECLARE_REAL(4096, 1024, 10, TrimAfterCopy);

DECLARE_REAL(4096, 1024, 50, TrimOnCopy);
DECLARE_REAL(4096, 1024, 50, TrimAfterCopy);

}   // namespace NCloud::NBlockStore::NStorage
