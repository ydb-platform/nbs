#include <cloud/blockstore/libs/rdma_test/rdma_test_environment.h>

#include <cloud/storage/core/libs/common/ring_buffer.h>

#include <library/cpp/testing/gbenchmark/benchmark.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

namespace {

enum class ERequestType
{
    Read,
    Write,
    Zero,
};

}   // namespace

void DoBenchmarkRdmaTarget(
    ui32 poolSize,
    size_t concurrency,
    size_t requestSize,
    size_t incCount,
    size_t decCount,
    ERequestType requestType,
    benchmark::State& state)
{
    Y_UNUSED(state);
    const size_t N = concurrency;
    TRdmaTestEnvironment env(requestSize * N, poolSize);

    using TWriteRequestFuture =
        NThreading::TFuture<NProto::TWriteDeviceBlocksResponse>;
    using TZeroRequestFuture =
        NThreading::TFuture<NProto::TZeroDeviceBlocksResponse>;
    using TReadRequestFuture =
        NThreading::TFuture<NProto::TReadDeviceBlocksResponse>;
    TRingBuffer<TWriteRequestFuture> writeRequests(concurrency);
    TRingBuffer<TZeroRequestFuture> zeroRequests(concurrency);
    TRingBuffer<TReadRequestFuture> readRequests(concurrency);

    auto finishWriteRequest = [](TWriteRequestFuture requestFuture)
    {
        const auto& response = requestFuture.GetValueSync();
        bool ok = response.GetError().GetCode() == S_OK ||
                  response.GetError().GetCode() == E_REJECTED;
        UNIT_ASSERT(ok);
    };

    auto finishZeroRequest = [](TZeroRequestFuture requestFuture)
    {
        const auto& response = requestFuture.GetValueSync();
        bool ok = response.GetError().GetCode() == S_OK ||
                  response.GetError().GetCode() == E_REJECTED;
        UNIT_ASSERT(ok);
    };

    auto finishReadRequest = [](TReadRequestFuture requestFuture)
    {
        const auto& response = requestFuture.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            response.GetError().GetMessage());
    };

    size_t count = 0;
    ui64 volumeRequestId = 0;
    size_t blockIndex = 0;
    for (const auto _: state) {
        if (incCount + decCount != 0) {
            size_t step = count % (incCount + decCount);
            if (step < incCount) {
                volumeRequestId += 100;
            } else {
                volumeRequestId--;
            }
            if (step == 0) {
                blockIndex++;
            }
        } else {
            blockIndex++;
        }

        if (requestType == ERequestType::Write) {
            const auto blockRange =
                TBlockRange64::WithLength(blockIndex % N, requestSize / 4_KB);
            auto writeFuture =
                env.Run(env.MakeWriteRequest(blockRange, 'A', volumeRequestId));
            auto oldWriteRequest =
                writeRequests.PushBack(std::move(writeFuture));
            if (oldWriteRequest) {
                finishWriteRequest(*oldWriteRequest);
            }
        } else if (requestType == ERequestType::Zero) {
            const auto blockRange =
                TBlockRange64::WithLength(blockIndex % N, requestSize / 4_KB);
            auto zeroFuture = env.Run(env.MakeZeroRequest(blockRange));
            auto oldZeroRequest =
                zeroRequests.PushBack(std::move(zeroFuture));
            if (oldZeroRequest) {
                finishZeroRequest(*oldZeroRequest);
            }
        } else if (requestType == ERequestType::Read) {
            const auto blockRange = TBlockRange64::WithLength(
                (blockIndex + 1) % N,
                requestSize / 4_KB);
            auto readFuture = env.Run(env.MakeReadRequest(blockRange));
            auto oldReadRequest =
                readRequests.PushBack(std::move(readFuture));
            if (oldReadRequest) {
                finishReadRequest(*oldReadRequest);
            }
        }
        ++count;
    }

    while (!writeRequests.IsEmpty()) {
        finishWriteRequest(*writeRequests.PopFront());
    }

    while (!zeroRequests.IsEmpty()) {
        finishZeroRequest(*zeroRequests.PopFront());
    }

    while (!readRequests.IsEmpty()) {
        finishReadRequest(*readRequests.PopFront());
    }
}

#define DECLARE_BENCH(poolSize, concurrency, requestSize, inc, dec, type)                  \
    void                                                                                   \
        BenchmarkRdma_##poolSize##_##concurrency##_##requestSize##_##inc##_##dec##_##type( \
            benchmark::State& state)                                                       \
    {                                                                                      \
        DoBenchmarkRdmaTarget(                                                             \
            poolSize,                                                                      \
            concurrency,                                                                   \
            requestSize,                                                                   \
            inc,                                                                           \
            dec,                                                                           \
            ERequestType::type,                                                            \
            state);                                                                        \
    }                                                                                      \
    BENCHMARK(                                                                             \
        BenchmarkRdma_##poolSize##_##concurrency##_##requestSize##_##inc##_##dec##_##type);

// Small block.
DECLARE_BENCH(1, 1, 4_KB, 0, 0, Read);
DECLARE_BENCH(1, 1, 4_KB, 0, 0, Write);
DECLARE_BENCH(1, 1, 4_KB, 0, 0, Zero);
DECLARE_BENCH(1, 32, 4_KB, 0, 0, Read);
DECLARE_BENCH(1, 32, 4_KB, 0, 0, Write);
DECLARE_BENCH(1, 32, 4_KB, 0, 0, Zero);
DECLARE_BENCH(4, 32, 4_KB, 0, 0, Read);
DECLARE_BENCH(4, 32, 4_KB, 0, 0, Write);
DECLARE_BENCH(4, 32, 4_KB, 0, 0, Zero);

// Large block.
DECLARE_BENCH(1, 1, 4_MB, 0, 0, Read);
DECLARE_BENCH(1, 1, 4_MB, 0, 0, Write);
DECLARE_BENCH(1, 1, 4_MB, 0, 0, Zero);
DECLARE_BENCH(1, 32, 4_MB, 0, 0, Read);
DECLARE_BENCH(1, 32, 4_MB, 0, 0, Write);
DECLARE_BENCH(1, 32, 4_MB, 0, 0, Zero);
DECLARE_BENCH(4, 32, 4_MB, 0, 0, Read);
DECLARE_BENCH(4, 32, 4_MB, 0, 0, Write);
DECLARE_BENCH(4, 32, 4_MB, 0, 0, Zero);

// Small block with increased VolumeRequestId.
DECLARE_BENCH(1, 1, 4_KB, 1, 0, Read);
DECLARE_BENCH(1, 1, 4_KB, 1, 0, Write);
DECLARE_BENCH(1, 1, 4_KB, 1, 0, Zero);
DECLARE_BENCH(1, 32, 4_KB, 1, 0, Read);
DECLARE_BENCH(1, 32, 4_KB, 1, 0, Write);
DECLARE_BENCH(1, 32, 4_KB, 1, 0, Zero);
DECLARE_BENCH(4, 32, 4_KB, 1, 0, Read);
DECLARE_BENCH(4, 32, 4_KB, 1, 0, Write);
DECLARE_BENCH(4, 32, 4_KB, 1, 0, Zero);

// Small block with VolumeRequestId from past.
DECLARE_BENCH(1, 1, 4_KB, 10, 5, Read);
DECLARE_BENCH(1, 1, 4_KB, 10, 5, Write);
DECLARE_BENCH(1, 1, 4_KB, 10, 5, Zero);
DECLARE_BENCH(1, 32, 4_KB, 10, 5, Read);
DECLARE_BENCH(1, 32, 4_KB, 10, 5, Write);
DECLARE_BENCH(1, 32, 4_KB, 10, 5, Zero);
DECLARE_BENCH(4, 32, 4_KB, 10, 5, Read);
DECLARE_BENCH(4, 32, 4_KB, 10, 5, Write);
DECLARE_BENCH(4, 32, 4_KB, 10, 5, Zero);

#undef DECLARE_BENCH

}   // namespace NCloud::NBlockStore::NStorage
