#include "backend.h"

#include <cloud/blockstore/libs/service/service_test.h>
#include <cloud/contrib/vhost/include/vhost/server.h>
#include <cloud/storage/core/libs/rdma/iface/client.h>

#include <library/cpp/testing/unittest/registar.h>

#include <tuple>

namespace NCloud::NBlockStore::NVHostServer {
namespace {

// Substitute the clock and libvhost queue boundary; asynchronous completion,
// classification and snapshots run the RDMA backend.
ui64 RdmaNowCycles = 0;
vhd_bdev_io* PendingBio = nullptr;
ui64 CompletedBios = 0;
vhd_bdev_io_result LastBioResult = VHD_BDEV_SUCCESS;

ui64 GetCycleCount()
{
    return RdmaNowCycles;
}

const auto vhd_dequeue_request = [](vhd_request_queue*, vhd_request* request) {
    if (!PendingBio) {
        return false;
    }
    request->io = reinterpret_cast<vhd_io*>(PendingBio);
    PendingBio = nullptr;
    return true;
};

const auto vhd_get_bdev_io = [](vhd_io* io) {
    return reinterpret_cast<vhd_bdev_io*>(io);
};

const auto vhd_complete_bio = [](vhd_io*, vhd_bdev_io_result result) {
    ++CompletedBios;
    LastBioResult = result;
};

}   // namespace
}   // namespace NCloud::NBlockStore::NVHostServer

#include "backend_rdma.cpp"

namespace NCloud::NBlockStore::NVHostServer {
namespace {

struct TUnusedRdmaClient final: NCloud::NStorage::NRdma::IClient
{
    using TEndpoint = NCloud::NStorage::NRdma::IClientEndpointPtr;
    using THandler = NCloud::IClientEndpointHandlerPtr;

    void Start() override {}
    void Stop() override {}
    void DumpHtml(IOutputStream&) const override {}
    bool IsAlignedDataEnabled() const override { return false; }
    NThreading::TFuture<TEndpoint> StartEndpoint(TString, ui32) override
    {
        return NThreading::MakeFuture(TEndpoint{});
    }
    TResultOrError<TEndpoint> StartEndpoint(TString, ui32, THandler) override
    {
        return MakeError(E_NOT_IMPLEMENTED);
    }
};

void CheckRdmaLatency(bool enabled)
{
    auto dataClient = std::make_shared<TTestService>();
    TRdmaBackend backend(
        CreateLoggingService("console"),
        +[](ILoggingServicePtr,
            IMonitoringServicePtr,
            NCloud::NStorage::NRdma::TClientConfigPtr) -> IClientPtr
        {
            return std::make_shared<TUnusedRdmaClient>();
        },
        dataClient);
    TOptions options;
    options.BlockSize = 4096;
    options.LatencySli = std::make_shared<const TLatencySliConfig>(
        TLatencySliConfig::TryParse(
            enabled ? "1;0:1:8193:1000;1:1:8193:2000" : "").value());
    backend.Init(options);
    TSimpleStats queueStats;
    CompletedBios = 0;

    for (bool write: {false, true}) {
        ui64 good = 0;
        ui64 bad = 0;
        ui64 unknown = 0;
        ui64 bytes = 0;
        ui64 errors = 0;
        ui64 count = 0;
        using E = ELatencySliResult;
        // Bytes, elapsed microseconds, response code and expected outcome.
        for (const auto& scenario:
             {std::tuple<ui64, ui64, ui32, E>{4096, 500, S_OK, E::Unknown},
              {4096, 1500, S_OK, E::Unknown},
              {4096, 500, E_TIMEOUT, E::Unknown},
              {4096, 500, E_IO, E::Unknown},
              {16384, 500, E_IO, E::Unknown}})
        {
            const auto [size, elapsedUs, code, expected] = scenario;
            const bool failed = code != S_OK;
            auto readPromise = NThreading::NewPromise<NProto::TReadBlocksLocalResponse>();
            auto writePromise = NThreading::NewPromise<NProto::TWriteBlocksLocalResponse>();
            dataClient->ReadBlocksLocalHandler = [&](const auto&) {
                return readPromise.GetFuture();
            };
            dataClient->WriteBlocksLocalHandler = [&](const auto&) {
                return writePromise.GetFuture();
            };
            vhd_bdev_io bio{};
            bio.type = write ? VHD_BDEV_WRITE : VHD_BDEV_READ;
            bio.total_sectors = size / VHD_SECTOR_SIZE;
            PendingBio = &bio;
            RdmaNowCycles = DurationToCyclesSafe(TDuration::Seconds(100));
            const auto before = CompletedBios;
            backend.ProcessQueue(0, nullptr, queueStats);
            UNIT_ASSERT_VALUES_EQUAL(before, CompletedBios);
            const auto pending = backend.GetCompletionStats(TDuration::Zero());
            UNIT_ASSERT(pending);
            UNIT_ASSERT_VALUES_EQUAL(before, pending->Completed);

            RdmaNowCycles += DurationToCyclesSafe(TDuration::MicroSeconds(elapsedUs));
            if (write) {
                NProto::TWriteBlocksLocalResponse response;
                if (failed) {
                    *response.MutableError() = MakeError(code);
                }
                writePromise.SetValue(response);
            } else {
                NProto::TReadBlocksLocalResponse response;
                if (failed) {
                    *response.MutableError() = MakeError(code);
                }
                readPromise.SetValue(response);
            }
            good += expected == E::Good;
            bad += expected == E::Bad;
            unknown += expected == E::Unknown;
            if (failed) {
                ++errors;
            } else {
                ++count;
                bytes += size;
            }
            UNIT_ASSERT_VALUES_EQUAL(before + 1, CompletedBios);
            UNIT_ASSERT_VALUES_EQUAL(
                int(failed ? VHD_BDEV_IOERR : VHD_BDEV_SUCCESS),
                int(LastBioResult));
            const auto snapshot = backend.GetCompletionStats(TDuration::Zero());
            UNIT_ASSERT(snapshot);
            UNIT_ASSERT_VALUES_EQUAL(CompletedBios, snapshot->Completed);
            const auto& stats = snapshot->Requests[write];
            UNIT_ASSERT_VALUES_EQUAL(enabled ? good : 0, stats.LatencyGood);
            UNIT_ASSERT_VALUES_EQUAL(enabled ? bad : 0, stats.LatencyBad);
            UNIT_ASSERT_VALUES_EQUAL(enabled ? unknown : 0, stats.LatencyUnknown);
            UNIT_ASSERT_VALUES_EQUAL(count, stats.FinalCount);
            UNIT_ASSERT_VALUES_EQUAL(errors, stats.FinalErrors);
            UNIT_ASSERT_VALUES_EQUAL(count, stats.Count);
            UNIT_ASSERT_VALUES_EQUAL(bytes, stats.Bytes);
            UNIT_ASSERT_VALUES_EQUAL(errors, stats.Errors);
            // No further completion is needed to refresh an idle snapshot.
            const auto idle = backend.GetCompletionStats(TDuration::Zero());
            UNIT_ASSERT(idle);
            UNIT_ASSERT_VALUES_EQUAL(count, idle->Requests[write].FinalCount);
            UNIT_ASSERT_VALUES_EQUAL(errors, idle->Requests[write].FinalErrors);
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(10, queueStats.Dequeued);
    UNIT_ASSERT_VALUES_EQUAL(10, queueStats.Submitted);
}

}   // namespace

Y_UNIT_TEST_SUITE(TRdmaLatencySliTest)
{
    Y_UNIT_TEST(ShouldClassifyDeferredRdmaCompletions)
    {
        CheckRdmaLatency(true);
    }

    Y_UNIT_TEST(ShouldLeaveRdmaLatencyDisabled)
    {
        CheckRdmaLatency(false);
    }
}

}   // namespace NCloud::NBlockStore::NVHostServer
