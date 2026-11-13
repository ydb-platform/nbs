#include "backend.h"

#include <cloud/blockstore/libs/service/service_test.h>
#include <cloud/contrib/vhost/include/vhost/server.h>
#include <cloud/storage/core/libs/rdma/iface/client.h>

#include <library/cpp/testing/unittest/registar.h>

#include <tuple>

namespace NCloud::NBlockStore::NVHostServer {
namespace {

// Substitute only the clock and libvhost queue boundary. Request translation,
// asynchronous completion and snapshots run the RDMA backend.
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
    options.ClientId = "client";
    options.LatencySli = std::make_shared<const TLatencySliConfig>(
        TLatencySliConfig::TryParse(
            enabled ? "1;0:1:8193:1000;1:1:8193:2000" : "").value());
    backend.Init(options);
    TSimpleStats queueStats;
    CompletedBios = 0;

    for (bool write: {false, true}) {
        ui64 bytes = 0;
        ui64 errors = 0;
        ui64 count = 0;
        // Bytes, elapsed us, failure.
        for (const auto& scenario:
             {std::tuple<ui64, ui64, bool>{4096, 500, false},
              {4096, 1500, false},
              {4096, 3000, false},
              {16384, 500, false},
              {16384, 500, true}})
        {
            const auto [size, elapsedUs, failed] = scenario;
            auto readPromise = NThreading::NewPromise<NProto::TReadBlocksLocalResponse>();
            auto writePromise = NThreading::NewPromise<NProto::TWriteBlocksLocalResponse>();
            ui64 submittedBytes = 0;
            ui64 startIndex = 0;
            TString clientId;
            dataClient->ReadBlocksLocalHandler = [&](const auto& request) {
                submittedBytes = ui64(request->GetBlocksCount()) * request->GetBlockSize();
                startIndex = request->GetStartIndex();
                clientId = request->GetHeaders().GetClientId();
                return readPromise.GetFuture();
            };
            dataClient->WriteBlocksLocalHandler = [&](const auto& request) {
                submittedBytes = ui64(request->BlocksCount) * request->GetBlockSize();
                startIndex = request->GetStartIndex();
                clientId = request->GetHeaders().GetClientId();
                return writePromise.GetFuture();
            };
            vhd_bdev_io bio{};
            bio.type = write ? VHD_BDEV_WRITE : VHD_BDEV_READ;
            bio.first_sector = 16;
            bio.total_sectors = size / VHD_SECTOR_SIZE;
            PendingBio = &bio;
            RdmaNowCycles = DurationToCyclesSafe(TDuration::Seconds(100));
            const auto before = CompletedBios;
            backend.ProcessQueue(0, nullptr, queueStats);
            UNIT_ASSERT_VALUES_EQUAL(size, submittedBytes);
            UNIT_ASSERT_VALUES_EQUAL(2, startIndex);
            UNIT_ASSERT_VALUES_EQUAL("client", clientId);
            UNIT_ASSERT_VALUES_EQUAL(before, CompletedBios);
            UNIT_ASSERT_VALUES_EQUAL(
                before, backend.GetCompletionStats(TDuration::Zero())->Completed);

            RdmaNowCycles += DurationToCyclesSafe(TDuration::MicroSeconds(elapsedUs));
            if (write) {
                NProto::TWriteBlocksLocalResponse response;
                if (failed) {
                    *response.MutableError() = MakeError(E_IO);
                }
                writePromise.SetValue(response);
            } else {
                NProto::TReadBlocksLocalResponse response;
                if (failed) {
                    *response.MutableError() = MakeError(E_IO);
                }
                readPromise.SetValue(response);
            }
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
            UNIT_ASSERT_VALUES_EQUAL(0, stats.LatencyGood);
            UNIT_ASSERT_VALUES_EQUAL(0, stats.LatencyBad);
            UNIT_ASSERT_VALUES_EQUAL(0, stats.LatencyUnknown);
            UNIT_ASSERT_VALUES_EQUAL(count, stats.Count);
            UNIT_ASSERT_VALUES_EQUAL(bytes, stats.Bytes);
            UNIT_ASSERT_VALUES_EQUAL(errors, stats.Errors);
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(10, queueStats.Dequeued);
    UNIT_ASSERT_VALUES_EQUAL(10, queueStats.Submitted);
}

}   // namespace

Y_UNIT_TEST_SUITE(TRdmaLatencySliTest)
{
    Y_UNIT_TEST(ShouldLeaveClassificationToFinalVhostCompletion)
    {
        CheckRdmaLatency(true);
    }

    Y_UNIT_TEST(ShouldLeaveRdmaLatencyDisabled)
    {
        CheckRdmaLatency(false);
    }
}

}   // namespace NCloud::NBlockStore::NVHostServer
