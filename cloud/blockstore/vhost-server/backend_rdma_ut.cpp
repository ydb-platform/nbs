#include "backend_rdma.h"
#include "server.h"

#include <cloud/blockstore/libs/service/service_test.h>
#include <cloud/contrib/vhost/virtio/virtio_blk_spec.h>
#include <cloud/storage/core/libs/vhost-client/monotonic_buffer_resource.h>
#include <cloud/storage/core/libs/vhost-client/vhost-client.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>
#include <util/string/printf.h>

#include <new>
#include <unistd.h>

namespace NCloud::NBlockStore::NVHostServer {
namespace {

void CheckRdmaLatency(bool enabled)
{
    auto service = std::make_shared<TTestService>();
    auto logging = CreateLoggingService("console");
    auto server = CreateServer(logging, CreateRdmaBackend(logging, service));
    TOptions options;
    options.SocketPath = Sprintf("rdma-latency-%d.sock", getpid());
    options.Serial = "rdma-latency";
    options.BlockSize = 4096;
    options.QueueCount = 1;
    options.NoChmod = true;
    options.Layout.push_back({
        .DevicePath = "rdma://localhost:10020/test-device",
        .ByteCount = 64_KB,
    });
    if (enabled) {
        options.LatencySli = std::make_shared<const TLatencySliConfig>(
            TLatencySliConfig::TryParse("1;0:4096:4097:1000;1:4096:4097:1000")
                .value());
    }

    server->Start(options);
    Y_DEFER { server->Stop(); };
    NVHost::TClient client(options.SocketPath, {.QueueCount = 1});
    UNIT_ASSERT(client.Init());
    Y_DEFER { client.DeInit(); };
    NVHost::TMonotonicBufferResource memory(client.GetMemory());

    for (bool write: {false, true}) {
        ui64 count = 0;
        ui64 errors = 0;
        for (ui32 code: {S_OK, E_IO}) {
            auto received = NThreading::NewPromise<void>();
            auto read =
                NThreading::NewPromise<NProto::TReadBlocksLocalResponse>();
            auto writeResponse =
                NThreading::NewPromise<NProto::TWriteBlocksLocalResponse>();
            service->ReadBlocksLocalHandler =
                [received, read](const auto&) mutable {
                    received.SetValue();
                    return read.GetFuture();
                };
            service->WriteBlocksLocalHandler =
                [received, writeResponse](const auto&) mutable {
                    received.SetValue();
                    return writeResponse.GetFuture();
                };
            // Release an outstanding I/O even if an assertion below fails.
            Y_DEFER {
                read.TrySetValue({});
                writeResponse.TrySetValue({});
            };

            auto header = memory.Allocate(
                sizeof(virtio_blk_req_hdr),
                alignof(virtio_blk_req_hdr));
            new (header.data()) virtio_blk_req_hdr{
                .type = static_cast<ui32>(
                    write ? VIRTIO_BLK_T_OUT : VIRTIO_BLK_T_IN),
            };
            auto data = memory.Allocate(options.BlockSize, options.BlockSize);
            auto status = memory.Allocate(1);
            auto completed = write
                ? client.WriteAsync(0, {header, data}, {status})
                : client.WriteAsync(0, {header}, {data, status});

            UNIT_ASSERT(received.GetFuture().Wait(TDuration::Seconds(5)));
            const auto pending = server->GetStats({}).SimpleStats.Requests[write];
            UNIT_ASSERT_VALUES_EQUAL(count, pending.FinalCount);
            UNIT_ASSERT_VALUES_EQUAL(errors, pending.FinalErrors);

            if (write) {
                NProto::TWriteBlocksLocalResponse response;
                if (code != S_OK) {
                    *response.MutableError() = MakeError(code);
                }
                writeResponse.SetValue(std::move(response));
            } else {
                NProto::TReadBlocksLocalResponse response;
                if (code != S_OK) {
                    *response.MutableError() = MakeError(code);
                }
                read.SetValue(std::move(response));
            }
            completed.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_VALUES_EQUAL(
                code == S_OK ? VIRTIO_BLK_S_OK : VIRTIO_BLK_S_IOERR,
                status[0]);
            if (code == S_OK) {
                ++count;
            } else {
                ++errors;
            }

            const auto snapshot = server->GetStats({});
            UNIT_ASSERT(snapshot.Fresh);
            const auto& stats = snapshot.SimpleStats.Requests[write];
            UNIT_ASSERT_VALUES_EQUAL(count, stats.FinalCount);
            UNIT_ASSERT_VALUES_EQUAL(errors, stats.FinalErrors);
            UNIT_ASSERT_VALUES_EQUAL(count, stats.Count);
            UNIT_ASSERT_VALUES_EQUAL(count * options.BlockSize, stats.Bytes);
            UNIT_ASSERT_VALUES_EQUAL(errors, stats.Errors);
            UNIT_ASSERT_VALUES_EQUAL(0, stats.LatencyGood);
            UNIT_ASSERT_VALUES_EQUAL(0, stats.LatencyBad);
            UNIT_ASSERT_VALUES_EQUAL(
                enabled ? count + errors : 0,
                stats.LatencyUnknown);
        }
    }
}

}   // namespace

Y_UNIT_TEST_SUITE(TRdmaLatencySliTest)
{
    Y_UNIT_TEST(ShouldCountDeferredRdmaCompletionsAsUnknown)
    {
        CheckRdmaLatency(true);
    }

    Y_UNIT_TEST(ShouldLeaveRdmaLatencyDisabled)
    {
        CheckRdmaLatency(false);
    }
}

}   // namespace NCloud::NBlockStore::NVHostServer
