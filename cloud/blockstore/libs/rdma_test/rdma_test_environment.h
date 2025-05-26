#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/rdma_test/memory_test_storage.h>
#include <cloud/blockstore/libs/rdma_test/server_test_async.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/multi_agent_write.h>
#include <cloud/blockstore/libs/storage/disk_agent/rdma_target.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TTestMultiAgentWriteHandler: public IMultiAgentWriteHandler
{
private:
    TDeque<NProto::TWriteDeviceBlocksRequest> Requests;
    TDeque<TMultiAgentWriteResponsePrivate> Responses;

public:
    void PushMockResponse(TMultiAgentWriteResponsePrivate response);
    std::optional<NProto::TWriteDeviceBlocksRequest> PopInterceptedRequest();

    // Implements IMultiAgentWriteHandler
    NThreading::TFuture<TMultiAgentWriteResponsePrivate> PerformMultiAgentWrite(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDeviceBlocksRequest> request) override;
};

using TTestMultiAgentWriteHandlerPtr = std::shared_ptr<TTestMultiAgentWriteHandler>;

struct TRdmaTestEnvironment
{
    const TString ClientId = "client_1";
    const TString Device_1 = "uuid-1";
    const TString Host = "host";
    const ui32 Port = 11111;
    const TTestMultiAgentWriteHandlerPtr MultiAgentWriteHandler;

    std::shared_ptr<TRdmaAsyncTestServer> Server{
        std::make_shared<TRdmaAsyncTestServer>()};

    std::shared_ptr<TMemoryTestStorage> Storage;

    IRdmaTargetPtr RdmaTarget;

    NMonitoring::TDynamicCountersPtr Counters{
        new NMonitoring::TDynamicCounters()};

    ILoggingServicePtr Logging = CreateLoggingService(
        "console",
        TLogSettings{TLOG_RESOURCES});

    std::shared_ptr<TDeviceClient> DeviceClient;

    TRdmaTestEnvironment(size_t deviceSize = 4_MB, ui32 poolSize = 1);

    virtual ~TRdmaTestEnvironment();

    static ui64 CalcChecksum(ui32 size, char fill);
    static void CheckResponse(
        const NProto::TReadDeviceBlocksResponse& response,
        const TBlockRange64& blockRange,
        char fill);

    NProto::TWriteDeviceBlocksRequest MakeWriteRequest(
        const TBlockRange64& blockRange,
        char fill,
        ui64 volumeRequestId = 0,
        bool isMultideviceRequest = false) const;

    NProto::TWriteDeviceBlocksRequest MakeMultiAgentWriteRequest(
        const TBlockRange64& blockRange,
        char fill,
        ui64 volumeRequestId) const;

    NProto::TReadDeviceBlocksRequest MakeReadRequest(
        const TBlockRange64& blockRange) const;

    NProto::TChecksumDeviceBlocksRequest MakeChecksumRequest(
        const TBlockRange64& blockRange) const;

    NProto::TZeroDeviceBlocksRequest MakeZeroRequest(
        const TBlockRange64& blockRange,
        ui64 volumeRequestId = 0,
        bool isMultideviceRequest = false) const;

    template <typename TRequest>
    auto Run(TRequest request)
    {
        return Server->Run(Host, Port, std::move(request));
    }
};

}   // namespace NCloud::NBlockStore::NStorage
