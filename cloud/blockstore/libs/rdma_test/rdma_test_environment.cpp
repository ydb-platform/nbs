#include "rdma_test_environment.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_client.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void TTestMultiAgentWriteHandler::PushMockResponse(
    TMultiAgentWriteDeviceBlocksResponse response)
{
    Responses.push_back(std::move(response));
}

std::optional<NProto::TWriteDeviceBlocksRequest>
TTestMultiAgentWriteHandler::PopInterceptedRequest()
{
    if (Requests.empty()) {
        return std::nullopt;
    }
    auto result = std::move(Requests.front());
    Requests.pop_front();
    return result;
}

NThreading::TFuture<TEvDiskAgentPrivate::TMultiAgentWriteDeviceBlocksResponse>
TTestMultiAgentWriteHandler::PerformMultiAgentWrite(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteDeviceBlocksRequest> request)
{
    Y_UNUSED(callContext);

    Requests.push_back(std::move(*request));
    if (Responses.empty()) {
        return NThreading::MakeFuture<TMultiAgentWriteDeviceBlocksResponse>(
            MakeError(E_FAIL, "Response not mocked"));
    }

    auto result = NThreading::MakeFuture<TMultiAgentWriteDeviceBlocksResponse>(
        Responses.front());

    Responses.pop_front();
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TRdmaTestEnvironment::TRdmaTestEnvironment(size_t deviceSize, ui32 poolSize)
    : MultiAgentWriteHandler(std::make_shared<TTestMultiAgentWriteHandler>())
    , Storage(std::make_shared<TMemoryTestStorage>(deviceSize))
{
    THashMap<TString, TStorageAdapterPtr> devices;
    devices[Device_1] = std::make_shared<TStorageAdapter>(
        Storage,
        4_KB,                // storageBlockSize
        true,                // normalize,
        TDuration::Zero(),   // maxRequestDuration
        TDuration::Zero()    // shutdownTimeout
    );

    TVector<TString> uuids;
    for (const auto& [key, value]: devices) {
        uuids.push_back(key);
    }

    DeviceClient = std::make_shared<TDeviceClient>(
        TDuration::MilliSeconds(100),
        uuids,
        Logging->CreateLog("BLOCKSTORE_DISK_AGENT"),
        false);   // kickOutOldClientsEnabled

    DeviceClient->AcquireDevices(
        uuids,
        ClientId,
        TInstant::Now(),
        NProto::VOLUME_ACCESS_READ_WRITE,
        0,
        "vol0",
        0);

    NProto::TRdmaTarget target;
    target.MutableEndpoint()->SetHost(Host);
    target.MutableEndpoint()->SetPort(Port);
    target.SetWorkerThreads(poolSize);

    constexpr bool rejectLateRequests = true;

    auto rdmaTargetConfig = std::make_shared<TRdmaTargetConfig>(
        rejectLateRequests,
        target);

    TOldRequestCounters oldRequestCounters{
        Counters->GetCounter("Delayed"),
        Counters->GetCounter("Rejected")};

    RdmaTarget = CreateRdmaTarget(
        std::move(rdmaTargetConfig),
        std::move(oldRequestCounters),
        Logging,
        Server,
        DeviceClient,
        MultiAgentWriteHandler,
        std::move(devices));

    RdmaTarget->Start();
}

TRdmaTestEnvironment::~TRdmaTestEnvironment()
{
    RdmaTarget->Stop();
}

// static
ui64 TRdmaTestEnvironment::CalcChecksum(ui32 size, char fill)
{
    TString data(size, fill);
    TBlockChecksum checksum;
    checksum.Extend(data.data(), data.size());
    return checksum.GetValue();
}

// static
void TRdmaTestEnvironment::CheckResponse(
    const NProto::TReadDeviceBlocksResponse& response,
    const TBlockRange64& blockRange,
    char fill)
{
    UNIT_ASSERT_VALUES_EQUAL_C(
        S_OK,
        response.GetError().GetCode(),
        response.GetError().GetMessage());

    UNIT_ASSERT_VALUES_EQUAL(
        blockRange.Size(),
        response.GetBlocks().BuffersSize());

    const TString expectedContent(
        response.GetBlocks().GetBuffers(0).size(),
        fill);
    for (int i = 0; i < response.GetBlocks().GetBuffers().size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(
            expectedContent,
            response.GetBlocks().GetBuffers(i),
            TStringBuilder() << "block " << i);
    }
}

NProto::TWriteDeviceBlocksRequest TRdmaTestEnvironment::MakeWriteRequest(
    const TBlockRange64& blockRange,
    char fill,
    ui64 volumeRequestId) const
{
    NProto::TWriteDeviceBlocksRequest result;
    result.SetDeviceUUID(Device_1);
    result.SetBlockSize(4_KB);
    result.SetStartIndex(blockRange.Start);
    result.MutableHeaders()->SetClientId(ClientId);
    result.SetVolumeRequestId(volumeRequestId);

    for (ui32 i = 0; i < blockRange.Size(); ++i) {
        *result.MutableBlocks()->AddBuffers() = TString(4_KB, fill);
    }
    return result;
}

NProto::TWriteDeviceBlocksRequest
TRdmaTestEnvironment::MakeMultiAgentWriteRequest(
    const TBlockRange64& blockRange,
    char fill,
    ui64 volumeRequestId) const
{
    NProto::TWriteDeviceBlocksRequest result;
    result.MutableHeaders()->SetClientId(ClientId);
    result.SetVolumeRequestId(volumeRequestId);
    result.SetBlockSize(4_KB);
    {
        auto* target = result.AddReplicationTargets();
        target->SetDeviceUUID(Device_1);
        target->SetStartIndex(blockRange.Start);
        target->SetNodeId(0);
        target->SetTimeout(1000);
    }
    {
        auto* target = result.AddReplicationTargets();
        target->SetDeviceUUID("Device2");
        target->SetStartIndex(blockRange.Start);
        target->SetNodeId(1);
        target->SetTimeout(1000);
    }

    for (ui32 i = 0; i < blockRange.Size(); ++i) {
        *result.MutableBlocks()->AddBuffers() = TString(4_KB, fill);
    }
    return result;
}

NProto::TReadDeviceBlocksRequest TRdmaTestEnvironment::MakeReadRequest(
    const TBlockRange64& blockRange) const
{
    NProto::TReadDeviceBlocksRequest result;
    result.SetDeviceUUID(Device_1);
    result.SetBlockSize(4_KB);
    result.SetStartIndex(blockRange.Start);
    result.SetBlocksCount(blockRange.Size());
    result.MutableHeaders()->SetClientId(ClientId);
    return result;
}

NProto::TChecksumDeviceBlocksRequest TRdmaTestEnvironment::MakeChecksumRequest(
    const TBlockRange64& blockRange) const
{
    NProto::TChecksumDeviceBlocksRequest result;
    result.SetDeviceUUID(Device_1);
    result.SetBlockSize(4_KB);
    result.SetStartIndex(blockRange.Start);
    result.SetBlocksCount(blockRange.Size());
    result.MutableHeaders()->SetClientId(ClientId);
    return result;
}

NProto::TZeroDeviceBlocksRequest TRdmaTestEnvironment::MakeZeroRequest(
    const TBlockRange64& blockRange,
    ui64 volumeRequestId) const
{
    NProto::TZeroDeviceBlocksRequest result;
    result.SetDeviceUUID(Device_1);
    result.SetBlockSize(4_KB);
    result.SetStartIndex(blockRange.Start);
    result.SetBlocksCount(blockRange.Size());
    result.MutableHeaders()->SetClientId(ClientId);
    result.SetVolumeRequestId(volumeRequestId);
    return result;
}

}   // namespace NCloud::NBlockStore::NStorage
