#include "rdma_target.h"

#include <cloud/blockstore/libs/rdma_test/memory_test_storage.h>
#include <cloud/blockstore/libs/rdma_test/rdma_test_environment.h>
#include <cloud/blockstore/libs/rdma_test/server_test_async.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_client.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRdmaTargetTest)
{
    Y_UNIT_TEST(ShouldProcessRequests)
    {
        TRdmaTestEnvironment env;

        const auto blockRange = TBlockRange64::WithLength(0, 1024);
        // Write A
        {
            auto responseFuture =
                env.Run(env.MakeWriteRequest(blockRange, 'A'));
            auto response = responseFuture.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                response.GetError().GetMessage());
        }

        // Read A
        {
            auto responseFuture = env.Run(env.MakeReadRequest(blockRange));
            auto response = responseFuture.GetValueSync();
            TRdmaTestEnvironment::CheckResponse(response, blockRange, 'A');
        }

        // Checksum A
        {
            auto responseFuture = env.Run(env.MakeChecksumRequest(blockRange));
            auto response = responseFuture.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                response.GetError().GetMessage());
            auto expected = TRdmaTestEnvironment::CalcChecksum(
                blockRange.Size() * 4_KB,
                'A');
            UNIT_ASSERT_EQUAL(expected, response.GetChecksum());
        }

        // Zero
        {
            auto responseFuture = env.Run(env.MakeZeroRequest(blockRange));
            auto response = responseFuture.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                response.GetError().GetMessage());
        }

        // Read 0
        {
            auto responseFuture = env.Run(env.MakeReadRequest(blockRange));
            auto response = responseFuture.GetValueSync();
            TRdmaTestEnvironment::CheckResponse(response, blockRange, 0);
        }

        // Checksum 0
        {
            auto responseFuture = env.Run(env.MakeChecksumRequest(blockRange));
            auto response = responseFuture.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                response.GetError().GetMessage());
            auto expected =
                TRdmaTestEnvironment::CalcChecksum(blockRange.Size() * 4_KB, 0);
            UNIT_ASSERT_EQUAL(expected, response.GetChecksum());
        }
    }

    Y_UNIT_TEST(ShouldDelayOverlappedRequest)
    {
        TRdmaTestEnvironment env(4_MB, 2);

        const auto blockRange = TBlockRange64::WithLength(0, 1024);

        // Set handbrake and begin write request.
        auto writeHandbrake = NThreading::NewPromise<void>();
        env.Storage->SetHandbrake(writeHandbrake.GetFuture());
        auto writeFuture = env.Run(env.MakeWriteRequest(blockRange, 'A', 100));

        // Request on handbrake. Check it not completed yet.
        writeFuture.Wait(TDuration::MilliSeconds(1000));
        UNIT_ASSERT_VALUES_EQUAL(false, writeFuture.HasValue());

        // Begin overlapped Zero request.
        auto singleDeviceZeroFuture =
            env.Run(env.MakeZeroRequest(blockRange, 99));

        // Begin overlapped Zero request.
        auto multiDeviceZeroFuture =
            env.Run(env.MakeZeroRequest(blockRange, 98));

        // Both Zero requests delayed. Check it. Let's wait a little.
        singleDeviceZeroFuture.Wait(TDuration::MilliSeconds(1000));
        UNIT_ASSERT_VALUES_EQUAL(false, singleDeviceZeroFuture.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(false, multiDeviceZeroFuture.HasValue());

        // Remove handbrake from write request. It completed with S_OK.
        writeHandbrake.SetValue();
        auto writeResponse = writeFuture.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            writeResponse.GetError().GetCode(),
            writeResponse.GetError().GetMessage());

        // Single-device Zero request completed with E_REJECTED.
        auto singleDeviceZeroResponse = singleDeviceZeroFuture.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            singleDeviceZeroResponse.GetError().GetCode(),
            singleDeviceZeroResponse.GetError().GetMessage());

        // Multi-device Zero request completed with E_REJECTED.
        auto multiDeviceZeroResponse = multiDeviceZeroFuture.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            multiDeviceZeroResponse.GetError().GetCode(),
            multiDeviceZeroResponse.GetError().GetMessage());

        auto delayedCounter = env.Counters->GetCounter("Delayed");
        auto rejectedCounter = env.Counters->GetCounter("Rejected");
        UNIT_ASSERT_VALUES_EQUAL(2, delayedCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, rejectedCounter->Val());
    }

    Y_UNIT_TEST(ShouldNotDelayNotOverlappedRequest)
    {
        TRdmaTestEnvironment env(8_MB, 2);

        const auto blockRange1 = TBlockRange64::WithLength(0, 1024);
        const auto blockRange2 = TBlockRange64::WithLength(1024, 1024);

        // Set handbrake and begin first write request.
        auto writeHandbrake = NThreading::NewPromise<void>();
        env.Storage->SetHandbrake(writeHandbrake.GetFuture());
        auto writeFuture = env.Run(env.MakeWriteRequest(blockRange1, 'A', 100));

        // Request on handbrake. Check it not completed yet.
        writeFuture.Wait(TDuration::MilliSeconds(1000));
        UNIT_ASSERT_VALUES_EQUAL(false, writeFuture.HasValue());

        // Check not overlapped request is executed immediately.
        auto writeFuture2 = env.Run(env.MakeWriteRequest(blockRange2, 99));
        auto writeResponse2 =
            writeFuture2.GetValue(TDuration::MilliSeconds(1000));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            writeResponse2.GetError().GetCode(),
            writeResponse2.GetError().GetMessage());

        // Remove handbrake from first write request. It completed with S_OK.
        writeHandbrake.SetValue();
        auto writeResponse = writeFuture.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            writeResponse.GetError().GetCode(),
            writeResponse.GetError().GetMessage());
    }

    Y_UNIT_TEST(ShouldNotDelayReadOverlappedRequest)
    {
        TRdmaTestEnvironment env(4_MB, 2);

        const auto blockRange = TBlockRange64::WithLength(0, 1024);

        // Set handbrake and begin first write request.
        auto writeHandbrake = NThreading::NewPromise<void>();
        env.Storage->SetHandbrake(writeHandbrake.GetFuture());
        auto writeFuture = env.Run(env.MakeWriteRequest(blockRange, 'A', 100));

        // Request on handbrake. Check it not completed yet.
        writeFuture.Wait(TDuration::MilliSeconds(1000));
        UNIT_ASSERT_VALUES_EQUAL(false, writeFuture.HasValue());

        // Check not overlapped request is executed immediately.
        auto readFuture = env.Run(env.MakeReadRequest(blockRange));
        auto readResponse = readFuture.GetValue(TDuration::MilliSeconds(1000));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            readResponse.GetError().GetCode(),
            readResponse.GetError().GetMessage());

        // Remove handbrake from first write request. It completed with S_OK.
        writeHandbrake.SetValue();
        auto writeResponse = writeFuture.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            writeResponse.GetError().GetCode(),
            writeResponse.GetError().GetMessage());
    }

    Y_UNIT_TEST(ShouldMakeSyncResponseForOverlappedRequest)
    {
        TRdmaTestEnvironment env;

        const auto blockRange = TBlockRange64::WithLength(0, 1024);

        // Make first Write request.
        auto writeFuture = env.Run(env.MakeWriteRequest(blockRange, 'A', 100));
        auto writeResponse = writeFuture.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            writeResponse.GetError().GetCode(),
            writeResponse.GetError().GetMessage());

        // Make overlapped Zero request. Request completed with E_REJECTED.
        auto singleDeviceZeroFuture =
            env.Run(env.MakeZeroRequest(blockRange, 99));
        auto singleDeviceZeroResponse = singleDeviceZeroFuture.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            singleDeviceZeroResponse.GetError().GetCode(),
            singleDeviceZeroResponse.GetError().GetMessage());

        auto delayedCounter = env.Counters->GetCounter("Delayed");
        auto rejectedCounter = env.Counters->GetCounter("Rejected");
        UNIT_ASSERT_VALUES_EQUAL(0, delayedCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, rejectedCounter->Val());
    }

    Y_UNIT_TEST(ShouldRespectDeviceErasure)
    {
        TRdmaTestEnvironment env(8_MB, 2);

        const auto blockRange = TBlockRange64::WithLength(0, 1024);

        {   // Write with id=100. This request should be executed successfully.
            auto responseFuture =
                env.Run(env.MakeWriteRequest(blockRange, 'A', 100));
            auto response = responseFuture.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                response.GetError().GetMessage());
        }

        {   // Try write with id=99. This request should be rejected.
            auto responseFuture = env.Run(env.MakeWriteRequest(
                TBlockRange64::WithLength(512, 1024),
                'A',
                99));
            auto response = responseFuture.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response.GetError().GetCode(),
                response.GetError().GetMessage());
        }

        // Secure erase device.
        env.RdmaTarget->DeviceSecureEraseFinish(
            env.Device_1,
            MakeError(S_OK));

        {   // Try write with id=99 again. This one should be executed
            // successfully.
            auto responseFuture = env.Run(env.MakeWriteRequest(
                TBlockRange64::WithLength(512, 1024),
                'A',
                99));
            auto response = responseFuture.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                response.GetError().GetMessage());
        }
    }

    Y_UNIT_TEST(ShouldRejectIoDuringSecureErase)
    {
        TRdmaTestEnvironment env(8_MB, 2);

        const auto blockRange = TBlockRange64::WithLength(0, 1024);

        auto res = env.RdmaTarget->DeviceSecureEraseStart(env.Device_1);

        {
            auto responseFuture =
                env.Run(env.MakeWriteRequest(blockRange, 'A', 100));
            auto response = responseFuture.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response.GetError().GetCode(),
                response.GetError().GetMessage());
        }

        env.RdmaTarget->DeviceSecureEraseFinish(
            env.Device_1,
            MakeError(S_OK));

        {
            auto responseFuture =
                env.Run(env.MakeWriteRequest(blockRange, 'A', 100));
            auto response = responseFuture.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                response.GetError().GetMessage());
        }
    }

    Y_UNIT_TEST(ShouldRejectSecureEraseDuringIo)
    {
        TRdmaTestEnvironment env(4_MB, 2);

        const auto blockRange = TBlockRange64::WithLength(0, 1024);

        // Set handbrake and begin first write request.
        auto writeHandbrake = NThreading::NewPromise<void>();
        env.Storage->SetHandbrake(writeHandbrake.GetFuture());
        auto writeFuture = env.Run(env.MakeWriteRequest(blockRange, 'A', 100));

        // Request on handbrake. Check it not completed yet.
        writeFuture.Wait(TDuration::MilliSeconds(1000));
        UNIT_ASSERT_VALUES_EQUAL(false, writeFuture.HasValue());

        auto error = env.RdmaTarget->DeviceSecureEraseStart(env.Device_1);
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, error.GetCode());

        writeHandbrake.SetValue();
        auto writeResponse = writeFuture.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            writeResponse.GetError().GetCode(),
            writeResponse.GetError().GetMessage());

        error = env.RdmaTarget->DeviceSecureEraseStart(env.Device_1);
        UNIT_ASSERT_C(!HasError(error), error);
    }

    Y_UNIT_TEST(ShouldDisableDevice)
    {
        TRdmaTestEnvironment env(8_MB, 2);

        const auto blockRange = TBlockRange64::WithLength(0, 1024);

        env.DeviceClient->DisableDevice(env.Device_1);

        UNIT_ASSERT_VALUES_EQUAL(0, env.Storage->ErrorCount);

        {
            auto responseFuture =
                env.Run(env.MakeWriteRequest(blockRange, 'A', 100));
            const auto& response = responseFuture.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_IO,
                response.GetError().GetCode(),
                response.GetError().GetMessage());
        }

        UNIT_ASSERT_VALUES_EQUAL(1, env.Storage->ErrorCount);

        env.DeviceClient->SuspendDevice(env.Device_1);

        {
            auto responseFuture =
                env.Run(env.MakeWriteRequest(blockRange, 'A', 100));
            const auto& response = responseFuture.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response.GetError().GetCode(),
                response.GetError().GetMessage());
        }

        UNIT_ASSERT_VALUES_EQUAL(1, env.Storage->ErrorCount);
    }

    Y_UNIT_TEST(ShouldForwardMultiAgentWriteRequest)
    {
        using TMultiAgentWriteDeviceBlocksResponse =
            TEvDiskAgentPrivate::TMultiAgentWriteDeviceBlocksResponse;

        TRdmaTestEnvironment env;

        {   // Mock response from actor system.
            TMultiAgentWriteDeviceBlocksResponse mockResponse;
            mockResponse.Error = MakeError(S_OK);
            mockResponse.ReplicationResponses.push_back(MakeError(S_OK));
            mockResponse.ReplicationResponses.push_back(MakeError(S_OK));
            env.MultiAgentWriteHandler->PushMockResponse(
                std::move(mockResponse));
        }

        NProto::TWriteDeviceBlocksRequest request =
            env.MakeMultiAgentWriteRequest(
                TBlockRange64::WithLength(64, 1),
                'A',
                100);

        const auto originalRequest = TStringBuilder() << request.AsJSON();
        // Check response from rdma endpoint.
        auto responseFuture = env.Run(std::move(request));
        const auto& response = responseFuture.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));

        // Check request forwarded to actor system match with original.
        auto interceptedRequest =
            env.MultiAgentWriteHandler->PopInterceptedRequest();
        UNIT_ASSERT(interceptedRequest);
        const auto forwardedRequest = TStringBuilder()
                                      << interceptedRequest->AsJSON();
        UNIT_ASSERT_VALUES_EQUAL(originalRequest, forwardedRequest);
    }

    Y_UNIT_TEST(ShouldDelayOverlappedMultiAgentWriteRequestAndReject)
    {
        using TMultiAgentWriteDeviceBlocksResponse =
            TEvDiskAgentPrivate::TMultiAgentWriteDeviceBlocksResponse;

        const auto blockRange = TBlockRange64::WithLength(0, 100);

        TRdmaTestEnvironment env(4_MB, 2);

        {   // Mock response from actor system.
            TMultiAgentWriteDeviceBlocksResponse mockResponse;
            mockResponse.Error = MakeError(S_OK);
            mockResponse.ReplicationResponses.push_back(MakeError(S_OK));
            mockResponse.ReplicationResponses.push_back(MakeError(S_OK));
            env.MultiAgentWriteHandler->PushMockResponse(
                std::move(mockResponse));
        }

        // Set handbrake and begin write request.
        auto writeHandbrake = NThreading::NewPromise<void>();
        NThreading::TFuture<NProto::TWriteDeviceBlocksResponse> writeFuture;
        {
            env.Storage->SetHandbrake(writeHandbrake.GetFuture());
            writeFuture = env.Run(env.MakeWriteRequest(blockRange, 'A', 100));

            // Request on handbrake. Check it not completed yet.
            writeFuture.Wait(TDuration::MilliSeconds(1000));
            UNIT_ASSERT_VALUES_EQUAL(false, writeFuture.HasValue());
        }

        // Begin MultiAgent complete overlapped request.
        NThreading::TFuture<NProto::TWriteDeviceBlocksResponse>
            completeOverlappedFuture;
        {
            NProto::TWriteDeviceBlocksRequest completeOverlappedRequest =
                env.MakeMultiAgentWriteRequest(blockRange, 'B', 99);
            completeOverlappedFuture =
                env.Run(std::move(completeOverlappedRequest));

            // MultiAgent request delayed. Check it. Let's wait a little.
            completeOverlappedFuture.Wait(TDuration::MilliSeconds(1000));
            UNIT_ASSERT_VALUES_EQUAL(
                false,
                completeOverlappedFuture.HasValue());
        }

        // Begin MultiAgent partial overlapped request.
        NThreading::TFuture<NProto::TWriteDeviceBlocksResponse>
            partialOverlappedFuture;
        {
            NProto::TWriteDeviceBlocksRequest partialOverlappedRequest =
                env.MakeMultiAgentWriteRequest(
                    TBlockRange64::WithLength(0, 200),
                    'C',
                    98);
            partialOverlappedFuture =
                env.Run(std::move(partialOverlappedRequest));

            // MultiAgent request delayed. Check it. Let's wait a little.
            partialOverlappedFuture.Wait(TDuration::MilliSeconds(1000));
            UNIT_ASSERT_VALUES_EQUAL(false, partialOverlappedFuture.HasValue());
        }

        // Remove handbrake from first write request. It completed with S_OK.
        writeHandbrake.SetValue();
        {
            const auto& writeResponse = writeFuture.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                writeResponse.GetError().GetCode(),
                FormatError(writeResponse.GetError()));
        }

        {
            // Complete overlapped MultiAgent request completed with E_REJECTED.
            const auto& response = completeOverlappedFuture.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response.GetError().GetCode(),
                FormatError(response.GetError()));
        }

        {
            // Partial overlapped MultiAgent request completed with E_REJECTED.
            const auto& response = partialOverlappedFuture.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response.GetError().GetCode(),
                FormatError(response.GetError()));
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
