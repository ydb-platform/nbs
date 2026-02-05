#include "service_ut.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/config.h>

// TODO: invalid reference
#include <cloud/blockstore/libs/storage/partition/part_events_private.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NPartition;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest>
void SendUndeliverableRequest(
    TTestActorRuntimeBase& runtime,
    ui32 nodeIdx,
    TAutoPtr<IEventHandle>& event)
{
    auto fakeRecipient = TActorId(
        event->Recipient.NodeId(),
        event->Recipient.PoolID(),
        0,
        event->Recipient.Hint());

    auto undeliveryActor = event->GetForwardOnNondeliveryRecipient();
    runtime.Send(
        new IEventHandle(
            fakeRecipient,
            event->Sender,
            event->Release<TRequest>().Release(),
            event->Flags,
            0,
            &undeliveryActor),
        nodeIdx);
}

};  // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceReadWriteZeroBlocksTest)
{
    Y_UNIT_TEST(ShouldHandleWriteErrors)
    {
        TTestEnv env(1, 1, 4);
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume(DefaultDiskId, DefaultBlocksCount);

        TString sessionId;
        {
            auto response = service.MountVolume();
            sessionId = response->Record.GetSessionId();
        }

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvWriteBlobResponse: {
                        auto* msg = event->Get<TEvPartitionPrivate::TEvWriteBlobResponse>();
                        auto& e = const_cast<NProto::TError&>(msg->Error);
                        e.SetCode(E_REJECTED);
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.SendWriteBlocksRequest(
            DefaultDiskId,
            TBlockRange64::WithLength(0, 1024),
            sessionId);
        auto response = service.RecvWriteBlocksResponse();
        UNIT_ASSERT_C(
           FAILED(response->GetStatus()),
           response->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldHandleWriteErrorsWhenUsingPipe)
    {
        TTestEnv env(1, 2, 4);
        ui32 nodeIdx1 = SetupTestEnv(env);
        ui32 nodeIdx2 = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service1(runtime, nodeIdx1);
        service1.CreateVolume(DefaultDiskId, DefaultBlocksCount);

        TString sessionId;
        {
            auto response = service1.MountVolume();
            sessionId = response->Record.GetSessionId();
        }

        TServiceClient service2(runtime, nodeIdx2);

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvWriteBlobResponse: {
                        auto* msg = event->Get<TEvPartitionPrivate::TEvWriteBlobResponse>();
                        auto& e = const_cast<NProto::TError&>(msg->Error);
                        e.SetCode(E_REJECTED);
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service2.SendWriteBlocksRequest(
            DefaultDiskId,
            TBlockRange64::WithLength(0, 1024),
            sessionId);
        auto response = service2.RecvWriteBlocksResponse();
        UNIT_ASSERT_C(
            FAILED(response->GetStatus()),
            response->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldRejectWriteAndZeroBlocksRequestsForReadOnlyMountedClients)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume();
        service.AssignVolume(DefaultDiskId, "foo", "bar");

        TString sessionId;
        {
            auto response = service.MountVolume(
                DefaultDiskId,
                "foo",
                "bar",
                NProto::IPC_GRPC,
                NProto::VOLUME_ACCESS_READ_ONLY,
                NProto::VOLUME_MOUNT_LOCAL);
            sessionId = response->Record.GetSessionId();
        }

        {
            service.SendWriteBlocksRequest(
                DefaultDiskId,
                TBlockRange64::WithLength(0, 1024),
                sessionId,
                char(1));
            auto response = service.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                response->GetStatus(),
                response->GetErrorReason()
            );
        }

        {
            service.SendZeroBlocksRequest(
                DefaultDiskId,
                0,
                sessionId);
            auto response = service.RecvZeroBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                response->GetStatus(),
                response->GetErrorReason()
            );
        }
    }

    Y_UNIT_TEST(ShouldNotConvertReadBlocksRequestsToLocalRequestsForRemotelyMountedVolumeTablet)
    {
        TTestEnv env(1, 2);
        ui32 nodeIdx1 = SetupTestEnv(env);
        ui32 nodeIdx2 = SetupTestEnv(env);

        TActorId volumeActorId;
        auto& runtime = env.GetRuntime();

        TServiceClient service1(runtime, nodeIdx1);
        service1.CreateVolume();
        service1.AssignVolume(DefaultDiskId, "foo", "bar");

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvServicePrivate::EvVolumeTabletStatus: {
                        auto* msg = event->Get<TEvServicePrivate::TEvVolumeTabletStatus>();
                        volumeActorId = msg->VolumeActor;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        // First mount in read-write mode and write something to be read later
        TString sessionId;
        {
            auto response = service1.MountVolume(
                DefaultDiskId,
                "foo",
                "bar",
                NProto::IPC_GRPC,
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_LOCAL);
            sessionId = response->Record.GetSessionId();
        }

        UNIT_ASSERT(volumeActorId);

        service1.WriteBlocks(
            DefaultDiskId,
            TBlockRange64::WithLength(0, 1024),
            sessionId,
            char(1));
        service1.UnmountVolume(DefaultDiskId, sessionId);

        // Re-mount in read-only mode
        service1.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_LOCAL);

        TServiceClient service2(runtime, nodeIdx2);

        {
            auto response = service2.MountVolume(
                DefaultDiskId,
                "foo",
                "bar",
                NProto::IPC_GRPC,
                NProto::VOLUME_ACCESS_READ_ONLY,
                NProto::VOLUME_MOUNT_REMOTE);
            sessionId = response->Record.GetSessionId();
        }

        bool detectedReadBlocksLocalRequestSentToVolumeActor = false;
        bool detectedReadBlocksLocalResponseFromVolumeActor = false;
        TActorId readBlocksLocalRequestSenderId;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvService::EvReadBlocksLocalRequest: {
                        if (event->Recipient == volumeActorId) {
                            detectedReadBlocksLocalRequestSentToVolumeActor = true;
                            readBlocksLocalRequestSenderId = event->Sender;
                        }
                        break;
                    }
                    case TEvService::EvReadBlocksLocalResponse: {
                        if (readBlocksLocalRequestSenderId &&
                            event->Recipient == readBlocksLocalRequestSenderId)
                        {
                            detectedReadBlocksLocalResponseFromVolumeActor = true;
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service2.ReadBlocks(DefaultDiskId, 0, sessionId);

        UNIT_ASSERT(!detectedReadBlocksLocalRequestSentToVolumeActor);
        UNIT_ASSERT(!detectedReadBlocksLocalResponseFromVolumeActor);
    }

    Y_UNIT_TEST(ShouldNotConvertWriteBlocksRequestsToLocalRequestsForRemotelyMountedVolumeTablet)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();
        service.AssignVolume(DefaultDiskId, "foo", "bar");

        // Ensure the volume tablet is ready to prevent service from starting it
        service.WaitForVolume();

        TString sessionId;
        {
            auto response = service.MountVolume(
                DefaultDiskId,
                "foo",
                "bar",
                NProto::IPC_GRPC,
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_REMOTE);
            sessionId = response->Record.GetSessionId();
        }

        bool detectedWriteBlocksLocalRequestSentToVolumeActor = false;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvService::EvWriteBlocksLocalRequest: {
                        detectedWriteBlocksLocalRequestSentToVolumeActor = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.WriteBlocks(
            DefaultDiskId,
            TBlockRange64::WithLength(0, 1024),
            sessionId,
            char(1));

        UNIT_ASSERT(!detectedWriteBlocksLocalRequestSentToVolumeActor);
    }

    Y_UNIT_TEST(ShouldConvertWriteBlocksLocalToGrpcForRemotelyMountedVolumeTablet)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        ui32 writeBlocksRequestUsingLocalIpcCount = 0;
        ui32 writeBlocksRequestUsingGrpcIpcCount = 0;
        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();
        service.AssignVolume(DefaultDiskId, "foo", "bar");

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                // non-matching recipient and recipient rewrite mean
                // the event went through pipe i.e. it has reached
                // volume or partition actor
                if (event->Recipient != event->GetRecipientRewrite()) {
                    switch (event->GetTypeRewrite()) {
                        case TEvService::EvWriteBlocksRequest: {
                            ++writeBlocksRequestUsingGrpcIpcCount;
                            break;
                        }
                        case TEvService::EvWriteBlocksLocalRequest: {
                            ++writeBlocksRequestUsingLocalIpcCount;
                            break;
                        }
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        // Ensure the volume tablet is ready to prevent service from starting it
        service.WaitForVolume();

        TString sessionId;
        {
            auto response = service.MountVolume(
                DefaultDiskId,
                "foo",
                "bar",
                NProto::IPC_VHOST,
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_REMOTE);
            sessionId = response->Record.GetSessionId();
        }

        auto writeBlocksRequest = std::make_unique<TEvService::TEvWriteBlocksLocalRequest>();
        writeBlocksRequest->Record.SetDiskId(DefaultDiskId);
        writeBlocksRequest->Record.SetSessionId(sessionId);
        writeBlocksRequest->Record.SetStartIndex(0);
        writeBlocksRequest->Record.MutableHeaders()->SetClientId(service.GetClientId());

        ui32 blocksCount = 1024;
        auto writeBlockContent = GetBlockContent(char(1));
        TSgList sglist;
        sglist.resize(blocksCount, {writeBlockContent.data(), writeBlockContent.size()});
        writeBlocksRequest->Record.Sglist = TGuardedSgList(std::move(sglist));
        writeBlocksRequest->Record.BlocksCount = blocksCount;
        writeBlocksRequest->Record.BlockSize = DefaultBlockSize;

        service.SendRequest(MakeStorageServiceId(), std::move(writeBlocksRequest));
        auto response = service.RecvWriteBlocksLocalResponse();

        UNIT_ASSERT_C(SUCCEEDED(response->GetStatus()), response->GetErrorReason());
        UNIT_ASSERT(writeBlocksRequestUsingLocalIpcCount == 1);
        UNIT_ASSERT(writeBlocksRequestUsingGrpcIpcCount > 0);
    }

    Y_UNIT_TEST(ShouldConvertReadBlocksLocalToGrpcForRemotelyMountedVolumeTablet)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        ui32 readBlocksResponseUsingLocalIpcCount = 0;
        ui32 readBlocksResponseNotUsingLocalIpcCount = 0;

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();
        service.AssignVolume(DefaultDiskId, "foo", "bar");

        // Write something to be read further
        TString sessionId;
        {
            auto response = service.MountVolume(DefaultDiskId, "foo", "bar");
            sessionId = response->Record.GetSessionId();
        }

        service.WriteBlocks(
            DefaultDiskId,
            TBlockRange64::WithLength(0, 1024),
            sessionId);
        service.UnmountVolume(DefaultDiskId, sessionId);

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvService::EvReadBlocksResponse: {
                        ++readBlocksResponseNotUsingLocalIpcCount;
                        break;
                    }
                    case TEvService::EvReadBlocksLocalResponse: {
                        ++readBlocksResponseUsingLocalIpcCount;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        // Ensure the volume tablet is ready to prevent service from starting it
        service.WaitForVolume();

        {
            auto response = service.MountVolume(
                DefaultDiskId,
                "foo",
                "bar",
                NProto::IPC_VHOST,
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_REMOTE);
            sessionId = response->Record.GetSessionId();
        }

        auto readBlocksRequest = std::make_unique<TEvService::TEvReadBlocksLocalRequest>();
        readBlocksRequest->Record.SetDiskId(DefaultDiskId);
        readBlocksRequest->Record.SetSessionId(sessionId);
        readBlocksRequest->Record.SetStartIndex(0);
        readBlocksRequest->Record.SetBlocksCount(1);
        readBlocksRequest->Record.MutableHeaders()->SetClientId(service.GetClientId());

        auto block = TString::Uninitialized(DefaultBlockSize);
        auto buffer = TGuardedBuffer(std::move(block));
        readBlocksRequest->Record.Sglist = buffer.GetGuardedSgList();
        readBlocksRequest->Record.BlockSize = DefaultBlockSize;

        service.SendRequest(MakeStorageServiceId(), std::move(readBlocksRequest));
        auto response = service.RecvReadBlocksLocalResponse();

        UNIT_ASSERT_C(SUCCEEDED(response->GetStatus()), response->GetErrorReason());
        UNIT_ASSERT_VALUES_EQUAL(8, response->Record.GetUnencryptedBlockMask().size());

        UNIT_ASSERT(readBlocksResponseUsingLocalIpcCount > 0);

        // From partition to volume;
        // from volume to remote request actor;
        // from remote request actor to service client
        UNIT_ASSERT(readBlocksResponseNotUsingLocalIpcCount == 3);
    }

    Y_UNIT_TEST(ShouldAllowReadWriteZeroBlocksRequestsFromRemotelyMountedService)
    {
        TTestEnv env(1, 2);
        ui32 nodeIdx1 = SetupTestEnv(env);
        ui32 nodeIdx2 = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service1(runtime, nodeIdx1);
        service1.CreateVolume();
        service1.AssignVolume(DefaultDiskId, "foo", "bar");

        TString service1SessionId;
        {
            auto response = service1.MountVolume(
                DefaultDiskId,
                "foo",
                "bar",
                NProto::IPC_GRPC,
                NProto::VOLUME_ACCESS_READ_ONLY,
                NProto::VOLUME_MOUNT_LOCAL);
            service1SessionId = response->Record.GetSessionId();
        }

        TServiceClient service2(runtime, nodeIdx2);

        TString service2SessionId;
        {
            auto response = service2.MountVolume(
                DefaultDiskId,
                "foo",
                "bar",
                NProto::IPC_GRPC,
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_REMOTE);
            service2SessionId = response->Record.GetSessionId();
        }

        service2.WriteBlocks(
            DefaultDiskId,
            TBlockRange64::WithLength(0, 1024),
            service2SessionId);
        service2.ZeroBlocks(DefaultDiskId, 0, service2SessionId);
        service2.ReadBlocks(DefaultDiskId, 0, service2SessionId);

        service2.UnmountVolume(service2SessionId);
        service1.UnmountVolume(service1SessionId);
    }

    Y_UNIT_TEST(ShouldSendConvertedRequestsToVolumeClientWithUndeliveryTracking)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();
        service.AssignVolume(DefaultDiskId, "foo", "bar");

        // Write something to be read further
        TString sessionId;
        {
            auto response = service.MountVolume(DefaultDiskId,
                "foo",
                "bar",
                NProto::IPC_GRPC,
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_REMOTE);
            sessionId = response->Record.GetSessionId();
        }

        bool patchRequest = true;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvService::EvReadBlocksRequest &&
                    event->Sender.NodeId() == event->Recipient.NodeId())
                {
                    if (patchRequest) {
                        patchRequest = false;
                        SendUndeliverableRequest<TEvService::TEvReadBlocksRequest>(
                            runtime,
                            nodeIdx,
                            event);
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto readBlocksRequest = std::make_unique<TEvService::TEvReadBlocksLocalRequest>();
        readBlocksRequest->Record.SetDiskId(DefaultDiskId);
        readBlocksRequest->Record.SetSessionId(sessionId);
        readBlocksRequest->Record.SetStartIndex(0);
        readBlocksRequest->Record.SetBlocksCount(1);
        readBlocksRequest->Record.MutableHeaders()->SetClientId(service.GetClientId());

        auto block = TString::Uninitialized(DefaultBlockSize);
        auto buffer = TGuardedBuffer(std::move(block));
        readBlocksRequest->Record.Sglist = buffer.GetGuardedSgList();
        readBlocksRequest->Record.BlockSize = DefaultBlockSize;

        service.SendRequest(MakeStorageServiceId(), std::move(readBlocksRequest));
        auto response = service.RecvReadBlocksLocalResponse();

        UNIT_ASSERT(FAILED(response->GetStatus()));
        UNIT_ASSERT(response->GetStatus() == E_REJECTED);
    }

    Y_UNIT_TEST(ShouldReceiveRemoteThrottlerDelay)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TActorId volumeActorId;
        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);

        service.CreateVolume(
            DefaultDiskId,
            DefaultBlocksCount,
            DefaultBlockSize,
            "",
            "",
            NProto::STORAGE_MEDIA_SSD);

        service.AssignVolume(DefaultDiskId, "foo", "bar");

        auto response = service.MountVolume(
            DefaultDiskId,
            "foo",
            "bar",
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE);
        auto sessionId = response->Record.GetSessionId();

        {
            auto response = service.ReadBlocks(DefaultDiskId, 0, sessionId);
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                response->Record.GetHeaders().GetThrottler().GetDelay());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                response->Record.GetDeprecatedThrottlerDelay());
        }

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvService::EvWriteBlocksResponse: {
                        auto* msg =
                            event->Get<TEvService::TEvWriteBlocksResponse>();
                        msg->Record.SetDeprecatedThrottlerDelay(1e6);
                        msg->Record.MutableHeaders()
                            ->MutableThrottler()
                            ->SetDelay(1e6);
                        break;
                    }
                    case TEvService::EvReadBlocksResponse: {
                        auto* msg =
                            event->Get<TEvService::TEvReadBlocksResponse>();
                        msg->Record.SetDeprecatedThrottlerDelay(1e6);
                        msg->Record.MutableHeaders()
                            ->MutableThrottler()
                            ->SetDelay(1e6);
                        break;
                    }
                    case TEvVolume::EvDescribeBlocksResponse: {
                        auto* msg =
                            event->Get<TEvVolume::TEvDescribeBlocksResponse>();
                        msg->Record.SetDeprecatedThrottlerDelay(1e6);
                        msg->Record.MutableHeaders()
                            ->MutableThrottler()
                            ->SetDelay(1e6);
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        {
            auto request = service.CreateReadBlocksRequest(DefaultDiskId, 0, sessionId);
            auto callContext = request->CallContext;
            callContext->SetPossiblePostponeDuration(
                TDuration::MicroSeconds(1'234));
            UNIT_ASSERT_VALUES_EQUAL(
                TDuration::MicroSeconds(1'234),
                callContext->GetPossiblePostponeDuration());
            service.SendRequest(MakeStorageServiceId(), std::move(request));
            auto response = service.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                1e6,
                response->Record.GetDeprecatedThrottlerDelay());
            UNIT_ASSERT_VALUES_EQUAL(
                1e6,
                response->Record.GetHeaders().GetThrottler().GetDelay());
            UNIT_ASSERT_VALUES_EQUAL(
                1e6,
                callContext->Time(EProcessingStage::Postponed).MicroSeconds());
            UNIT_ASSERT_VALUES_EQUAL(
                TDuration::Zero(),
                callContext->GetPossiblePostponeDuration());
        }

        {
            auto request = service.CreateWriteBlocksRequest(
                DefaultDiskId,
                TBlockRange64::MakeOneBlock(0),
                sessionId);
            auto callContext = request->CallContext;
            callContext->SetPossiblePostponeDuration(
                TDuration::MicroSeconds(1'234));
            UNIT_ASSERT_VALUES_EQUAL(
                TDuration::MicroSeconds(1'234),
                callContext->GetPossiblePostponeDuration());
            service.SendRequest(MakeStorageServiceId(), std::move(request));
            auto response = service.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                1e6,
                response->Record.GetDeprecatedThrottlerDelay());
            UNIT_ASSERT_VALUES_EQUAL(
                1e6,
                response->Record.GetHeaders().GetThrottler().GetDelay());
            UNIT_ASSERT_VALUES_EQUAL(
                1e6,
                callContext->Time(EProcessingStage::Postponed).MicroSeconds());
            UNIT_ASSERT_VALUES_EQUAL(
                TDuration::Zero(),
                callContext->GetPossiblePostponeDuration());
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
