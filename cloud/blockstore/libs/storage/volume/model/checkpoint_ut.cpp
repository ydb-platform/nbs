#include "checkpoint.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCheckpointStore)
{
    Y_UNIT_TEST(EmptyPersistentState)
    {
        TCheckpointStore store({}, "disk-1");

        UNIT_ASSERT_VALUES_EQUAL(false, store.DoesCheckpointBlockingWritesExist());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
        ui64 requestId = 0;
        UNIT_ASSERT_VALUES_EQUAL(false, store.HasRequestToExecute(&requestId));
        UNIT_ASSERT_VALUES_EQUAL(0, requestId);
    }

    Y_UNIT_TEST(HasPersistentState)
    {
        const TCheckpointRequest initialState[] = {
            TCheckpointRequest{
                1,
                "checkpoint-1",
                TInstant::Now(),
                ECheckpointRequestType::Create,
                ECheckpointRequestState::Rejected,
                ECheckpointType::Normal},

            TCheckpointRequest{
                2,
                "checkpoint-2",
                TInstant::Now(),
                ECheckpointRequestType::Create,
                ECheckpointRequestState::Completed,
                ECheckpointType::Normal},

            TCheckpointRequest{
                3,
                "checkpoint-1",
                TInstant::Now(),
                ECheckpointRequestType::Create,
                ECheckpointRequestState::Completed,
                ECheckpointType::Normal},
            TCheckpointRequest{
                4,
                "checkpoint-1",
                TInstant::Now(),
                ECheckpointRequestType::DeleteData,
                ECheckpointRequestState::Completed,
                ECheckpointType::Normal},

            TCheckpointRequest{
                5,
                "checkpoint-4",
                TInstant::Now(),
                ECheckpointRequestType::Create,
                ECheckpointRequestState::Completed,
                ECheckpointType::Normal},
            TCheckpointRequest{
                6,
                "checkpoint-4",
                TInstant::Now(),
                ECheckpointRequestType::DeleteData,
                ECheckpointRequestState::Completed,
                ECheckpointType::Normal},
            TCheckpointRequest{
                7,
                "checkpoint-4",
                TInstant::Now(),
                ECheckpointRequestType::Delete,
                ECheckpointRequestState::Completed,
                ECheckpointType::Normal},

            TCheckpointRequest{
                10,
                "checkpoint-3",
                TInstant::Now(),
                ECheckpointRequestType::Create,
                ECheckpointRequestState::Saved,
                ECheckpointType::Normal},

            TCheckpointRequest{
                11,
                "checkpoint-4",
                TInstant::Now(),
                ECheckpointRequestType::Create,
                ECheckpointRequestState::Received,
                ECheckpointType::Normal},

            TCheckpointRequest{
                8,
                "checkpoint-4",
                TInstant::Now(),
                ECheckpointRequestType::DeleteData,
                ECheckpointRequestState::Completed,
                ECheckpointType::Normal},
            TCheckpointRequest{
                9,
                "checkpoint-4",
                TInstant::Now(),
                ECheckpointRequestType::Delete,
                ECheckpointRequestState::Completed,
                ECheckpointType::Normal},

            TCheckpointRequest{
                12,
                "checkpoint-5",
                TInstant::Now(),
                ECheckpointRequestType::CreateWithoutData,
                ECheckpointRequestState::Completed,
                ECheckpointType::Normal},

            TCheckpointRequest{
                13,
                "checkpoint-6",
                TInstant::Now(),
                ECheckpointRequestType::Create,
                ECheckpointRequestState::Completed,
                ECheckpointType::Normal,
                "shadow-disk-6",
                EShadowDiskState::Preparing,
                512},

        };
        TCheckpointStore store(
            TVector<TCheckpointRequest>{
                std::begin(initialState),
                std::end(initialState)},
            "disk-1");

        UNIT_ASSERT_VALUES_EQUAL(true, store.DoesCheckpointBlockingWritesExist());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());

        ui64 requestId = 0;
        UNIT_ASSERT_VALUES_EQUAL(true, store.HasRequestToExecute(&requestId));
        UNIT_ASSERT_VALUES_EQUAL(10, requestId);

        auto checkpoints = store.GetActiveCheckpoints();
        UNIT_ASSERT_VALUES_EQUAL(4, checkpoints.size());
        UNIT_ASSERT_VALUES_EQUAL(true, checkpoints.contains("checkpoint-1"));
        UNIT_ASSERT_VALUES_EQUAL(true, checkpoints.contains("checkpoint-2"));
        UNIT_ASSERT_VALUES_EQUAL(true, checkpoints.contains("checkpoint-5"));
        UNIT_ASSERT_VALUES_EQUAL(true, checkpoints.contains("checkpoint-6"));

        UNIT_ASSERT_VALUES_EQUAL(
            true,
            store.DoesCheckpointHaveData("checkpoint-2"));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            store.DoesCheckpointHaveData("checkpoint-1"));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            store.DoesCheckpointHaveData("checkpoint-5"));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            store.DoesCheckpointHaveData("checkpoint-6"));

        UNIT_ASSERT_VALUES_EQUAL(1, store.GetCheckpointsWithData().size());

        UNIT_ASSERT_VALUES_EQUAL(
            true,
            store.IsCheckpointDataPreparingOrPresent("checkpoint-2"));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            store.IsCheckpointDataPreparingOrPresent("checkpoint-1"));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            store.IsCheckpointDataPreparingOrPresent("checkpoint-5"));
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            store.IsCheckpointDataPreparingOrPresent("checkpoint-6"));

        // The checkpoint without the shadow disk has the correct state.
        UNIT_ASSERT_VALUES_EQUAL(
            checkpoints["checkpoint-1"].ShadowDiskState,
            EShadowDiskState::None);

        // Checkpoint with the shadow disk loads the state.
        UNIT_ASSERT_VALUES_EQUAL(
            checkpoints["checkpoint-6"].ShadowDiskId,
            "shadow-disk-6");
        UNIT_ASSERT_VALUES_EQUAL(
            checkpoints["checkpoint-6"].ShadowDiskState,
            EShadowDiskState::Preparing);
        UNIT_ASSERT_VALUES_EQUAL(
            checkpoints["checkpoint-6"].ProcessedBlockCount,
            512);

        UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointDeleted("checkpoint-1"));
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointDeleted("checkpoint-2"));
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointDeleted("checkpoint-3"));
        UNIT_ASSERT_VALUES_EQUAL(true, store.IsCheckpointDeleted("checkpoint-4"));
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointDeleted("checkpoint-5"));
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointDeleted("checkpoint-5"));

        UNIT_ASSERT_VALUES_EQUAL(false, store.HasSavedRequest("checkpoint-1"));
        UNIT_ASSERT_VALUES_EQUAL(false, store.HasSavedRequest("checkpoint-2"));
        UNIT_ASSERT_VALUES_EQUAL(true, store.HasSavedRequest("checkpoint-3"));
        UNIT_ASSERT_VALUES_EQUAL(false, store.HasSavedRequest("checkpoint-4"));
        UNIT_ASSERT_VALUES_EQUAL(false, store.HasSavedRequest("checkpoint-5"));
        UNIT_ASSERT_VALUES_EQUAL(false, store.HasSavedRequest("checkpoint-6"));
    }

    Y_UNIT_TEST(CreateFail)
    {
        TCheckpointStore store({}, "disk-1");

        const auto& request = store.CreateNew(
            "checkpoint",
            TInstant::Now(),
            ECheckpointRequestType::Create,
            ECheckpointType::Normal);

        UNIT_ASSERT_VALUES_EQUAL(false, store.DoesCheckpointBlockingWritesExist());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
        ui64 requestId = 0;
        UNIT_ASSERT_VALUES_EQUAL(false, store.HasRequestToExecute(&requestId));
        UNIT_ASSERT_VALUES_EQUAL(0, requestId);
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointDeleted("checkpoint"));

        store.SetCheckpointRequestSaved(request.RequestId);
        UNIT_ASSERT_VALUES_EQUAL(false, store.DoesCheckpointBlockingWritesExist());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
        UNIT_ASSERT_VALUES_EQUAL(true, store.HasRequestToExecute(&requestId));
        UNIT_ASSERT_VALUES_EQUAL(request.RequestId, requestId);
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointDeleted("checkpoint"));

        store.SetCheckpointRequestInProgress(requestId);
        UNIT_ASSERT_VALUES_EQUAL(false, store.DoesCheckpointBlockingWritesExist());
        UNIT_ASSERT_VALUES_EQUAL(true, store.IsCheckpointBeingCreated());
        UNIT_ASSERT_VALUES_EQUAL(true, store.IsRequestInProgress());
        UNIT_ASSERT_VALUES_EQUAL(true, store.HasRequestToExecute(&requestId));
        UNIT_ASSERT_VALUES_EQUAL(request.RequestId, requestId);
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointDeleted("checkpoint"));

        store.SetCheckpointRequestFinished(
            requestId,
            false,
            TString(),   // ShadowDiskId
            EShadowDiskState::None);
        auto checkpoints = store.GetActiveCheckpoints();
        UNIT_ASSERT_VALUES_EQUAL(0, checkpoints.size());
        UNIT_ASSERT_VALUES_EQUAL(false, store.DoesCheckpointBlockingWritesExist());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
        UNIT_ASSERT_VALUES_EQUAL(false, store.HasRequestToExecute(&requestId));
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointDeleted("checkpoint"));
    }

    Y_UNIT_TEST(CreateSuccess)
    {
        TCheckpointStore store({}, "disk-1");

        UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointDeleted("checkpoint"));

        const auto& request = store.CreateNew(
            "checkpoint",
            TInstant::Now(),
            ECheckpointRequestType::Create,
            ECheckpointType::Normal);
        store.SetCheckpointRequestSaved(request.RequestId);
        ui64 requestId = 0;
        UNIT_ASSERT_VALUES_EQUAL(true, store.HasRequestToExecute(&requestId));
        store.SetCheckpointRequestInProgress(requestId);
        store.SetCheckpointRequestFinished(
            requestId,
            true,
            TString(),   // ShadowDiskId
            EShadowDiskState::None);
        auto checkpoints = store.GetActiveCheckpoints();
        UNIT_ASSERT_VALUES_EQUAL(1, checkpoints.size());
        UNIT_ASSERT_VALUES_EQUAL(true, store.DoesCheckpointBlockingWritesExist());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
        UNIT_ASSERT_VALUES_EQUAL(false, store.HasRequestToExecute(&requestId));
        UNIT_ASSERT_EQUAL(
            ECheckpointData::DataPresent,
            checkpoints[request.CheckpointId].Data);
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointDeleted("checkpoint"));
    }

    Y_UNIT_TEST(CreateWithShadowDisk)
    {
        TCheckpointStore store({}, "disk-1");
        const TString checkpointId = "checkpoint";

        const auto& request = store.CreateNew(
            checkpointId,
            TInstant::Now(),
            ECheckpointRequestType::Create,
            ECheckpointType::Normal);
        store.SetCheckpointRequestSaved(request.RequestId);
        ui64 requestId = 0;
        UNIT_ASSERT_VALUES_EQUAL(true, store.HasRequestToExecute(&requestId));
        store.SetCheckpointRequestInProgress(requestId);
        store.SetCheckpointRequestFinished(
            requestId,
            true,
            "shadow-disk-id",
            EShadowDiskState::New);

        UNIT_ASSERT_VALUES_EQUAL(store.GetActiveCheckpoints().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            store.DoesCheckpointBlockingWritesExist());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
        UNIT_ASSERT_VALUES_EQUAL(false, store.HasRequestToExecute(&requestId));

        auto checkpoint = store.GetCheckpoint(checkpointId);
        UNIT_ASSERT(checkpoint.has_value());
        UNIT_ASSERT_VALUES_EQUAL(checkpoint->ShadowDiskId, "shadow-disk-id");
        UNIT_ASSERT_VALUES_EQUAL(
            checkpoint->Data,
            ECheckpointData::DataPresent);
        UNIT_ASSERT_VALUES_EQUAL(
            checkpoint->ShadowDiskState,
            EShadowDiskState::New);
        UNIT_ASSERT_VALUES_EQUAL(checkpoint->ProcessedBlockCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(checkpoint->TotalBlockCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(checkpoint->HasShadowActor, false);

        // Advance fill progress
        store.SetShadowDiskState(
            checkpointId,
            EShadowDiskState::Preparing,
            512,
            1024);

        checkpoint = store.GetCheckpoint(checkpointId);

        UNIT_ASSERT_VALUES_EQUAL(
            checkpoint->ShadowDiskState,
            EShadowDiskState::Preparing);
        UNIT_ASSERT_VALUES_EQUAL(checkpoint->ProcessedBlockCount, 512);
        UNIT_ASSERT_VALUES_EQUAL(checkpoint->TotalBlockCount, 1024);

        // Finish fill progress
        store.SetShadowDiskState(
            checkpointId,
            EShadowDiskState::Ready,
            1024,
            1024);

        checkpoint = store.GetCheckpoint(checkpointId);

        UNIT_ASSERT_VALUES_EQUAL(
            checkpoint->ShadowDiskState,
            EShadowDiskState::Ready);
        UNIT_ASSERT_VALUES_EQUAL(checkpoint->ProcessedBlockCount, 1024);
        UNIT_ASSERT_VALUES_EQUAL(checkpoint->TotalBlockCount, 1024);

        // Check actor created mark
        UNIT_ASSERT_VALUES_EQUAL(store.HasShadowActor(checkpointId), false);
        store.ShadowActorCreated(checkpointId);
        UNIT_ASSERT_VALUES_EQUAL(store.HasShadowActor(checkpointId), true);
        store.ShadowActorDestroyed(checkpointId);
        UNIT_ASSERT_VALUES_EQUAL(store.HasShadowActor(checkpointId), false);
    }

    Y_UNIT_TEST(RepeatRequests)
    {
        TCheckpointStore store({}, "disk-1");

        // repeat create checkpoint.
        for (int i = 0; i < 10; ++i) {
            const auto& request = store.CreateNew(
                "checkpoint",
                TInstant::Now(),
                ECheckpointRequestType::Create,
                ECheckpointType::Normal);

            store.SetCheckpointRequestSaved(request.RequestId);
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
            ui64 requestId = 0;
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                store.HasRequestToExecute(&requestId));

            store.SetCheckpointRequestInProgress(requestId);
            UNIT_ASSERT_VALUES_EQUAL(true, store.IsCheckpointBeingCreated());
            UNIT_ASSERT_VALUES_EQUAL(true, store.IsRequestInProgress());

            store.SetCheckpointRequestFinished(
                requestId,
                true,
                TString(),   // ShadowDiskId
                EShadowDiskState::None);
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
            auto checkpoints = store.GetActiveCheckpoints();
            UNIT_ASSERT_VALUES_EQUAL(1, checkpoints.size());
            UNIT_ASSERT_VALUES_EQUAL(true, store.DoesCheckpointBlockingWritesExist());
            UNIT_ASSERT_EQUAL(
                ECheckpointData::DataPresent,
                checkpoints[request.CheckpointId].Data);
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                store.DoesCheckpointHaveData(request.CheckpointId));
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointDeleted("checkpoint"));
        }

        // repeat delete data.
        for (int i = 0; i < 10; ++i) {
            const auto& request = store.CreateNew(
                "checkpoint",
                TInstant::Now(),
                ECheckpointRequestType::DeleteData,
                ECheckpointType::Normal);

            store.SetCheckpointRequestSaved(request.RequestId);
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
            ui64 requestId = 0;
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                store.HasRequestToExecute(&requestId));

            store.SetCheckpointRequestInProgress(requestId);
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
            UNIT_ASSERT_VALUES_EQUAL(true, store.IsRequestInProgress());

            store.SetCheckpointRequestFinished(
                requestId,
                true,
                TString(),   // ShadowDiskId
                EShadowDiskState::None);
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
            auto checkpoints = store.GetActiveCheckpoints();
            UNIT_ASSERT_VALUES_EQUAL(1, checkpoints.size());
            UNIT_ASSERT_EQUAL(
                ECheckpointData::DataDeleted,
                checkpoints[request.CheckpointId].Data);
            UNIT_ASSERT_VALUES_EQUAL(
                false,
                store.DoesCheckpointBlockingWritesExist());
            UNIT_ASSERT_VALUES_EQUAL(
                false,
                store.DoesCheckpointHaveData(request.CheckpointId));
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointDeleted("checkpoint"));
        }

        // repeat delete checkpoint.
        for (int i = 0; i < 10; ++i) {
            const auto& request = store.CreateNew(
                "checkpoint",
                TInstant::Now(),
                ECheckpointRequestType::Delete,
                ECheckpointType::Normal);
            store.SetCheckpointRequestSaved(request.RequestId);
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
            ui64 requestId = 0;
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                store.HasRequestToExecute(&requestId));

            store.SetCheckpointRequestInProgress(requestId);
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
            UNIT_ASSERT_VALUES_EQUAL(true, store.IsRequestInProgress());

            store.SetCheckpointRequestFinished(
                requestId,
                true,
                TString(),   // ShadowDiskId
                EShadowDiskState::None);
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
            auto checkpoints = store.GetActiveCheckpoints();
            UNIT_ASSERT_VALUES_EQUAL(0, checkpoints.size());
            UNIT_ASSERT_VALUES_EQUAL(
                false,
                store.DoesCheckpointBlockingWritesExist());
            UNIT_ASSERT_VALUES_EQUAL(true, store.IsCheckpointDeleted("checkpoint"));
        }
    }

    Y_UNIT_TEST(MultiCheckpoint)
    {
        TCheckpointStore store({}, "disk-1");

        auto createCheckpoint = [&](TString checkpointId)
        {
            const auto& request = store.CreateNew(
                checkpointId,
                TInstant::Now(),
                ECheckpointRequestType::Create,
                ECheckpointType::Normal);

            store.SetCheckpointRequestSaved(request.RequestId);
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
            ui64 requestId = 0;
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                store.HasRequestToExecute(&requestId));

            store.SetCheckpointRequestInProgress(requestId);
            UNIT_ASSERT_VALUES_EQUAL(true, store.IsCheckpointBeingCreated());
            UNIT_ASSERT_VALUES_EQUAL(true, store.IsRequestInProgress());

            store.SetCheckpointRequestFinished(
                requestId,
                true,
                TString(),   // ShadowDiskId
                EShadowDiskState::None);
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
            auto checkpoints = store.GetActiveCheckpoints();
            UNIT_ASSERT_VALUES_EQUAL(true, store.DoesCheckpointBlockingWritesExist());
            UNIT_ASSERT_EQUAL(
                ECheckpointData::DataPresent,
                checkpoints[request.CheckpointId].Data);
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                store.DoesCheckpointHaveData(request.CheckpointId));
        };

        auto createCheckpointWithoutData = [&](TString checkpointId)
        {
            const auto& request = store.CreateNew(
                checkpointId,
                TInstant::Now(),
                ECheckpointRequestType::CreateWithoutData,
                ECheckpointType::Normal);

            store.SetCheckpointRequestSaved(request.RequestId);
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
            ui64 requestId = 0;
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                store.HasRequestToExecute(&requestId));

            store.SetCheckpointRequestInProgress(requestId);
            UNIT_ASSERT_VALUES_EQUAL(true, store.IsCheckpointBeingCreated());
            UNIT_ASSERT_VALUES_EQUAL(true, store.IsRequestInProgress());

            store.SetCheckpointRequestFinished(
                requestId,
                true,
                TString(),   // ShadowDiskId
                EShadowDiskState::None);
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
            auto checkpoints = store.GetActiveCheckpoints();
            UNIT_ASSERT_EQUAL(
                ECheckpointData::DataDeleted,
                checkpoints[request.CheckpointId].Data);
            UNIT_ASSERT_VALUES_EQUAL(
                false,
                store.DoesCheckpointHaveData(request.CheckpointId));
        };

        auto deleteCheckpointData = [&](TString checkpointId)
        {
            const auto& request = store.CreateNew(
                checkpointId,
                TInstant::Now(),
                ECheckpointRequestType::DeleteData,
                ECheckpointType::Normal);

            store.SetCheckpointRequestSaved(request.RequestId);
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
            ui64 requestId = 0;
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                store.HasRequestToExecute(&requestId));

            store.SetCheckpointRequestInProgress(requestId);
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
            UNIT_ASSERT_VALUES_EQUAL(true, store.IsRequestInProgress());

            store.SetCheckpointRequestFinished(
                requestId,
                true,
                TString(),   // ShadowDiskId
                EShadowDiskState::None);
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
            auto checkpoints = store.GetActiveCheckpoints();
            UNIT_ASSERT_EQUAL(
                ECheckpointData::DataDeleted,
                checkpoints[request.CheckpointId].Data);
            UNIT_ASSERT_VALUES_EQUAL(
                false,
                store.DoesCheckpointHaveData(request.CheckpointId));
        };

        auto deleteCheckpoint = [&](TString checkpointId)
        {
            const auto& request = store.CreateNew(
                checkpointId,
                TInstant::Now(),
                ECheckpointRequestType::Delete,
                ECheckpointType::Normal);
            store.SetCheckpointRequestSaved(request.RequestId);
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
            ui64 requestId = 0;
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                store.HasRequestToExecute(&requestId));

            store.SetCheckpointRequestInProgress(requestId);
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
            UNIT_ASSERT_VALUES_EQUAL(true, store.IsRequestInProgress());

            store.SetCheckpointRequestFinished(
                requestId,
                true,
                TString(),   // ShadowDiskId
                EShadowDiskState::None);
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());

            UNIT_ASSERT_VALUES_EQUAL(true, store.IsCheckpointDeleted(checkpointId));
        };

        {
            createCheckpoint("checkpoint-1");
            UNIT_ASSERT_VALUES_EQUAL(1, store.GetCheckpointsWithData().size());
            UNIT_ASSERT_VALUES_EQUAL(1, store.GetActiveCheckpoints().size());
            createCheckpoint("checkpoint-2");
            UNIT_ASSERT_VALUES_EQUAL(2, store.GetCheckpointsWithData().size());
            UNIT_ASSERT_VALUES_EQUAL(2, store.GetActiveCheckpoints().size());
            createCheckpoint("checkpoint-3");
            UNIT_ASSERT_VALUES_EQUAL(3, store.GetCheckpointsWithData().size());
            UNIT_ASSERT_VALUES_EQUAL(3, store.GetActiveCheckpoints().size());

            deleteCheckpointData("checkpoint-1");
            UNIT_ASSERT_VALUES_EQUAL(true, store.DoesCheckpointBlockingWritesExist());
            UNIT_ASSERT_VALUES_EQUAL(2, store.GetCheckpointsWithData().size());
            UNIT_ASSERT_VALUES_EQUAL(3, store.GetActiveCheckpoints().size());
            deleteCheckpointData("checkpoint-2");
            UNIT_ASSERT_VALUES_EQUAL(true, store.DoesCheckpointBlockingWritesExist());
            UNIT_ASSERT_VALUES_EQUAL(1, store.GetCheckpointsWithData().size());
            UNIT_ASSERT_VALUES_EQUAL(3, store.GetActiveCheckpoints().size());

            deleteCheckpointData("checkpoint-3");
            UNIT_ASSERT_VALUES_EQUAL(
                false,
                store.DoesCheckpointBlockingWritesExist());
            UNIT_ASSERT_VALUES_EQUAL(0, store.GetCheckpointsWithData().size());
            UNIT_ASSERT_VALUES_EQUAL(3, store.GetActiveCheckpoints().size());

            deleteCheckpoint("checkpoint-1");
            UNIT_ASSERT_VALUES_EQUAL(2, store.GetActiveCheckpoints().size());
            deleteCheckpoint("checkpoint-2");
            UNIT_ASSERT_VALUES_EQUAL(1, store.GetActiveCheckpoints().size());
            deleteCheckpoint("checkpoint-3");
            UNIT_ASSERT_VALUES_EQUAL(0, store.GetActiveCheckpoints().size());

            createCheckpointWithoutData("checkpoint-4");
            UNIT_ASSERT_VALUES_EQUAL(false, store.DoesCheckpointBlockingWritesExist());
            UNIT_ASSERT_VALUES_EQUAL(0, store.GetCheckpointsWithData().size());
            UNIT_ASSERT_VALUES_EQUAL(1, store.GetActiveCheckpoints().size());
            createCheckpoint("checkpoint-1");
            UNIT_ASSERT_VALUES_EQUAL(true, store.DoesCheckpointBlockingWritesExist());
            UNIT_ASSERT_VALUES_EQUAL(1, store.GetCheckpointsWithData().size());
            UNIT_ASSERT_VALUES_EQUAL(2, store.GetActiveCheckpoints().size());
            deleteCheckpointData("checkpoint-1");
            UNIT_ASSERT_VALUES_EQUAL(false, store.DoesCheckpointBlockingWritesExist());
            UNIT_ASSERT_VALUES_EQUAL(0, store.GetCheckpointsWithData().size());
            UNIT_ASSERT_VALUES_EQUAL(2, store.GetActiveCheckpoints().size());
            deleteCheckpoint("checkpoint-4");
            UNIT_ASSERT_VALUES_EQUAL(1, store.GetActiveCheckpoints().size());
            deleteCheckpoint("checkpoint-1");
            UNIT_ASSERT_VALUES_EQUAL(false, store.DoesCheckpointBlockingWritesExist());
            UNIT_ASSERT_VALUES_EQUAL(0, store.GetCheckpointsWithData().size());
            UNIT_ASSERT_VALUES_EQUAL(0, store.GetActiveCheckpoints().size());

            for (TString checkpointId : {"checkpoint-1", "checkpoint-2", "checkpoint-3", "checkpoint-4"}) {
                UNIT_ASSERT_VALUES_EQUAL(true, store.IsCheckpointDeleted(checkpointId));
            }
        }
    }

    Y_UNIT_TEST(CheckpointRequestValidation)
    {
        TCheckpointStore store({}, "disk-1");

        auto executeRequest = [&](
            TString checkpointId,
            ECheckpointRequestType requestType,
            ECheckpointType checkpointType,
            ECheckpointRequestValidityStatus expectedValidityStatus)
        {
            const auto& request = store.CreateNew(
                checkpointId,
                TInstant::Now(),
                requestType,
                checkpointType);

            TString message;
            auto validityStatus = store.ValidateCheckpointRequest(checkpointId, requestType, checkpointType, &message);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(expectedValidityStatus), static_cast<int>(validityStatus));

            store.SetCheckpointRequestSaved(request.RequestId);
            store.SetCheckpointRequestInProgress(request.RequestId);
            bool success = validityStatus != ECheckpointRequestValidityStatus::Invalid;
            store.SetCheckpointRequestFinished(
                request.RequestId,
                success,
                TString(),   // ShadowDiskId
                EShadowDiskState::None);
        };

        struct Step {
            ECheckpointRequestType requestType;
            ECheckpointRequestValidityStatus expectedValidityStatus;
        };

        int checkpointIndex = 0;

        auto executeRequests = [&](ECheckpointType checkpointType, const TVector<Step>& steps)
        {
            TString checkpointId = TStringBuilder() << "checkpoint_" << checkpointIndex++;

            for (const Step& step : steps) {
                executeRequest(checkpointId, step.requestType, checkpointType, step.expectedValidityStatus);
            }
        };

        executeRequests(ECheckpointType::Normal, TVector<Step>{
            {ECheckpointRequestType::DeleteData, ECheckpointRequestValidityStatus::Invalid},
            {ECheckpointRequestType::Delete, ECheckpointRequestValidityStatus::Already},
            {ECheckpointRequestType::Create, ECheckpointRequestValidityStatus::Ok},
            {ECheckpointRequestType::Create, ECheckpointRequestValidityStatus::Already},
            {ECheckpointRequestType::DeleteData, ECheckpointRequestValidityStatus::Ok},
            {ECheckpointRequestType::Create, ECheckpointRequestValidityStatus::Invalid},
            {ECheckpointRequestType::DeleteData, ECheckpointRequestValidityStatus::Already},
            {ECheckpointRequestType::Delete, ECheckpointRequestValidityStatus::Ok},
            {ECheckpointRequestType::Delete, ECheckpointRequestValidityStatus::Already},
            {ECheckpointRequestType::Create, ECheckpointRequestValidityStatus::Invalid},
            {ECheckpointRequestType::DeleteData, ECheckpointRequestValidityStatus::Invalid},
        });

        executeRequests(ECheckpointType::Normal, TVector<Step>{
            {ECheckpointRequestType::Create, ECheckpointRequestValidityStatus::Ok},
            {ECheckpointRequestType::CreateWithoutData, ECheckpointRequestValidityStatus::Invalid},
            {ECheckpointRequestType::CreateWithoutData, ECheckpointRequestValidityStatus::Invalid},
            {ECheckpointRequestType::DeleteData, ECheckpointRequestValidityStatus::Ok},
            {ECheckpointRequestType::CreateWithoutData, ECheckpointRequestValidityStatus::Already},
            {ECheckpointRequestType::CreateWithoutData, ECheckpointRequestValidityStatus::Already},
            {ECheckpointRequestType::Delete, ECheckpointRequestValidityStatus::Ok},
            {ECheckpointRequestType::CreateWithoutData, ECheckpointRequestValidityStatus::Invalid},
        });

        executeRequests(ECheckpointType::Normal, TVector<Step>{
            {ECheckpointRequestType::CreateWithoutData, ECheckpointRequestValidityStatus::Ok},
            {ECheckpointRequestType::CreateWithoutData, ECheckpointRequestValidityStatus::Already},
            {ECheckpointRequestType::Create, ECheckpointRequestValidityStatus::Invalid},
            {ECheckpointRequestType::DeleteData, ECheckpointRequestValidityStatus::Already},
            {ECheckpointRequestType::CreateWithoutData, ECheckpointRequestValidityStatus::Already},
        });

        for (auto checkpointType : {
            ECheckpointType::Normal,
            ECheckpointType::Light})
        {
            executeRequests(checkpointType, TVector<Step>{
                {ECheckpointRequestType::Delete, ECheckpointRequestValidityStatus::Already},
                {ECheckpointRequestType::Create, ECheckpointRequestValidityStatus::Ok},
                {ECheckpointRequestType::Create, ECheckpointRequestValidityStatus::Already},
                {ECheckpointRequestType::Delete, ECheckpointRequestValidityStatus::Ok},
                {ECheckpointRequestType::Create, ECheckpointRequestValidityStatus::Invalid},
                {ECheckpointRequestType::Delete, ECheckpointRequestValidityStatus::Already},
            });
        }

        executeRequests(ECheckpointType::Light, TVector<Step>{
            {ECheckpointRequestType::DeleteData, ECheckpointRequestValidityStatus::Invalid},
            {ECheckpointRequestType::Create, ECheckpointRequestValidityStatus::Ok},
            {ECheckpointRequestType::DeleteData, ECheckpointRequestValidityStatus::Invalid},
            {ECheckpointRequestType::Delete, ECheckpointRequestValidityStatus::Ok},
            {ECheckpointRequestType::DeleteData, ECheckpointRequestValidityStatus::Invalid},
        });

        executeRequests(ECheckpointType::Light, TVector<Step>{
            {ECheckpointRequestType::CreateWithoutData, ECheckpointRequestValidityStatus::Invalid},
            {ECheckpointRequestType::Create, ECheckpointRequestValidityStatus::Ok},
            {ECheckpointRequestType::CreateWithoutData, ECheckpointRequestValidityStatus::Invalid},
        });

        executeRequest(
            "",
            ECheckpointRequestType::Create,
            ECheckpointType::Normal,
            ECheckpointRequestValidityStatus::Invalid);
        executeRequest(
            "",
            ECheckpointRequestType::Create,
            ECheckpointType::Light,
            ECheckpointRequestValidityStatus::Invalid);
    }

    Y_UNIT_TEST(PostponedCheckpointRequestsQueue)
    {
        TPostponedCheckpointRequestsQueue<ui64> postponedRequests;

        postponedRequests.AddPostponedRequest("ch_1", {1, 1});
        postponedRequests.AddPostponedRequest("ch_1", {2, 2});
        postponedRequests.AddPostponedRequest("ch_2", {3, 3});

        UNIT_ASSERT_VALUES_EQUAL(true, postponedRequests.HasPostponedRequest("ch_1"));
        UNIT_ASSERT_VALUES_EQUAL(true, postponedRequests.HasPostponedRequest("ch_2"));
        UNIT_ASSERT_VALUES_EQUAL(false, postponedRequests.HasPostponedRequest("ch_0"));

        auto request = postponedRequests.GetPostponedRequest("ch_1");
        UNIT_ASSERT_VALUES_EQUAL(true, request.has_value());
        UNIT_ASSERT_VALUES_EQUAL(1, request->RequestId);

        postponedRequests.RemovePostponedRequest("ch_1");
        UNIT_ASSERT_VALUES_EQUAL(true, postponedRequests.HasPostponedRequest("ch_1"));

        request = postponedRequests.GetPostponedRequest("ch_1");
        UNIT_ASSERT_VALUES_EQUAL(true, request.has_value());
        UNIT_ASSERT_VALUES_EQUAL(2, request->RequestId);

        postponedRequests.RemovePostponedRequest("ch_1");
        UNIT_ASSERT_VALUES_EQUAL(false, postponedRequests.HasPostponedRequest("ch_1"));

        postponedRequests.AddPostponedRequest("ch_1", {4, 4});
        UNIT_ASSERT_VALUES_EQUAL(true, postponedRequests.HasPostponedRequest("ch_1"));

        request = postponedRequests.GetPostponedRequest("ch_0");
        UNIT_ASSERT_VALUES_EQUAL(false, request.has_value());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
