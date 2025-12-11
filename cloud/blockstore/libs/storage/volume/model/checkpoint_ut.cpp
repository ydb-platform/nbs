#include "checkpoint.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCheckpointStore)
{
    Y_UNIT_TEST(EmptyPersistentState)
    {
        TCheckpointStore store({}, "disk-1");

        UNIT_ASSERT_VALUES_EQUAL(
            false,
            store.DoesCheckpointBlockingWritesExist());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
        UNIT_ASSERT_VALUES_EQUAL(0, store.GetRequestIdsToProcess().size());
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
                ECheckpointType::Normal,
                false},

            TCheckpointRequest{
                2,
                "checkpoint-2",
                TInstant::Now(),
                ECheckpointRequestType::Create,
                ECheckpointRequestState::Completed,
                ECheckpointType::Normal,
                false},

            TCheckpointRequest{
                3,
                "checkpoint-1",
                TInstant::Now(),
                ECheckpointRequestType::Create,
                ECheckpointRequestState::Completed,
                ECheckpointType::Normal,
                false},
            TCheckpointRequest{
                4,
                "checkpoint-1",
                TInstant::Now(),
                ECheckpointRequestType::DeleteData,
                ECheckpointRequestState::Completed,
                ECheckpointType::Normal,
                false},

            TCheckpointRequest{
                5,
                "checkpoint-4",
                TInstant::Now(),
                ECheckpointRequestType::Create,
                ECheckpointRequestState::Completed,
                ECheckpointType::Normal,
                false},
            TCheckpointRequest{
                6,
                "checkpoint-4",
                TInstant::Now(),
                ECheckpointRequestType::DeleteData,
                ECheckpointRequestState::Completed,
                ECheckpointType::Normal,
                false},
            TCheckpointRequest{
                7,
                "checkpoint-4",
                TInstant::Now(),
                ECheckpointRequestType::Delete,
                ECheckpointRequestState::Completed,
                ECheckpointType::Normal,
                false},

            TCheckpointRequest{
                10,
                "checkpoint-3",
                TInstant::Now(),
                ECheckpointRequestType::Create,
                ECheckpointRequestState::Saved,
                ECheckpointType::Normal,
                false},

            TCheckpointRequest{
                11,
                "checkpoint-4",
                TInstant::Now(),
                ECheckpointRequestType::Create,
                ECheckpointRequestState::Received,
                ECheckpointType::Normal,
                false},

            TCheckpointRequest{
                8,
                "checkpoint-4",
                TInstant::Now(),
                ECheckpointRequestType::DeleteData,
                ECheckpointRequestState::Completed,
                ECheckpointType::Normal,
                false},
            TCheckpointRequest{
                9,
                "checkpoint-4",
                TInstant::Now(),
                ECheckpointRequestType::Delete,
                ECheckpointRequestState::Completed,
                ECheckpointType::Normal,
                false},

            TCheckpointRequest{
                12,
                "checkpoint-5",
                TInstant::Now(),
                ECheckpointRequestType::CreateWithoutData,
                ECheckpointRequestState::Completed,
                ECheckpointType::Normal,
                false},

            TCheckpointRequest{
                13,
                "checkpoint-6",
                TInstant::Now(),
                ECheckpointRequestType::Create,
                ECheckpointRequestState::Completed,
                ECheckpointType::Normal,
                "shadow-disk-6",
                EShadowDiskState::Preparing,
                512,
                TString()},

            TCheckpointRequest{
                14,
                "checkpoint-7",
                TInstant::Now(),
                ECheckpointRequestType::Create,
                ECheckpointRequestState::Completed,
                ECheckpointType::Normal,
                TString(),
                EShadowDiskState::Error,
                0,
                "disk-error"},
        };
        TCheckpointStore store(
            TVector<TCheckpointRequest>{
                std::begin(initialState),
                std::end(initialState)},
            "disk-1");

        UNIT_ASSERT_VALUES_EQUAL(
            true,
            store.DoesCheckpointBlockingWritesExist());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());

        auto requestsToProcess = store.GetRequestIdsToProcess();
        UNIT_ASSERT_VALUES_EQUAL(2, requestsToProcess.size());
        UNIT_ASSERT_VALUES_EQUAL(10, requestsToProcess[0]);
        UNIT_ASSERT_VALUES_EQUAL(11, requestsToProcess[1]);

        auto checkpoints = store.GetActiveCheckpoints();
        UNIT_ASSERT_VALUES_EQUAL(5, checkpoints.size());
        UNIT_ASSERT(checkpoints.contains("checkpoint-1"));
        UNIT_ASSERT(checkpoints.contains("checkpoint-2"));
        UNIT_ASSERT(checkpoints.contains("checkpoint-5"));
        UNIT_ASSERT(checkpoints.contains("checkpoint-6"));
        UNIT_ASSERT(checkpoints.contains("checkpoint-7"));

        UNIT_ASSERT(!store.DoesCheckpointHaveData("checkpoint-1"));
        UNIT_ASSERT(store.DoesCheckpointHaveData("checkpoint-2"));
        UNIT_ASSERT(!store.DoesCheckpointHaveData("checkpoint-5"));
        UNIT_ASSERT(store.DoesCheckpointHaveData("checkpoint-6"));
        UNIT_ASSERT(store.DoesCheckpointHaveData("checkpoint-7"));

        UNIT_ASSERT(!store.IsCheckpointDeleted("checkpoint-1"));
        UNIT_ASSERT(!store.IsCheckpointDeleted("checkpoint-2"));
        UNIT_ASSERT(!store.IsCheckpointDeleted("checkpoint-3"));
        UNIT_ASSERT(store.IsCheckpointDeleted("checkpoint-4"));
        UNIT_ASSERT(!store.IsCheckpointDeleted("checkpoint-5"));
        UNIT_ASSERT(!store.IsCheckpointDeleted("checkpoint-6"));
        UNIT_ASSERT(!store.IsCheckpointDeleted("checkpoint-7"));

        // The checkpoint without the shadow disk has the correct state.
        UNIT_ASSERT(!checkpoints["checkpoint-1"].IsShadowDiskBased());
        UNIT_ASSERT_VALUES_EQUAL(
            checkpoints["checkpoint-1"].ShadowDiskState,
            EShadowDiskState::None);

        // Checkpoint with the shadow disk loads the state.
        UNIT_ASSERT(checkpoints["checkpoint-6"].IsShadowDiskBased());
        UNIT_ASSERT_VALUES_EQUAL(
            checkpoints["checkpoint-6"].ShadowDiskId,
            "shadow-disk-6");
        UNIT_ASSERT_VALUES_EQUAL(
            checkpoints["checkpoint-6"].ShadowDiskState,
            EShadowDiskState::Preparing);
        UNIT_ASSERT_VALUES_EQUAL(
            checkpoints["checkpoint-6"].ProcessedBlockCount,
            512);

        // Checkpoint with disk error.
        UNIT_ASSERT(checkpoints["checkpoint-7"].IsShadowDiskBased());
        UNIT_ASSERT_VALUES_EQUAL(
            EShadowDiskState::Error,
            checkpoints["checkpoint-7"].ShadowDiskState);
    }

    Y_UNIT_TEST(CreateFail)
    {
        TCheckpointStore store({}, "disk-1");

        const auto& request = store.MakeCreateCheckpointRequest(
            "checkpoint",
            TInstant::Now(),
            ECheckpointRequestType::Create,
            ECheckpointType::Normal,
            false);

        UNIT_ASSERT_VALUES_EQUAL(
            false,
            store.DoesCheckpointBlockingWritesExist());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
        ui64 requestId = 0;
        UNIT_ASSERT_VALUES_EQUAL(1, store.GetRequestIdsToProcess().size());
        requestId = store.GetRequestIdsToProcess()[0];
        UNIT_ASSERT_VALUES_EQUAL(request.RequestId, requestId);
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            store.IsCheckpointDeleted("checkpoint"));

        store.SetCheckpointRequestSaved(request.RequestId);
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            store.DoesCheckpointBlockingWritesExist());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
        UNIT_ASSERT_VALUES_EQUAL(1, store.GetRequestIdsToProcess().size());
        UNIT_ASSERT_VALUES_EQUAL(request.RequestId, requestId);
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            store.IsCheckpointDeleted("checkpoint"));

        store.SetCheckpointRequestInProgress(requestId);
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            store.DoesCheckpointBlockingWritesExist());
        UNIT_ASSERT_VALUES_EQUAL(true, store.IsCheckpointBeingCreated());
        UNIT_ASSERT_VALUES_EQUAL(true, store.IsRequestInProgress());
        UNIT_ASSERT_VALUES_EQUAL(1, store.GetRequestIdsToProcess().size());
        UNIT_ASSERT_VALUES_EQUAL(request.RequestId, requestId);
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            store.IsCheckpointDeleted("checkpoint"));

        store.SetCheckpointRequestFinished(
            requestId,
            false,
            TString(),   // ShadowDiskId
            EShadowDiskState::None);
        auto checkpoints = store.GetActiveCheckpoints();
        UNIT_ASSERT_VALUES_EQUAL(0, checkpoints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            store.DoesCheckpointBlockingWritesExist());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
        UNIT_ASSERT_VALUES_EQUAL(0, store.GetRequestIdsToProcess().size());
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            store.IsCheckpointDeleted("checkpoint"));
    }

    Y_UNIT_TEST(CreateSuccess)
    {
        TCheckpointStore store({}, "disk-1");

        UNIT_ASSERT_VALUES_EQUAL(
            false,
            store.IsCheckpointDeleted("checkpoint"));

        const auto& request = store.MakeCreateCheckpointRequest(
            "checkpoint",
            TInstant::Now(),
            ECheckpointRequestType::Create,
            ECheckpointType::Normal,
            false);
        store.SetCheckpointRequestSaved(request.RequestId);
        ui64 requestId = 0;
        UNIT_ASSERT_VALUES_EQUAL(1, store.GetRequestIdsToProcess().size());
        requestId = store.GetRequestIdsToProcess()[0];
        store.SetCheckpointRequestInProgress(requestId);
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            store.DoesCheckpointBlockingWritesExist());
        store.SetCheckpointRequestFinished(
            requestId,
            true,
            TString(),   // ShadowDiskId
            EShadowDiskState::None);
        auto checkpoints = store.GetActiveCheckpoints();
        UNIT_ASSERT_VALUES_EQUAL(1, checkpoints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            store.DoesCheckpointBlockingWritesExist());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
        UNIT_ASSERT_VALUES_EQUAL(0, store.GetRequestIdsToProcess().size());
        UNIT_ASSERT_EQUAL(
            ECheckpointData::DataPresent,
            checkpoints[request.CheckpointId].Data);
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            store.IsCheckpointDeleted("checkpoint"));
    }

    Y_UNIT_TEST(CreateWithShadowDisk)
    {
        TCheckpointStore store({}, "disk-1");
        const TString checkpointId = "checkpoint";

        const auto& request = store.MakeCreateCheckpointRequest(
            checkpointId,
            TInstant::Now(),
            ECheckpointRequestType::Create,
            ECheckpointType::Normal,
            true);
        store.SetCheckpointRequestSaved(request.RequestId);
        ui64 requestId = 0;
        UNIT_ASSERT_VALUES_EQUAL(1, store.GetRequestIdsToProcess().size());
        requestId = store.GetRequestIdsToProcess()[0];
        store.SetCheckpointRequestInProgress(requestId);
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            store.DoesCheckpointBlockingWritesExist());
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
        UNIT_ASSERT_VALUES_EQUAL(0, store.GetRequestIdsToProcess().size());

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
        UNIT_ASSERT_VALUES_EQUAL(checkpoint->HasShadowActor, false);

        // Advance fill progress
        store.SetShadowDiskState(
            checkpointId,
            EShadowDiskState::Preparing,
            512);

        checkpoint = store.GetCheckpoint(checkpointId);

        UNIT_ASSERT_VALUES_EQUAL(
            checkpoint->ShadowDiskState,
            EShadowDiskState::Preparing);
        UNIT_ASSERT_VALUES_EQUAL(checkpoint->ProcessedBlockCount, 512);

        // Finish fill progress
        store.SetShadowDiskState(checkpointId, EShadowDiskState::Ready, 1024);

        checkpoint = store.GetCheckpoint(checkpointId);

        UNIT_ASSERT_VALUES_EQUAL(
            checkpoint->ShadowDiskState,
            EShadowDiskState::Ready);
        UNIT_ASSERT_VALUES_EQUAL(checkpoint->ProcessedBlockCount, 1024);

        // Check actor created mark
        UNIT_ASSERT_VALUES_EQUAL(store.HasShadowActor(checkpointId), false);
        store.ShadowActorCreated(checkpointId);
        UNIT_ASSERT_VALUES_EQUAL(store.HasShadowActor(checkpointId), true);
        store.ShadowActorDestroyed(checkpointId);
        UNIT_ASSERT_VALUES_EQUAL(store.HasShadowActor(checkpointId), false);
    }

    Y_UNIT_TEST(SetInProgressBeforeSave)
    {
        TCheckpointStore store({}, "disk-1");

        const auto& request = store.MakeCreateCheckpointRequest(
            "checkpoint",
            TInstant::Now(),
            ECheckpointRequestType::Create,
            ECheckpointType::Normal,
            false);

        ui64 requestId = request.RequestId;

        store.SetCheckpointRequestInProgress(requestId);
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            store.DoesCheckpointBlockingWritesExist());
        UNIT_ASSERT_VALUES_EQUAL(true, store.IsCheckpointBeingCreated());
        UNIT_ASSERT_VALUES_EQUAL(true, store.IsRequestInProgress());
        UNIT_ASSERT_VALUES_EQUAL(1, store.GetRequestIdsToProcess().size());
        UNIT_ASSERT_VALUES_EQUAL(request.RequestId, requestId);

        store.SetCheckpointRequestSaved(request.RequestId);
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            store.DoesCheckpointBlockingWritesExist());
        UNIT_ASSERT_VALUES_EQUAL(true, store.IsCheckpointBeingCreated());
        UNIT_ASSERT_VALUES_EQUAL(true, store.IsRequestInProgress());
        UNIT_ASSERT_VALUES_EQUAL(1, store.GetRequestIdsToProcess().size());
        UNIT_ASSERT_VALUES_EQUAL(request.RequestId, requestId);

        store.SetCheckpointRequestFinished(
            requestId,
            true,
            TString(),   // ShadowDiskId
            EShadowDiskState::None);
        auto checkpoints = store.GetActiveCheckpoints();
        UNIT_ASSERT_VALUES_EQUAL(1, checkpoints.size());
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            store.DoesCheckpointBlockingWritesExist());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
        UNIT_ASSERT_VALUES_EQUAL(0, store.GetRequestIdsToProcess().size());
        UNIT_ASSERT_EQUAL(
            ECheckpointData::DataPresent,
            checkpoints[request.CheckpointId].Data);
    }

    Y_UNIT_TEST(RemoveCheckpointRequest)
    {
        TCheckpointStore store({}, "disk-1");

        const auto& request = store.MakeCreateCheckpointRequest(
            "checkpoint",
            TInstant::Now(),
            ECheckpointRequestType::Create,
            ECheckpointType::Normal,
            false);
        ui64 requestId = 0;
        UNIT_ASSERT_VALUES_EQUAL(1, store.GetRequestIdsToProcess().size());
        requestId = store.GetRequestIdsToProcess()[0];
        UNIT_ASSERT_VALUES_EQUAL(request.RequestId, requestId);

        store.RemoveCheckpointRequest(requestId);
        UNIT_ASSERT_VALUES_EQUAL(0, store.GetRequestIdsToProcess().size());
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            store.DoesCheckpointBlockingWritesExist());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
        UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            store.IsCheckpointDeleted("checkpoint"));
    }

    Y_UNIT_TEST(RepeatRequests)
    {
        TCheckpointStore store({}, "disk-1");

        // repeat create checkpoint.
        for (int i = 0; i < 10; ++i) {
            const auto& request = store.MakeCreateCheckpointRequest(
                "checkpoint",
                TInstant::Now(),
                ECheckpointRequestType::Create,
                ECheckpointType::Normal,
                false);

            store.SetCheckpointRequestSaved(request.RequestId);
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
            ui64 requestId = 0;
            UNIT_ASSERT_VALUES_EQUAL(1, store.GetRequestIdsToProcess().size());
            requestId = store.GetRequestIdsToProcess()[0];

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
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                store.DoesCheckpointBlockingWritesExist());
            UNIT_ASSERT_EQUAL(
                ECheckpointData::DataPresent,
                checkpoints[request.CheckpointId].Data);
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                store.DoesCheckpointHaveData(request.CheckpointId));
            UNIT_ASSERT_VALUES_EQUAL(
                false,
                store.IsCheckpointDeleted("checkpoint"));
        }

        // repeat delete data.
        for (int i = 0; i < 10; ++i) {
            const auto& request = store.MakeDeleteCheckpointDataRequest(
                "checkpoint",
                TInstant::Now());

            store.SetCheckpointRequestSaved(request.RequestId);
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
            ui64 requestId = 0;
            UNIT_ASSERT_VALUES_EQUAL(1, store.GetRequestIdsToProcess().size());
            requestId = store.GetRequestIdsToProcess()[0];

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
        }
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            store.IsCheckpointDeleted("checkpoint"));

        // repeat delete checkpoint.
        for (int i = 0; i < 10; ++i) {
            const auto& request = store.MakeDeleteCheckpointRequest(
                "checkpoint",
                TInstant::Now());
            store.SetCheckpointRequestSaved(request.RequestId);
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
            ui64 requestId = 0;
            UNIT_ASSERT_VALUES_EQUAL(1, store.GetRequestIdsToProcess().size());
            requestId = store.GetRequestIdsToProcess()[0];

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
        }
        UNIT_ASSERT_VALUES_EQUAL(true, store.IsCheckpointDeleted("checkpoint"));
    }

    Y_UNIT_TEST(MultiCheckpoint)
    {
        TCheckpointStore store({}, "disk-1");

        auto createCheckpoint = [&](TString checkpointId)
        {
            const auto& request = store.MakeCreateCheckpointRequest(
                checkpointId,
                TInstant::Now(),
                ECheckpointRequestType::Create,
                ECheckpointType::Normal,
                false);

            store.SetCheckpointRequestSaved(request.RequestId);
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
            ui64 requestId = 0;
            UNIT_ASSERT_VALUES_EQUAL(1, store.GetRequestIdsToProcess().size());
            requestId = store.GetRequestIdsToProcess()[0];

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
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                store.DoesCheckpointBlockingWritesExist());
            UNIT_ASSERT_EQUAL(
                ECheckpointData::DataPresent,
                checkpoints[request.CheckpointId].Data);
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                store.DoesCheckpointHaveData(request.CheckpointId));
        };

        auto createCheckpointWithoutData = [&](TString checkpointId)
        {
            const auto& request = store.MakeCreateCheckpointRequest(
                checkpointId,
                TInstant::Now(),
                ECheckpointRequestType::CreateWithoutData,
                ECheckpointType::Normal,
                false);

            store.SetCheckpointRequestSaved(request.RequestId);
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
            ui64 requestId = 0;
            UNIT_ASSERT_VALUES_EQUAL(1, store.GetRequestIdsToProcess().size());
            requestId = store.GetRequestIdsToProcess()[0];

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
            const auto& request = store.MakeDeleteCheckpointDataRequest(
                checkpointId,
                TInstant::Now());

            store.SetCheckpointRequestSaved(request.RequestId);
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
            ui64 requestId = 0;
            UNIT_ASSERT_VALUES_EQUAL(1, store.GetRequestIdsToProcess().size());
            requestId = store.GetRequestIdsToProcess()[0];

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
            const auto& request = store.MakeDeleteCheckpointRequest(
                checkpointId,
                TInstant::Now());
            store.SetCheckpointRequestSaved(request.RequestId);
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsCheckpointBeingCreated());
            UNIT_ASSERT_VALUES_EQUAL(false, store.IsRequestInProgress());
            ui64 requestId = 0;
            UNIT_ASSERT_VALUES_EQUAL(1, store.GetRequestIdsToProcess().size());
            requestId = store.GetRequestIdsToProcess()[0];

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
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                store.IsCheckpointDeleted(checkpointId));
        };

        auto addAndRemoveCheckpointRequest = [&](TString checkpointId)
        {
            const auto& request = store.MakeDeleteCheckpointRequest(
                checkpointId,
                TInstant::Now());
            ui64 requestId = 0;
            UNIT_ASSERT_VALUES_EQUAL(1, store.GetRequestIdsToProcess().size());
            requestId = store.GetRequestIdsToProcess()[0];
            UNIT_ASSERT_VALUES_EQUAL(request.RequestId, requestId);

            store.RemoveCheckpointRequest(request.RequestId);

            UNIT_ASSERT_VALUES_EQUAL(0, store.GetRequestIdsToProcess().size());
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

            addAndRemoveCheckpointRequest("checkpoint-not-executed");
            UNIT_ASSERT_VALUES_EQUAL(3, store.GetCheckpointsWithData().size());
            UNIT_ASSERT_VALUES_EQUAL(3, store.GetActiveCheckpoints().size());
            UNIT_ASSERT_VALUES_EQUAL(
                false,
                store.IsCheckpointDeleted("checkpoint-not-executed"));

            deleteCheckpointData("checkpoint-1");
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                store.DoesCheckpointBlockingWritesExist());
            UNIT_ASSERT_VALUES_EQUAL(2, store.GetCheckpointsWithData().size());
            UNIT_ASSERT_VALUES_EQUAL(3, store.GetActiveCheckpoints().size());
            deleteCheckpointData("checkpoint-2");
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                store.DoesCheckpointBlockingWritesExist());
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
            UNIT_ASSERT_VALUES_EQUAL(
                false,
                store.DoesCheckpointBlockingWritesExist());
            UNIT_ASSERT_VALUES_EQUAL(0, store.GetCheckpointsWithData().size());
            UNIT_ASSERT_VALUES_EQUAL(1, store.GetActiveCheckpoints().size());
            createCheckpoint("checkpoint-1");
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                store.DoesCheckpointBlockingWritesExist());
            UNIT_ASSERT_VALUES_EQUAL(1, store.GetCheckpointsWithData().size());
            UNIT_ASSERT_VALUES_EQUAL(2, store.GetActiveCheckpoints().size());
            deleteCheckpointData("checkpoint-1");
            UNIT_ASSERT_VALUES_EQUAL(
                false,
                store.DoesCheckpointBlockingWritesExist());
            UNIT_ASSERT_VALUES_EQUAL(0, store.GetCheckpointsWithData().size());
            UNIT_ASSERT_VALUES_EQUAL(2, store.GetActiveCheckpoints().size());
            deleteCheckpoint("checkpoint-4");
            UNIT_ASSERT_VALUES_EQUAL(1, store.GetActiveCheckpoints().size());
            deleteCheckpoint("checkpoint-1");
            UNIT_ASSERT_VALUES_EQUAL(
                false,
                store.DoesCheckpointBlockingWritesExist());
            UNIT_ASSERT_VALUES_EQUAL(0, store.GetCheckpointsWithData().size());
            UNIT_ASSERT_VALUES_EQUAL(0, store.GetActiveCheckpoints().size());

            for (TString checkpointId:
                 {"checkpoint-1",
                  "checkpoint-2",
                  "checkpoint-3",
                  "checkpoint-4"})
            {
                UNIT_ASSERT_VALUES_EQUAL(
                    true,
                    store.IsCheckpointDeleted(checkpointId));
            }

            addAndRemoveCheckpointRequest("checkpoint-1");
            UNIT_ASSERT_VALUES_EQUAL(0, store.GetCheckpointsWithData().size());
            UNIT_ASSERT_VALUES_EQUAL(0, store.GetActiveCheckpoints().size());
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                store.IsCheckpointDeleted("checkpoint-1"));
        }
    }

    Y_UNIT_TEST(CheckpointRequestValidation)
    {
        TCheckpointStore store({}, "disk-1");

        auto makeCheckpointRequest = [&](const TString& checkpointId,
                                         ECheckpointRequestType requestType,
                                         ECheckpointType checkpointType)
        {
            switch (requestType) {
                case ECheckpointRequestType::Create:
                case ECheckpointRequestType::CreateWithoutData:
                    return store.MakeCreateCheckpointRequest(
                        checkpointId,
                        TInstant::Now(),
                        requestType,
                        checkpointType,
                        false   // useShadowDisk
                    );
                case ECheckpointRequestType::DeleteData:
                    return store.MakeDeleteCheckpointDataRequest(
                        checkpointId,
                        TInstant::Now());
                case ECheckpointRequestType::Delete:
                    return store.MakeDeleteCheckpointRequest(
                        checkpointId,
                        TInstant::Now());
            }
        };

        auto executeRequest =
            [&](const TString& checkpointId,
                ECheckpointRequestType requestType,
                ECheckpointType checkpointType,
                std::optional<EWellKnownResultCodes> expectedErrorCode)
        {
            auto request = makeCheckpointRequest(
                checkpointId,
                requestType,
                checkpointType);

            store.SetCheckpointRequestInProgress(request.RequestId);

            auto error = store.ValidateCheckpointRequest(
                checkpointId,
                requestType,
                checkpointType);

            UNIT_ASSERT_VALUES_EQUAL(
                expectedErrorCode.has_value(),
                error.has_value());
            if (error) {
                UNIT_ASSERT_VALUES_EQUAL(
                    *expectedErrorCode,
                    static_cast<EWellKnownResultCodes>(error->GetCode()));
            }

            if (!error) {
                store.SetCheckpointRequestSaved(request.RequestId);
                store.SetCheckpointRequestFinished(
                    request.RequestId,
                    true,        // completed
                    TString(),   // ShadowDiskId
                    EShadowDiskState::None);
            }
        };

        struct TStep
        {
            ECheckpointRequestType RequestType;
            std::optional<EWellKnownResultCodes> ExpectedErrorCode;
        };

        int checkpointIndex = 0;

        auto executeRequests =
            [&](ECheckpointType checkpointType, const TVector<TStep>& steps)
        {
            TString checkpointId = TStringBuilder()
                                   << "checkpoint_" << checkpointIndex++;

            for (const TStep& step: steps) {
                executeRequest(
                    checkpointId,
                    step.RequestType,
                    checkpointType,
                    step.ExpectedErrorCode);
            }
        };

        executeRequests(
            ECheckpointType::Normal,
            TVector<TStep>{
                {ECheckpointRequestType::DeleteData, E_PRECONDITION_FAILED},
                {ECheckpointRequestType::Delete, S_ALREADY},
                {ECheckpointRequestType::Create, std::nullopt},
                {ECheckpointRequestType::Create, S_ALREADY},
                {ECheckpointRequestType::DeleteData, std::nullopt},
                {ECheckpointRequestType::Create, E_PRECONDITION_FAILED},
                {ECheckpointRequestType::DeleteData, S_ALREADY},
                {ECheckpointRequestType::Delete, std::nullopt},
                {ECheckpointRequestType::Delete, S_ALREADY},
                {ECheckpointRequestType::Create, E_PRECONDITION_FAILED},
                {ECheckpointRequestType::DeleteData, E_PRECONDITION_FAILED},
            });

        executeRequests(
            ECheckpointType::Normal,
            TVector<TStep>{
                {ECheckpointRequestType::Create, std::nullopt},
                {ECheckpointRequestType::CreateWithoutData,
                 E_PRECONDITION_FAILED},
                {ECheckpointRequestType::CreateWithoutData,
                 E_PRECONDITION_FAILED},
                {ECheckpointRequestType::DeleteData, std::nullopt},
                {ECheckpointRequestType::CreateWithoutData, S_ALREADY},
                {ECheckpointRequestType::CreateWithoutData, S_ALREADY},
                {ECheckpointRequestType::Delete, std::nullopt},
                {ECheckpointRequestType::CreateWithoutData,
                 E_PRECONDITION_FAILED},
            });

        executeRequests(
            ECheckpointType::Normal,
            TVector<TStep>{
                {ECheckpointRequestType::CreateWithoutData, std::nullopt},
                {ECheckpointRequestType::CreateWithoutData, S_ALREADY},
                {ECheckpointRequestType::Create, E_PRECONDITION_FAILED},
                {ECheckpointRequestType::DeleteData, S_ALREADY},
                {ECheckpointRequestType::CreateWithoutData, S_ALREADY},
            });

        for (auto checkpointType:
             {ECheckpointType::Normal, ECheckpointType::Light})
        {
            executeRequests(
                checkpointType,
                TVector<TStep>{
                    {ECheckpointRequestType::Delete, S_ALREADY},
                    {ECheckpointRequestType::Create, std::nullopt},
                    {ECheckpointRequestType::Create, S_ALREADY},
                    {ECheckpointRequestType::Delete, std::nullopt},
                    {ECheckpointRequestType::Create, E_PRECONDITION_FAILED},
                    {ECheckpointRequestType::Delete, S_ALREADY},
                });
        }

        executeRequests(
            ECheckpointType::Light,
            TVector<TStep>{
                {ECheckpointRequestType::DeleteData, E_ARGUMENT},
                {ECheckpointRequestType::Create, std::nullopt},
                {ECheckpointRequestType::DeleteData, E_ARGUMENT},
                {ECheckpointRequestType::Delete, std::nullopt},
                {ECheckpointRequestType::DeleteData, E_ARGUMENT},
            });

        executeRequests(
            ECheckpointType::Light,
            TVector<TStep>{
                {ECheckpointRequestType::CreateWithoutData, E_ARGUMENT},
                {ECheckpointRequestType::Create, std::nullopt},
                {ECheckpointRequestType::CreateWithoutData, E_ARGUMENT},
            });

        executeRequest(
            "",
            ECheckpointRequestType::Create,
            ECheckpointType::Normal,
            E_ARGUMENT);
        executeRequest(
            "",
            ECheckpointRequestType::Create,
            ECheckpointType::Light,
            E_ARGUMENT);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
