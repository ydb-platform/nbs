#include "volume_database.h"

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/testlib/test_executor.h>
#include <cloud/blockstore/public/api/protos/mount.pb.h>

#include <contrib/ydb/library/actors/core/actorid.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/guid.h>

#include <optional>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

bool operator==(
    const TRuntimeVolumeParamsValue& lhs,
    const TRuntimeVolumeParamsValue& rhs)
{
    return lhs.Key == rhs.Key
        && lhs.Value == rhs.Value
        && lhs.ValidUntil == rhs.ValidUntil;
}

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TVolumeClientInfo CreateVolumeClientInfo(
    NProto::EVolumeAccessMode accessMode,
    NProto::EVolumeMountMode mountMode,
    ui32 mountFlags)
{
    NProto::TVolumeClientInfo info;
    info.SetClientId(CreateGuidAsString());
    info.SetVolumeAccessMode(accessMode);
    info.SetVolumeMountMode(mountMode);
    info.SetMountFlags(mountFlags);
    return info;
}

THistoryLogItem CreateAddClient(
    TInstant timestamp,
    ui64 seqNo,
    const NProto::TVolumeClientInfo& add,
    const NProto::TError& error,
    const NActors::TActorId& pipeServer,
    const NActors::TActorId& senderId)
{
    THistoryLogItem res;
    res.Key = { timestamp, seqNo };

    NProto::TVolumeOperation& op = res.Operation;
    *op.MutableAdd() = add;
    *op.MutableError() = error;
    auto& requesterInfo = *op.MutableRequesterInfo();
    requesterInfo.SetLocalPipeServerId(ToString(pipeServer));
    requesterInfo.SetSenderActorId(ToString(senderId));
    return res;
}

THistoryLogItem CreateRemoveClient(
    TInstant timestamp,
    ui64 seqNo,
    const TString& clientId,
    const TString& reason,
    const NProto::TError& error)
{
    THistoryLogItem res;
    res.Key = { timestamp, seqNo };

    NProto::TVolumeOperation& op = res.Operation;
    NProto::TRemoveClientOperation removeInfo;
    removeInfo.SetClientId(clientId);
    removeInfo.SetReason(reason);
    *op.MutableRemove() = removeInfo;
    *op.MutableError() = error;
    return res;
}

void CheckAddClientLog(
    const NProto::TVolumeClientInfo& client,
    const NProto::TVolumeClientInfo& record)
{
    UNIT_ASSERT_VALUES_EQUAL(client.GetClientId(), record.GetClientId());
    UNIT_ASSERT_VALUES_EQUAL(
        static_cast<ui32>(client.GetVolumeAccessMode()),
        static_cast<ui32>(record.GetVolumeAccessMode()));
    UNIT_ASSERT_VALUES_EQUAL(
        static_cast<ui32>(client.GetVolumeMountMode()),
        static_cast<ui32>(record.GetVolumeMountMode()));
}

void CheckRemoveClientLog(
    const TString& client,
    const TString& reason,
    const NProto::TRemoveClientOperation& record)
{
    UNIT_ASSERT_VALUES_EQUAL(client, record.GetClientId());
    UNIT_ASSERT_VALUES_EQUAL(reason, record.GetReason());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeDatabaseTest)
{
    // TODO: tests for
    //  * meta
    //  * history
    //  * part stats
    //  * used blocks

    Y_UNIT_TEST(ShouldStoreClients)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TVolumeDatabase db) {
            db.InitSchema();
        });

        executor.WriteTx([&] (TVolumeDatabase db) {
            ui32 mountFlags = 0;
            SetProtoFlag(mountFlags, NProto::MF_THROTTLING_DISABLED);

            NProto::TVolumeClientInfo info;
            info.SetClientId("c1");
            info.SetHost("h1");
            info.SetVolumeMountMode(NProto::VOLUME_MOUNT_LOCAL);
            info.SetVolumeAccessMode(NProto::VOLUME_ACCESS_READ_WRITE);
            info.SetMountSeqNumber(1);
            db.WriteClient(info);

            info = {};
            info.SetClientId("c2");
            info.SetHost("h2");
            info.SetVolumeMountMode(NProto::VOLUME_MOUNT_REMOTE);
            info.SetVolumeAccessMode(NProto::VOLUME_ACCESS_READ_ONLY);
            info.SetMountFlags(mountFlags);
            info.SetMountSeqNumber(2);
            db.WriteClient(info);

            info = {};
            info.SetClientId("c3");
            info.SetHost("h3");
            info.SetVolumeMountMode(NProto::VOLUME_MOUNT_REMOTE);
            info.SetVolumeAccessMode(NProto::VOLUME_ACCESS_READ_ONLY);
            info.SetMountSeqNumber(3);
            db.WriteClient(info);

            info = {};
            mountFlags = 0;
            SetProtoFlag(mountFlags, NProto::MF_FORCE_WRITE);
            info.SetClientId("c4");
            info.SetHost("h4");
            info.SetVolumeMountMode(NProto::VOLUME_MOUNT_REMOTE);
            info.SetVolumeAccessMode(NProto::VOLUME_ACCESS_READ_ONLY);
            info.SetMountFlags(mountFlags);
            info.SetMountSeqNumber(4);
            db.WriteClient(info);

            info.SetMountFlags(mountFlags);
        });

        THashMap<TString, TVolumeClientState> infos;

        executor.ReadTx([&] (TVolumeDatabase db) {
            UNIT_ASSERT(db.ReadClients(infos));

            auto info1 = infos["c1"].GetVolumeClientInfo();
            auto info2 = infos["c2"].GetVolumeClientInfo();
            auto info3 = infos["c3"].GetVolumeClientInfo();
            auto info4 = infos["c4"].GetVolumeClientInfo();

            UNIT_ASSERT_VALUES_EQUAL(4, infos.size());

            UNIT_ASSERT_VALUES_EQUAL("c1", info1.GetClientId());
            UNIT_ASSERT_VALUES_EQUAL("h1", info1.GetHost());
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(NProto::VOLUME_MOUNT_LOCAL),
                static_cast<ui32>(info1.GetVolumeMountMode()));
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(NProto::VOLUME_ACCESS_READ_WRITE),
                static_cast<ui32>(info1.GetVolumeAccessMode()));
            UNIT_ASSERT(!HasProtoFlag(
                info1.GetMountFlags(),
                NProto::MF_THROTTLING_DISABLED));
            UNIT_ASSERT(!HasProtoFlag(
                info1.GetMountFlags(),
                NProto::MF_FORCE_WRITE));
            UNIT_ASSERT_VALUES_EQUAL(1, info1.GetMountSeqNumber());

            UNIT_ASSERT_VALUES_EQUAL("c2", info2.GetClientId());
            UNIT_ASSERT_VALUES_EQUAL("h2", info2.GetHost());
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(NProto::VOLUME_MOUNT_REMOTE),
                static_cast<ui32>(info2.GetVolumeMountMode()));
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(NProto::VOLUME_ACCESS_READ_ONLY),
                static_cast<ui32>(info2.GetVolumeAccessMode()));
            UNIT_ASSERT(HasProtoFlag(
                info2.GetMountFlags(),
                NProto::MF_THROTTLING_DISABLED));
            UNIT_ASSERT(!HasProtoFlag(
                info2.GetMountFlags(),
                NProto::MF_FORCE_WRITE));
            UNIT_ASSERT_VALUES_EQUAL(2, info2.GetMountSeqNumber());

            UNIT_ASSERT_VALUES_EQUAL("c3", info3.GetClientId());
            UNIT_ASSERT_VALUES_EQUAL("h3", info3.GetHost());
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(NProto::VOLUME_MOUNT_REMOTE),
                static_cast<ui32>(info3.GetVolumeMountMode()));
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(NProto::VOLUME_ACCESS_READ_ONLY),
                static_cast<ui32>(info3.GetVolumeAccessMode()));
            UNIT_ASSERT(!HasProtoFlag(
                info3.GetMountFlags(),
                NProto::MF_THROTTLING_DISABLED));
            UNIT_ASSERT(!HasProtoFlag(
                info3.GetMountFlags(),
                NProto::MF_FORCE_WRITE));
            UNIT_ASSERT_VALUES_EQUAL(3, info3.GetMountSeqNumber());

            UNIT_ASSERT_VALUES_EQUAL("c4", info4.GetClientId());
            UNIT_ASSERT_VALUES_EQUAL("h4", info4.GetHost());
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(NProto::VOLUME_MOUNT_REMOTE),
                static_cast<ui32>(info4.GetVolumeMountMode()));
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(NProto::VOLUME_ACCESS_READ_ONLY),
                static_cast<ui32>(info4.GetVolumeAccessMode()));
            UNIT_ASSERT(!HasProtoFlag(
                info4.GetMountFlags(),
                NProto::MF_THROTTLING_DISABLED));
            UNIT_ASSERT(HasProtoFlag(
                info4.GetMountFlags(),
                NProto::MF_FORCE_WRITE));
            UNIT_ASSERT_VALUES_EQUAL(4, info4.GetMountSeqNumber());
        });
    }

    Y_UNIT_TEST(ShouldStoreCheckpointRequests)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TVolumeDatabase db) {
            db.InitSchema();
        });

        executor.WriteTx([&] (TVolumeDatabase db) {
            db.WriteCheckpointRequest(TCheckpointRequest{
                1,
                "xxx",
                TInstant::Seconds(111),
                ECheckpointRequestType::Create,
                ECheckpointRequestState::Saved,
                ECheckpointType::Normal,
                true});
            db.WriteCheckpointRequest(TCheckpointRequest{
                2,
                "yyy",
                TInstant::Seconds(222),
                ECheckpointRequestType::Create,
                ECheckpointRequestState::Saved,
                ECheckpointType::Normal,
                false});
            db.WriteCheckpointRequest(TCheckpointRequest{
                3,
                "zzz",
                TInstant::Seconds(333),
                ECheckpointRequestType::Create,
                ECheckpointRequestState::Saved,
                ECheckpointType::Normal,
                true});
            db.WriteCheckpointRequest(TCheckpointRequest{
                4,
                "xxx",
                TInstant::Seconds(444),
                ECheckpointRequestType::Create,
                ECheckpointRequestState::Saved,
                ECheckpointType::Normal,
                false});
            db.UpdateCheckpointRequest(
                1,
                true,
                "shadow-disk-id",
                EShadowDiskState::New,
                TString());
            db.UpdateCheckpointRequest(
                3,
                false,
                "",
                EShadowDiskState::None,
                "Error message");
        });

        executor.ReadTx([&] (TVolumeDatabase db) {
            TVector<TCheckpointRequest> requests;
            THashMap<TString, TInstant> deletedCheckpoints;
            TVector<ui64> outdatedCheckpointRequestIds;

            UNIT_ASSERT(db.ReadCheckpointRequests(
                deletedCheckpoints,
                requests,
                outdatedCheckpointRequestIds));

            UNIT_ASSERT_VALUES_EQUAL(4, requests.size());
            UNIT_ASSERT_VALUES_EQUAL(1, requests[0].RequestId);
            UNIT_ASSERT_VALUES_EQUAL("xxx", requests[0].CheckpointId);
            UNIT_ASSERT_VALUES_EQUAL("shadow-disk-id", requests[0].ShadowDiskId);
            UNIT_ASSERT_VALUES_EQUAL(
                TInstant::Seconds(111),
                requests[0].Timestamp
            );
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(ECheckpointRequestState::Completed),
                static_cast<ui32>(requests[0].State)
            );
            UNIT_ASSERT_VALUES_EQUAL(2, requests[1].RequestId);
            UNIT_ASSERT_VALUES_EQUAL("yyy", requests[1].CheckpointId);
            UNIT_ASSERT_VALUES_EQUAL("", requests[1].ShadowDiskId);
            UNIT_ASSERT_VALUES_EQUAL(
                TInstant::Seconds(222),
                requests[1].Timestamp
            );
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(ECheckpointRequestState::Saved),
                static_cast<ui32>(requests[1].State)
            );
            UNIT_ASSERT_VALUES_EQUAL(3, requests[2].RequestId);
            UNIT_ASSERT_VALUES_EQUAL("zzz", requests[2].CheckpointId);
            UNIT_ASSERT_VALUES_EQUAL("", requests[2].ShadowDiskId);
            UNIT_ASSERT_VALUES_EQUAL("Error message", requests[2].CheckpointError);
            UNIT_ASSERT_VALUES_EQUAL(
                TInstant::Seconds(333),
                requests[2].Timestamp
            );
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(ECheckpointRequestState::Rejected),
                static_cast<ui32>(requests[2].State)
            );
            UNIT_ASSERT_VALUES_EQUAL(4, requests[3].RequestId);
            UNIT_ASSERT_VALUES_EQUAL("xxx", requests[3].CheckpointId);
            UNIT_ASSERT_VALUES_EQUAL("", requests[3].ShadowDiskId);
            UNIT_ASSERT_VALUES_EQUAL(
                TInstant::Seconds(444),
                requests[3].Timestamp
            );
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(ECheckpointRequestState::Saved),
                static_cast<ui32>(requests[3].State)
            );
        });
    }

    Y_UNIT_TEST(ShouldStoreCheckpointShadowDiskInfo)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TVolumeDatabase db) { db.InitSchema(); });

        executor.WriteTx(
            [&](TVolumeDatabase db)
            {
                db.WriteCheckpointRequest(TCheckpointRequest{
                    1,
                    "xxx",
                    TInstant::Seconds(111),
                    ECheckpointRequestType::Create,
                    ECheckpointRequestState::Saved,
                    ECheckpointType::Normal,
                    false});
                db.UpdateCheckpointRequest(
                    1,
                    true,
                    "shadow-disk-id",
                    EShadowDiskState::New,
                    TString());
            });

        executor.ReadTx(
            [&](TVolumeDatabase db)
            {
                TVector<TCheckpointRequest> requests;
                THashMap<TString, TInstant> deletedCheckpoints;
                TVector<ui64> outdatedCheckpointRequestIds;

                UNIT_ASSERT(db.ReadCheckpointRequests(
                    deletedCheckpoints,
                    requests,
                    outdatedCheckpointRequestIds));

                UNIT_ASSERT_VALUES_EQUAL(1, requests.size());
                UNIT_ASSERT_VALUES_EQUAL(1, requests[0].RequestId);
                UNIT_ASSERT_VALUES_EQUAL("xxx", requests[0].CheckpointId);
                UNIT_ASSERT_VALUES_EQUAL(
                    "shadow-disk-id",
                    requests[0].ShadowDiskId);
                UNIT_ASSERT_VALUES_EQUAL(
                    TInstant::Seconds(111),
                    requests[0].Timestamp);
                UNIT_ASSERT_VALUES_EQUAL(
                    static_cast<ui32>(ECheckpointRequestState::Completed),
                    static_cast<ui32>(requests[0].State));
                UNIT_ASSERT_VALUES_EQUAL(
                    0,
                    requests[0].ShadowDiskProcessedBlockCount);
                UNIT_ASSERT_VALUES_EQUAL(
                    EShadowDiskState::New,
                    requests[0].ShadowDiskState);
            });

        executor.WriteTx(
            [&](TVolumeDatabase db)
            {
                db.UpdateShadowDiskState(
                    1,
                    512,
                    EShadowDiskState::Preparing);
            });

        executor.ReadTx(
            [&](TVolumeDatabase db)
            {
                TVector<TCheckpointRequest> requests;
                THashMap<TString, TInstant> deletedCheckpoints;
                TVector<ui64> outdatedCheckpointRequestIds;

                UNIT_ASSERT(db.ReadCheckpointRequests(
                    deletedCheckpoints,
                    requests,
                    outdatedCheckpointRequestIds));

                UNIT_ASSERT_VALUES_EQUAL(1, requests.size());
                UNIT_ASSERT_VALUES_EQUAL(1, requests[0].RequestId);
                UNIT_ASSERT_VALUES_EQUAL("xxx", requests[0].CheckpointId);
                UNIT_ASSERT_VALUES_EQUAL(
                    "shadow-disk-id",
                    requests[0].ShadowDiskId);
                UNIT_ASSERT_VALUES_EQUAL(
                    512,
                    requests[0].ShadowDiskProcessedBlockCount);
                UNIT_ASSERT_VALUES_EQUAL(
                    EShadowDiskState::Preparing,
                    requests[0].ShadowDiskState);
            });

        executor.WriteTx(
            [&](TVolumeDatabase db)
            {
                db.UpdateShadowDiskState(
                    1,
                    1024,
                    EShadowDiskState::Ready);
            });

        executor.ReadTx(
            [&](TVolumeDatabase db)
            {
                TVector<TCheckpointRequest> requests;
                THashMap<TString, TInstant> deletedCheckpoints;
                TVector<ui64> outdatedCheckpointRequestIds;

                UNIT_ASSERT(db.ReadCheckpointRequests(
                    deletedCheckpoints,
                    requests,
                    outdatedCheckpointRequestIds));

                UNIT_ASSERT_VALUES_EQUAL(1, requests.size());
                UNIT_ASSERT_VALUES_EQUAL(1, requests[0].RequestId);
                UNIT_ASSERT_VALUES_EQUAL("xxx", requests[0].CheckpointId);
                UNIT_ASSERT_VALUES_EQUAL(
                    "shadow-disk-id",
                    requests[0].ShadowDiskId);
                UNIT_ASSERT_VALUES_EQUAL(
                    1024,
                    requests[0].ShadowDiskProcessedBlockCount);
                UNIT_ASSERT_VALUES_EQUAL(
                    EShadowDiskState::Ready,
                    requests[0].ShadowDiskState);
            });
    }

    Y_UNIT_TEST(ShouldStorePartStats)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TVolumeDatabase db) {
            db.InitSchema();
        });

        executor.WriteTx([&] (TVolumeDatabase db) {
            for (ui32 i = 0; i < 2; ++i) {
                NProto::TCachedPartStats stats;
                stats.SetMixedBytesCount(1 * i);
                stats.SetMergedBytesCount(2 * i);
                stats.SetFreshBytesCount(3 * i);
                stats.SetUsedBytesCount(4 * i);
                stats.SetBytesCount(5 * i);
                stats.SetCheckpointBytes(6 * i);
                stats.SetCompactionScore(7 * i);
                stats.SetCompactionGarbageScore(8 * i);
                stats.SetCleanupQueueBytes(9 * i);
                stats.SetGarbageQueueBytes(10 * i);
                stats.SetLogicalUsedBytesCount(11 * i);

                db.WritePartStats(i, stats);
            }
        });

        executor.ReadTx([&] (TVolumeDatabase db) {
            TVector<TVolumeDatabase::TPartStats> stats;
            UNIT_ASSERT(db.ReadPartStats(stats));
            UNIT_ASSERT_VALUES_EQUAL(2, stats.size());

            for (ui32 i = 0; i < 2; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(i, stats[i].TabletId);
                const auto& stat = stats[i].Stats;
                UNIT_ASSERT_VALUES_EQUAL(1 * i, stat.GetMixedBytesCount());
                UNIT_ASSERT_VALUES_EQUAL(2 * i, stat.GetMergedBytesCount());
                UNIT_ASSERT_VALUES_EQUAL(3 * i, stat.GetFreshBytesCount());
                UNIT_ASSERT_VALUES_EQUAL(4 * i, stat.GetUsedBytesCount());
                UNIT_ASSERT_VALUES_EQUAL(5 * i, stat.GetBytesCount());
                UNIT_ASSERT_VALUES_EQUAL(6 * i, stat.GetCheckpointBytes());
                UNIT_ASSERT_VALUES_EQUAL(7 * i, stat.GetCompactionScore());
                UNIT_ASSERT_VALUES_EQUAL(8 * i, stat.GetCompactionGarbageScore());
                UNIT_ASSERT_VALUES_EQUAL(9 * i, stat.GetCleanupQueueBytes());
                UNIT_ASSERT_VALUES_EQUAL(10 * i, stat.GetGarbageQueueBytes());
                UNIT_ASSERT_VALUES_EQUAL(11 * i, stat.GetLogicalUsedBytesCount());
            }
        });
    }

    Y_UNIT_TEST(ShouldStoreNonReplPartStats)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TVolumeDatabase db) {
            db.InitSchema();
        });

        executor.WriteTx([&] (TVolumeDatabase db) {
            for (ui32 i = 0; i < 2; ++i) {
                NProto::TCachedPartStats stats;
                stats.SetMixedBytesCount(1 * i);
                stats.SetMergedBytesCount(2 * i);
                stats.SetFreshBytesCount(3 * i);
                stats.SetUsedBytesCount(4 * i);
                stats.SetBytesCount(5 * i);
                stats.SetCheckpointBytes(6 * i);
                stats.SetCompactionScore(7 * i);
                stats.SetCompactionGarbageScore(8 * i);
                stats.SetCleanupQueueBytes(9 * i);
                stats.SetGarbageQueueBytes(10 * i);
                stats.SetLogicalUsedBytesCount(11 * i);

                db.WriteNonReplPartStats(i, stats);
            }
        });

        executor.ReadTx([&] (TVolumeDatabase db) {
            TVector<TVolumeDatabase::TPartStats> stats;
            UNIT_ASSERT(db.ReadNonReplPartStats(stats));
            UNIT_ASSERT_VALUES_EQUAL(2, stats.size());

            for (ui32 i = 0; i < 2; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(i, stats[i].TabletId);
                const auto& stat = stats[i].Stats;
                UNIT_ASSERT_VALUES_EQUAL(1 * i, stat.GetMixedBytesCount());
                UNIT_ASSERT_VALUES_EQUAL(2 * i, stat.GetMergedBytesCount());
                UNIT_ASSERT_VALUES_EQUAL(3 * i, stat.GetFreshBytesCount());
                UNIT_ASSERT_VALUES_EQUAL(4 * i, stat.GetUsedBytesCount());
                UNIT_ASSERT_VALUES_EQUAL(5 * i, stat.GetBytesCount());
                UNIT_ASSERT_VALUES_EQUAL(6 * i, stat.GetCheckpointBytes());
                UNIT_ASSERT_VALUES_EQUAL(7 * i, stat.GetCompactionScore());
                UNIT_ASSERT_VALUES_EQUAL(8 * i, stat.GetCompactionGarbageScore());
                UNIT_ASSERT_VALUES_EQUAL(9 * i, stat.GetCleanupQueueBytes());
                UNIT_ASSERT_VALUES_EQUAL(10 * i, stat.GetGarbageQueueBytes());
                UNIT_ASSERT_VALUES_EQUAL(11 * i, stat.GetLogicalUsedBytesCount());
            }
        });
    }

    Y_UNIT_TEST(ShouldRemoveOutdatedDeletedCheckpointsFromHistory)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TVolumeDatabase db) {
            db.InitSchema();
        });

        executor.WriteTx([&] (TVolumeDatabase db) {
            db.WriteCheckpointRequest(TCheckpointRequest{
                1,
                "xxx",
                TInstant::Seconds(111),
                ECheckpointRequestType::Create,
                ECheckpointRequestState::Saved,
                ECheckpointType::Normal,
                false});
            db.WriteCheckpointRequest(TCheckpointRequest{
                2,
                "yyy",
                TInstant::Seconds(222),
                ECheckpointRequestType::Create,
                ECheckpointRequestState::Saved,
                ECheckpointType::Normal,
                false});
            db.WriteCheckpointRequest(TCheckpointRequest{
                3,
                "xxx",
                TInstant::Seconds(333),
                ECheckpointRequestType::Delete,
                ECheckpointRequestState::Saved,
                ECheckpointType::Normal,
                false});
            db.WriteCheckpointRequest(TCheckpointRequest{
                4,
                "yyy",
                TInstant::Seconds(444),
                ECheckpointRequestType::Delete,
                ECheckpointRequestState::Saved,
                ECheckpointType::Normal,
                false});
            db.UpdateCheckpointRequest(
                1,
                true,
                "",
                EShadowDiskState::None,
                TString());
            db.UpdateCheckpointRequest(
                2,
                true,
                "",
                EShadowDiskState::None,
                TString());
            db.UpdateCheckpointRequest(
                3,
                true,
                "",
                EShadowDiskState::None,
                TString());
            db.UpdateCheckpointRequest(
                4,
                true,
                "",
                EShadowDiskState::None,
                TString());
        });

        executor.ReadTx([&] (TVolumeDatabase db) {
            TVector<TCheckpointRequest> requests;
            THashMap<TString, TInstant> deletedCheckpoints;
            TVector<ui64> outdatedCheckpointRequestIds;

            UNIT_ASSERT(db.CollectCheckpointsToDelete(
                TDuration::Days(1),
                TInstant::Seconds(0),
                deletedCheckpoints));

            UNIT_ASSERT_VALUES_EQUAL(0, deletedCheckpoints.size());

            UNIT_ASSERT(db.ReadCheckpointRequests(
                deletedCheckpoints,
                requests,
                outdatedCheckpointRequestIds));

            UNIT_ASSERT_VALUES_EQUAL(0, outdatedCheckpointRequestIds.size());

            UNIT_ASSERT_VALUES_EQUAL(4, requests.size());
            UNIT_ASSERT_VALUES_EQUAL(1, requests[0].RequestId);
            UNIT_ASSERT_VALUES_EQUAL("xxx", requests[0].CheckpointId);
            UNIT_ASSERT_VALUES_EQUAL(
                TInstant::Seconds(111),
                requests[0].Timestamp
            );
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(ECheckpointRequestState::Completed),
                static_cast<ui32>(requests[0].State)
            );
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(ECheckpointRequestType::Create),
                static_cast<ui32>(requests[0].ReqType)
            );

            UNIT_ASSERT_VALUES_EQUAL(2, requests[1].RequestId);
            UNIT_ASSERT_VALUES_EQUAL("yyy", requests[1].CheckpointId);
            UNIT_ASSERT_VALUES_EQUAL(
                TInstant::Seconds(222),
                requests[1].Timestamp
            );
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(ECheckpointRequestState::Completed),
                static_cast<ui32>(requests[1].State)
            );
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(ECheckpointRequestType::Create),
                static_cast<ui32>(requests[1].ReqType)
            );

            UNIT_ASSERT_VALUES_EQUAL(3, requests[2].RequestId);
            UNIT_ASSERT_VALUES_EQUAL("xxx", requests[2].CheckpointId);
            UNIT_ASSERT_VALUES_EQUAL(
                TInstant::Seconds(333),
                requests[2].Timestamp
            );
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(ECheckpointRequestState::Completed),
                static_cast<ui32>(requests[2].State)
            );
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(ECheckpointRequestType::Delete),
                static_cast<ui32>(requests[2].ReqType)
            );

            UNIT_ASSERT_VALUES_EQUAL(4, requests[3].RequestId);
            UNIT_ASSERT_VALUES_EQUAL("yyy", requests[3].CheckpointId);
            UNIT_ASSERT_VALUES_EQUAL(
                TInstant::Seconds(444),
                requests[3].Timestamp
            );
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(ECheckpointRequestState::Completed),
                static_cast<ui32>(requests[3].State)
            );
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(ECheckpointRequestType::Delete),
                static_cast<ui32>(requests[3].ReqType)
            );
        });

        executor.ReadTx([&] (TVolumeDatabase db) {
            TVector<TCheckpointRequest> requests;
            THashMap<TString, TInstant> deletedCheckpoints;
            TVector<ui64> outdatedCheckpointRequestIds;

            UNIT_ASSERT(db.CollectCheckpointsToDelete(
                TDuration::Days(1),
                TInstant::Seconds(333) + TDuration::Days(1),
                deletedCheckpoints));

            UNIT_ASSERT_VALUES_EQUAL(1, deletedCheckpoints.size());

            UNIT_ASSERT(db.ReadCheckpointRequests(
                deletedCheckpoints,
                requests,
                outdatedCheckpointRequestIds));

            UNIT_ASSERT_VALUES_EQUAL(2, outdatedCheckpointRequestIds.size());

            UNIT_ASSERT_VALUES_EQUAL(2, requests.size());
            UNIT_ASSERT_VALUES_EQUAL(2, requests[0].RequestId);
            UNIT_ASSERT_VALUES_EQUAL("yyy", requests[0].CheckpointId);
            UNIT_ASSERT_VALUES_EQUAL(
                TInstant::Seconds(222),
                requests[0].Timestamp
            );
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(ECheckpointRequestState::Completed),
                static_cast<ui32>(requests[0].State)
            );
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(ECheckpointRequestType::Create),
                static_cast<ui32>(requests[0].ReqType)
            );

            UNIT_ASSERT_VALUES_EQUAL(4, requests[1].RequestId);
            UNIT_ASSERT_VALUES_EQUAL("yyy", requests[1].CheckpointId);
            UNIT_ASSERT_VALUES_EQUAL(
                TInstant::Seconds(444),
                requests[1].Timestamp
            );
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(ECheckpointRequestState::Completed),
                static_cast<ui32>(requests[1].State)
            );
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(ECheckpointRequestType::Delete),
                static_cast<ui32>(requests[1].ReqType)
            );

        });

        executor.WriteTx([&] (TVolumeDatabase db) {
            db.WriteCheckpointRequest(TCheckpointRequest{
                5,
                "yyy",
                TInstant::Seconds(555),
                ECheckpointRequestType::Create,
                ECheckpointRequestState::Saved,
                ECheckpointType::Normal,
                false});
            db.UpdateCheckpointRequest(
                5,
                true,
                "",
                EShadowDiskState::None,
                TString());
        });

        executor.ReadTx([&] (TVolumeDatabase db) {
            TVector<TCheckpointRequest> requests;
            THashMap<TString, TInstant> deletedCheckpoints;
            TVector<ui64> outdatedCheckpointRequestIds;

            UNIT_ASSERT(db.CollectCheckpointsToDelete(
                TDuration::Days(1),
                TInstant::Seconds(444) + TDuration::Days(1),
                deletedCheckpoints));

            UNIT_ASSERT_VALUES_EQUAL(2, deletedCheckpoints.size());

            UNIT_ASSERT(db.ReadCheckpointRequests(
                deletedCheckpoints,
                requests,
                outdatedCheckpointRequestIds));

            UNIT_ASSERT_VALUES_EQUAL(4, outdatedCheckpointRequestIds.size());

            UNIT_ASSERT_VALUES_EQUAL(1, requests.size());
        });
    }

    Y_UNIT_TEST(ShouldStoreVolumeHistory)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TVolumeDatabase db) {
            db.InitSchema();
        });

        auto one = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);

        auto two = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE,
            0);

        executor.WriteTx([&] (TVolumeDatabase db) {
            // zero
            db.WriteHistory(CreateAddClient({}, 0, one, {}, {}, {}));
            db.WriteHistory(CreateAddClient({}, 1, two, {}, {}, {}));

            // first
            db.WriteHistory(
                CreateAddClient(TInstant::Seconds(1), 0, one, {}, {}, {}));

            // second
            db.WriteHistory(
                CreateRemoveClient(TInstant::Seconds(2), 0, one.GetClientId(), "reason1", {}));
            db.WriteHistory(
                CreateRemoveClient(TInstant::Seconds(2), 1, two.GetClientId(), "reason2", {}));
         });

        executor.ReadTx([&] (TVolumeDatabase db) {
            TVolumeMountHistorySlice records;

            UNIT_ASSERT(db.ReadHistory(
                { TInstant::Seconds(2), 1 },
                TInstant::Seconds(0),
                100,
                records));

            UNIT_ASSERT_VALUES_EQUAL(5, records.Items.size());
            UNIT_ASSERT_C(!records.HasMoreItems(), "Should read all records");

            CheckRemoveClientLog(two.GetClientId(), "reason2", records.Items[0].Operation.GetRemove());
            CheckRemoveClientLog(one.GetClientId(), "reason1", records.Items[1].Operation.GetRemove());

            CheckAddClientLog(one, records.Items[2].Operation.GetAdd());

            CheckAddClientLog(two, records.Items[3].Operation.GetAdd());
            CheckAddClientLog(one, records.Items[4].Operation.GetAdd());
        });

        executor.ReadTx([&] (TVolumeDatabase db) {
            TVolumeMountHistorySlice records;

            UNIT_ASSERT(db.ReadHistory(
                { TInstant::Seconds(2), 1 },
                TInstant::Seconds(0),
                100,
                records));

            UNIT_ASSERT_VALUES_EQUAL(5, records.Items.size());
            UNIT_ASSERT_C(!records.HasMoreItems(), "Should read all records");

            CheckRemoveClientLog(two.GetClientId(), "reason2", records.Items[0].Operation.GetRemove());
            CheckRemoveClientLog(one.GetClientId(), "reason1", records.Items[1].Operation.GetRemove());

            CheckAddClientLog(one, records.Items[2].Operation.GetAdd());

            CheckAddClientLog(two, records.Items[3].Operation.GetAdd());
            CheckAddClientLog(one, records.Items[4].Operation.GetAdd());
        });
    }

    Y_UNIT_TEST(ShouldReadOutdatedHistory)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TVolumeDatabase db) {
            db.InitSchema();
        });

        auto one = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);

        auto two = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE,
            0);

        executor.WriteTx([&] (TVolumeDatabase db) {
            // zero
            db.WriteHistory(CreateAddClient({}, 0, one, {}, {}, {}));
            db.WriteHistory(CreateAddClient({}, 1, two, {}, {}, {}));

            // first
            db.WriteHistory(CreateAddClient(TInstant::Seconds(1), 0, one, {}, {}, {}));

            // second
            db.WriteHistory(
                CreateRemoveClient(TInstant::Seconds(2), 0, one.GetClientId(), "reason1", {}));
            db.WriteHistory(
                CreateRemoveClient(TInstant::Seconds(2), 1, two.GetClientId(), "reason2", {}));
         });

        executor.ReadTx([&] (TVolumeDatabase db) {
            TVector<THistoryLogKey> records;

            UNIT_ASSERT(db.ReadOutdatedHistory(
                TInstant::Seconds(1),
                1,
                records));

            UNIT_ASSERT_VALUES_EQUAL(1, records.size());

            UNIT_ASSERT_VALUES_EQUAL(THistoryLogKey(TInstant::Seconds(1)), records[0]);
        });

        executor.ReadTx([&] (TVolumeDatabase db) {
            TVector<THistoryLogKey> records;

            UNIT_ASSERT(db.ReadOutdatedHistory(
                TInstant::Seconds(1),
                Max<ui32>(),
                records));

            UNIT_ASSERT_VALUES_EQUAL(3, records.size());

            UNIT_ASSERT_VALUES_EQUAL(THistoryLogKey(TInstant::Seconds(1)), records[0]);
            UNIT_ASSERT_VALUES_EQUAL(THistoryLogKey({}, 1), records[1]);
            UNIT_ASSERT_VALUES_EQUAL(THistoryLogKey({}, 0), records[2]);
        });
    }

    Y_UNIT_TEST(ShouldReadRecordsInSpecifiedRange)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TVolumeDatabase db) {
            db.InitSchema();
        });

        auto one = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);

        auto two = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE,
            0);

        executor.WriteTx([&] (TVolumeDatabase db) {
            // zero
            db.WriteHistory(CreateAddClient({}, 0, one, {}, {}, {}));
            db.WriteHistory(CreateAddClient({}, 1, two, {}, {}, {}));

            // first
            db.WriteHistory(CreateAddClient(TInstant::Seconds(1), 0, one, {}, {}, {}));

            // second
            db.WriteHistory(
                CreateRemoveClient(TInstant::Seconds(2), 0, one.GetClientId(), "reason1", {}));
            db.WriteHistory(
                CreateRemoveClient(TInstant::Seconds(2), 1, two.GetClientId(), "reason2", {}));
         });

        executor.ReadTx([&] (TVolumeDatabase db) {
            TVolumeMountHistorySlice records;

            UNIT_ASSERT(db.ReadHistory(
                {TInstant::Seconds(2), 1},
                TInstant::Seconds(1),
                100,
                records));

            UNIT_ASSERT_VALUES_EQUAL(3, records.Items.size());
            UNIT_ASSERT_C(
                !records.HasMoreItems(),
                "Should not take into account outdated records");

            CheckRemoveClientLog(two.GetClientId(), "reason2", records.Items[0].Operation.GetRemove());
            CheckRemoveClientLog(one.GetClientId(), "reason1", records.Items[1].Operation.GetRemove());

            CheckAddClientLog(one, records.Items[2].Operation.GetAdd());
        });
    }

    Y_UNIT_TEST(ShouldReadExactNumberOfRecords)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TVolumeDatabase db) {
            db.InitSchema();
        });

        auto one = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);

        auto two = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_ONLY,
            NProto::VOLUME_MOUNT_REMOTE,
            0);

        executor.WriteTx([&] (TVolumeDatabase db) {
            // zero
            db.WriteHistory(CreateAddClient({}, 0, one, {}, {}, {}));
            db.WriteHistory(CreateAddClient({}, 1, two, {}, {}, {}));

            db.WriteHistory(CreateAddClient(TInstant::Seconds(1), 0, one, {}, {}, {}));
            db.WriteHistory(CreateAddClient(TInstant::Seconds(1), 1, two, {}, {}, {}));
         });

        executor.ReadTx([&] (TVolumeDatabase db) {
            TVolumeMountHistorySlice records;

            UNIT_ASSERT(db.ReadHistory(
                {TInstant::Seconds(2), 1},
                {},
                2,
                records));

            UNIT_ASSERT_VALUES_EQUAL(2, records.Items.size());
            UNIT_ASSERT_C(records.HasMoreItems(), "Should not read all records");

            CheckAddClientLog(two, records.Items[0].Operation.GetAdd());
            CheckAddClientLog(one, records.Items[1].Operation.GetAdd());
        });

        executor.ReadTx([&] (TVolumeDatabase db) {
            TVolumeMountHistorySlice records;

            UNIT_ASSERT(db.ReadHistory(
                {TInstant::Seconds(2), 1},
                {},
                0,
                records));

            UNIT_ASSERT_VALUES_EQUAL(0, records.Items.size());
            UNIT_ASSERT_C(records.HasMoreItems(), "Should not read all records");
        });
    }

    Y_UNIT_TEST(ShouldStoreThrottlerState)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TVolumeDatabase db) {
            db.InitSchema();
        });

        TMaybe<TVolumeDatabase::TThrottlerStateInfo> stateInfo;

        executor.ReadTx([&] (TVolumeDatabase db) {
            UNIT_ASSERT(db.ReadThrottlerState(stateInfo));

            UNIT_ASSERT(!stateInfo.Defined());
        });

        executor.WriteTx([&] (TVolumeDatabase db) {
            TVolumeDatabase::TThrottlerStateInfo info;
            info.Budget = 31415;
            db.WriteThrottlerState(info);
        });

        executor.ReadTx([&] (TVolumeDatabase db) {
            UNIT_ASSERT(db.ReadThrottlerState(stateInfo));

            UNIT_ASSERT(stateInfo.Defined());
            UNIT_ASSERT_VALUES_EQUAL(31415, stateInfo->Budget);
        });
    }

    Y_UNIT_TEST(ShouldStoreMetaHistory)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TVolumeDatabase db) {
            db.InitSchema();
        });

        TVector<TVolumeMetaHistoryItem> metas;

        executor.ReadTx([&] (TVolumeDatabase db) {
            UNIT_ASSERT(db.ReadMetaHistory(metas));
            UNIT_ASSERT_VALUES_EQUAL(0, metas.size());
        });

        executor.WriteTx([&] (TVolumeDatabase db) {
            NProto::TVolumeMeta meta;
            auto* device = meta.MutableDevices()->Add();
            device->SetDeviceUUID("uuid1");
            device->SetBlocksCount(1024);
            db.WriteMetaHistory(1, {TInstant::Seconds(10), meta});
            device->SetDeviceUUID("uuid2");
            db.WriteMetaHistory(2, {TInstant::Seconds(15), meta});
        });

        executor.ReadTx([&] (TVolumeDatabase db) {
            UNIT_ASSERT(db.ReadMetaHistory(metas));

            UNIT_ASSERT_VALUES_EQUAL(2, metas.size());
            UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(10), metas[0].Timestamp);
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid1",
                metas[0].Meta.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                1024,
                metas[0].Meta.GetDevices(0).GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(15), metas[1].Timestamp);
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid2",
                metas[1].Meta.GetDevices(0).GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                1024,
                metas[1].Meta.GetDevices(0).GetBlocksCount());
        });
    }

    Y_UNIT_TEST(ShouldDeleteExtremelyOldMetaHistory)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TVolumeDatabase db) {
            db.InitSchema();
        });

        TVector<TVolumeMetaHistoryItem> metas;

        executor.ReadTx([&] (TVolumeDatabase db) {
            UNIT_ASSERT(db.ReadMetaHistory(metas));
            UNIT_ASSERT_VALUES_EQUAL(0, metas.size());
        });

        const ui32 lastVersion = 2000;

        executor.WriteTx([&] (TVolumeDatabase db) {
            NProto::TVolumeMeta meta;
            auto* device = meta.MutableDevices()->Add();
            device->SetBlocksCount(1024);
            for (ui32 i = 0; i < lastVersion; ++i) {
                device->SetDeviceUUID(Sprintf("uuid%u", i));
                db.WriteMetaHistory(i, {TInstant::Seconds(i), meta});
            }
        });

        executor.ReadTx([&] (TVolumeDatabase db) {
            UNIT_ASSERT(db.ReadMetaHistory(metas));

            UNIT_ASSERT_VALUES_EQUAL(1000, metas.size());
            for (ui32 i = 0; i < metas.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    Sprintf("uuid%u", lastVersion - 1000 + i),
                    metas[i].Meta.GetDevices(0).GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(
                    TInstant::Seconds(lastVersion - 1000 + i),
                    metas[i].Timestamp);
                UNIT_ASSERT_VALUES_EQUAL(
                    1024,
                    metas[i].Meta.GetDevices(0).GetBlocksCount());
            }
        });
    }

    Y_UNIT_TEST(ShouldUpdateVolumeParams)
    {
        const TString keyToUpdate = "key_to_update";
        const TString keyToInsert = "key_to_insert";
        const TString keyToIgnore = "key_to_ignore";
        const TString valueOld = "1";
        const TString valueNew = "2";
        const TInstant validUntilOld = TInstant::MicroSeconds(100);
        const TInstant validUntilNew = TInstant::MicroSeconds(200);

        const auto paramVecToMap = [](const TVector<TRuntimeVolumeParamsValue>& params) {
            THashMap<TString, TRuntimeVolumeParamsValue> map;
            for (const auto& param: params) {
                map.try_emplace(param.Key, param);
            }
            return map;
        };

        // initialize data
        TTestExecutor executor;
        executor.WriteTx([&] (TVolumeDatabase db) {
            db.InitSchema();
        });

        executor.WriteTx([&] (TVolumeDatabase db) {
            db.WriteVolumeParams({
                {keyToUpdate, valueOld, validUntilOld},
                {keyToIgnore, valueOld, validUntilOld}
            });
        });

        TVector<TRuntimeVolumeParamsValue> volumeParams;
        executor.ReadTx([&] (TVolumeDatabase db) {
            UNIT_ASSERT(db.ReadVolumeParams(volumeParams));
            UNIT_ASSERT_VALUES_EQUAL(
                paramVecToMap(volumeParams),
                paramVecToMap({
                    {keyToUpdate, valueOld, validUntilOld},
                    {keyToIgnore, valueOld, validUntilOld}
                })
            );
        });

        // update data
        executor.WriteTx([&] (TVolumeDatabase db) {
            db.WriteVolumeParams({
                {keyToUpdate, valueNew, validUntilNew},
                {keyToInsert, valueOld, validUntilOld}
            });
        });

        volumeParams.clear();
        executor.ReadTx([&] (TVolumeDatabase db) {
            UNIT_ASSERT(db.ReadVolumeParams(volumeParams));
            UNIT_ASSERT_VALUES_EQUAL(
                paramVecToMap(volumeParams),
                paramVecToMap({
                    {keyToUpdate, valueNew, validUntilNew},
                    {keyToIgnore, valueOld, validUntilOld},
                    {keyToInsert, valueOld, validUntilOld}
                })
            );
        });
    }

    Y_UNIT_TEST(ShouldStoreStorageConfig)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TVolumeDatabase db) {
            db.InitSchema();

            NProto::TVolumeMeta meta;
            meta.MutableVolumeConfig()->SetDiskId("vol0");
            db.WriteMeta(meta);
        });

        TMaybe<NProto::TStorageServiceConfig> serviceConfig;

        executor.ReadTx([&] (TVolumeDatabase db) {
            UNIT_ASSERT(db.ReadStorageConfig(serviceConfig));

            UNIT_ASSERT(!serviceConfig.Defined());
        });

        executor.WriteTx([&] (TVolumeDatabase db) {
            NProto::TStorageServiceConfig config;
            config.SetCompactionRangeCountPerRun(100);
            db.WriteStorageConfig(config);
        });

        executor.ReadTx([&] (TVolumeDatabase db) {
            UNIT_ASSERT(db.ReadStorageConfig(serviceConfig));

            UNIT_ASSERT(serviceConfig.Defined());
            UNIT_ASSERT_VALUES_EQUAL(
                100, serviceConfig->GetCompactionRangeCountPerRun());
        });
    }

    Y_UNIT_TEST(ShouldStoreFollowers)
    {
        TTestExecutor executor;

        TFollowerDiskInfo follower1{
            .LinkUUID = "xxxxx",
            .FollowerDiskId = "volume_1",
            .ScaleUnitId = "SU_1"};

        TFollowerDiskInfo follower2{
            .LinkUUID = "yyyyy",
            .FollowerDiskId = "volume_2",
            .State = TFollowerDiskInfo::EState::Preparing,
            .MigratedBytes = 1_MB};

        executor.WriteTx(
            [&](TVolumeDatabase db)
            {
                db.InitSchema();
                db.WriteFollower(follower1);
                db.WriteFollower(follower2);
            });

        executor.ReadTx(
            [&](TVolumeDatabase db)
            {
                TFollowerDisks readFollowers;
                UNIT_ASSERT(db.ReadFollowers(readFollowers));
                UNIT_ASSERT_VALUES_EQUAL(2, readFollowers.size());
                UNIT_ASSERT_EQUAL(follower1, readFollowers[0]);
                UNIT_ASSERT_EQUAL(follower2, readFollowers[1]);
            });

        follower1.MigratedBytes = 2_MB;
        follower1.State = TFollowerDiskInfo::EState::Ready;

        executor.WriteTx(
            [&](TVolumeDatabase db)
            {
                db.InitSchema();
                db.WriteFollower(follower1);
            });

        executor.ReadTx(
            [&](TVolumeDatabase db)
            {
                TFollowerDisks readFollowers;
                UNIT_ASSERT(db.ReadFollowers(readFollowers));
                UNIT_ASSERT_VALUES_EQUAL(2, readFollowers.size());
                UNIT_ASSERT_EQUAL(follower1, readFollowers[0]);
                UNIT_ASSERT_EQUAL(follower2, readFollowers[1]);
            });

        executor.WriteTx(
            [&](TVolumeDatabase db)
            {
                db.InitSchema();
                db.DeleteFollower(follower1);
            });

        executor.ReadTx(
            [&](TVolumeDatabase db)
            {
                TFollowerDisks readFollowers;
                UNIT_ASSERT(db.ReadFollowers(readFollowers));
                UNIT_ASSERT_VALUES_EQUAL(1, readFollowers.size());
                UNIT_ASSERT_EQUAL(follower2, readFollowers[0]);
            });
    }
}

}   // namespace NCloud::NBlockStore::NStorage

////////////////////////////////////////////////////////////////////////////////

template <>
void Out<NCloud::NBlockStore::NStorage::THistoryLogKey>(
    IOutputStream& out,
    const NCloud::NBlockStore::NStorage::THistoryLogKey& value)
{
    out << '{'
        << "Timestamp: " << value.Timestamp << ", "
        << "Seqno: " << value.SeqNo
        << '}';
}

template <>
void Out<NCloud::NBlockStore::NStorage::TRuntimeVolumeParamsValue>(
    IOutputStream& out,
    const NCloud::NBlockStore::NStorage::TRuntimeVolumeParamsValue& value)
{
    out << '{'
        << "Key: " << value.Key << ", "
        << "Value: " << value.Value << ", "
        << "ValidUntil: " << value.ValidUntil
        << '}';
}
