#include "shard_creation_state_companion.h"

#include <cloud/filestore/libs/storage/api/components.h>
#include <cloud/filestore/libs/storage/api/ss_proxy.h>

#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/core/base/events.h>
#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

#include <limits>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

namespace {

using EMode = TShardCreationStateCompanion::EMode;

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf FileSystemId = "test-fs";
constexpr ui32 ShardCount = 6;

TVector<NKikimrFileStore::TConfig> MakeShardConfigs(ui32 shardCount)
{
    TVector<NKikimrFileStore::TConfig> shardConfigs(shardCount);
    for (ui32 shardIndex = 0; shardIndex < shardCount; ++shardIndex) {
        shardConfigs[shardIndex].SetFileSystemId(
            TStringBuilder() << FileSystemId << "_s" << shardIndex);
        shardConfigs[shardIndex].SetBlocksCount(1024 + shardIndex);
    }

    return shardConfigs;
}

TVector<NKikimrFileStore::TConfig> MakeShardConfigs()
{
    return MakeShardConfigs(ShardCount);
}

ui64 CalculateNewShardConfigsHashForTest(
    ui32 topologyShardCount,
    const TVector<NKikimrFileStore::TConfig>& shardConfigs)
{
    const auto result =
        CalculateNewShardConfigsHash(topologyShardCount, shardConfigs);
    UNIT_ASSERT_C(!HasError(result), FormatError(result.GetError()));
    return result.GetResult();
}

NProtoPrivate::TFileSystemShardCreationState MakeState(
    ui32 version,
    std::initializer_list<ui32> createdShards,
    ui32 topologyShardCount,
    const TVector<NKikimrFileStore::TConfig>& shardConfigs)
{
    const ui32 targetShardCount = shardConfigs.size();
    UNIT_ASSERT_C(
        topologyShardCount <= targetShardCount,
        "Invalid test shard range");

    NCloud::TCompressedBitmap bitmap(targetShardCount);
    for (const auto shardIndex: createdShards) {
        UNIT_ASSERT_C(
            shardIndex < targetShardCount,
            "Invalid test created shard index");

        bitmap.Set(shardIndex, shardIndex + 1);
    }

    NProtoPrivate::TFileSystemShardCreationState state;
    state.SetVersion(version);
    state.SetTopologyShardCount(topologyShardCount);
    state.SetTargetShardCount(targetShardCount);
    state.SetTargetShardConfigHash(
        CalculateNewShardConfigsHashForTest(
            topologyShardCount,
            shardConfigs));
    SaveCompressedBitmap(
        bitmap,
        targetShardCount,
        *state.MutableCreatedShardBitmap());

    return state;
}

NProtoPrivate::TFileSystemShardCreationState MakeState(
    ui32 version,
    std::initializer_list<ui32> createdShards)
{
    return MakeState(version, createdShards, 0, MakeShardConfigs());
}

enum ETestEvents
{
    EvScheduleShardCreation = EventSpaceBegin(NKikimr::TEvents::ES_USERSPACE + 42),
    EvShardCreationScheduled,
};

struct TEvScheduleShardCreation final
    : public TEventBase<TEvScheduleShardCreation, EvScheduleShardCreation>
{
    bool Complete;
    ui32 Limit;

    TEvScheduleShardCreation(bool complete, ui32 limit)
        : Complete(complete)
        , Limit(limit)
    {}

    DEFINE_SIMPLE_LOCAL_EVENT(TEvScheduleShardCreation, "TEvScheduleShardCreation");
};

struct TEvShardCreationScheduled final
    : public TEventBase<TEvShardCreationScheduled, EvShardCreationScheduled>
{
    bool HasShardsToCreate;

    explicit TEvShardCreationScheduled(bool hasShardsToCreate)
        : HasShardsToCreate(hasShardsToCreate)
    {}

    DEFINE_SIMPLE_LOCAL_EVENT(
        TEvShardCreationScheduled,
        "TEvShardCreationScheduled");
};

class TShardSchedulingActor final: public TActor<TShardSchedulingActor>
{
private:
    TShardCreationStateCompanion Companion{
        TString{FileSystemId},
        EMode::Create};
    TVector<NKikimrFileStore::TConfig> ShardConfigs = MakeShardConfigs();

public:
    explicit TShardSchedulingActor(ui32 topologyShardCount)
        : TActor(&TThis::StateWork)
    {
        Companion.InitializeShardCreation(topologyShardCount, ShardCount);
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvScheduleShardCreation, HandleScheduleShardCreation);

            default:
                Y_ABORT("Unexpected event");
        }
    }

    void HandleScheduleShardCreation(
        const TEvScheduleShardCreation::TPtr& ev,
        const TActorContext& ctx)
    {
        if (ev->Get()->Complete) {
            Companion.OnShardCreated(
                ctx,
                ShardConfigs,
                ev->Get()->Limit);
        } else {
            Companion.StartShardCreation(ctx, ShardConfigs, ev->Get()->Limit);
        }
        ctx.Send(
            ev->Sender,
            new TEvShardCreationScheduled(Companion.HasShardsToCreate()));
    }
};

struct TActorSystem: TTestActorRuntimeBase
{
    void Start()
    {
        InitNodes();
        AppendToLogSettings(
            TFileStoreComponents::START,
            TFileStoreComponents::END,
            GetComponentName);
        SetDispatchTimeout(TDuration::Seconds(5));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TShardCreationStateCompanionTest)
{
    Y_UNIT_TEST(ShouldTrackPersistentShardCreationStateSupport)
    {
        TShardCreationStateCompanion companion(TString{FileSystemId}, EMode::Create);

        UNIT_ASSERT(!companion.IsPersistentStateLoaded());
        UNIT_ASSERT(!companion.IsPersistentStateSupported());

        companion.MarkPersistentStateUnsupported();
        UNIT_ASSERT(companion.IsPersistentStateLoaded());
        UNIT_ASSERT(!companion.IsPersistentStateSupported());

        companion.SetShardCreationState(MakeState(42, {1}));
        UNIT_ASSERT(companion.IsPersistentStateLoaded());
        UNIT_ASSERT(companion.IsPersistentStateSupported());
        UNIT_ASSERT_VALUES_EQUAL(42, companion.GetShardCreationStateVersion());
    }

    Y_UNIT_TEST(ShouldScheduleShardCreationWithinLimit)
    {
        TActorSystem runtime;
        runtime.Start();
        const auto sender = runtime.AllocateEdgeActor();
        const auto ssProxy = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeSSProxyServiceId(), ssProxy);
        const auto actorId = runtime.Register(new TShardSchedulingActor(2));

        auto schedule = [&](bool complete, ui32 limit) {
            runtime.Send(new IEventHandle(
                actorId,
                sender,
                new TEvScheduleShardCreation(complete, limit)));
            return runtime.GrabEdgeEvent<TEvShardCreationScheduled>(sender)
                ->Get()->HasShardsToCreate;
        };
        auto assertNextShard = [&](ui32 shardIndex) {
            const auto request = runtime.GrabEdgeEvent<
                TEvSSProxy::TEvCreateFileStoreRequest>(ssProxy);
            UNIT_ASSERT_VALUES_EQUAL(shardIndex, request->Cookie);
            UNIT_ASSERT_VALUES_EQUAL(
                MakeShardConfigs()[shardIndex].GetFileSystemId(),
                request->Get()->Config.GetFileSystemId());
        };

        UNIT_ASSERT(schedule(false, 2));
        assertNextShard(2);
        assertNextShard(3);
        UNIT_ASSERT(schedule(true, 2));
        assertNextShard(4);
        UNIT_ASSERT(schedule(true, 2));
        assertNextShard(5);
        UNIT_ASSERT(schedule(true, 2));
        UNIT_ASSERT(!schedule(true, 2));
    }

    Y_UNIT_TEST(ShouldIgnoreVolatileShardConfigFieldsInNewShardConfigsHash)
    {
        const auto shardConfigs = MakeShardConfigs();

        auto sameShardConfigs = shardConfigs;
        sameShardConfigs[3].SetVersion(42);
        sameShardConfigs[3].SetCreationTs(100500);
        sameShardConfigs[3].SetAlterTs(100501);

        UNIT_ASSERT_VALUES_EQUAL(
            CalculateNewShardConfigsHashForTest(0, shardConfigs),
            CalculateNewShardConfigsHashForTest(0, sameShardConfigs));

        auto differentShardConfigs = shardConfigs;
        differentShardConfigs[3].SetBlocksCount(
            differentShardConfigs[3].GetBlocksCount() + 1);

        UNIT_ASSERT_VALUES_UNEQUAL(
            CalculateNewShardConfigsHashForTest(0, shardConfigs),
            CalculateNewShardConfigsHashForTest(0, differentShardConfigs));
    }

    Y_UNIT_TEST(ShouldRejectInvalidNewShardConfigsHashRange)
    {
        const auto result = CalculateNewShardConfigsHash(
            ShardCount + 1,
            MakeShardConfigs());

        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, result.GetError().GetCode());
        UNIT_ASSERT(result.GetError().GetMessage().Contains(
            "Invalid shard creation range"));
    }

    Y_UNIT_TEST(ShouldLoadPersistedCreatedShardBitmap)
    {
        TShardCreationStateCompanion companion(TString{FileSystemId}, EMode::Create);

        const auto shardConfigs = MakeShardConfigs();

        companion.SetShardCreationState(MakeState(42, {1, 4}));
        auto error = companion.SetupCreatedShardBitmap(0, shardConfigs);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));

        UNIT_ASSERT_VALUES_EQUAL(42, companion.GetShardCreationStateVersion());
        UNIT_ASSERT(companion.HasCreatedShardBitmap());
        UNIT_ASSERT_VALUES_EQUAL(0, companion.GetTopologyShardCount());
        UNIT_ASSERT_VALUES_EQUAL(ShardCount, companion.GetTargetShardCount());
        UNIT_ASSERT_VALUES_EQUAL(
            CalculateNewShardConfigsHashForTest(0, shardConfigs),
            companion.GetNewShardConfigsHash());
        UNIT_ASSERT(!companion.IsShardCreated(0));
        UNIT_ASSERT(companion.IsShardCreated(1));
        UNIT_ASSERT(!companion.IsShardCreated(2));
        UNIT_ASSERT(companion.IsShardCreated(4));
    }

    Y_UNIT_TEST(ShouldRejectHugePersistedShardCreationBitmapOnSetup)
    {
        TShardCreationStateCompanion companion(TString{FileSystemId}, EMode::Create);

        const auto shardConfigs = MakeShardConfigs();
        auto state = MakeState(42, {});
        state.MutableCreatedShardBitmap()->SetBitCount(
            std::numeric_limits<ui64>::max());

        companion.SetShardCreationState(state);
        const auto error = companion.SetupCreatedShardBitmap(0, shardConfigs);

        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
        UNIT_ASSERT_C(
            error.GetMessage().Contains("exceeds limit"),
            error.GetMessage());
    }

    Y_UNIT_TEST(ShouldRejectConflictingUncommittedShardCreationState)
    {
        TShardCreationStateCompanion companion(TString{FileSystemId}, EMode::Create);

        const auto originalShardConfigs = MakeShardConfigs();
        auto conflictingShardConfigs = originalShardConfigs;
        conflictingShardConfigs[3].SetBlocksCount(
            conflictingShardConfigs[3].GetBlocksCount() + 1);

        companion.SetShardCreationState(
            MakeState(42, {3}, 0, originalShardConfigs));
        const auto error =
            companion.SetupCreatedShardBitmap(0, conflictingShardConfigs);

        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, error.GetCode());
        UNIT_ASSERT(error.GetMessage().Contains(
            "Unfinished shard creation state conflicts with current plan"));
        UNIT_ASSERT(error.GetMessage().Contains(
            "Retry the previous create/resize request with the same "
            "parameters"));
    }

    Y_UNIT_TEST(ShouldResetStaleCommittedShardCreationState)
    {
        TShardCreationStateCompanion companion(TString{FileSystemId}, EMode::Create);

        const auto originalShardConfigs = MakeShardConfigs();
        const auto nextShardConfigs = MakeShardConfigs(8);

        companion.SetShardCreationState(
            MakeState(42, {1, 4}, 0, originalShardConfigs));
        const auto error =
            companion.SetupCreatedShardBitmap(ShardCount, nextShardConfigs);

        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        UNIT_ASSERT_VALUES_EQUAL(ShardCount, companion.GetTopologyShardCount());
        UNIT_ASSERT_VALUES_EQUAL(
            nextShardConfigs.size(),
            companion.GetTargetShardCount());
        UNIT_ASSERT_VALUES_EQUAL(
            CalculateNewShardConfigsHashForTest(
                ShardCount,
                nextShardConfigs),
            companion.GetNewShardConfigsHash());
        UNIT_ASSERT(!companion.IsShardCreated(1));
        UNIT_ASSERT(!companion.IsShardCreated(4));
        UNIT_ASSERT(!companion.IsShardCreated(6));
    }
}

}   // namespace NCloud::NFileStore::NStorage
