#include "shard_creation_state_companion.h"

#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/core/base/events.h>
#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

namespace {

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

NProtoPrivate::TFileSystemShardCreationState MakeState(
    ui32 version,
    std::initializer_list<ui32> createdShards,
    ui32 baseShardCount,
    const TVector<NKikimrFileStore::TConfig>& shardConfigs)
{
    const ui32 targetShardCount = shardConfigs.size();
    UNIT_ASSERT_C(
        baseShardCount <= targetShardCount,
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
    state.SetBaseShardCount(baseShardCount);
    state.SetTargetShardCount(targetShardCount);
    state.SetTargetShardConfigHash(CalculateShardCreationTargetHash(
        baseShardCount,
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

NCloud::TCompressedBitmap LoadCreatedShardBitmap(
    const NProtoPrivate::TFileSystemShardCreationState& state)
{
    return LoadCompressedBitmap(
        state.GetCreatedShardBitmap(),
        state.GetCreatedShardBitmap().GetBitCount());
}

////////////////////////////////////////////////////////////////////////////////

enum ETestEvents
{
    EvUpdateShardCreatedState =
        EventSpaceBegin(NKikimr::TEvents::ES_USERSPACE + 42),
};

struct TEvUpdateShardCreatedState final
    : public TEventBase<TEvUpdateShardCreatedState, EvUpdateShardCreatedState>
{
    ui32 ShardIndex = 0;

    explicit TEvUpdateShardCreatedState(ui32 shardIndex)
        : ShardIndex(shardIndex)
    {}

    DEFINE_SIMPLE_LOCAL_EVENT(
        TEvUpdateShardCreatedState,
        "TEvUpdateShardCreatedState");
};

class TActorWithCompanion final
    : public TActor<TActorWithCompanion>
{
private:
    TShardCreationStateCompanion Companion{
        TString{FileSystemId},
        TString{FileSystemId},
        "shard creation state unavailable"};

public:
    explicit TActorWithCompanion(
        NProtoPrivate::TFileSystemShardCreationState state)
        : TActor(&TThis::StateWork)
    {
        const auto shardConfigs = MakeShardConfigs();
        Companion.SetShardCreationState(std::move(state));
        const auto error =
            Companion.SetupCreatedShardBitmap(0, shardConfigs);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvUpdateShardCreatedState, HandleUpdateShardCreatedState);

            default:
                Y_ABORT("Unexpected event");
        }
    }

    void HandleUpdateShardCreatedState(
        const TEvUpdateShardCreatedState::TPtr& ev,
        const TActorContext& ctx)
    {
        Companion.UpdateShardCreatedState(ctx, ev->Get()->ShardIndex);
    }
};

struct TActorSystem
    : TTestActorRuntimeBase
{
    void Start()
    {
        InitNodes();
        SetDispatchTimeout(TDuration::Seconds(5));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TShardCreationStateCompanionTest)
{
    Y_UNIT_TEST(ShouldIgnoreVolatileShardConfigFieldsInTargetHash)
    {
        const auto shardConfigs = MakeShardConfigs();

        auto sameShardConfigs = shardConfigs;
        sameShardConfigs[3].SetVersion(42);
        sameShardConfigs[3].SetCreationTs(100500);
        sameShardConfigs[3].SetAlterTs(100501);

        UNIT_ASSERT_VALUES_EQUAL(
            CalculateShardCreationTargetHash(0, shardConfigs),
            CalculateShardCreationTargetHash(0, sameShardConfigs));

        auto differentShardConfigs = shardConfigs;
        differentShardConfigs[3].SetBlocksCount(
            differentShardConfigs[3].GetBlocksCount() + 1);

        UNIT_ASSERT_VALUES_UNEQUAL(
            CalculateShardCreationTargetHash(0, shardConfigs),
            CalculateShardCreationTargetHash(0, differentShardConfigs));
    }

    Y_UNIT_TEST(ShouldLoadPersistedCreatedShardBitmap)
    {
        TShardCreationStateCompanion companion(
            TString{FileSystemId},
            TString{FileSystemId},
            "shard creation state unavailable");

        const auto shardConfigs = MakeShardConfigs();

        companion.SetShardCreationState(MakeState(42, {1, 4}));
        auto error = companion.SetupCreatedShardBitmap(0, shardConfigs);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));

        UNIT_ASSERT_VALUES_EQUAL(42, companion.GetShardCreationStateVersion());
        UNIT_ASSERT(companion.HasCreatedShardBitmap());
        UNIT_ASSERT_VALUES_EQUAL(0, companion.GetBaseShardCount());
        UNIT_ASSERT_VALUES_EQUAL(ShardCount, companion.GetTargetShardCount());
        UNIT_ASSERT_VALUES_EQUAL(
            CalculateShardCreationTargetHash(0, shardConfigs),
            companion.GetTargetShardConfigHash());
        UNIT_ASSERT(!companion.IsShardCreated(0));
        UNIT_ASSERT(companion.IsShardCreated(1));
        UNIT_ASSERT(!companion.IsShardCreated(2));
        UNIT_ASSERT(companion.IsShardCreated(4));
        UNIT_ASSERT(!companion.HasUnpersistedCreatedShards());
    }

    Y_UNIT_TEST(ShouldMergeCreatedShardBitmapAndDetectUnpersistedShards)
    {
        TShardCreationStateCompanion companion(
            TString{FileSystemId},
            TString{FileSystemId},
            "shard creation state unavailable");

        const auto shardConfigs = MakeShardConfigs();

        companion.SetShardCreationState(MakeState(10, {1}));
        auto error = companion.SetupCreatedShardBitmap(0, shardConfigs);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));

        companion.MergeCreatedShardBitmap(
            MakeState(11, {1, 3}).GetCreatedShardBitmap());

        UNIT_ASSERT(companion.IsShardCreated(1));
        UNIT_ASSERT(companion.IsShardCreated(3));
        UNIT_ASSERT(companion.HasUnpersistedCreatedShards());

        companion.SetShardCreationState(MakeState(11, {1, 3}));

        UNIT_ASSERT(!companion.HasUnpersistedCreatedShards());
        UNIT_ASSERT_VALUES_EQUAL(11, companion.GetShardCreationStateVersion());
    }

    Y_UNIT_TEST(ShouldSendUpdatedShardCreationState)
    {
        TActorSystem runtime;
        runtime.Start();

        const auto sender = runtime.AllocateEdgeActor();
        const auto tabletProxy = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeIndexTabletProxyServiceId(), tabletProxy);

        const auto actorId =
            runtime.Register(new TActorWithCompanion(MakeState(42, {1})));

        runtime.Send(new IEventHandle(
            actorId,
            sender,
            new TEvUpdateShardCreatedState(3)));

        auto request = runtime.GrabEdgeEvent<
            TEvIndexTablet::TEvUnsafeChangeTabletStateRequest>(
            tabletProxy);

        const auto& record = request->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(FileSystemId, record.GetFileSystemId());
        UNIT_ASSERT(record.HasShardCreationState());

        const auto& state = record.GetShardCreationState();
        UNIT_ASSERT_VALUES_EQUAL(42, state.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetBaseShardCount());
        UNIT_ASSERT_VALUES_EQUAL(ShardCount, state.GetTargetShardCount());
        UNIT_ASSERT_VALUES_EQUAL(
            CalculateShardCreationTargetHash(0, MakeShardConfigs()),
            state.GetTargetShardConfigHash());
        UNIT_ASSERT_VALUES_EQUAL(
            ShardCount,
            state.GetCreatedShardBitmap().GetBitCount());

        const auto bitmap = LoadCreatedShardBitmap(state);
        UNIT_ASSERT_VALUES_EQUAL(2, bitmap.Count());
        UNIT_ASSERT(!bitmap.Test(0));
        UNIT_ASSERT(bitmap.Test(1));
        UNIT_ASSERT(!bitmap.Test(2));
        UNIT_ASSERT(bitmap.Test(3));
    }

    Y_UNIT_TEST(ShouldRejectConflictingUncommittedShardCreationState)
    {
        TShardCreationStateCompanion companion(
            TString{FileSystemId},
            TString{FileSystemId},
            "shard creation state unavailable");

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
            "Unfinished shard creation state conflicts with current target"));
        UNIT_ASSERT(error.GetMessage().Contains(
            "Retry the previous create/resize request with the same "
            "parameters"));
    }

    Y_UNIT_TEST(ShouldResetStaleCommittedShardCreationState)
    {
        TShardCreationStateCompanion companion(
            TString{FileSystemId},
            TString{FileSystemId},
            "shard creation state unavailable");

        const auto originalShardConfigs = MakeShardConfigs();
        const auto nextShardConfigs = MakeShardConfigs(8);

        companion.SetShardCreationState(
            MakeState(42, {1, 4}, 0, originalShardConfigs));
        const auto error =
            companion.SetupCreatedShardBitmap(ShardCount, nextShardConfigs);

        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        UNIT_ASSERT_VALUES_EQUAL(ShardCount, companion.GetBaseShardCount());
        UNIT_ASSERT_VALUES_EQUAL(
            nextShardConfigs.size(),
            companion.GetTargetShardCount());
        UNIT_ASSERT_VALUES_EQUAL(
            CalculateShardCreationTargetHash(
                ShardCount,
                nextShardConfigs),
            companion.GetTargetShardConfigHash());
        UNIT_ASSERT(!companion.IsShardCreated(1));
        UNIT_ASSERT(!companion.IsShardCreated(4));
        UNIT_ASSERT(!companion.IsShardCreated(6));
        UNIT_ASSERT(!companion.HasUnpersistedCreatedShards());
    }

}

}   // namespace NCloud::NFileStore::NStorage
