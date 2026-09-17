#include "shard_creation_state_companion.h"

#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

#include <contrib/ydb/core/base/events.h>
#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf FileSystemId = "test-fs";
constexpr ui64 ShardCount = 6;

NProtoPrivate::TFileSystemShardCreationState MakeState(
    ui32 version,
    std::initializer_list<ui32> createdShards)
{
    NCloud::TCompressedBitmap bitmap(ShardCount);
    for (const auto shardIndex: createdShards) {
        bitmap.Set(shardIndex, shardIndex + 1);
    }

    NProtoPrivate::TFileSystemShardCreationState state;
    state.SetVersion(version);
    SaveCompressedBitmap(
        bitmap,
        ShardCount,
        *state.MutableCreatedShardBitmap());

    return state;
}

NCloud::TCompressedBitmap LoadCreatedShardBitmap(
    const NProtoPrivate::TFileSystemShardCreationState& state)
{
    return LoadCompressedBitmap(state.GetCreatedShardBitmap(), ShardCount);
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
        Companion.SetShardCreationState(std::move(state));
        Companion.SetupCreatedShardBitmap(ShardCount);
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
    Y_UNIT_TEST(ShouldLoadPersistedCreatedShardBitmap)
    {
        TShardCreationStateCompanion companion(
            TString{FileSystemId},
            TString{FileSystemId},
            "shard creation state unavailable");

        companion.SetShardCreationState(MakeState(42, {1, 4}));
        companion.SetupCreatedShardBitmap(ShardCount);

        UNIT_ASSERT_VALUES_EQUAL(42, companion.GetShardCreationStateVersion());
        UNIT_ASSERT(companion.HasCreatedShardBitmap());
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

        companion.SetShardCreationState(MakeState(10, {1}));
        companion.SetupCreatedShardBitmap(ShardCount);

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
}

}   // namespace NCloud::NFileStore::NStorage
