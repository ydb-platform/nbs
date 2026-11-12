#include "shard_creation_state_companion.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

#include <limits>

namespace NCloud::NFileStore::NStorage {

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

ui64 CalculateNewShardConfigsHashForTest(
    ui32 baseShardCount,
    const TVector<NKikimrFileStore::TConfig>& shardConfigs)
{
    const auto result =
        CalculateNewShardConfigsHash(baseShardCount, shardConfigs);
    UNIT_ASSERT_C(!HasError(result), FormatError(result.GetError()));
    return result.GetResult();
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
    state.SetTargetShardConfigHash(
        CalculateNewShardConfigsHashForTest(baseShardCount, shardConfigs));
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TShardCreationStateCompanionTest)
{
    Y_UNIT_TEST(ShouldTrackPersistentShardCreationStateSupport)
    {
        TShardCreationStateCompanion companion(TString{FileSystemId});

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
        TShardCreationStateCompanion companion(TString{FileSystemId});

        const auto shardConfigs = MakeShardConfigs();

        companion.SetShardCreationState(MakeState(42, {1, 4}));
        auto error = companion.SetupCreatedShardBitmap(0, shardConfigs);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));

        UNIT_ASSERT_VALUES_EQUAL(42, companion.GetShardCreationStateVersion());
        UNIT_ASSERT(companion.HasCreatedShardBitmap());
        UNIT_ASSERT_VALUES_EQUAL(0, companion.GetBaseShardCount());
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
        TShardCreationStateCompanion companion(TString{FileSystemId});

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
        TShardCreationStateCompanion companion(TString{FileSystemId});

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
        TShardCreationStateCompanion companion(TString{FileSystemId});

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
