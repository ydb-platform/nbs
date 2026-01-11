#include "part_channels_state.h"

#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>
#include <cloud/blockstore/libs/storage/partition/part_schema.h>
#include <cloud/blockstore/libs/storage/testlib/test_executor.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

namespace {

////////////////////////////////////////////////////////////////////////////////

const ui32 DefaultBlockCount = 1000;
const ui32 DataChannelStart = 3;

////////////////////////////////////////////////////////////////////////////////

NProto::TPartitionConfig DefaultConfig(size_t channelCount, size_t blockCount)
{
    NProto::TPartitionConfig config;
    config.SetBlockSize(DefaultBlockSize);
    config.SetBlocksCount(blockCount);

    auto cps = config.MutableExplicitChannelProfiles();
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::System));
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Log));
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Index));

    for (ui32 i = 0; i < channelCount; ++i) {
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
    }

    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Fresh));

    return config;
}

TFreeSpaceConfig DefaultFreeSpaceConfig()
{
    return {
        0.25,   // free space threshold
        0.15,   // min free space
    };
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartitionChannelsStateTest)
{

#define CHECK_DISK_SPACE_SCORE(expected)                                \
            UNIT_ASSERT_DOUBLES_EQUAL(                                  \
                expected,                                               \
                state.GetBackpressureDiskSpaceScore(),                  \
                1e-5                                                    \
            )                                                           \
// CHECK_DISK_SPACE_SCORE

    Y_UNIT_TEST(UpdatePermissions)
    {
        const size_t maxChannelCount = 3;

        for (size_t channelCount = 1; channelCount <= maxChannelCount;
             ++channelCount)
        {
            TPartitionChannelsState state(
                DefaultConfig(channelCount, DefaultBlockCount),
                DefaultFreeSpaceConfig(),
                Max(),             // maxIORequestsInFlight
                0,                 // reassignChannelsPercentageThreshold
                100,               // reassignFreshChannelsPercentageThreshold
                100,               // reassignMixedChannelsPercentageThreshold
                false,             // reassignSystemChannelsImmediately
                channelCount + 4   // channelCount
            );

            UNIT_ASSERT(state.IsCompactionAllowed());

            ui32 channelId = 0;
            const double baseScore = 1;
            const double maxScore = 100;

            while (channelId < 3) {
                UNIT_ASSERT(!state.UpdatePermissions(
                    channelId,
                    EChannelPermission::SystemWritesAllowed |
                        EChannelPermission::UserWritesAllowed));
                UNIT_ASSERT(state.IsCompactionAllowed());
                CHECK_DISK_SPACE_SCORE(baseScore);

                UNIT_ASSERT(!state.UpdatePermissions(
                    channelId,
                    EChannelPermission::SystemWritesAllowed));
                UNIT_ASSERT(state.IsCompactionAllowed());
                CHECK_DISK_SPACE_SCORE(baseScore);

                UNIT_ASSERT(state.UpdatePermissions(
                    channelId,
                    EChannelPermission::UserWritesAllowed));
                UNIT_ASSERT(!state.IsCompactionAllowed());
                CHECK_DISK_SPACE_SCORE(maxScore);

                UNIT_ASSERT(!state.UpdatePermissions(channelId, {}));
                UNIT_ASSERT(!state.IsCompactionAllowed());
                CHECK_DISK_SPACE_SCORE(maxScore);

                UNIT_ASSERT(state.UpdatePermissions(
                    channelId,
                    EChannelPermission::SystemWritesAllowed |
                        EChannelPermission::UserWritesAllowed));
                CHECK_DISK_SPACE_SCORE(baseScore);

                ++channelId;
            }

            const TVector<TVector<double>> expectedScores{
                {baseScore, maxScore},
                {baseScore, 2, maxScore},
                {baseScore, 1.5, 3, maxScore},
            };
            while (channelId <= 2 + channelCount) {
                const auto currentScore =
                    expectedScores[channelCount - 1][channelId - 3];
                const auto nextScore =
                    expectedScores[channelCount - 1][channelId - 3 + 1];

                UNIT_ASSERT(!state.UpdatePermissions(
                    channelId,
                    EChannelPermission::SystemWritesAllowed |
                        EChannelPermission::UserWritesAllowed));
                UNIT_ASSERT(state.IsCompactionAllowed());
                CHECK_DISK_SPACE_SCORE(currentScore);

                UNIT_ASSERT(state.UpdatePermissions(
                    channelId,
                    EChannelPermission::SystemWritesAllowed));
                UNIT_ASSERT(state.IsCompactionAllowed());
                CHECK_DISK_SPACE_SCORE(nextScore);

                UNIT_ASSERT(state.UpdatePermissions(
                    channelId,
                    EChannelPermission::UserWritesAllowed));
                UNIT_ASSERT_VALUES_EQUAL(
                    channelId < 2 + channelCount,
                    state.IsCompactionAllowed());
                CHECK_DISK_SPACE_SCORE(currentScore);

                UNIT_ASSERT(state.UpdatePermissions(channelId, {}));
                UNIT_ASSERT_VALUES_EQUAL(
                    channelId < 2 + channelCount,
                    state.IsCompactionAllowed());
                CHECK_DISK_SPACE_SCORE(nextScore);

                ++channelId;
            }

            // TODO: fresh channels
        }
    }

    Y_UNIT_TEST(TestReassignedChannelsCollection)
    {
        TPartitionChannelsState state(
            DefaultConfig(2, DefaultBlockCount),
            DefaultFreeSpaceConfig(),
            Max(),   // maxIORequestsInFlight
            0,       // reassignChannelsPercentageThreshold
            100,     // reassignFreshChannelsPercentageThreshold
            100,     // reassignMixedChannelsPercentageThreshold
            false,   // reassignSystemChannelsImmediately
            6        // channelCount
        );

        UNIT_ASSERT(
            state.IsWriteAllowed(EChannelPermission::UserWritesAllowed)
        );
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetChannelsToReassign().size());

        state.UpdatePermissions(
            DataChannelStart,
            EChannelPermission::SystemWritesAllowed
        );

        UNIT_ASSERT(
            state.IsWriteAllowed(EChannelPermission::UserWritesAllowed)
        );
        auto channelsToReassign = state.GetChannelsToReassign();
        UNIT_ASSERT_VALUES_EQUAL(1, channelsToReassign.size());
        UNIT_ASSERT_VALUES_EQUAL(DataChannelStart, channelsToReassign[0]);

        state.UpdatePermissions(
            DataChannelStart + 1,
            EChannelPermission::SystemWritesAllowed
        );

        UNIT_ASSERT(
            !state.IsWriteAllowed(EChannelPermission::UserWritesAllowed)
        );
        channelsToReassign = state.GetChannelsToReassign();
        UNIT_ASSERT_VALUES_EQUAL(2, channelsToReassign.size());
        UNIT_ASSERT_VALUES_EQUAL(DataChannelStart, channelsToReassign[0]);
        UNIT_ASSERT_VALUES_EQUAL(DataChannelStart + 1, channelsToReassign[1]);

        state.RegisterReassignRequestFromBlobStorage(DataChannelStart + 2);
        channelsToReassign = state.GetChannelsToReassign();
        UNIT_ASSERT_VALUES_EQUAL(3, channelsToReassign.size());
        UNIT_ASSERT_VALUES_EQUAL(DataChannelStart, channelsToReassign[0]);
        UNIT_ASSERT_VALUES_EQUAL(DataChannelStart + 1, channelsToReassign[1]);
        UNIT_ASSERT_VALUES_EQUAL(DataChannelStart + 2, channelsToReassign[2]);
    }

    Y_UNIT_TEST(TestReassignedChannelsPercentageThreshold)
    {
        TPartitionChannelsState state(
            DefaultConfig(96, DefaultBlockCount),
            DefaultFreeSpaceConfig(),
            Max(),   // maxIORequestsInFlight
            10,      // reassignChannelsPercentageThreshold
            100,     // reassignFreshChannelsPercentageThreshold
            100,     // reassignMixedChannelsPercentageThreshold
            false,   // reassignSystemChannelsImmediately
            100      // channelCount
        );

        UNIT_ASSERT(
            state.IsWriteAllowed(EChannelPermission::UserWritesAllowed)
        );
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetChannelsToReassign().size());

        for (ui32 i = 0; i < 9; ++i) {
            state.UpdatePermissions(
                DataChannelStart + i,
                EChannelPermission::SystemWritesAllowed
            );
        }

        UNIT_ASSERT(
            state.IsWriteAllowed(EChannelPermission::UserWritesAllowed)
        );
        auto channelsToReassign = state.GetChannelsToReassign();
        UNIT_ASSERT_VALUES_EQUAL(0, channelsToReassign.size());

        state.UpdatePermissions(
            DataChannelStart + 9,
            EChannelPermission::SystemWritesAllowed
        );

        UNIT_ASSERT(
            state.IsWriteAllowed(EChannelPermission::UserWritesAllowed)
        );
        channelsToReassign = state.GetChannelsToReassign();
        UNIT_ASSERT_VALUES_EQUAL(10, channelsToReassign.size());
        UNIT_ASSERT_VALUES_EQUAL(DataChannelStart, channelsToReassign[0]);
        UNIT_ASSERT_VALUES_EQUAL(DataChannelStart + 9, channelsToReassign[9]);

        for (ui32 i = 0; i < 10; ++i) {
            state.UpdatePermissions(
                DataChannelStart + i,
                EChannelPermission::UserWritesAllowed
                    | EChannelPermission::SystemWritesAllowed
            );
        }

        UNIT_ASSERT(
            state.IsWriteAllowed(EChannelPermission::UserWritesAllowed)
        );
        channelsToReassign = state.GetChannelsToReassign();
        UNIT_ASSERT_VALUES_EQUAL(0, channelsToReassign.size());

        state.UpdatePermissions(
            0,
            EChannelPermission::SystemWritesAllowed
        );
        UNIT_ASSERT(
            !state.IsWriteAllowed(EChannelPermission::UserWritesAllowed)
        );
        channelsToReassign = state.GetChannelsToReassign();
        UNIT_ASSERT_VALUES_EQUAL(1, channelsToReassign.size());
        UNIT_ASSERT_VALUES_EQUAL(0, channelsToReassign[0]);
    }

    Y_UNIT_TEST(UpdateChannelFreeSpaceShare)
    {
        const size_t maxChannelCount = 3;

        for (size_t channelCount = 1; channelCount <= maxChannelCount; ++channelCount) {
            TPartitionChannelsState state(
                DefaultConfig(channelCount, DefaultBlockCount),
                DefaultFreeSpaceConfig(),
                Max(),             // maxIORequestsInFlight
                0,                 // reassignChannelsPercentageThreshold
                100,               // reassignFreshChannelsPercentageThreshold
                100,               // reassignMixedChannelsPercentageThreshold
                false,             // reassignSystemChannelsImmediately
                channelCount + 4   // channelCount
            );

            const double baseScore = 1;
            const double maxScore = 100;

            for (ui32 ch = 0; ch < state.GetChannelCount(); ++ch) {
                const auto kind = state.GetChannelDataKind(ch);
                switch (kind) {
                    case EChannelDataKind::System:
                    case EChannelDataKind::Log:
                    case EChannelDataKind::Index: {
                        UNIT_ASSERT(!state.UpdateChannelFreeSpaceShare(ch, 0));
                        UNIT_ASSERT(!state.UpdateChannelFreeSpaceShare(ch, 0.25));
                        CHECK_DISK_SPACE_SCORE(baseScore);

                        UNIT_ASSERT(state.UpdateChannelFreeSpaceShare(ch, 0.20));
                        CHECK_DISK_SPACE_SCORE(2);

                        UNIT_ASSERT(state.UpdateChannelFreeSpaceShare(ch, 0.15));
                        CHECK_DISK_SPACE_SCORE(maxScore);

                        UNIT_ASSERT(!state.UpdateChannelFreeSpaceShare(ch, 0));
                        UNIT_ASSERT(state.UpdateChannelFreeSpaceShare(ch, 1));

                        CHECK_DISK_SPACE_SCORE(baseScore);

                        break;
                    }
                    case EChannelDataKind::Mixed:
                    case EChannelDataKind::Merged: {
                        UNIT_ASSERT(state.UpdateChannelFreeSpaceShare(ch, 0.2));

                        constexpr ui32 FirstDataChannel = 3;
                        CHECK_DISK_SPACE_SCORE(
                            1 / (1 - 0.5 * (ch - FirstDataChannel + 1) / channelCount)
                        );
                        break;
                    }

                    case EChannelDataKind::Fresh: {
                        UNIT_ASSERT(state.UpdateChannelFreeSpaceShare(ch, 0.2));
                        CHECK_DISK_SPACE_SCORE(maxScore);
                        break;
                    }

                    default: {
                        Y_ABORT("unsupported kind: %u", static_cast<ui32>(kind));
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(PickProperNextChannel)
    {
        for (auto kind: {EChannelDataKind::Mixed, EChannelDataKind::Merged}) {
            TPartitionChannelsState state(
                DefaultConfig(MaxMergedChannelCount, DefaultBlockCount),
                DefaultFreeSpaceConfig(),
                Max(),   // maxIORequestsInFlight
                0,       // reassignChannelsPercentageThreshold
                100,     // reassignFreshChannelsPercentageThreshold
                100,     // reassignMixedChannelsPercentageThreshold
                false,   // reassignSystemChannelsImmediately
                MaxDataChannelCount   // channelCount
            );

            auto blobId = state.GenerateBlobId(
                kind,
                EChannelPermission::UserWritesAllowed,
                1,
                1024);
            UNIT_ASSERT(blobId.Channel() == TPartitionSchema::FirstDataChannel);
        }
    }

    Y_UNIT_TEST(PickProperNextChannelWithExplicitChannelProfiles)
    {
        NProto::TPartitionConfig config;
        config.SetBlockSize(DefaultBlockSize);
        config.SetBlocksCount(DefaultBlockCount);
        auto cps = config.MutableExplicitChannelProfiles();
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::System));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Log));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Index));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Mixed));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Mixed));

        TPartitionChannelsState state(
            config,
            DefaultFreeSpaceConfig(),
            Max(),   // maxIORequestsInFlight
            0,       // reassignChannelsPercentageThreshold
            100,     // reassignFreshChannelsPercentageThreshold
            100,     // reassignMixedChannelsPercentageThreshold
            false,   // reassignSystemChannelsImmediately
            config.ExplicitChannelProfilesSize());

        const auto perm = EChannelPermission::UserWritesAllowed;
        auto kind = EChannelDataKind::Merged;

        auto blobId = state.GenerateBlobId(kind, perm, 1, 1024);
        UNIT_ASSERT(blobId.Channel() == 3);

        kind = EChannelDataKind::Mixed;

        blobId = state.GenerateBlobId(kind, perm, 1, 1024);
        UNIT_ASSERT(blobId.Channel() == 5);
        blobId = state.GenerateBlobId(kind, perm, 1, 1024);
        UNIT_ASSERT(blobId.Channel() == 10);
        blobId = state.GenerateBlobId(kind, perm, 1, 1024);
        UNIT_ASSERT(blobId.Channel() == 5);

        kind = EChannelDataKind::Merged;

        blobId = state.GenerateBlobId(kind, perm, 1, 1024);
        UNIT_ASSERT(blobId.Channel() == 4);
        blobId = state.GenerateBlobId(kind, perm, 1, 1024);
        UNIT_ASSERT(blobId.Channel() == 6);
        blobId = state.GenerateBlobId(kind, perm, 1, 1024);
        UNIT_ASSERT(blobId.Channel() == 7);
        blobId = state.GenerateBlobId(kind, perm, 1, 1024);
        UNIT_ASSERT(blobId.Channel() == 8);
        blobId = state.GenerateBlobId(kind, perm, 1, 1024);
        UNIT_ASSERT(blobId.Channel() == 9);
        blobId = state.GenerateBlobId(kind, perm, 1, 1024);
        UNIT_ASSERT(blobId.Channel() == 3);
    }

    Y_UNIT_TEST(PickNextChannelWithProperFreeSpaceShare)
    {
        for (auto kind: {EChannelDataKind::Mixed, EChannelDataKind::Merged}) {
            TPartitionChannelsState state(
                DefaultConfig(2, DefaultBlockCount),
                DefaultFreeSpaceConfig(),
                Max(),   // maxIORequestsInFlight
                0,       // reassignChannelsPercentageThreshold
                100,     // reassignFreshChannelsPercentageThreshold
                100,     // reassignMixedChannelsPercentageThreshold
                false,   // reassignSystemChannelsImmediately
                6        // channelCount
            );

            {
                auto blobId = state.GenerateBlobId(
                    kind,
                    EChannelPermission::UserWritesAllowed,
                    1,
                    1024
                );
                UNIT_ASSERT_VALUES_EQUAL(
                    ui32(TPartitionSchema::FirstDataChannel),
                    blobId.Channel()
                );
                auto blobId2 = state.GenerateBlobId(
                    kind,
                    EChannelPermission::UserWritesAllowed,
                    1,
                    1024
                );
                UNIT_ASSERT_VALUES_EQUAL(
                    ui32(TPartitionSchema::FirstDataChannel + 1),
                    blobId2.Channel()
                );
            }

            UNIT_ASSERT_VALUES_EQUAL(0, state.GetAlmostFullChannelCount());
            state.UpdateChannelFreeSpaceShare(
                TPartitionSchema::FirstDataChannel,
                0.15
            );
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetAlmostFullChannelCount());

            for (ui32 i = 0; i < 10; ++i) {
                auto blobId = state.GenerateBlobId(
                    kind,
                    EChannelPermission::UserWritesAllowed,
                    1,
                    1024
                );
                UNIT_ASSERT_VALUES_EQUAL(
                    ui32(TPartitionSchema::FirstDataChannel + 1),
                    blobId.Channel()
                );
            }

            state.UpdateChannelFreeSpaceShare(
                TPartitionSchema::FirstDataChannel,
                0.16
            );
            UNIT_ASSERT_VALUES_EQUAL(1, state.GetAlmostFullChannelCount());

            ui32 firstChannelSelected = 0;
            for (ui32 i = 0; i < 1000; ++i) {
                auto blobId = state.GenerateBlobId(
                    kind,
                    EChannelPermission::UserWritesAllowed,
                    1,
                    1024
                );
                if (blobId.Channel() == TPartitionSchema::FirstDataChannel) {
                    ++firstChannelSelected;
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(
                        ui32(TPartitionSchema::FirstDataChannel + 1),
                        blobId.Channel()
                    );
                }
            }

            UNIT_ASSERT(firstChannelSelected < 150 && firstChannelSelected > 50);

            state.UpdateChannelFreeSpaceShare(
                TPartitionSchema::FirstDataChannel,
                0.16
            );
            state.UpdateChannelFreeSpaceShare(
                TPartitionSchema::FirstDataChannel + 1,
                0.161
            );
            UNIT_ASSERT_VALUES_EQUAL(2, state.GetAlmostFullChannelCount());

            firstChannelSelected = 0;
            for (ui32 i = 0; i < 1000; ++i) {
                auto blobId = state.GenerateBlobId(
                    kind,
                    EChannelPermission::UserWritesAllowed,
                    1,
                    1024
                );
                if (blobId.Channel() == TPartitionSchema::FirstDataChannel) {
                    ++firstChannelSelected;
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(
                        ui32(TPartitionSchema::FirstDataChannel + 1),
                        blobId.Channel()
                    );
                }
            }

            UNIT_ASSERT(firstChannelSelected < 150 && firstChannelSelected > 50);
        }
    }

    Y_UNIT_TEST(PickMergedChannelIfAllMixedChannelsAreFull)
    {
        NProto::TPartitionConfig config;
        config.SetBlockSize(DefaultBlockSize);
        config.SetBlocksCount(DefaultBlockCount);
        config.AddExplicitChannelProfiles()->SetDataKind(
            static_cast<ui32>(EChannelDataKind::System));
        config.AddExplicitChannelProfiles()->SetDataKind(
            static_cast<ui32>(EChannelDataKind::Log));
        config.AddExplicitChannelProfiles()->SetDataKind(
            static_cast<ui32>(EChannelDataKind::Index));
        config.AddExplicitChannelProfiles()->SetDataKind(
            static_cast<ui32>(EChannelDataKind::Mixed));
        config.AddExplicitChannelProfiles()->SetDataKind(
            static_cast<ui32>(EChannelDataKind::Merged));

        TPartitionChannelsState state(
            config,
            DefaultFreeSpaceConfig(),
            Max(),   // maxIORequestsInFlight
            0,       // reassignChannelsPercentageThreshold
            100,     // reassignFreshChannelsPercentageThreshold
            100,     // reassignMixedChannelsPercentageThreshold
            false,   // reassignSystemChannelsImmediately
            config.ExplicitChannelProfilesSize());

        {
            auto mixedBlobId = state.GenerateBlobId(
                EChannelDataKind::Mixed,
                EChannelPermission::UserWritesAllowed,
                1,
                1024
            );
            UNIT_ASSERT_VALUES_EQUAL(
                ui32(TPartitionSchema::FirstDataChannel),
                mixedBlobId.Channel()
            );
            auto mergedBlobId = state.GenerateBlobId(
                EChannelDataKind::Merged,
                EChannelPermission::UserWritesAllowed,
                1,
                1024
            );
            UNIT_ASSERT_VALUES_EQUAL(
                ui32(TPartitionSchema::FirstDataChannel + 1),
                mergedBlobId.Channel()
            );
        }

        state.UpdatePermissions(TPartitionSchema::FirstDataChannel, {});

        {
            auto mixedBlobId = state.GenerateBlobId(
                EChannelDataKind::Mixed,
                EChannelPermission::UserWritesAllowed,
                1,
                1024
            );
            UNIT_ASSERT_VALUES_EQUAL(
                ui32(TPartitionSchema::FirstDataChannel + 1),
                mixedBlobId.Channel()
            );
        }
    }

    Y_UNIT_TEST(TestReassignedMixedChannelsPercentageThreshold)
    {
        const ui32 mixedChannelCount = 10;
        const ui32 mergedChannelCount = 10;
        const ui32 reassignMixedChannelsPercentageThreshold = 20;

        NProto::TPartitionConfig config;
        config.SetBlockSize(DefaultBlockSize);
        config.SetBlocksCount(DefaultBlockCount);

        auto* cps = config.MutableExplicitChannelProfiles();
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::System));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Log));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Index));
        for (ui32 i = 0; i < mergedChannelCount; ++i) {
            cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
        }
        for (ui32 i = 0; i < mixedChannelCount; ++i) {
            cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Mixed));
        }
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Fresh));

        TPartitionChannelsState state(
            config,
            DefaultFreeSpaceConfig(),
            Max(),   // maxIORequestsInFlight
            100,     // reassignChannelsPercentageThreshold
            100,     // reassignFreshChannelsPercentageThreshold
            reassignMixedChannelsPercentageThreshold,   // reassignMixedChannelsPercentageThreshold
            false,   // reassignSystemChannelsImmediately
            mixedChannelCount + mergedChannelCount + DataChannelStart +
                1   // channelCount
        );

        UNIT_ASSERT_VALUES_EQUAL(0, state.GetChannelsToReassign().size());

        {
            state.UpdatePermissions(
                DataChannelStart + mergedChannelCount,
                EChannelPermission::SystemWritesAllowed);

            UNIT_ASSERT_VALUES_EQUAL(0, state.GetChannelsToReassign().size());

            state.UpdatePermissions(
                DataChannelStart + mergedChannelCount + 5,
                EChannelPermission::SystemWritesAllowed);

            const auto channelsToReassign = state.GetChannelsToReassign();
            UNIT_ASSERT_VALUES_EQUAL(2, channelsToReassign.size());
            UNIT_ASSERT_VALUES_EQUAL(
                DataChannelStart + mergedChannelCount,
                channelsToReassign[0]);
            UNIT_ASSERT_VALUES_EQUAL(
                DataChannelStart + mergedChannelCount + 5,
                channelsToReassign[1]);
        }

        {
            state.UpdatePermissions(
                DataChannelStart + mergedChannelCount,
                EChannelPermission::UserWritesAllowed |
                    EChannelPermission::SystemWritesAllowed);
            state.UpdatePermissions(
                DataChannelStart + mergedChannelCount + 5,
                EChannelPermission::UserWritesAllowed |
                    EChannelPermission::SystemWritesAllowed);
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetChannelsToReassign().size());
        }
    }

    Y_UNIT_TEST(TestReassignSystemChannelsImmediately)
    {
        const ui32 mixedChannelCount = 10;
        const ui32 mergedChannelCount = 10;

        NProto::TPartitionConfig config;
        config.SetBlockSize(DefaultBlockSize);
        config.SetBlocksCount(DefaultBlockCount);

        auto* cps = config.MutableExplicitChannelProfiles();
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::System));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Log));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Index));
        for (ui32 i = 0; i < mergedChannelCount; ++i) {
            cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
        }
        for (ui32 i = 0; i < mixedChannelCount; ++i) {
            cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Mixed));
        }
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Fresh));

        TPartitionChannelsState state(
            config,
            DefaultFreeSpaceConfig(),
            Max(),   // maxIORequestsInFlight
            100,     // reassignChannelsPercentageThreshold
            100,     // reassignFreshChannelsPercentageThreshold
            100,     // reassignMixedChannelsPercentageThreshold
            true,    // reassignSystemChannelsImmediately
            mixedChannelCount + mergedChannelCount + DataChannelStart +
                1   // channelCount
        );

        UNIT_ASSERT_VALUES_EQUAL(0, state.GetChannelsToReassign().size());

        {
            state.UpdatePermissions(
                1, // Log
                EChannelPermission::SystemWritesAllowed);

            const auto channelsToReassign = state.GetChannelsToReassign();
            UNIT_ASSERT_VALUES_EQUAL(1, channelsToReassign.size());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                channelsToReassign[0]);
        }

        {
            state.UpdatePermissions(
                1,
                EChannelPermission::UserWritesAllowed |
                    EChannelPermission::SystemWritesAllowed);
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetChannelsToReassign().size());
        }
    }

    Y_UNIT_TEST(TestReassignFreshChannelsAfterCertainThreshold)
    {
        const ui32 mixedChannelCount = 10;
        const ui32 mergedChannelCount = 10;
        const ui32 freshChannelCount = 10;
        const ui32 reassignFreshChannelsPercentageThreshold = 20;

        NProto::TPartitionConfig config;
        config.SetBlockSize(DefaultBlockSize);
        config.SetBlocksCount(DefaultBlockCount);

        auto* cps = config.MutableExplicitChannelProfiles();
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::System));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Log));
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Index));
        for (ui32 i = 0; i < mergedChannelCount; ++i) {
            cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
        }
        for (ui32 i = 0; i < mixedChannelCount; ++i) {
            cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Mixed));
        }
        for (ui32 i = 0; i < freshChannelCount; ++i) {
            cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Fresh));
        }

        TPartitionChannelsState state(
            config,
            DefaultFreeSpaceConfig(),
            Max(),   // maxIORequestsInFlight
            100,     // reassignChannelsPercentageThreshold
            reassignFreshChannelsPercentageThreshold,   // reassignFreshChannelsPercentageThreshold
            100,     // reassignMixedChannelsPercentageThreshold
            false,   // reassignSystemChannelsImmediately
            mixedChannelCount + mergedChannelCount + DataChannelStart +
                freshChannelCount   // channelCount
        );

        UNIT_ASSERT_VALUES_EQUAL(0, state.GetChannelsToReassign().size());

        {
            state.UpdatePermissions(
                DataChannelStart + mergedChannelCount + mixedChannelCount,
                EChannelPermission::SystemWritesAllowed);

            UNIT_ASSERT_VALUES_EQUAL(0, state.GetChannelsToReassign().size());

            state.UpdatePermissions(
                DataChannelStart + mergedChannelCount + mixedChannelCount + 5,
                EChannelPermission::SystemWritesAllowed);

            const auto channelsToReassign = state.GetChannelsToReassign();
            UNIT_ASSERT_VALUES_EQUAL(2, channelsToReassign.size());
            UNIT_ASSERT_VALUES_EQUAL(
                DataChannelStart + mergedChannelCount + mixedChannelCount,
                channelsToReassign[0]);
            UNIT_ASSERT_VALUES_EQUAL(
                DataChannelStart + mergedChannelCount + mixedChannelCount + 5,
                channelsToReassign[1]);
        }

        {
            state.UpdatePermissions(
                DataChannelStart + mergedChannelCount + mixedChannelCount,
                EChannelPermission::UserWritesAllowed |
                    EChannelPermission::SystemWritesAllowed);
            state.UpdatePermissions(
                DataChannelStart + mergedChannelCount + mixedChannelCount + 5,
                EChannelPermission::UserWritesAllowed |
                    EChannelPermission::SystemWritesAllowed);
            UNIT_ASSERT_VALUES_EQUAL(0, state.GetChannelsToReassign().size());
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
