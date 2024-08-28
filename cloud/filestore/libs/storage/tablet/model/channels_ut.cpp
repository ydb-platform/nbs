#include "channels.h"

#include <cloud/filestore/libs/storage/testlib/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

const double freeSpaceThreshold = 0.25;
const double minFreeSpace = 0.10;

////////////////////////////////////////////////////////////////////////////////

#define CHECK_SELECTED_CHANNEL(dataKind, expected)                             \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        expected,                                                              \
        *channels.SelectChannel(                                               \
            EChannelDataKind::dataKind,                                        \
            minFreeSpace,                                                      \
            freeSpaceThreshold));                                              \
//  CHECK_SELECTED_CHANNEL

#define CHECK_SELECTED_CHANNEL_EMPTY(dataKind)                                 \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        false,                                                                 \
        channels.SelectChannel(                                                \
            EChannelDataKind::dataKind,                                        \
            minFreeSpace,                                                      \
            freeSpaceThreshold).Defined());                                    \
//  CHECK_SELECTED_CHANNEL_EMPTY

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 ChannelCount = 7;

////////////////////////////////////////////////////////////////////////////////

TChannels SetupChannels(ui32 channelCount = ChannelCount)
{
    TChannels channels;
    channels.AddChannel(0, EChannelDataKind::System, "ssd");
    channels.AddChannel(1, EChannelDataKind::Index, "ssd");
    channels.AddChannel(2, EChannelDataKind::Fresh, "ssd");
    for (ui32 channel = 3; channel < channelCount; ++channel) {
        channels.AddChannel(channel, EChannelDataKind::Mixed, "ssd");
    }
    return channels;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TChannelsTest)
{
    Y_UNIT_TEST(ShouldMarkChannelsAsUnwritable)
    {
        TChannels channels = SetupChannels();
        CHECK_SELECTED_CHANNEL(Index, 1);
        CHECK_SELECTED_CHANNEL(Index, 1);
        CHECK_SELECTED_CHANNEL(Fresh, 2);
        CHECK_SELECTED_CHANNEL(Mixed, 3);
        CHECK_SELECTED_CHANNEL(Mixed, 4);
        CHECK_SELECTED_CHANNEL(Mixed, 5);
        CHECK_SELECTED_CHANNEL(Mixed, 6);
        CHECK_SELECTED_CHANNEL(Mixed, 3);
        ASSERT_VECTORS_EQUAL(
            TVector<ui32>{},
            channels.GetUnwritableChannels()
        );

        channels.UpdateChannelStats(1, false, false, 0);
        channels.UpdateChannelStats(3, false, false, 0);
        channels.UpdateChannelStats(4, false, false, 0);

        CHECK_SELECTED_CHANNEL_EMPTY(Index);
        CHECK_SELECTED_CHANNEL(Fresh, 2);
        CHECK_SELECTED_CHANNEL(Mixed, 5);
        CHECK_SELECTED_CHANNEL(Mixed, 6);
        CHECK_SELECTED_CHANNEL(Mixed, 5);

        ASSERT_VECTORS_EQUAL(
            TVector<ui32>({ 1, 3, 4 }),
            channels.GetUnwritableChannels()
        );

        // check idempotency
        channels.UpdateChannelStats(6, false, false, 0);
        channels.UpdateChannelStats(6, false, false, 0);

        CHECK_SELECTED_CHANNEL_EMPTY(Index);
        CHECK_SELECTED_CHANNEL(Fresh, 2);
        CHECK_SELECTED_CHANNEL(Mixed, 5);
        CHECK_SELECTED_CHANNEL(Mixed, 5);

        ASSERT_VECTORS_EQUAL(
            TVector<ui32>({ 1, 3, 4, 6 }),
            channels.GetUnwritableChannels()
        );

        channels.UpdateChannelStats(2, false, false, 0);
        channels.UpdateChannelStats(5, false, false, 0);

        CHECK_SELECTED_CHANNEL_EMPTY(Index);
        CHECK_SELECTED_CHANNEL_EMPTY(Fresh);
        CHECK_SELECTED_CHANNEL_EMPTY(Mixed);

        ASSERT_VECTORS_EQUAL(
            TVector<ui32>({ 1, 2, 3, 4, 5, 6 }),
            channels.GetUnwritableChannels()
        );
    }

    Y_UNIT_TEST(ShouldGetChannelsToMove)
    {
        const ui32 t = 10;

        TChannels channels = SetupChannels(100);
        ASSERT_VECTORS_EQUAL(
            TVector<ui32>{},
            channels.GetChannelsToMove(t));

        for (ui32 c = 10; c < 19; ++c) {
            channels.UpdateChannelStats(c, true, true, 0);
        }

        ASSERT_VECTORS_EQUAL(
            TVector<ui32>{},
            channels.GetChannelsToMove(t));

        channels.UpdateChannelStats(19, true, true, 0);

        ASSERT_VECTORS_EQUAL(
            TVector<ui32>({10, 11, 12, 13, 14, 15, 16, 17, 18, 19}),
            channels.GetChannelsToMove(t));
    }

    Y_UNIT_TEST(ShouldBalanceChannelsBasedOnFreeSpace)
    {
        const ui32 channelCount = 11;
        TChannels channels = SetupChannels(channelCount);
        channels.UpdateChannelStats(3, true, false, 0);
        channels.UpdateChannelStats(4, false, false, 0);
        channels.UpdateChannelStats(5, false, false, 0.9);
        channels.UpdateChannelStats(6, true, false, 0.175);
        channels.UpdateChannelStats(7, true, false, 0.05);
        channels.UpdateChannelStats(8, true, false, 0.1);
        channels.UpdateChannelStats(9, true, false, 0.25);
        channels.UpdateChannelStats(10, true, false, 0.5);

        TVector<ui32> counts(channelCount);
        const ui32 iters = 10000;
        for (ui32 i = 0; i < iters; ++i) {
            auto selected = channels.SelectChannel(
                EChannelDataKind::Mixed,
                minFreeSpace,
                freeSpaceThreshold);
            UNIT_ASSERT(selected.Defined());
            ++counts[*selected];
        }

        // Writable == false
        UNIT_ASSERT_VALUES_EQUAL(0, counts[4]);
        UNIT_ASSERT_VALUES_EQUAL(0, counts[5]);

        // freeSpace <= minFreeSpace
        UNIT_ASSERT_VALUES_EQUAL(0, counts[7]);
        UNIT_ASSERT_VALUES_EQUAL(0, counts[8]);

        // 3, 6, 9, 10 channels remain
        // channel 6 should be selected 2 times less often then the other ones
        const auto halfShare = iters / 7.;
        const auto otherShare = 2 * halfShare;
        UNIT_ASSERT_DOUBLES_EQUAL(otherShare, counts[3], halfShare * 0.2);
        UNIT_ASSERT_DOUBLES_EQUAL(halfShare, counts[6], halfShare * 0.2);
        UNIT_ASSERT_DOUBLES_EQUAL(otherShare, counts[9], halfShare * 0.2);
        UNIT_ASSERT_DOUBLES_EQUAL(otherShare, counts[10], halfShare * 0.2);
    }

    Y_UNIT_TEST(ShouldSelectChannelWithLargestFreeSpaceShareIfAllChannelsAreAlmostFull)
    {
        const ui32 channelCount = 11;
        TChannels channels = SetupChannels(channelCount);
        channels.UpdateChannelStats(3, true, false, 0.01);
        channels.UpdateChannelStats(4, false, false, 0);
        channels.UpdateChannelStats(5, false, false, 0.9);
        channels.UpdateChannelStats(6, true, false, 0.075);
        channels.UpdateChannelStats(7, true, false, 0.05);
        channels.UpdateChannelStats(8, true, false, 0.01);
        channels.UpdateChannelStats(9, true, false, 0.025);
        channels.UpdateChannelStats(10, true, false, 0.05);

        TVector<ui32> counts(channelCount);
        const ui32 iters = 1000;
        for (ui32 i = 0; i < iters; ++i) {
            auto selected = channels.SelectChannel(
                EChannelDataKind::Mixed,
                minFreeSpace,
                freeSpaceThreshold);
            UNIT_ASSERT(selected.Defined());
            ++counts[*selected];
        }

        // Writable == false
        UNIT_ASSERT_VALUES_EQUAL(0, counts[4]);
        UNIT_ASSERT_VALUES_EQUAL(0, counts[5]);

        // best free space share
        UNIT_ASSERT_VALUES_EQUAL(iters, counts[6]);

        // other channels
        UNIT_ASSERT_VALUES_EQUAL(0, counts[3]);
        UNIT_ASSERT_VALUES_EQUAL(0, counts[7]);
        UNIT_ASSERT_VALUES_EQUAL(0, counts[8]);
        UNIT_ASSERT_VALUES_EQUAL(0, counts[9]);
        UNIT_ASSERT_VALUES_EQUAL(0, counts[10]);
    }
}

}   // namespace NCloud::NFileStore::NStorage
