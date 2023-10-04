#include "channels.h"

#include <cloud/filestore/libs/storage/testlib/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

#define CHECK_SELECTED_CHANNEL(dataKind, expected)                             \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        expected,                                                              \
        *channels.SelectChannel(EChannelDataKind::dataKind));                  \
//  CHECK_SELECTED_CHANNEL

#define CHECK_SELECTED_CHANNEL_EMPTY(dataKind)                                 \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        false,                                                                 \
        channels.SelectChannel(EChannelDataKind::dataKind).Defined());         \
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

        channels.RegisterUnwritableChannel(1);
        channels.RegisterUnwritableChannel(3);
        channels.RegisterUnwritableChannel(4);

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
        channels.RegisterUnwritableChannel(6);
        channels.RegisterUnwritableChannel(6);

        CHECK_SELECTED_CHANNEL_EMPTY(Index);
        CHECK_SELECTED_CHANNEL(Fresh, 2);
        CHECK_SELECTED_CHANNEL(Mixed, 5);
        CHECK_SELECTED_CHANNEL(Mixed, 5);

        ASSERT_VECTORS_EQUAL(
            TVector<ui32>({ 1, 3, 4, 6 }),
            channels.GetUnwritableChannels()
        );

        channels.RegisterUnwritableChannel(2);
        channels.RegisterUnwritableChannel(5);

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
            channels.RegisterChannelToMove(c);
        }

        ASSERT_VECTORS_EQUAL(
            TVector<ui32>{},
            channels.GetChannelsToMove(t));

        channels.RegisterChannelToMove(19);

        ASSERT_VECTORS_EQUAL(
            TVector<ui32>({10, 11, 12, 13, 14, 15, 16, 17, 18, 19}),
            channels.GetChannelsToMove(t));
    }
}

}   // namespace NCloud::NFileStore::NStorage
