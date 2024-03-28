#include "throttler.h"

#include <cloud/contrib/vhost/bio.h>
#include <cloud/storage/core/libs/common/timer_test.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NVHostServer {

enum class EIoType
{
    Read,
    Write
};

/* libvhost virtio-blk private IO structure */
struct virtio_blk_io
{
    void* opaque[2];
    struct vhd_io io;
    struct vhd_bdev_io bdev_io;
};

struct virtio_blk_io GetIo(EIoType ioType, size_t size)
{
    virtio_blk_io vbio;
    vbio.bdev_io.type =
        ioType == EIoType::Read ? VHD_BDEV_READ : VHD_BDEV_WRITE;
    vbio.bdev_io.total_sectors = size >> 9;

    return vbio;
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TThrottlerTest)
{
    Y_UNIT_TEST(ShouldThrottleRequestsBWCorrectly)
    {
        auto timer = std::make_shared<TTestTimer>();
        timer->AdvanceTime(TDuration::Seconds(1));

        auto checkBW = [&timer](auto throttler, auto vbio)
        {
            UNIT_ASSERT_VALUES_EQUAL(throttler->ThrottleIo(&vbio.io), false);
            UNIT_ASSERT_VALUES_EQUAL(throttler->ThrottleIo(&vbio.io), true);
            UNIT_ASSERT_VALUES_EQUAL(throttler->HasThrottledIos(), true);

            timer->AdvanceTime(TDuration::Seconds(1));
            auto resumedIo = throttler->ResumeNextThrottledIo();
            UNIT_ASSERT_VALUES_EQUAL(throttler->HasThrottledIos(), false);
            UNIT_ASSERT_VALUES_EQUAL(resumedIo, &vbio.io);
        };

        checkBW(
            CreateThrottler(
                timer,
                50_MB,   // maxReadBandwidth
                50,      // maxReadIops
                0,       // maxWriteBandwidth
                0        // maxWriteIops
                ),
            GetIo(EIoType::Read, 40_MB));

        checkBW(
            CreateThrottler(
                timer,
                0,       // maxReadBandwidth
                0,       // maxReadIops
                50_MB,   // maxWriteBandwidth
                50       // maxWriteIops
                ),
            GetIo(EIoType::Write, 40_MB));
    }

    Y_UNIT_TEST(ShouldThrottleRequestsIopsCorrectly)
    {
        auto timer = std::make_shared<TTestTimer>();
        timer->AdvanceTime(TDuration::Seconds(1));

        auto checkIops = [&timer](auto throttler, auto vbio)
        {
            UNIT_ASSERT_VALUES_EQUAL(throttler->ThrottleIo(&vbio.io), false);
            UNIT_ASSERT_VALUES_EQUAL(throttler->ThrottleIo(&vbio.io), false);
            UNIT_ASSERT_VALUES_EQUAL(throttler->ThrottleIo(&vbio.io), true);
            UNIT_ASSERT_VALUES_EQUAL(throttler->HasThrottledIos(), true);

            timer->AdvanceTime(TDuration::Seconds(1));
            auto resumedIo = throttler->ResumeNextThrottledIo();
            UNIT_ASSERT_VALUES_EQUAL(throttler->HasThrottledIos(), false);
            UNIT_ASSERT_VALUES_EQUAL(resumedIo, &vbio.io);
        };

        checkIops(
            CreateThrottler(
                timer,
                0,        // maxReadBandwidth
                2,        // maxReadIops
                0,        // maxWriteBandwidth
                0         // maxWriteIops
                ),
            GetIo(EIoType::Read, 40_MB));

        checkIops(
            CreateThrottler(
                timer,
                0,        // maxReadBandwidth
                0,        // maxReadIops
                0,        // maxWriteBandwidth
                2         // maxWriteIops
                ),
            GetIo(EIoType::Write, 40_MB));
    }

    Y_UNIT_TEST(DontThrottleRequestsWhenStopped)
    {
        auto timer = std::make_shared<TTestTimer>();
        timer->AdvanceTime(TDuration::Seconds(1));

        auto vbio = GetIo(EIoType::Write, 40_MB);
        auto throttler = CreateThrottler(
            timer,
            0,    // maxReadBandwidth
            1,    // maxReadIops
            0,    // maxWriteBandwidth
            1     // maxWriteIops
        );

        UNIT_ASSERT_VALUES_EQUAL(throttler->ThrottleIo(&vbio.io), false);

        for (int i = 0; i < 5; i++) {
            UNIT_ASSERT_VALUES_EQUAL(throttler->ThrottleIo(&vbio.io), true);
        }

        throttler->Stop();

        for (int i = 0; i < 5; i++) {
            UNIT_ASSERT_VALUES_EQUAL(throttler->ThrottleIo(&vbio.io), false);
        }
    }
}

}   // namespace NCloud::NBlockStore::NVHostServer
