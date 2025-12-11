#include "poll.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(
    TTimerHandleTest){Y_UNIT_TEST(ShouldTrigger){TPollHandle pollHandle;
TTimerHandle timerHandle;

pollHandle.Attach(timerHandle.Handle(), EPOLLIN | EPOLLET, &timerHandle);

timerHandle.Set(TDuration::MicroSeconds(1));

size_t signaled = pollHandle.Wait(TDuration::Seconds(1));
UNIT_ASSERT_EQUAL(signaled, 1);
UNIT_ASSERT_EQUAL(pollHandle.GetEvent(0).data.ptr, &timerHandle);

signaled = pollHandle.Wait(TDuration::Seconds(1));
UNIT_ASSERT_EQUAL(signaled, 0);
}   // namespace NCloud::NBlockStore::NRdma

Y_UNIT_TEST(ShouldCancel)
{
    TPollHandle pollHandle;
    TTimerHandle timerHandle;
    pollHandle.Attach(timerHandle.Handle(), EPOLLIN, &timerHandle);

    // cancel armed timer that hasn't triggered yet
    timerHandle.Set(TDuration::MicroSeconds(1));
    timerHandle.Clear();

    size_t signaled = pollHandle.Wait(TDuration::Seconds(1));
    UNIT_ASSERT_EQUAL(signaled, 0);

    timerHandle.Set(TDuration::MicroSeconds(1));
    signaled = pollHandle.Wait(TDuration::Seconds(1));
    UNIT_ASSERT_EQUAL(signaled, 1);

    // won't go away until consumed or cleared
    signaled = pollHandle.Wait(TDuration::Seconds(1));
    UNIT_ASSERT_EQUAL(signaled, 1);

    // discards triggered events
    timerHandle.Clear();
    signaled = pollHandle.Wait(TDuration::Seconds(1));
    UNIT_ASSERT_EQUAL(signaled, 0);
}
}
;

}   // namespace NCloud::NBlockStore::NRdma
