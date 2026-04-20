#include "poll.h"

#include <library/cpp/testing/gtest/gtest.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

TEST(TTimerHandleTest, ShouldTrigger)
{
    TPollHandle pollHandle;
    TTimerHandle timerHandle;

    pollHandle.Attach(
        timerHandle.Handle(),
        EPOLLIN | EPOLLET,
        &timerHandle);

    timerHandle.Set(TDuration::MicroSeconds(1));

    size_t signaled = pollHandle.Wait(TDuration::Seconds(1));
    ASSERT_EQ(signaled, 1u);
    ASSERT_EQ(pollHandle.GetEvent(0).data.ptr, &timerHandle);

    signaled = pollHandle.Wait(TDuration::Seconds(1));
    ASSERT_EQ(signaled, 0u);
}

TEST(TTimerHandleTest, ShouldCancel)
{
    TPollHandle pollHandle;
    TTimerHandle timerHandle;
    pollHandle.Attach(timerHandle.Handle(), EPOLLIN, &timerHandle);

    // cancel armed timer that hasn't triggered yet
    timerHandle.Set(TDuration::MicroSeconds(1));
    timerHandle.Clear();

    size_t signaled = pollHandle.Wait(TDuration::Seconds(1));
    ASSERT_EQ(signaled, 0u);

    timerHandle.Set(TDuration::MicroSeconds(1));
    signaled = pollHandle.Wait(TDuration::Seconds(1));
    ASSERT_EQ(signaled, 1u);

    // won't go away until consumed or cleared
    signaled = pollHandle.Wait(TDuration::Seconds(1));
    ASSERT_EQ(signaled, 1u);

    // discards triggered events
    timerHandle.Clear();
    signaled = pollHandle.Wait(TDuration::Seconds(1));
    ASSERT_EQ(signaled, 0u);
}

}   // namespace NCloud::NBlockStore::NRdma
