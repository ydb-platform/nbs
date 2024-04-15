#include "app.h"

#include <library/cpp/actors/util/should_continue.h>

#include <library/cpp/testing/unittest/registar.h>


namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TAppMainThreadTest)
{
    Y_UNIT_TEST(ShouldHandleStopBeforeAppMain)
    {
        TProgramShouldContinue shouldContinue;
        AppCreate();
        AppStop();
        AppMain(shouldContinue);
    }
}

}   // namespace NCloud
