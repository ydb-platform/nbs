#include "monitoring.h"

#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>

#include <cerrno>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMonitoringServiceTest)
{
    Y_UNIT_TEST(ShouldStartOnAvailablePort)
    {
        const auto port = NTesting::GetFreePort();
        auto monitoring = CreateMonitoringService(port, "127.0.0.1");

        UNIT_ASSERT_NO_EXCEPTION(monitoring->Start());
        monitoring->Stop();
    }

    Y_UNIT_TEST(ShouldFailToStartWhenPortIsInUse)
    {
        const auto port = NTesting::GetFreePort();
        auto first = CreateMonitoringService(port, "127.0.0.1");
        auto second = CreateMonitoringService(port, "127.0.0.1");

        first->Start();

        UNIT_ASSERT_EXCEPTION_SATISFIES(
            second->Start(),
            TSystemError,
            [](const TSystemError& e) { return e.Status() == EADDRINUSE; });

        second->Stop();
        first->Stop();
    }
}

}   // namespace NCloud
