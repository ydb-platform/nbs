#include "logbroker.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NLogbroker {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

static constexpr TDuration WaitTimeout = TDuration::Seconds(30);

////////////////////////////////////////////////////////////////////////////////

auto CreateData()
{
    TVector<TMessage> messages{{"hello", 100}, {"world", 200}};

    return messages;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLogbrokerIfaceTest)
{
    Y_UNIT_TEST(ShouldStubWriteData)
    {
        auto service = CreateServiceStub();
        service->Start();

        auto r = service->Write(CreateData(), Now()).GetValue(WaitTimeout);

        UNIT_ASSERT_C(!HasError(r), "Unexpected error: " << FormatError(r));

        service->Stop();
    }
}

}   // namespace NCloud::NBlockStore::NLogbroker
