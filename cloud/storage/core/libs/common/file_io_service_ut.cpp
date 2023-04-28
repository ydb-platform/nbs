#include "file_io_service.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/file.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFileIOServiceTest)
{
    Y_UNIT_TEST(ShouldStubServiceCB)
    {
        auto service = CreateFileIOServiceStub();
        service->Start();

        TFileHandle dummy {INVALID_FHANDLE};

        TArrayRef<char> buffer { nullptr, 1024 };

        {
            NProto::TError error = MakeError(E_FAIL);

            service->AsyncRead(dummy, 0, buffer, [&] (const auto& er, ui32 n) {
                UNIT_ASSERT_VALUES_EQUAL(buffer.size(), n);
                error = er;
            });

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        }

        {
            NProto::TError error = MakeError(E_FAIL);

            service->AsyncWrite(dummy, 0, buffer, [&] (const auto& er, ui32 n) {
                UNIT_ASSERT_VALUES_EQUAL(buffer.size(), n);
                error = er;
            });

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        }

        service->Stop();
    }

    Y_UNIT_TEST(ShouldStubServiceFuture)
    {
        auto service = CreateFileIOServiceStub();
        service->Start();

        TFileHandle dummy {INVALID_FHANDLE};

        TArrayRef<char> buffer {nullptr, 1024};

        UNIT_ASSERT_VALUES_EQUAL(
            buffer.size(),
            service->AsyncRead(dummy, 0, buffer).GetValueSync());

        UNIT_ASSERT_VALUES_EQUAL(
            buffer.size(),
            service->AsyncWrite(dummy, 0, buffer).GetValueSync());

        service->Stop();
    }
}

}   // namespace NCloud
