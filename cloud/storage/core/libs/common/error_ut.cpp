#include "error.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTestResponse
{
private:
    NProto::TError Error;

public:
    TTestResponse()
    {}

    TTestResponse(ui32 code, TString message = {})
        : Error(MakeError(code, message))
    {}

    const NProto::TError& GetError() const
    {
        return Error;
    }

    NProto::TError* MutableError()
    {
        return &Error;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(GetDiagnosticsErrorKindTest)
{
    // TODO: Do not retrieve E_TIMEOUT, E_THROTTLED from error message
    // on client side: NBS-568
    Y_UNIT_TEST(ShouldRetrieveErrorKindThrottlingFromErrorMessage)
    {
        NProto::TError e;
        e.SetCode(E_REJECTED);
        e.SetMessage("Throttled");

        UNIT_ASSERT_EQUAL(
            EDiagnosticsErrorKind::ErrorThrottling,
            GetDiagnosticsErrorKind(e));
    }

    Y_UNIT_TEST(ShouldWrapExceptionInResponse)
    {
        auto response = SafeExecute<TTestResponse>([] {
            throw TServiceError(E_REJECTED) << "request cancelled";
            return TTestResponse();
        });
    }

    Y_UNIT_TEST(ShouldExtractResponse)
    {
        auto future = NThreading::MakeFuture<TTestResponse>();
        auto response = ExtractResponse(future);
    }

    Y_UNIT_TEST(ShouldGetResultOrError)
    {
        auto future = NThreading::MakeFuture();
        auto response = ResultOrError(future);
    }

    Y_UNIT_TEST(ShouldDecomposeResultOrError)
    {
        {
            const TString expected = "expected";

            auto [value, error] = TResultOrError<TString>(expected);

            UNIT_ASSERT(!HasError(error));
            UNIT_ASSERT_VALUES_EQUAL(expected, value);
        }

        {
            const auto expected = MakeError(E_REJECTED, "Throttled");

            auto [value, error] = TResultOrError<TString>(expected);

            UNIT_ASSERT(HasError(error));
            UNIT_ASSERT_VALUES_EQUAL(expected.GetCode(), error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(expected.GetMessage(), error.GetMessage());
        }

        {
            auto [value, error] = TResultOrError<TMoveOnly>(TMoveOnly{});

            UNIT_ASSERT(!HasError(error));
            const TMoveOnly moveOnly = std::move(value);
            Y_UNUSED(moveOnly);
        }
    }
}

}   // namespace NCloud
