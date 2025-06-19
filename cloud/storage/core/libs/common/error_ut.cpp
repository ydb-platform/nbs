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

    Y_UNIT_TEST(ShouldRetrieveErrorKindRejectedByCheckpointFromErrorMessage)
    {
        NProto::TError e;
        e.SetCode(E_REJECTED);
        e.SetMessage(
            "Checkpoint reject request. WriteBlocksLocal is not allowed if a "
            "checkpoint exists");

        UNIT_ASSERT_EQUAL(
            EDiagnosticsErrorKind::ErrorWriteRejectedByCheckpoint,
            GetDiagnosticsErrorKind(e));
    }

    Y_UNIT_TEST(ShouldWrapExceptionInResponse)
    {
        auto response = SafeExecute<TTestResponse>(
            []
            {
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

    Y_UNIT_TEST(ShouldCorrectlyInterpretGrpcErrors)
    {
        {
            constexpr EWellKnownResultCodes errors[] = {
                E_GRPC_CANCELLED,
                E_GRPC_UNKNOWN,
                E_GRPC_INVALID_ARGUMENT,
                E_GRPC_DEADLINE_EXCEEDED,
                E_GRPC_NOT_FOUND,
                E_GRPC_ALREADY_EXISTS,
                E_GRPC_PERMISSION_DENIED,
                E_GRPC_RESOURCE_EXHAUSTED,
                E_GRPC_FAILED_PRECONDITION,
                E_GRPC_ABORTED,
                E_GRPC_OUT_OF_RANGE,
                E_GRPC_INTERNAL,
                E_GRPC_UNAVAILABLE,
                E_GRPC_DATA_LOSS,
                E_GRPC_UNAUTHENTICATED};

            for (auto errorCode: errors) {
                NProto::TError e;
                e.SetCode(errorCode);

                UNIT_ASSERT_VALUES_EQUAL(
                    EErrorKind::ErrorRetriable,
                    GetErrorKind(e));
            }
        }

        {
            NProto::TError e;
            e.SetCode(E_GRPC_UNIMPLEMENTED);

            UNIT_ASSERT_VALUES_EQUAL(EErrorKind::ErrorFatal, GetErrorKind(e));
        }
    }

    Y_UNIT_TEST(ShouldCorrectlyInterpretErrorKindForSystemErrors)
    {
        constexpr std::array FatalSystemErrors = {EIO, ENODATA};

        for (ui32 i = 0; i < 1000; i++) {
            NProto::TError e = MakeError(MAKE_SYSTEM_ERROR(i));
            if (FindPtr(FatalSystemErrors, i)) {
                UNIT_ASSERT_VALUES_EQUAL(
                    EErrorKind::ErrorFatal,
                    GetErrorKind(e));
            } else {
                UNIT_ASSERT_VALUES_EQUAL(
                    EErrorKind::ErrorRetriable,
                    GetErrorKind(e));
            }
        }
    }
}

}   // namespace NCloud
