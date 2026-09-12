#include "secure_erase_state.h"

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

void AssertStatus(
    const TSecureEraseState& state,
    const TString& deviceId,
    ESecureEraseStatus expected)
{
    const auto* erase = state.Find(deviceId);
    UNIT_ASSERT(erase);
    UNIT_ASSERT(erase->Status == expected);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSecureEraseStateTest)
{
    Y_UNIT_TEST(ShouldAddAndFindSecureErase)
    {
        TSecureEraseState state;

        UNIT_ASSERT(!state.Find("device-1"));
        auto& erase = state.GetOrAdd("device-1");
        erase.Generation = 42;

        UNIT_ASSERT_VALUES_EQUAL(42, state.Find("device-1")->Generation);

        const auto& constState = state;
        UNIT_ASSERT_VALUES_EQUAL(
            42,
            constState.Find("device-1")->Generation);
        UNIT_ASSERT(&erase == &state.GetOrAdd("device-1"));
    }

    Y_UNIT_TEST(ShouldReturnWaitingDevices)
    {
        TSecureEraseState state;
        state.GetOrAdd("waiting-1");
        state.GetOrAdd("waiting-2");
        state.GetOrAdd("completed").Status = ESecureEraseStatus::Completed;

        const auto devices = state.GetDevicesToErase();
        THashSet<TString> actual(devices.begin(), devices.end());

        UNIT_ASSERT_VALUES_EQUAL(2, actual.size());
        UNIT_ASSERT(actual.contains("waiting-1"));
        UNIT_ASSERT(actual.contains("waiting-2"));
    }

    Y_UNIT_TEST(ShouldReturnAllRequests)
    {
        TSecureEraseState state;
        auto request1 = MakeIntrusive<TRequestInfo>();
        auto request2 = MakeIntrusive<TRequestInfo>();
        state.GetOrAdd("device-1").Requests.push_back(request1);
        state.GetOrAdd("device-2").Requests.push_back(request2);

        const auto requests = state.GetRequests();

        UNIT_ASSERT_VALUES_EQUAL(2, requests.size());
        UNIT_ASSERT(
            std::find(requests.begin(), requests.end(), request1) !=
            requests.end());
        UNIT_ASSERT(
            std::find(requests.begin(), requests.end(), request2) !=
            requests.end());
    }

    Y_UNIT_TEST(ShouldHandleGenerationAndIdempotencyKey)
    {
        TSecureEraseState state;

        UNIT_ASSERT(!state.HandleRequest("device-1", 2, 10));
        const auto* erase = state.Find("device-1");
        UNIT_ASSERT(erase);
        UNIT_ASSERT_VALUES_EQUAL(2, erase->Generation);
        UNIT_ASSERT_VALUES_EQUAL(10, erase->IdempotencyKey);

        auto error = state.HandleRequest("device-2", 1, 20);
        UNIT_ASSERT(error);
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, error->GetCode());
        UNIT_ASSERT(!state.Find("device-2"));

        UNIT_ASSERT(!state.HandleRequest("legacy-device", 0, 123));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            state.Find("legacy-device")->IdempotencyKey);
    }

    Y_UNIT_TEST(ShouldReturnSuccessfulIdempotentResult)
    {
        TSecureEraseState state;
        UNIT_ASSERT(!state.HandleRequest("device-1", 1, 10));
        state.Start("device-1", "device-name-1");
        state.Complete("device-1", {});

        auto error = state.HandleRequest("device-1", 1, 10);

        UNIT_ASSERT(error);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error->GetCode());
    }

    Y_UNIT_TEST(ShouldRetryFailedOrNonIdempotentRequest)
    {
        TSecureEraseState state;
        UNIT_ASSERT(!state.HandleRequest("device-1", 1, 10));
        state.Start("device-1", "device-name-1");
        state.Complete("device-1", MakeError(E_IO, "erase failed"));

        UNIT_ASSERT(!state.HandleRequest("device-1", 1, 10));
        UNIT_ASSERT(!state.HandleRequest("device-1", 1, 11));
    }

    Y_UNIT_TEST(ShouldCheckWhetherSecureEraseCanStart)
    {
        TSecureEraseState state;
        state.GetOrAdd("device-1");
        state.GetOrAdd("device-2");
        state.GetOrAdd("device-3");

        UNIT_ASSERT(state.CanStart("device-1", "name-1", 2));
        UNIT_ASSERT(!state.CanStart("unknown", "unknown-name", 2));

        state.Start("device-1", "name-1");
        UNIT_ASSERT(!state.CanStart("device-1", "name-1", 2));
        UNIT_ASSERT(!state.CanStart("device-2", "name-1", 2));
        UNIT_ASSERT(state.CanStart("device-2", "name-2", 2));

        state.Start("device-2", "name-2");
        UNIT_ASSERT(!state.CanStart("device-3", "name-3", 2));
    }

    Y_UNIT_TEST(ShouldStartAndCompleteSecureErase)
    {
        TSecureEraseState state;
        state.GetOrAdd("device-1");

        state.Start("device-1", "device-name-1");

        UNIT_ASSERT(state.IsEraseInProgress("device-1"));
        AssertStatus(state, "device-1", ESecureEraseStatus::InProgress);
        UNIT_ASSERT_VALUES_EQUAL(
            "device-name-1",
            state.Find("device-1")->DeviceName);

        const auto error = MakeError(E_IO, "erase failed");
        auto& erase = state.Complete("device-1", error);

        UNIT_ASSERT(!state.IsEraseInProgress("device-1"));
        AssertStatus(state, "device-1", ESecureEraseStatus::Completed);
        UNIT_ASSERT_VALUES_EQUAL(E_IO, erase.Error.GetCode());
        UNIT_ASSERT(!state.CanStart("device-1", "device-name-1", 1));
    }

    Y_UNIT_TEST(ShouldAllowIoWithoutSecureErase)
    {
        TSecureEraseState state;

        UNIT_ASSERT(
            state.ApproveIo("unknown") == EIoApproveStatus::Allow);
    }

    Y_UNIT_TEST(ShouldAllowIoAndRemoveCompletedSecureErase)
    {
        TSecureEraseState state;
        state.GetOrAdd("device-1");
        state.Start("device-1", "device-name-1");
        state.Complete("device-1", {});

        UNIT_ASSERT(
            state.ApproveIo("device-1") == EIoApproveStatus::Allow);
        UNIT_ASSERT(!state.Find("device-1"));
    }

    Y_UNIT_TEST(ShouldDeclineIoWhileSecureEraseIsWaiting)
    {
        TSecureEraseState state;
        state.GetOrAdd("device-1");

        UNIT_ASSERT(
            state.ApproveIo("device-1") == EIoApproveStatus::Decline);
        AssertStatus(state, "device-1", ESecureEraseStatus::Wait);
    }

    Y_UNIT_TEST(ShouldDeclineIoDuringSecureErase)
    {
        TSecureEraseState state;
        state.GetOrAdd("device-1");
        state.Start("device-1", "device-name-1");

        UNIT_ASSERT(
            state.ApproveIo("device-1") == EIoApproveStatus::Decline);
        AssertStatus(state, "device-1", ESecureEraseStatus::InProgress);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
