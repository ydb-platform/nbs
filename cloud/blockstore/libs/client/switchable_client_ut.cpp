#include "switchable_client.h"

#include <cloud/blockstore/libs/service/service_test.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NClient {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString PrimaryDiskId = "primary_disk";
const TString SecondaryDiskId = "secondary_disk";

const TString PrimarySessionId = "session1";
const TString SecondarySessionId = "session2";

enum class EHandledOn
{
    Primary,
    Secondary,
};

template <typename T>
concept HasSessionId = requires(T& obj) {
    { obj.GetSessionId() } -> std::convertible_to<TString>;
};

template <typename TRequest, typename TResponse>
class TTestMethod
{
private:
    using TFunc = std::function<NThreading::TFuture<TResponse>(
        std::shared_ptr<TRequest> request)>;

    std::shared_ptr<TTestService> PrimaryClient;
    std::shared_ptr<TTestService> SecondaryClient;

    size_t PrimaryRequestCount = 0;
    size_t SecondaryRequestCount = 0;

public:
    TTestMethod(
        std::shared_ptr<TTestService> primaryClient,
        std::shared_ptr<TTestService> secondaryClient)
        : PrimaryClient(std::move(primaryClient))
        , SecondaryClient(std::move(secondaryClient))

    {
        TFunc primaryHandler = [&](std::shared_ptr<TRequest> request)

        {
            UNIT_ASSERT_VALUES_EQUAL(PrimaryDiskId, request->GetDiskId());

            if constexpr (HasSessionId<TRequest>) {
                UNIT_ASSERT_VALUES_EQUAL(
                    PrimarySessionId,
                    request->GetSessionId());
            }

            ++PrimaryRequestCount;
            TResponse response;
            *response.MutableError() = MakeError(E_ARGUMENT, "some error");
            return MakeFuture(std::move(response));
        };
        PrimaryClient->SetHandler(std::move(primaryHandler));

        TFunc secondaryHandler = [&](std::shared_ptr<TRequest> request)
        {
            UNIT_ASSERT_VALUES_EQUAL(SecondaryDiskId, request->GetDiskId());

            if constexpr (HasSessionId<TRequest>) {
                UNIT_ASSERT_VALUES_EQUAL(
                    SecondarySessionId,
                    request->GetSessionId());
            }

            ++SecondaryRequestCount;
            TResponse response;
            *response.MutableError() =
                MakeError(E_ABORTED, "some another error");
            return MakeFuture(std::move(response));
        };
        SecondaryClient->SetHandler(std::move(secondaryHandler));
    }

    [[nodiscard]] size_t GetPrimaryRequestCount() const
    {
        return PrimaryRequestCount;
    }

    [[nodiscard]] size_t GetSecondaryRequestCount() const
    {
        return SecondaryRequestCount;
    }
};

struct TTestDataPlainMethods
{
    TTestMethod<NProto::TReadBlocksRequest, NProto::TReadBlocksResponse> Read;
    TTestMethod<
        NProto::TReadBlocksLocalRequest,
        NProto::TReadBlocksLocalResponse>
        ReadLocal;
    TTestMethod<NProto::TWriteBlocksRequest, NProto::TWriteBlocksResponse>
        Write;
    TTestMethod<
        NProto::TWriteBlocksLocalRequest,
        NProto::TWriteBlocksLocalResponse>
        WriteLocal;
    TTestMethod<NProto::TZeroBlocksRequest, NProto::TZeroBlocksResponse> Zero;

    TTestDataPlainMethods(
        std::shared_ptr<TTestService> primaryClient,
        std::shared_ptr<TTestService> secondaryClient)
        : Read{primaryClient, secondaryClient}
        , ReadLocal(primaryClient, secondaryClient)
        , Write(primaryClient, secondaryClient)
        , WriteLocal(primaryClient, secondaryClient)
        , Zero(primaryClient, secondaryClient)
    {}
};

#define BLOCKSTORE_DECLARE_EXECUTE(name, ...)                                \
    [[maybe_unused]] NThreading::TFuture<NProto::T##name##Response> Execute( \
        IBlockStorePtr blockstore,                                           \
        TCallContextPtr callContext,                                         \
        std::shared_ptr<NProto::T##name##Request> request)                   \
    {                                                                        \
        return blockstore->name(std::move(callContext), std::move(request)); \
    }

BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_EXECUTE)
#undef BLOCKSTORE_DECLARE_EXECUTE

template <typename TRequest, typename TResponse>
void Check(
    std::shared_ptr<TTestService> primaryClient,
    std::shared_ptr<TTestService> secondaryClient,
    IBlockStorePtr switchableClient,
    EHandledOn handledOn)
{
    TTestMethod<TRequest, TResponse> testMethod(
        std::move(primaryClient),
        std::move(secondaryClient));

    auto request = std::make_shared<TRequest>();
    request->SetDiskId(PrimaryDiskId);
    if constexpr (HasSessionId<TRequest>) {
        request->SetSessionId(PrimarySessionId);
    }

    auto future = Execute(
        std::move(switchableClient),
        MakeIntrusive<TCallContext>(),
        std::move(request));

    const auto& response = future.GetValue(TDuration::Seconds(5));
    UNIT_ASSERT_VALUES_EQUAL(
        handledOn == EHandledOn::Primary ? 1 : 0,
        testMethod.GetPrimaryRequestCount());
    UNIT_ASSERT_VALUES_EQUAL(
        handledOn == EHandledOn::Secondary ? 1 : 0,
        testMethod.GetSecondaryRequestCount());
    UNIT_ASSERT_VALUES_EQUAL_C(
        handledOn == EHandledOn::Primary ? E_ARGUMENT : E_ABORTED,
        response.GetError().GetCode(),
        FormatError(response.GetError()));
}

template <typename TRequest, typename TResponse>
TFuture<TResponse> CheckRequestPaused(
    std::shared_ptr<TTestService> primaryClient,
    std::shared_ptr<TTestService> secondaryClient,
    IBlockStorePtr switchableClient)
{
    TTestMethod<TRequest, TResponse> testMethod(
        std::move(primaryClient),
        std::move(secondaryClient));

    auto request = std::make_shared<TRequest>();
    request->SetDiskId(PrimaryDiskId);
    if constexpr (HasSessionId<TRequest>) {
        request->SetSessionId(PrimarySessionId);
    }

    auto future = Execute(
        std::move(switchableClient),
        MakeIntrusive<TCallContext>(),
        std::move(request));

    future.Wait(TDuration::Seconds(0.5));
    UNIT_ASSERT(!future.HasValue());
    UNIT_ASSERT_VALUES_EQUAL(0, testMethod.GetPrimaryRequestCount());
    UNIT_ASSERT_VALUES_EQUAL(0, testMethod.GetSecondaryRequestCount());
    return future;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSwitchableClientTest)
{
    Y_UNIT_TEST(ShouldForwardRequestsToPrimaryClient)
    {
        auto client1 = std::make_shared<TTestService>();
        auto client2 = std::make_shared<TTestService>();

        auto switchableClient = CreateSwitchableClient(
            CreateLoggingService("console"),
            PrimaryDiskId,
            client1);

        Check<NProto::TReadBlocksRequest, NProto::TReadBlocksResponse>(
            client1,
            client2,
            switchableClient,
            EHandledOn::Primary);
        Check<
            NProto::TReadBlocksLocalRequest,
            NProto::TReadBlocksLocalResponse>(
            client1,
            client2,
            switchableClient,
            EHandledOn::Primary);

        Check<NProto::TWriteBlocksRequest, NProto::TWriteBlocksResponse>(
            client1,
            client2,
            switchableClient,
            EHandledOn::Primary);
        Check<
            NProto::TWriteBlocksLocalRequest,
            NProto::TWriteBlocksLocalResponse>(
            client1,
            client2,
            switchableClient,
            EHandledOn::Primary);
        Check<NProto::TZeroBlocksRequest, NProto::TZeroBlocksResponse>(
            client1,
            client2,
            switchableClient,
            EHandledOn::Primary);

        Check<NProto::TMountVolumeRequest, NProto::TMountVolumeResponse>(
            client1,
            client2,
            switchableClient,
            EHandledOn::Primary);
    }

    void DoShouldPauseAndUnpauseRequests(bool doSwitch)
    {
        auto client1 = std::make_shared<TTestService>();
        auto client2 = std::make_shared<TTestService>();

        auto switchableClient = CreateSwitchableClient(
            CreateLoggingService("console"),
            PrimaryDiskId,
            client1);

        // Disable request forwarding
        switchableClient->BeforeSwitching();

        // Data-plane request should pause
        auto readFuture = CheckRequestPaused<
            NProto::TReadBlocksRequest,
            NProto::TReadBlocksResponse>(client1, client2, switchableClient);
        auto readLocalFuture = CheckRequestPaused<
            NProto::TReadBlocksLocalRequest,
            NProto::TReadBlocksLocalResponse>(
            client1,
            client2,
            switchableClient);
        auto writeFuture = CheckRequestPaused<
            NProto::TWriteBlocksRequest,
            NProto::TWriteBlocksResponse>(client1, client2, switchableClient);
        auto writeLocalFuture = CheckRequestPaused<
            NProto::TWriteBlocksLocalRequest,
            NProto::TWriteBlocksLocalResponse>(
            client1,
            client2,
            switchableClient);
        auto zeroFuture = CheckRequestPaused<
            NProto::TZeroBlocksRequest,
            NProto::TZeroBlocksResponse>(client1, client2, switchableClient);

        // Mount request shouldn't pause
        Check<NProto::TMountVolumeRequest, NProto::TMountVolumeResponse>(
            client1,
            client2,
            switchableClient,
            EHandledOn::Primary);

        if (doSwitch) {
            switchableClient->Switch(
                client2,
                SecondaryDiskId,
                SecondarySessionId);
        }

        TTestDataPlainMethods testMethods{client1, client2};
        // unpause requests
        switchableClient->AfterSwitching();

        auto checkRequest =
            [doSwitch](const auto& future, const auto& testMethod)
        {
            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_VALUES_EQUAL_C(
                doSwitch ? E_ABORTED : E_ARGUMENT,
                response.GetError().GetCode(),
                FormatError(response.GetError()));
            UNIT_ASSERT_VALUES_EQUAL(
                doSwitch ? 0 : 1,
                testMethod.GetPrimaryRequestCount());
            UNIT_ASSERT_VALUES_EQUAL(
                doSwitch ? 1 : 0,
                testMethod.GetSecondaryRequestCount());
        };
        checkRequest(readFuture, testMethods.Read);
        checkRequest(readLocalFuture, testMethods.ReadLocal);
        checkRequest(writeFuture, testMethods.Write);
        checkRequest(writeLocalFuture, testMethods.WriteLocal);
        checkRequest(zeroFuture, testMethods.Zero);
    }

    Y_UNIT_TEST(ShouldPauseAndUnpauseRequestsWithoutSwitching)
    {
        DoShouldPauseAndUnpauseRequests(false);
    }

    Y_UNIT_TEST(ShouldPauseAndUnpauseRequestsWithSwitching)
    {
        DoShouldPauseAndUnpauseRequests(true);
    }

    Y_UNIT_TEST(ShouldForwardRequestsToSecondaryClient)
    {
        auto client1 = std::make_shared<TTestService>();
        auto client2 = std::make_shared<TTestService>();

        auto switchableClient = CreateSwitchableClient(
            CreateLoggingService("console"),
            PrimaryDiskId,
            client1);

        switchableClient->BeforeSwitching();

        // Switch client to secondary client.
        switchableClient->Switch(client2, SecondaryDiskId, SecondarySessionId);

        // Read/Write/Zero requests routed to secondary client
        Check<NProto::TReadBlocksRequest, NProto::TReadBlocksResponse>(
            client1,
            client2,
            switchableClient,
            EHandledOn::Secondary);
        Check<
            NProto::TReadBlocksLocalRequest,
            NProto::TReadBlocksLocalResponse>(
            client1,
            client2,
            switchableClient,
            EHandledOn::Secondary);

        Check<NProto::TWriteBlocksRequest, NProto::TWriteBlocksResponse>(
            client1,
            client2,
            switchableClient,
            EHandledOn::Secondary);
        Check<
            NProto::TWriteBlocksLocalRequest,
            NProto::TWriteBlocksLocalResponse>(
            client1,
            client2,
            switchableClient,
            EHandledOn::Secondary);
        Check<NProto::TZeroBlocksRequest, NProto::TZeroBlocksResponse>(
            client1,
            client2,
            switchableClient,
            EHandledOn::Secondary);

        // Control-plane requests routed to primary client
        Check<NProto::TMountVolumeRequest, NProto::TMountVolumeResponse>(
            client1,
            client2,
            switchableClient,
            EHandledOn::Primary);
    }

    Y_UNIT_TEST(ShouldForwardRequestsToSecondaryClientAfterSwitch)
    {
        auto client1 = std::make_shared<TTestService>();
        auto client2 = std::make_shared<TTestService>();

        auto switchableClient = CreateSwitchableClient(
            CreateLoggingService("console"),
            PrimaryDiskId,
            client1);

        {
            // Switch client to secondary client.
            auto guard = CreateSessionSwitchingGuard(switchableClient);

            // Check request paused after BeforeSwitching() called by guard.
            auto zeroFuture = CheckRequestPaused<
                NProto::TZeroBlocksRequest,
                NProto::TZeroBlocksResponse>(
                client1,
                client2,
                switchableClient);

            // Perform switch
            switchableClient->Switch(
                client2,
                SecondaryDiskId,
                SecondarySessionId);

            // Request should be routed ot secondary immediately since the
            // switch happened.
            Check<NProto::TReadBlocksRequest, NProto::TReadBlocksResponse>(
                client1,
                client2,
                switchableClient,
                EHandledOn::Secondary);

            TTestMethod<NProto::TZeroBlocksRequest, NProto::TZeroBlocksResponse>
                testZeroMethod(client1, client2);

            // guard should call AfterSwitching().
            guard.reset();

            // Check request unpaused
            const auto& response = zeroFuture.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ABORTED,
                response.GetError().GetCode(),
                FormatError(response.GetError()));
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                testZeroMethod.GetPrimaryRequestCount());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                testZeroMethod.GetSecondaryRequestCount());
        }

        // Request routed to secondary client after AfterSwitching() called by
        // guard.
        Check<
            NProto::TReadBlocksLocalRequest,
            NProto::TReadBlocksLocalResponse>(
            client1,
            client2,
            switchableClient,
            EHandledOn::Secondary);

        // Control-plane requests routed to primary client
        Check<NProto::TMountVolumeRequest, NProto::TMountVolumeResponse>(
            client1,
            client2,
            switchableClient,
            EHandledOn::Primary);
    }
}

}   // namespace NCloud::NBlockStore::NClient
