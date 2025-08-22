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
concept HasDiskId = requires(T& obj) {
    {
        obj.GetDiskId()
    } -> std::same_as<void>;
};

template <typename T>
concept HasSessionId = requires(T& obj) {
    {
        obj.GetSessionId()
    } -> std::same_as<void>;
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
            if constexpr (HasDiskId<TRequest>) {
                UNIT_ASSERT_VALUES_EQUAL(PrimaryDiskId, request->GetDiskId());
            }
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
            if constexpr (HasDiskId<TRequest>) {
                UNIT_ASSERT_VALUES_EQUAL(SecondaryDiskId, request->GetDiskId());
            }
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

}   // namespace

namespace Details {

#define BLOCKSTORE_DECLARE_EXECUTE(name, ...)                                \
    NThreading::TFuture<NProto::T##name##Response> Execute(                  \
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

}   // namespace Details

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

        Details::Check<NProto::TReadBlocksRequest, NProto::TReadBlocksResponse>(
            client1,
            client2,
            switchableClient,
            EHandledOn::Primary);
        Details::Check<
            NProto::TReadBlocksLocalRequest,
            NProto::TReadBlocksLocalResponse>(
            client1,
            client2,
            switchableClient,
            EHandledOn::Primary);
        Details::
            Check<NProto::TWriteBlocksRequest, NProto::TWriteBlocksResponse>(
                client1,
                client2,
                switchableClient,
                EHandledOn::Primary);
        Details::Check<
            NProto::TWriteBlocksLocalRequest,
            NProto::TWriteBlocksLocalResponse>(
            client1,
            client2,
            switchableClient,
            EHandledOn::Primary);
        Details::Check<NProto::TZeroBlocksRequest, NProto::TZeroBlocksResponse>(
            client1,
            client2,
            switchableClient,
            EHandledOn::Primary);

        Details::
            Check<NProto::TMountVolumeRequest, NProto::TMountVolumeResponse>(
                client1,
                client2,
                switchableClient,
                EHandledOn::Primary);
    }

    Y_UNIT_TEST(ShouldForwardRequestsToSecondaryClient)
    {
        auto client1 = std::make_shared<TTestService>();
        auto client2 = std::make_shared<TTestService>();

        auto switchableClient = CreateSwitchableClient(
            CreateLoggingService("console"),
            PrimaryDiskId,
            client1);

        // Switch client to secondary client.
        switchableClient->Switch(client2, SecondaryDiskId, SecondarySessionId);

        Details::Check<NProto::TReadBlocksRequest, NProto::TReadBlocksResponse>(
            client1,
            client2,
            switchableClient,
            EHandledOn::Secondary);
        Details::Check<
            NProto::TReadBlocksLocalRequest,
            NProto::TReadBlocksLocalResponse>(
            client1,
            client2,
            switchableClient,
            EHandledOn::Secondary);
        Details::
            Check<NProto::TWriteBlocksRequest, NProto::TWriteBlocksResponse>(
                client1,
                client2,
                switchableClient,
                EHandledOn::Secondary);
        Details::Check<
            NProto::TWriteBlocksLocalRequest,
            NProto::TWriteBlocksLocalResponse>(
            client1,
            client2,
            switchableClient,
            EHandledOn::Secondary);
        Details::Check<NProto::TZeroBlocksRequest, NProto::TZeroBlocksResponse>(
            client1,
            client2,
            switchableClient,
            EHandledOn::Secondary);

        // Only Read/Write/Zero requests switched to secondary client
        Details::
            Check<NProto::TMountVolumeRequest, NProto::TMountVolumeResponse>(
                client1,
                client2,
                switchableClient,
                EHandledOn::Primary);
    }
}

}   // namespace NCloud::NBlockStore::NClient
