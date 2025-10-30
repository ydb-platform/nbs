#include "switchable_session.h"

#include "switchable_client.h"

#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/durable.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/service/service_test.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NClient {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

static const TString DefaultMountToken = "mountToken";
static const TString DefaultClientVersionInfo = "aa@bbb";
static const TString DefaultClientId = "client-id";
static constexpr ui64 DefaultBlockCount = 1024;

enum class EHandledOn
{
    Primary,
    Secondary,
};

class TStorageHelper
{
public:
    static NThreading::TFuture<NProto::TZeroBlocksResponse> DoExecute(
        IStorage* storage,
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request)
    {
        return storage->ZeroBlocks(std::move(callContext), std::move(request));
    }

    static NThreading::TFuture<NProto::TReadBlocksLocalResponse> DoExecute(
        IStorage* storage,
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
    {
        return storage->ReadBlocksLocal(
            std::move(callContext),
            std::move(request));
    }

    static NThreading::TFuture<NProto::TWriteBlocksLocalResponse> DoExecute(
        IStorage* storage,
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
    {
        return storage->WriteBlocksLocal(
            std::move(callContext),
            std::move(request));
    }
};

template <typename TRequest, typename TResponse>
class TTestMethod
{
private:
    using TFunc = std::function<NThreading::TFuture<TResponse>(
        std::shared_ptr<TRequest> request)>;

    const TString PrimaryDiskId;
    const TString PrimarySessionId;
    const TString SecondaryDiskId;
    const TString SecondarySessionId;

    std::shared_ptr<TTestService> PrimaryClient;
    std::shared_ptr<TTestService> SecondaryClient;

    size_t PrimaryRequestCount = 0;
    size_t SecondaryRequestCount = 0;

public:
    TTestMethod(
        const TString& primaryDiskId,
        const TString& primarySessionId,
        const TString& secondaryDiskId,
        const TString& secondarySessionId,
        std::shared_ptr<TTestService> primaryClient,
        std::shared_ptr<TTestService> secondaryClient)
        : PrimaryDiskId(primaryDiskId)
        , PrimarySessionId(primarySessionId)
        , SecondaryDiskId(secondaryDiskId)
        , SecondarySessionId(secondarySessionId)
        , PrimaryClient(std::move(primaryClient))
        , SecondaryClient(std::move(secondaryClient))

    {
        TFunc primaryHandler = [&](std::shared_ptr<TRequest> request)

        {
            UNIT_ASSERT_VALUES_EQUAL(PrimaryDiskId, request->GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL(PrimarySessionId, request->GetSessionId());

            ++PrimaryRequestCount;
            TResponse response;
            *response.MutableError() = MakeError(E_ARGUMENT, "some error");
            return MakeFuture(std::move(response));
        };
        PrimaryClient->SetHandler(std::move(primaryHandler));

        TFunc secondaryHandler = [&](std::shared_ptr<TRequest> request)
        {
            UNIT_ASSERT_VALUES_EQUAL(SecondaryDiskId, request->GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL(
                SecondarySessionId,
                request->GetSessionId());

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

template <typename TRequest, typename TResponse>
void Check(
    ISessionPtr session,
    std::shared_ptr<TTestService> client1,
    std::shared_ptr<TTestService> client2,
    const TString& diskId,
    const TString& sessionId,
    const TString& secondaryDiskId,
    const TString& secondarySessionId,
    EHandledOn handledOn)
{
    TTestMethod<TRequest, TResponse> testMethod(
        diskId,
        sessionId,
        secondaryDiskId,
        secondarySessionId,
        std::move(client1),
        std::move(client2));

    auto request = std::make_shared<TRequest>();
    request->SetDiskId(diskId);
    request->SetSessionId(sessionId);

    auto future = TStorageHelper::DoExecute(
        session.get(),
        MakeIntrusive<TCallContext>(),
        std::move(request));

    const auto& response = future.GetValue(TDuration::Seconds(5));
    UNIT_ASSERT_VALUES_EQUAL_C(
        handledOn == EHandledOn::Primary ? E_ARGUMENT : E_ABORTED,
        response.GetError().GetCode(),
        FormatError(response.GetError()));

    UNIT_ASSERT_VALUES_EQUAL(
        handledOn == EHandledOn::Primary ? 1 : 0,
        testMethod.GetPrimaryRequestCount());
    UNIT_ASSERT_VALUES_EQUAL(
        handledOn == EHandledOn::Secondary ? 1 : 0,
        testMethod.GetSecondaryRequestCount());
}

struct TTestSession
{
    ISchedulerPtr Scheduler;
    std::shared_ptr<TTestService> DataClient;
    ISwitchableBlockStorePtr SwitchableClient;
    ISessionPtr Session;
};

TTestSession CreateTestSession(const TString& diskId, const TString& sessionId)
{
    //   Session
    //      |
    //      v
    // DurableClient
    //      |
    //      v
    // SwitchableClient
    //      |
    //      v
    //  TTestService

    auto timer = CreateCpuCycleTimer();
    auto scheduler = CreateScheduler(timer);
    auto logging = CreateLoggingService("console");
    auto config = std::make_shared<TClientAppConfig>();
    auto requestStats = CreateRequestStatsStub();
    auto volumeStats = CreateVolumeStatsStub();
    auto retryPolicy = CreateRetryPolicy(config, NProto::STORAGE_MEDIA_DEFAULT);

    scheduler->Start();

    auto client = std::make_shared<TTestService>();
    {
        client->MountVolumeHandler =
            [=](std::shared_ptr<NProto::TMountVolumeRequest> request)
        {
            UNIT_ASSERT_EQUAL(request->GetDiskId(), diskId);
            UNIT_ASSERT_EQUAL(request->GetToken(), DefaultMountToken);

            NProto::TMountVolumeResponse response;
            response.SetSessionId(sessionId);

            auto& volume = *response.MutableVolume();
            volume.SetDiskId(request->GetDiskId());
            volume.SetBlockSize(DefaultBlockSize);
            volume.SetBlocksCount(DefaultBlockCount);

            return MakeFuture(response);
        };
        client->UnmountVolumeHandler =
            [=](std::shared_ptr<NProto::TUnmountVolumeRequest> request)
        {
            UNIT_ASSERT_EQUAL(request->GetDiskId(), diskId);
            return MakeFuture<NProto::TUnmountVolumeResponse>();
        };
    }

    auto switchableClient = CreateSwitchableClient(logging, diskId, client);

    auto durable = CreateDurableClient(
        config,
        switchableClient,
        retryPolicy,
        logging,
        timer,
        scheduler,
        requestStats,
        volumeStats);

    auto session = CreateSession(
        timer,
        scheduler,
        logging,
        requestStats,
        volumeStats,
        durable,
        config,
        TSessionConfig{
            .DiskId = diskId,
            .MountToken = DefaultMountToken,
            .ClientVersionInfo = DefaultClientVersionInfo});
    {
        NProto::THeaders headers;
        headers.SetClientId(DefaultClientId);
        auto future =
            session->MountVolume(MakeIntrusive<TCallContext>(), headers);
    }

    return TTestSession{
        .Scheduler = scheduler,
        .DataClient = client,
        .SwitchableClient = switchableClient,
        .Session = session};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSwitchableSessionTest)
{
    Y_UNIT_TEST(ShouldForwardRequestsToPrimaryClient)
    {
        TTestSession session1 = CreateTestSession("disk-1", "session-1");
        TTestSession session2 = CreateTestSession("disk-2", "session-2");

        auto switchableSession = CreateSwitchableSession(
            CreateLoggingService("console"),
            session1.Scheduler,
            "disk-1",
            session1.Session,
            session1.SwitchableClient);

        Check<
            NProto::TReadBlocksLocalRequest,
            NProto::TReadBlocksLocalResponse>(
            switchableSession,
            session1.DataClient,
            session2.DataClient,
            "disk-1",
            "session-1",
            "disk-2",
            "session-2",
            EHandledOn::Primary);
        Check<
            NProto::TWriteBlocksLocalRequest,
            NProto::TWriteBlocksLocalResponse>(
            switchableSession,
            session1.DataClient,
            session2.DataClient,
            "disk-1",
            "session-1",
            "disk-2",
            "session-2",
            EHandledOn::Primary);
        Check<NProto::TZeroBlocksRequest, NProto::TZeroBlocksResponse>(
            switchableSession,
            session1.DataClient,
            session2.DataClient,
            "disk-1",
            "session-1",
            "disk-2",
            "session-2",
            EHandledOn::Primary);
    }

    Y_UNIT_TEST(ShouldForwardRequestsToSecondaryClientAfterSwitch)
    {
        TTestSession session1 = CreateTestSession("disk-1", "session-1");
        TTestSession session2 = CreateTestSession("disk-2", "session-2");

        auto switchableSession = CreateSwitchableSession(
            CreateLoggingService("console"),
            session1.Scheduler,
            "disk-1",
            session1.Session,
            session1.SwitchableClient);

        session1.SwitchableClient->BeforeSwitching();
        auto drainFuture = switchableSession->SwitchSession(
            "disk-2",
            "session-2",
            session2.Session,
            session2.SwitchableClient);
        session1.SwitchableClient->AfterSwitching();

        Check<
            NProto::TReadBlocksLocalRequest,
            NProto::TReadBlocksLocalResponse>(
            switchableSession,
            session1.DataClient,
            session2.DataClient,
            "disk-1",
            "session-1",
            "disk-2",
            "session-2",
            EHandledOn::Secondary);
        Check<
            NProto::TWriteBlocksLocalRequest,
            NProto::TWriteBlocksLocalResponse>(
            switchableSession,
            session1.DataClient,
            session2.DataClient,
            "disk-1",
            "session-1",
            "disk-2",
            "session-2",
            EHandledOn::Secondary);
        Check<NProto::TZeroBlocksRequest, NProto::TZeroBlocksResponse>(
            switchableSession,
            session1.DataClient,
            session2.DataClient,
            "disk-1",
            "session-1",
            "disk-2",
            "session-2",
            EHandledOn::Secondary);

        drainFuture.Wait(TDuration::Seconds(15));
        UNIT_ASSERT_VALUES_EQUAL(true, drainFuture.HasValue());
    }

    Y_UNIT_TEST(ShouldForwardRetriedRequestsToSecondaryClientAfterSwitch)
    {
        TTestSession session1 = CreateTestSession("disk-1", "session-1");
        TTestSession session2 = CreateTestSession("disk-2", "session-2");

        auto switchableSession = CreateSwitchableSession(
            CreateLoggingService("console"),
            session1.Scheduler,
            "disk-1",
            session1.Session,
            session1.SwitchableClient);

        // Prepare the handler that will slow down the response to the request
        // so that at the time of switching the session there will be requests
        // in flight.
        TPromise<NProto::TReadBlocksLocalResponse> readPromise =
            NewPromise<NProto::TReadBlocksLocalResponse>();
        size_t primaryReadCount = 0;
        session1.DataClient->SetHandler(
            [&](std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
            {
                Y_UNUSED(request);
                ++primaryReadCount;
                return readPromise;
            });

        // Send request to primary disk before session switch
        auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
        request->SetDiskId("disk-1");
        request->SetSessionId("session-1");

        auto readFuture = TStorageHelper::DoExecute(
            switchableSession.get(),
            MakeIntrusive<TCallContext>(),
            std::move(request));

        // The request is processed on the lower layer (TTestService).
        UNIT_ASSERT_VALUES_EQUAL(false, readFuture.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(1, primaryReadCount);

        // Preapare the handler for the secondary disk, which will response S_OK
        // for request.
        size_t secondaryRequestCount = 0;
        session2.DataClient->SetHandler(
            [&](std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
            {
                UNIT_ASSERT_VALUES_EQUAL("disk-2", request->GetDiskId());
                UNIT_ASSERT_VALUES_EQUAL("session-2", request->GetSessionId());

                ++secondaryRequestCount;
                NProto::TReadBlocksLocalResponse response;
                *response.MutableError() = MakeError(S_OK);
                return MakeFuture(std::move(response));
            });

        // Start switching to the second session
        session1.SwitchableClient->BeforeSwitching();
        auto drainFuture = switchableSession->SwitchSession(
            "disk-2",
            "session-2",
            session2.Session,
            session2.SwitchableClient);
        session1.SwitchableClient->AfterSwitching();

        // Check that the session has not switched, as there are requests in
        // flight.
        Cerr << "Before drainFuture.Wait\n";
        drainFuture.Wait(TDuration::Seconds(2.5));
        UNIT_ASSERT_VALUES_EQUAL(false, drainFuture.HasValue());

        // Responding with a retriable error to the request from the first disk.
        // This will cause the durable client to retry request, and the request
        // will forwarded by switchable client to second session that will
        // successfully complete the request.
        {
            NProto::TReadBlocksLocalResponse response;
            *response.MutableError() = MakeError(E_REJECTED, "some error");
            Cerr << "Before readPromise.SetValue\n";
            readPromise.SetValue(std::move(response));
            Cerr << "After readPromise.SetValue\n";
        }

        // Check that the request was completed successfully.
        const auto& readResponse = readFuture.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(1, secondaryRequestCount);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            readResponse.GetError().GetCode(),
            FormatError(readResponse.GetError()));

        // Checking that the old session has drained
        drainFuture.Wait(TDuration::Seconds(3));
        UNIT_ASSERT_VALUES_EQUAL(true, drainFuture.HasValue());
    }

    Y_UNIT_TEST(ShouldSwitchMultipleTimes)
    {
        constexpr size_t SwitchCount = 10;

        TString oldDiskId = "disk-0";
        TString oldSessionId = "session-0";
        TTestSession oldSession = CreateTestSession(oldDiskId, oldSessionId);

        auto switchableSession = CreateSwitchableSession(
            CreateLoggingService("console"),
            oldSession.Scheduler,
            oldDiskId,
            oldSession.Session,
            oldSession.SwitchableClient);

        for (size_t i = 1; i < SwitchCount; ++i) {
            TString newDiskId = "disk-" + ToString(i);
            TString newSessionId = "session-" + ToString(i);

            TTestSession newSession =
                CreateTestSession(newDiskId, newSessionId);

            oldSession.SwitchableClient->BeforeSwitching();
            auto drainFuture = switchableSession->SwitchSession(
                newDiskId,
                newSessionId,
                newSession.Session,
                newSession.SwitchableClient);
            oldSession.SwitchableClient->AfterSwitching();

            Check<
                NProto::TReadBlocksLocalRequest,
                NProto::TReadBlocksLocalResponse>(
                switchableSession,
                oldSession.DataClient,
                newSession.DataClient,
                oldDiskId,
                oldSessionId,
                newDiskId,
                newSessionId,
                EHandledOn::Secondary);

            drainFuture.Wait(TDuration::Seconds(3));
            UNIT_ASSERT_VALUES_EQUAL(true, drainFuture.HasValue());

            oldSession = newSession;
            oldDiskId = newDiskId;
            oldSessionId = newSessionId;
        }
    }
}

}   // namespace NCloud::NBlockStore::NClient
