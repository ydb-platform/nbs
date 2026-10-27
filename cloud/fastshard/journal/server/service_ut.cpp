#include "service.h"

#include "device_manager.h"

#include <cloud/fastshard/journal/iface/device.h>
#include <cloud/fastshard/journal/iface/journalled_device.h>
#include <cloud/fastshard/protos/device.pb.h>

#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>

#include <util/datetime/base.h>
#include <util/generic/hash_set.h>
#include <util/generic/size_literals.h>
#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/string/printf.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>
#include <util/thread/pool.h>

#include <atomic>
#include <optional>
#include <utility>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultBlockSize = 4_KB;

////////////////////////////////////////////////////////////////////////////////

struct TZeroPagesCall
{
    TString DeviceUUID;
    TPageRangeRef Region;
    ui32 BlockSize = 0;
    TVector<TPageRangeRef> Ranges;
};

////////////////////////////////////////////////////////////////////////////////

struct TTestDevice final: public IDevice
{
    TString DeviceUUID;
    TPageRangeRef Region;
    ui32 BlockSize = 0;

    TVector<TZeroPagesCall>& ZeroPagesCalls;
    NProto::TError ZeroPagesError;

    TTestDevice(
            TString deviceUUID,
            TPageRangeRef region,
            ui32 blockSize,
            TVector<TZeroPagesCall>& zeroPagesCalls,
            NProto::TError zeroPagesError)
        : DeviceUUID(std::move(deviceUUID))
        , Region(region)
        , BlockSize(blockSize)
        , ZeroPagesCalls(zeroPagesCalls)
        , ZeroPagesError(std::move(zeroPagesError))
    {}

    [[nodiscard]] auto ReadPages(TVector<TPageRangeRef> /*rangeRefs*/)
        -> TFuture<TResultOrError<TVector<TBuffer>>> final
    {
        UNIT_FAIL("unexpected ReadPages");
        return {};
    }

    [[nodiscard]] auto WritePages(TVector<TPageRange> /*ranges*/)
        -> TFuture<NProto::TError> final
    {
        UNIT_FAIL("unexpected WritePages");
        return {};
    }

    [[nodiscard]] auto ZeroPages(TVector<TPageRangeRef> ranges)
        -> TFuture<NProto::TError> final
    {
        ZeroPagesCalls.push_back(
            {.DeviceUUID = DeviceUUID,
             .Region = Region,
             .BlockSize = BlockSize,
             .Ranges = std::move(ranges)});

        return MakeFuture(ZeroPagesError);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TAccessDeviceCall
{
    TString DeviceUUID;
    TString ClientId;
    NProto::EAccessMode AccessMode = NProto::ACCESS_READ_ONLY;
};

////////////////////////////////////////////////////////////////////////////////

struct TTestDeviceManager final: public IDeviceManager
{
    // The clients that have acquired the devices.
    THashSet<TString> Clients;

    TVector<NProto::TAcquireDevicesRequest> AcquireDevicesRequests;
    TVector<NProto::TReleaseDevicesRequest> ReleaseDevicesRequests;
    TVector<TAccessDeviceCall> AccessDeviceCalls;
    TVector<TZeroPagesCall> ZeroPagesCalls;

    NProto::TError ZeroPagesError;

    [[nodiscard]] auto AcquireDevices(NProto::TAcquireDevicesRequest request)
        -> TFuture<NProto::TAcquireDevicesResponse> final
    {
        AcquireDevicesRequests.push_back(std::move(request));

        NProto::TAcquireDevicesResponse response;
        *response.MutableError() = MakeError(E_BS_INVALID_SESSION, "acquire");
        return MakeFuture(std::move(response));
    }

    [[nodiscard]] auto ReleaseDevices(NProto::TReleaseDevicesRequest request)
        -> TFuture<NProto::TReleaseDevicesResponse> final
    {
        ReleaseDevicesRequests.push_back(std::move(request));

        NProto::TReleaseDevicesResponse response;
        *response.MutableError() = MakeError(E_REJECTED, "release");
        return MakeFuture(std::move(response));
    }

    [[nodiscard]] NProto::TError AccessDevice(
        const TString& deviceUUID,
        const TString& clientId,
        NProto::EAccessMode accessMode) final
    {
        AccessDeviceCalls.push_back(
            {.DeviceUUID = deviceUUID,
             .ClientId = clientId,
             .AccessMode = accessMode});

        if (!Clients.contains(clientId)) {
            return MakeError(E_BS_INVALID_SESSION, "not acquired");
        }

        return {};
    }

    [[nodiscard]] IDevicePtr CreateDevice(
        const TString& deviceUUID,
        TPageRangeRef region,
        ui32 blockSize) final
    {
        return std::make_shared<TTestDevice>(
            deviceUUID,
            region,
            blockSize,
            ZeroPagesCalls,
            ZeroPagesError);
    }
};

////////////////////////////////////////////////////////////////////////////////

// Answers every request with the name of the device in the error message, so
// the tests can tell which device has served a request. The service starts and
// stops the device in the executor, so the state is shared between threads.
struct TTestJournalledDevice final: public IJournalledDevice
{
    const TString Name;
    NProto::TError StartError;

    // When set, Start does not complete until CompleteStart is called.
    bool DeferStart = false;

    // The service starts the devices in the executor and a deferred device
    // from the thread that completes the previous one, so the start state is
    // shared between threads and kept under one lock: a start count observed
    // under the lock means the start promise is already set.
    TMutex Lock;
    std::optional<TPromise<NProto::TError>> StartPromise;
    ui32 StartCount = 0;

    std::atomic<ui32> StopCount = 0;

    explicit TTestJournalledDevice(TString name)
        : Name(std::move(name))
    {}

    TFuture<NProto::TError> Start() noexcept final
    {
        auto guard = Guard(Lock);

        ++StartCount;

        if (!DeferStart) {
            return MakeFuture(StartError);
        }

        //
        // Fail a double start instead of asserting on it: Start may not
        // throw, so the start counts are checked at the end of the test
        //

        if (StartPromise) {
            return MakeFuture(
                MakeError(E_FAIL, Name + " is already starting"));
        }

        StartPromise = NewPromise<NProto::TError>();
        return StartPromise->GetFuture();
    }

    void CompleteStart()
    {
        auto promise = TakeStartPromise();
        UNIT_ASSERT_C(promise, Name + " is not starting");

        //
        // Set the value outside the lock: the service starts the next device
        // right in the subscriber of this promise
        //

        promise->SetValue(StartError);
    }

    bool IsStarting()
    {
        auto guard = Guard(Lock);
        return StartPromise.has_value();
    }

    ui32 GetStartCount()
    {
        auto guard = Guard(Lock);
        return StartCount;
    }

private:
    std::optional<TPromise<NProto::TError>> TakeStartPromise()
    {
        auto guard = Guard(Lock);
        return std::exchange(StartPromise, std::nullopt);
    }

public:

    TFuture<void> Stop() noexcept final
    {
        ++StopCount;
        return MakeFuture();
    }

    [[nodiscard]] auto ReadPages(NProto::TReadPagesRequest /*request*/)
        -> TFuture<NProto::TReadPagesResponse> final
    {
        return MakeResponse<NProto::TReadPagesResponse>();
    }

    [[nodiscard]] auto WriteLogRecord(
        NProto::TWriteLogRecordRequest /*request*/)
        -> TFuture<NProto::TWriteLogRecordResponse> final
    {
        return MakeResponse<NProto::TWriteLogRecordResponse>();
    }

    [[nodiscard]] auto ReadJournalTail(
        NProto::TReadJournalTailRequest /*request*/)
        -> TFuture<NProto::TReadJournalTailResponse> final
    {
        return MakeResponse<NProto::TReadJournalTailResponse>();
    }

    [[nodiscard]] auto AdvanceLsnLowWatermark(
        NProto::TAdvanceLsnLowWatermarkRequest /*request*/)
        -> TFuture<NProto::TAdvanceLsnLowWatermarkResponse> final
    {
        return MakeResponse<NProto::TAdvanceLsnLowWatermarkResponse>();
    }

private:
    template <typename TResponse>
    TFuture<TResponse> MakeResponse() const
    {
        TResponse response;
        *response.MutableError() = MakeError(S_OK, Name);
        return MakeFuture(std::move(response));
    }
};

////////////////////////////////////////////////////////////////////////////////

// Hands out the test devices by their UUIDs: the service asks for a device
// when it starts and again every time it formats the device.
struct TTestJournalledDeviceFactory final: public IJournalledDeviceFactory
{
    THashMap<TString, std::shared_ptr<TTestJournalledDevice>> Devices;
    THashMap<TString, NProto::TError> CreateErrors;

    TResultOrError<IJournalledDevicePtr> Create(
        const TJournalledDeviceConfig& config) final
    {
        with_lock (Lock) {
            CreatedUUIDs.push_back(config.DeviceUUID);
        }

        if (const auto* error = CreateErrors.FindPtr(config.DeviceUUID)) {
            return *error;
        }

        const auto* device = Devices.FindPtr(config.DeviceUUID);
        Y_ABORT_UNLESS(
            device,
            "unexpected device %s",
            config.DeviceUUID.Quote().c_str());

        return IJournalledDevicePtr(*device);
    }

    TVector<TString> GetCreatedUUIDs()
    {
        auto guard = Guard(Lock);
        return CreatedUUIDs;
    }

private:
    TAdaptiveLock Lock;
    TVector<TString> CreatedUUIDs;
};

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    const TString ClientId = "client-id";

    const TJournalledDeviceConfig Configs[2]{
        {.DeviceUUID = "uuid-1",
         .BlocksCount = 1024,
         .BlockSize = DefaultBlockSize,
         .LogMetaSize = 16 * DefaultBlockSize,
         .LogDataSize = 64 * DefaultBlockSize},
        {.DeviceUUID = "uuid-2",
         .BlocksCount = 2048,
         .BlockSize = DefaultBlockSize,
         .LogMetaSize = 0,
         .LogDataSize = 0},
    };

    NMonitoring::TDynamicCountersPtr Counters;
    TExecutorPtr Executor;

    std::shared_ptr<TTestDeviceManager> DeviceManager;
    std::shared_ptr<TTestJournalledDeviceFactory> Factory;
    TVector<std::shared_ptr<TTestJournalledDevice>> Devices;
    IServerBackendPtr Service;

    // The devices of the last CreateDeferredStartService call, so that a
    // failed test can still let a pending start finish.
    TVector<std::shared_ptr<TTestJournalledDevice>> DeferredDevices;
    TThreadPool StartThread;

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        InitCriticalEventsCounter(Counters);

        StartThread.Start(1);

        Executor = TExecutor::Create("TestExecutor");
        Executor->Start();

        DeviceManager = std::make_shared<TTestDeviceManager>();
        DeviceManager->Clients.insert(ClientId);

        Factory = std::make_shared<TTestJournalledDeviceFactory>();

        TVector<TJournalledDeviceConfig> configs;
        for (const auto& config: Configs) {
            auto device =
                std::make_shared<TTestJournalledDevice>(config.DeviceUUID);
            Devices.push_back(device);
            Factory->Devices.emplace(config.DeviceUUID, std::move(device));
            configs.push_back(config);
        }

        Service = CreateService(
            Executor,
            DeviceManager,
            Factory,
            std::move(configs),
            std::size(Configs)   // restoreConcurrency
        );
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {
        //
        // Complete the starts a failed test has left pending, otherwise the
        // service's Start never returns and the thread cannot be stopped
        //

        while (StartingCount(DeferredDevices)) {
            CompleteAnyStart(DeferredDevices);
        }

        StartThread.Stop();

        Service.reset();
        Executor->Stop();
    }

    void StartService()
    {
        Service->Start();
    }

    // Creates a service over |count| devices that complete their start only
    // when told to.
    auto CreateDeferredStartService(ui32 count, ui32 restoreConcurrency)
        -> std::pair<
            IServerBackendPtr,
            TVector<std::shared_ptr<TTestJournalledDevice>>>
    {
        TVector<std::shared_ptr<TTestJournalledDevice>> devices;
        TVector<TJournalledDeviceConfig> configs;

        for (ui32 i = 0; i != count; ++i) {
            auto config = Configs[0];
            config.DeviceUUID = Sprintf("deferred-%02u", i);

            auto device =
                std::make_shared<TTestJournalledDevice>(config.DeviceUUID);
            device->DeferStart = true;

            devices.push_back(device);
            Factory->Devices.emplace(config.DeviceUUID, std::move(device));
            configs.push_back(std::move(config));
        }

        auto service = CreateService(
            Executor,
            DeviceManager,
            Factory,
            std::move(configs),
            restoreConcurrency);

        DeferredDevices = devices;

        return {std::move(service), std::move(devices)};
    }

    // The service's Start blocks until every device has started, while the
    // deferred devices complete only when the test tells them to, so the
    // start runs on its own thread.
    TFuture<void> StartInBackground(IServerBackendPtr service)
    {
        return Async(
            [service = std::move(service)] { service->Start(); },
            StartThread);
    }

    // Polls until the condition holds; fails the test after a while.
    template <typename TCondition>
    static void WaitUntil(const TString& what, TCondition condition)
    {
        const auto deadline = TInstant::Now() + TDuration::Seconds(30);

        while (!condition()) {
            UNIT_ASSERT_C(
                TInstant::Now() < deadline,
                "timed out waiting for " + what);

            Sleep(TDuration::MilliSeconds(1));
        }
    }

    using TTestDevices = TVector<std::shared_ptr<TTestJournalledDevice>>;

    static size_t StartingCount(const TTestDevices& devices)
    {
        return CountIf(
            devices,
            [](const auto& device) { return device->IsStarting(); });
    }

    static size_t StartedCount(const TTestDevices& devices)
    {
        return CountIf(
            devices,
            [](const auto& device) { return device->GetStartCount() != 0; });
    }

    // A double start fails the device instead of the test, so the start
    // counts are checked at the end.
    static void AssertStartedOnce(const TTestDevices& devices)
    {
        for (const auto& device: devices) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                1,
                device->GetStartCount(),
                device->Name);
        }
    }

    // The service starts the devices in no particular order, so the tests
    // complete whichever device is starting.
    static void CompleteAnyStart(const TTestDevices& devices)
    {
        auto it = FindIf(
            devices,
            [](const auto& device) { return device->IsStarting(); });

        UNIT_ASSERT_C(it != devices.end(), "no device is starting");
        (*it)->CompleteStart();
    }

    // Sends each kind of request that reaches the journalled device and
    // returns the errors of the responses.
    TVector<NProto::TError> SendJournalledDeviceRequests(
        const TString& deviceUUID)
    {
        TVector<NProto::TError> errors;
        for (auto& [error, _]:
             SendJournalledDeviceRequests(deviceUUID, ClientId))
        {
            errors.push_back(std::move(error));
        }
        return errors;
    }

    i64 CriticalEventCount(const TString& name) const
    {
        return Counters->GetCounter("AppCriticalEvents/" + name, true)->Val();
    }

    template <typename TRequest>
    static TRequest MakeRequest(const TString& deviceUUID, const TString& clientId)
    {
        TRequest request;
        request.MutableHeaders()->SetClientId(clientId);
        request.SetDeviceUUID(deviceUUID);
        return request;
    }

    using TResponses = TVector<std::pair<NProto::TError, NProto::EAccessMode>>;

    // Sends each kind of device request and returns the errors of the
    // responses along with the access mode each kind requires.
    TResponses SendDeviceRequests(
        const TString& deviceUUID,
        const TString& clientId)
    {
        auto responses = SendJournalledDeviceRequests(deviceUUID, clientId);
        responses.emplace_back(
            Service
                ->FormatDevice(MakeRequest<NProto::TFormatDeviceRequest>(
                    deviceUUID,
                    clientId))
                .GetValueSync()
                .GetError(),
            NProto::ACCESS_READ_WRITE);
        return responses;
    }

    // Sends each kind of request that reaches the journalled device, i.e.
    // all of them but FormatDevice.
    TResponses SendJournalledDeviceRequests(
        const TString& deviceUUID,
        const TString& clientId)
    {
        return {
            {Service
                 ->ReadPages(MakeRequest<NProto::TReadPagesRequest>(
                     deviceUUID,
                     clientId))
                 .GetValueSync()
                 .GetError(),
             NProto::ACCESS_READ_ONLY},
            {Service
                 ->WriteLogRecord(MakeRequest<NProto::TWriteLogRecordRequest>(
                     deviceUUID,
                     clientId))
                 .GetValueSync()
                 .GetError(),
             NProto::ACCESS_READ_WRITE},
            {Service
                 ->ReadJournalTail(
                     MakeRequest<NProto::TReadJournalTailRequest>(
                         deviceUUID,
                         clientId))
                 .GetValueSync()
                 .GetError(),
             NProto::ACCESS_READ_ONLY},
            {Service
                 ->AdvanceLsnLowWatermark(
                     MakeRequest<NProto::TAdvanceLsnLowWatermarkRequest>(
                         deviceUUID,
                         clientId))
                 .GetValueSync()
                 .GetError(),
             NProto::ACCESS_READ_WRITE},
        };
    }

    TFuture<NProto::TFormatDeviceResponse> FormatDeviceAsync(
        const TString& deviceUUID,
        bool wholeDevice)
    {
        auto request =
            MakeRequest<NProto::TFormatDeviceRequest>(deviceUUID, ClientId);
        request.SetWholeDevice(wholeDevice);

        return Service->FormatDevice(std::move(request));
    }

    NProto::TError FormatDevice(const TString& deviceUUID, bool wholeDevice)
    {
        return FormatDeviceAsync(deviceUUID, wholeDevice)
            .GetValueSync()
            .GetError();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceTest)
{
    Y_UNIT_TEST_F(ShouldRouteRequestsToDevices, TFixture)
    {
        StartService();

        for (const auto& config: Configs) {
            const auto& uuid = config.DeviceUUID;

            DeviceManager->AccessDeviceCalls.clear();

            const auto responses = SendDeviceRequests(uuid, ClientId);

            //
            // Expect every request but FormatDevice (the last one) to reach the
            // journalled device: the test device answers S_OK with its own name
            //

            for (size_t i = 0; i + 1 < responses.size(); ++i) {
                const auto& error = responses[i].first;
                UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), i);
                UNIT_ASSERT_VALUES_EQUAL_C(uuid, error.GetMessage(), i);
            }

            //
            // Expect FormatDevice to succeed and to report S_FALSE only when the
            // device has no journal: then there is nothing to wipe
            //

            UNIT_ASSERT_VALUES_EQUAL_C(
                config.LogMetaSize ? S_OK : S_FALSE,
                responses.back().first.GetCode(),
                uuid);

            // every request checks the client access to the device
            const auto& calls = DeviceManager->AccessDeviceCalls;
            UNIT_ASSERT_VALUES_EQUAL(responses.size(), calls.size());

            for (size_t i = 0; i != calls.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(uuid, calls[i].DeviceUUID, i);
                UNIT_ASSERT_VALUES_EQUAL_C(ClientId, calls[i].ClientId, i);
                UNIT_ASSERT_EQUAL_C(
                    responses[i].second,
                    calls[i].AccessMode,
                    i);
            }
        }
    }

    Y_UNIT_TEST_F(ShouldRejectRequestsToDevicesThatAreNotStarted, TFixture)
    {
        // The devices are created on start, until then nothing serves them

        for (const auto& config: Configs) {
            for (const auto& error:
                 SendJournalledDeviceRequests(config.DeviceUUID))
            {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    E_INVALID_STATE,
                    error.GetCode(),
                    FormatError(error));
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(0, Factory->GetCreatedUUIDs().size());

        for (const auto& device: Devices) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                0,
                device->GetStartCount(),
                device->Name);
        }
    }

    Y_UNIT_TEST_F(ShouldRejectUnknownDevice, TFixture)
    {
        StartService();

        const TString unknownUuid = "unknown";

        for (const auto& [error, _]: SendDeviceRequests(unknownUuid, ClientId)) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_NOT_FOUND,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_STRING_CONTAINS(
                error.GetMessage(),
                "Device " + unknownUuid.Quote() + " not found");
        }

        // the request is rejected before the client access is checked
        UNIT_ASSERT_VALUES_EQUAL(0, DeviceManager->AccessDeviceCalls.size());
    }

    Y_UNIT_TEST_F(ShouldRejectRequestsWithoutDeviceUUID, TFixture)
    {
        StartService();

        for (const auto& [error, _]: SendDeviceRequests({}, ClientId)) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_STRING_CONTAINS(error.GetMessage(), "empty device UUID");
        }

        UNIT_ASSERT_VALUES_EQUAL(0, DeviceManager->AccessDeviceCalls.size());
    }

    Y_UNIT_TEST_F(ShouldRejectRequestsWithoutClientId, TFixture)
    {
        StartService();

        for (const auto& [error, _]:
             SendDeviceRequests(Configs[0].DeviceUUID, {}))
        {
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_STRING_CONTAINS(error.GetMessage(), "empty client id");
        }

        UNIT_ASSERT_VALUES_EQUAL(0, DeviceManager->AccessDeviceCalls.size());
    }

    Y_UNIT_TEST_F(ShouldRejectClientsWithoutAccess, TFixture)
    {
        StartService();

        for (const auto& [error, _]:
             SendDeviceRequests(Configs[0].DeviceUUID, "other-client-id"))
        {
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_BS_INVALID_SESSION,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_VALUES_EQUAL("not acquired", error.GetMessage());
        }

        // nothing is formatted
        UNIT_ASSERT_VALUES_EQUAL(0, DeviceManager->ZeroPagesCalls.size());
        UNIT_ASSERT_VALUES_EQUAL(0, Devices[0]->StopCount.load());
    }

    Y_UNIT_TEST_F(ShouldForwardAcquireAndReleaseDevices, TFixture)
    {
        {
            NProto::TAcquireDevicesRequest request;
            request.MutableHeaders()->SetClientId(ClientId);
            *request.MutableDeviceUUIDs()->Add() = Configs[0].DeviceUUID;
            request.SetFastshardId("fastshard-id");
            request.SetGeneration(42);

            const auto response =
                Service->AcquireDevices(request).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(
                E_BS_INVALID_SESSION,
                response.GetError().GetCode());

            UNIT_ASSERT_VALUES_EQUAL(
                1,
                DeviceManager->AcquireDevicesRequests.size());
            UNIT_ASSERT_VALUES_EQUAL(
                request.DebugString(),
                DeviceManager->AcquireDevicesRequests[0].DebugString());
        }

        {
            NProto::TReleaseDevicesRequest request;
            request.MutableHeaders()->SetClientId(ClientId);
            *request.MutableDeviceUUIDs()->Add() = Configs[0].DeviceUUID;
            request.SetFastshardId("fastshard-id");
            request.SetGeneration(42);

            const auto response =
                Service->ReleaseDevices(request).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response.GetError().GetCode());

            UNIT_ASSERT_VALUES_EQUAL(
                1,
                DeviceManager->ReleaseDevicesRequests.size());
            UNIT_ASSERT_VALUES_EQUAL(
                request.DebugString(),
                DeviceManager->ReleaseDevicesRequests[0].DebugString());
        }
    }

    Y_UNIT_TEST_F(ShouldFormatJournalMetadata, TFixture)
    {
        StartService();

        const auto& config = Configs[0];
        const auto& device = Devices[0];
        const ui64 logMetaBlockCount = config.LogMetaSize / config.BlockSize;

        {
            const auto error = FormatDevice(config.DeviceUUID, false);
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));
        }

        const auto& calls = DeviceManager->ZeroPagesCalls;
        UNIT_ASSERT_VALUES_EQUAL(1, calls.size());

        const auto& call = calls[0];
        UNIT_ASSERT_VALUES_EQUAL(config.DeviceUUID, call.DeviceUUID);
        UNIT_ASSERT_VALUES_EQUAL(config.BlockSize, call.BlockSize);
        UNIT_ASSERT_VALUES_EQUAL(0, call.Region.FirstPageNo);
        UNIT_ASSERT_VALUES_EQUAL(logMetaBlockCount, call.Region.PageCount);

        UNIT_ASSERT_VALUES_EQUAL(1, call.Ranges.size());
        UNIT_ASSERT_VALUES_EQUAL(0, call.Ranges[0].FirstPageNo);
        UNIT_ASSERT_VALUES_EQUAL(logMetaBlockCount, call.Ranges[0].PageCount);

        //
        // Check that the journalled device was stopped before the wipe and
        // recreated after it
        //

        UNIT_ASSERT_VALUES_EQUAL(1, device->StopCount.load());
        UNIT_ASSERT_VALUES_EQUAL(2, device->GetStartCount());

        const auto created = Factory->GetCreatedUUIDs();
        UNIT_ASSERT_VALUES_EQUAL(2, Count(created, config.DeviceUUID));

        //
        // Check that the other device was left alone
        //

        UNIT_ASSERT_VALUES_EQUAL(0, Devices[1]->StopCount.load());
        UNIT_ASSERT_VALUES_EQUAL(1, Devices[1]->GetStartCount());

        //
        // Check that the recreated device serves the requests again
        //

        for (const auto& error: SendJournalledDeviceRequests(config.DeviceUUID))
        {
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_VALUES_EQUAL(config.DeviceUUID, error.GetMessage());
        }
    }

    Y_UNIT_TEST_F(ShouldFormatWholeDevice, TFixture)
    {
        StartService();

        for (const auto& config: Configs) {
            DeviceManager->ZeroPagesCalls.clear();

            {
                const auto error = FormatDevice(config.DeviceUUID, true);
                UNIT_ASSERT_VALUES_EQUAL_C(
                    S_OK,
                    error.GetCode(),
                    FormatError(error));
            }

            const auto& calls = DeviceManager->ZeroPagesCalls;
            UNIT_ASSERT_VALUES_EQUAL(1, calls.size());

            const auto& call = calls[0];
            UNIT_ASSERT_VALUES_EQUAL(config.DeviceUUID, call.DeviceUUID);
            UNIT_ASSERT_VALUES_EQUAL(config.BlockSize, call.BlockSize);
            UNIT_ASSERT_VALUES_EQUAL(0, call.Region.FirstPageNo);
            UNIT_ASSERT_VALUES_EQUAL(config.BlocksCount, call.Region.PageCount);

            UNIT_ASSERT_VALUES_EQUAL(1, call.Ranges.size());
            UNIT_ASSERT_VALUES_EQUAL(0, call.Ranges[0].FirstPageNo);
            UNIT_ASSERT_VALUES_EQUAL(
                config.BlocksCount,
                call.Ranges[0].PageCount);
        }

        for (const auto& device: Devices) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                1,
                device->StopCount.load(),
                device->Name);
            UNIT_ASSERT_VALUES_EQUAL_C(
                2,
                device->GetStartCount(),
                device->Name);
        }
    }

    Y_UNIT_TEST_F(ShouldNotFormatDeviceWithoutJournal, TFixture)
    {
        StartService();

        // The device has no journal, so the default format has nothing to wipe

        const auto error = FormatDevice(Configs[1].DeviceUUID, false);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_FALSE,
            error.GetCode(),
            FormatError(error));

        UNIT_ASSERT_VALUES_EQUAL(0, DeviceManager->ZeroPagesCalls.size());

        //
        // Check that the journalled device was not touched: there was nothing
        // to wipe
        //

        UNIT_ASSERT_VALUES_EQUAL(0, Devices[1]->StopCount.load());
        UNIT_ASSERT_VALUES_EQUAL(1, Devices[1]->GetStartCount());
    }

    Y_UNIT_TEST_F(ShouldReportFormatErrors, TFixture)
    {
        StartService();

        DeviceManager->ZeroPagesError = MakeError(E_IO, "zero failed");

        const auto& config = Configs[0];

        const auto error = FormatDevice(config.DeviceUUID, true);
        UNIT_ASSERT_VALUES_EQUAL_C(E_IO, error.GetCode(), FormatError(error));
        UNIT_ASSERT_VALUES_EQUAL("zero failed", error.GetMessage());

        //
        // Check that the journalled device was stopped and not recreated: the
        // device serves nothing until the next format succeeds
        //

        UNIT_ASSERT_VALUES_EQUAL(1, Devices[0]->StopCount.load());
        UNIT_ASSERT_VALUES_EQUAL(1, Devices[0]->GetStartCount());

        for (const auto& error: SendJournalledDeviceRequests(config.DeviceUUID))
        {
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_INVALID_STATE,
                error.GetCode(),
                FormatError(error));
        }

        //
        // Check that the other device keeps working
        //

        for (const auto& error:
             SendJournalledDeviceRequests(Configs[1].DeviceUUID))
        {
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        DeviceManager->ZeroPagesError = {};

        {
            const auto error = FormatDevice(config.DeviceUUID, true);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        //
        // Check that the failed device was not stopped again: there was no
        // device to stop
        //

        UNIT_ASSERT_VALUES_EQUAL(1, Devices[0]->StopCount.load());
        UNIT_ASSERT_VALUES_EQUAL(2, Devices[0]->GetStartCount());

        for (const auto& error: SendJournalledDeviceRequests(config.DeviceUUID))
        {
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }
    }

    Y_UNIT_TEST_F(ShouldRejectParallelFormats, TFixture)
    {
        StartService();

        const auto& config = Configs[0];
        const auto& device = Devices[0];

        //
        // Defer the start of the recreated device so the format hangs in the
        // middle
        //

        device->DeferStart = true;

        auto request = MakeRequest<NProto::TFormatDeviceRequest>(
            config.DeviceUUID,
            ClientId);
        request.SetWholeDevice(true);

        auto first = Service->FormatDevice(request);
        WaitUntil("the device to start", [&] { return device->IsStarting(); });
        UNIT_ASSERT(!first.HasValue());

        //
        // Expect a non-retriable error: a retry would wipe the device again
        //

        {
            const auto error =
                Service->FormatDevice(request).GetValueSync().GetError();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_INVALID_STATE,
                error.GetCode(),
                FormatError(error));
        }

        //
        // Check that only the first format wiped the device
        //

        UNIT_ASSERT_VALUES_EQUAL(1, DeviceManager->ZeroPagesCalls.size());

        device->CompleteStart();

        const auto error = first.GetValueSync().GetError();
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));

        //
        // Format the device again: the completed format has released it
        //

        device->DeferStart = false;
        {
            const auto error = FormatDevice(config.DeviceUUID, true);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }
    }

    Y_UNIT_TEST_F(ShouldWaitForFormatOnStop, TFixture)
    {
        StartService();

        const auto& config = Configs[0];
        const auto& device = Devices[0];

        //
        // Defer the start of the recreated device so the format hangs in the
        // middle
        //

        device->DeferStart = true;

        auto format = FormatDeviceAsync(config.DeviceUUID, true);
        WaitUntil("the device to start", [&] { return device->IsStarting(); });

        //
        // Stop the service on its own thread: the stop waits for the format,
        // which completes only when the test tells the device to start
        //

        auto stop = Async([&] { Service->Stop(); }, StartThread);
        Sleep(TDuration::MilliSeconds(10));
        UNIT_ASSERT(!stop.HasValue());
        UNIT_ASSERT(!format.HasValue());

        device->CompleteStart();

        stop.GetValueSync();
        const auto error = format.GetValueSync().GetError();
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));

        //
        // Check that the device was stopped twice: by the format and by the
        // stop
        //

        UNIT_ASSERT_VALUES_EQUAL(2, device->StopCount.load());
        UNIT_ASSERT_VALUES_EQUAL(1, Devices[1]->StopCount.load());

        //
        // Expect a format after the stop to be rejected: it would start a
        // device nobody stops
        //

        DeviceManager->ZeroPagesCalls.clear();
        {
            const auto error = FormatDevice(config.DeviceUUID, true);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_INVALID_STATE,
                error.GetCode(),
                FormatError(error));
        }
        UNIT_ASSERT_VALUES_EQUAL(0, DeviceManager->ZeroPagesCalls.size());
        UNIT_ASSERT_VALUES_EQUAL(2, device->GetStartCount());
    }

    Y_UNIT_TEST_F(ShouldListDevices, TFixture)
    {
        NProto::TListDevicesRequest request;
        request.MutableHeaders()->SetClientId(ClientId);

        const auto response =
            Service->ListDevices(std::move(request)).GetValueSync();
        const auto& error = response.GetError();
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));

        // The data part is what the journal leaves of the device, a device
        // without a journal is all data

        UNIT_ASSERT_VALUES_EQUAL(std::size(Configs), response.DevicesSize());

        for (size_t i = 0; i != std::size(Configs); ++i) {
            const auto& config = Configs[i];
            const auto& info = response.GetDevices(i);

            UNIT_ASSERT_VALUES_EQUAL(config.DeviceUUID, info.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(config.BlockSize, info.GetBlockSize());
            UNIT_ASSERT_VALUES_EQUAL(config.LogMetaSize, info.GetLogMetaSize());
            UNIT_ASSERT_VALUES_EQUAL(config.LogDataSize, info.GetLogDataSize());
            UNIT_ASSERT_VALUES_EQUAL(
                config.BlocksCount * config.BlockSize - config.LogMetaSize -
                    config.LogDataSize,
                info.GetDataSize());
        }
    }

    Y_UNIT_TEST_F(ShouldListDevicesSortedByUUID, TFixture)
    {
        constexpr ui32 DeviceCount = 32;

        TVector<TJournalledDeviceConfig> configs;
        for (ui32 i = DeviceCount; i != 0; --i) {
            const TString uuid = Sprintf("uuid-%02u", i);
            Factory->Devices.emplace(
                uuid,
                std::make_shared<TTestJournalledDevice>(uuid));
            configs.push_back(
                {.DeviceUUID = uuid,
                 .BlocksCount = 1024,
                 .BlockSize = DefaultBlockSize});
        }

        auto service = CreateService(
            Executor,
            DeviceManager,
            Factory,
            std::move(configs),
            1);

        const auto response =
            service->ListDevices(NProto::TListDevicesRequest()).GetValueSync();
        const auto& error = response.GetError();
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));
        UNIT_ASSERT_VALUES_EQUAL(DeviceCount, response.DevicesSize());

        for (ui32 i = 0; i != DeviceCount; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                Sprintf("uuid-%02u", i + 1),
                response.GetDevices(i).GetDeviceUUID());
        }
    }

    Y_UNIT_TEST_F(ShouldListDevicesWithoutAccessCheck, TFixture)
    {
        // Listing does not touch the devices, so any client may list them

        for (const TString clientId: {"", "unknown-client"}) {
            NProto::TListDevicesRequest request;
            request.MutableHeaders()->SetClientId(clientId);

            const auto response =
                Service->ListDevices(std::move(request)).GetValueSync();
            const auto& error = response.GetError();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_VALUES_EQUAL(
                std::size(Configs),
                response.DevicesSize());
        }

        UNIT_ASSERT_VALUES_EQUAL(0, DeviceManager->AccessDeviceCalls.size());
    }

    Y_UNIT_TEST_F(ShouldStartAndStopDevices, TFixture)
    {
        // A device that fails to start is reported, the others keep working

        Devices[0]->StartError = MakeError(E_IO, "start failed");

        StartService();

        for (const auto& device: Devices) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                1,
                device->GetStartCount(),
                device->Name);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            CriticalEventCount("JournalledDeviceCreationError"));

        for (const auto& error:
             SendJournalledDeviceRequests(Configs[0].DeviceUUID))
        {
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_INVALID_STATE,
                error.GetCode(),
                FormatError(error));
        }

        for (const auto& error:
             SendJournalledDeviceRequests(Configs[1].DeviceUUID))
        {
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        Service->Stop();

        // the device that failed to start is not there to be stopped
        UNIT_ASSERT_VALUES_EQUAL(0, Devices[0]->StopCount.load());
        UNIT_ASSERT_VALUES_EQUAL(1, Devices[1]->StopCount.load());
    }

    Y_UNIT_TEST_F(ShouldReportDevicesThatFailToCreate, TFixture)
    {
        // A device the factory refuses to make is reported just like one that
        // fails to start

        const auto& config = Configs[0];
        Factory->CreateErrors[config.DeviceUUID] =
            MakeError(E_ARGUMENT, "bad config");

        StartService();

        UNIT_ASSERT_VALUES_EQUAL(0, Devices[0]->GetStartCount());
        UNIT_ASSERT_VALUES_EQUAL(1, Devices[1]->GetStartCount());

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            CriticalEventCount("JournalledDeviceCreationError"));

        for (const auto& error: SendJournalledDeviceRequests(config.DeviceUUID))
        {
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_INVALID_STATE,
                error.GetCode(),
                FormatError(error));
        }

        //
        // Expect the format to fail the same way and leave the device failed
        //

        {
            const auto error = FormatDevice(config.DeviceUUID, true);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                error.GetCode(),
                FormatError(error));
        }

        UNIT_ASSERT_VALUES_EQUAL(1, DeviceManager->ZeroPagesCalls.size());
        UNIT_ASSERT_VALUES_EQUAL(0, Devices[0]->GetStartCount());
    }

    Y_UNIT_TEST_F(ShouldRestoreJournalsWithLimitedConcurrency, TFixture)
    {
        constexpr ui32 DeviceCount = 8;
        constexpr ui32 RestoreConcurrency = 3;

        auto [service, devices] =
            CreateDeferredStartService(DeviceCount, RestoreConcurrency);

        auto started = StartInBackground(service);

        //
        // Wait for the first devices to come up: only RestoreConcurrency of
        // them restore at once, the rest are held back
        //

        WaitUntil(
            "the first devices to start",
            [&] { return StartedCount(devices) == RestoreConcurrency; });

        UNIT_ASSERT_VALUES_EQUAL(RestoreConcurrency, StartingCount(devices));
        UNIT_ASSERT(!started.HasValue());

        //
        // Complete the devices one by one: each device that completes lets
        // exactly one more in
        //

        for (ui32 i = 0; i != DeviceCount; ++i) {
            CompleteAnyStart(devices);

            const ui32 completed = i + 1;
            const ui32 expectedStarted =
                Min(DeviceCount, completed + RestoreConcurrency);

            WaitUntil(
                "the next device to start",
                [&] { return StartedCount(devices) == expectedStarted; });

            UNIT_ASSERT_VALUES_EQUAL_C(
                expectedStarted - completed,
                StartingCount(devices),
                i);

            if (completed != DeviceCount) {
                UNIT_ASSERT_C(!started.HasValue(), i);
            }
        }

        started.GetValueSync();
        AssertStartedOnce(devices);
    }

    Y_UNIT_TEST_F(ShouldRestoreJournalsOneAtATimeWithZeroConcurrency, TFixture)
    {
        //
        // Treat a zero restore concurrency as one rather than holding every
        // device back forever
        //

        constexpr ui32 DeviceCount = 3;

        auto [service, devices] = CreateDeferredStartService(DeviceCount, 0);

        auto started = StartInBackground(service);

        for (ui32 i = 0; i != DeviceCount; ++i) {
            WaitUntil(
                "the next device to start",
                [&] { return StartedCount(devices) == i + 1; });

            UNIT_ASSERT_VALUES_EQUAL_C(1, StartingCount(devices), i);
            UNIT_ASSERT_C(!started.HasValue(), i);

            CompleteAnyStart(devices);
        }

        started.GetValueSync();
        AssertStartedOnce(devices);
    }

    Y_UNIT_TEST_F(ShouldReportDevicesThatFailToRestore, TFixture)
    {
        //
        // Check that a failed restore does not hold the others back: the next
        // device is let in and the start completes
        //

        constexpr ui32 DeviceCount = 3;

        auto [service, devices] = CreateDeferredStartService(DeviceCount, 1);
        devices[0]->StartError = MakeError(E_IO, "restore failed");

        auto started = StartInBackground(service);

        for (ui32 i = 0; i != DeviceCount; ++i) {
            WaitUntil(
                "the next device to start",
                [&] { return StartedCount(devices) == i + 1; });

            CompleteAnyStart(devices);
        }

        started.GetValueSync();
        AssertStartedOnce(devices);

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            CriticalEventCount("JournalledDeviceCreationError"));
    }
}

}   // namespace NCloud::NJournalled
