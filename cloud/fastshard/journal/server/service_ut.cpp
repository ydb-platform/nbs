#include "service.h"

#include "device_manager.h"

#include <cloud/fastshard/journal/iface/device.h>
#include <cloud/fastshard/journal/iface/journalled_device.h>
#include <cloud/fastshard/protos/device.pb.h>

#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/string/printf.h>
#include <util/system/guard.h>
#include <util/system/spinlock.h>

#include <atomic>
#include <optional>
#include <thread>
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

    [[nodiscard]] auto WritePages(
        TVector<TPageRange> /*ranges*/) -> TFuture<NProto::TError> final
    {
        UNIT_FAIL("unexpected WritePages");
        return {};
    }

    [[nodiscard]] auto ZeroPages(
        TVector<TPageRangeRef> ranges) -> TFuture<NProto::TError> final
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

    std::atomic<ui32> StartCount = 0;
    std::atomic<ui32> StopCount = 0;

    explicit TTestJournalledDevice(TString name)
        : Name(std::move(name))
    {}

    TFuture<NProto::TError> Start() final
    {
        ++StartCount;

        if (!DeferStart) {
            return MakeFuture(StartError);
        }

        auto guard = Guard(Lock);
        Y_ABORT_UNLESS(!StartPromise, "%s is already starting", Name.c_str());
        StartPromise = NewPromise<NProto::TError>();
        return StartPromise->GetFuture();
    }

    bool IsStarting()
    {
        auto guard = Guard(Lock);
        return StartPromise.has_value();
    }

    void CompleteStart()
    {
        std::optional<TPromise<NProto::TError>> promise;
        with_lock (Lock) {
            promise = std::exchange(StartPromise, std::nullopt);
        }

        UNIT_ASSERT_C(promise, Name + " is not starting");
        promise->SetValue(StartError);
    }

    TFuture<NProto::TError> Stop() final
    {
        ++StopCount;
        return MakeFuture<NProto::TError>();
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
    TAdaptiveLock Lock;
    std::optional<TPromise<NProto::TError>> StartPromise;

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

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        InitCriticalEventsCounter(Counters);

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
        Service.reset();
        Executor->Stop();
    }

    void StartService()
    {
        Service->Start();
    }

    // Start blocks until every device has started, so the tests that
    // complete the starts by hand run it apart.
    struct TBackgroundStart
    {
        std::atomic<bool> Done = false;
        std::thread Thread;

        explicit TBackgroundStart(IServerBackendPtr service)
            : Thread(
                  [this, service = std::move(service)]
                  {
                      service->Start();
                      Done = true;
                  })
        {}

        ~TBackgroundStart()
        {
            Wait();
        }

        void Wait()
        {
            if (Thread.joinable()) {
                Thread.join();
            }
        }
    };

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

        return {std::move(service), std::move(devices)};
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
            [](const auto& device) { return device->StartCount.load() != 0; });
    }

    // The devices are started in the executor, so the tests wait for the
    // service to catch up.
    template <typename TCondition>
    static void WaitUntil(TCondition&& condition)
    {
        const auto deadline = TInstant::Now() + TDuration::Seconds(30);
        while (!condition()) {
            UNIT_ASSERT_C(TInstant::Now() < deadline, "wait timed out");
            Sleep(TDuration::MilliSeconds(1));
        }
    }

    static void WaitForStarting(const TTestDevices& devices, size_t count)
    {
        WaitUntil([&] { return StartingCount(devices) == count; });
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
    static TRequest MakeRequest(
        const TString& deviceUUID,
        const TString& clientId)
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
                 ->ReadJournalTail(MakeRequest<NProto::TReadJournalTailRequest>(
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

    NProto::TError FormatDevice(const TString& deviceUUID, bool wholeDevice)
    {
        auto request =
            MakeRequest<NProto::TFormatDeviceRequest>(deviceUUID, ClientId);
        request.SetWholeDevice(wholeDevice);

        return Service->FormatDevice(std::move(request))
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

            // FormatDevice has nothing to wipe on a device without a journal,
            // so it may answer S_FALSE
            for (const auto& [error, _]: responses) {
                UNIT_ASSERT_C(!HasError(error), FormatError(error));
            }

            // the requests but FormatDevice (the last one) reach the
            // journalled device, FormatDevice goes to the device itself
            for (size_t i = 0; i + 1 < responses.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    uuid,
                    responses[i].first.GetMessage(),
                    i);
            }

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
                device->StartCount.load(),
                device->Name);
        }
    }

    Y_UNIT_TEST_F(ShouldRejectUnknownDevice, TFixture)
    {
        StartService();

        const TString unknownUuid = "unknown";

        for (const auto& [error, _]: SendDeviceRequests(unknownUuid, ClientId))
        {
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
            UNIT_ASSERT_STRING_CONTAINS(
                error.GetMessage(),
                "empty device UUID");
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
        UNIT_ASSERT_VALUES_EQUAL(logMetaBlockCount, call.Region.PageCount);

        UNIT_ASSERT_VALUES_EQUAL(1, call.Ranges.size());
        UNIT_ASSERT_VALUES_EQUAL(0, call.Ranges[0].FirstPageNo);
        UNIT_ASSERT_VALUES_EQUAL(logMetaBlockCount, call.Ranges[0].PageCount);

        // the journalled device is stopped before the wipe and recreated
        // after it
        UNIT_ASSERT_VALUES_EQUAL(1, device->StopCount.load());
        UNIT_ASSERT_VALUES_EQUAL(2, device->StartCount.load());

        const auto created = Factory->GetCreatedUUIDs();
        UNIT_ASSERT_VALUES_EQUAL(2, Count(created, config.DeviceUUID));

        // the other device is left alone
        UNIT_ASSERT_VALUES_EQUAL(0, Devices[1]->StopCount.load());
        UNIT_ASSERT_VALUES_EQUAL(1, Devices[1]->StartCount.load());

        // the recreated device serves the requests again
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
                device->StartCount.load(),
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

        // the journalled device is not touched
        UNIT_ASSERT_VALUES_EQUAL(0, Devices[1]->StopCount.load());
        UNIT_ASSERT_VALUES_EQUAL(1, Devices[1]->StartCount.load());
    }

    Y_UNIT_TEST_F(ShouldReportFormatErrors, TFixture)
    {
        StartService();

        DeviceManager->ZeroPagesError = MakeError(E_IO, "zero failed");

        const auto& config = Configs[0];

        const auto error = FormatDevice(config.DeviceUUID, true);
        UNIT_ASSERT_VALUES_EQUAL_C(E_IO, error.GetCode(), FormatError(error));
        UNIT_ASSERT_VALUES_EQUAL("zero failed", error.GetMessage());

        // the journalled device is stopped and not recreated, so the device
        // serves nothing until the next format succeeds
        UNIT_ASSERT_VALUES_EQUAL(1, Devices[0]->StopCount.load());
        UNIT_ASSERT_VALUES_EQUAL(1, Devices[0]->StartCount.load());

        for (const auto& error: SendJournalledDeviceRequests(config.DeviceUUID))
        {
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_INVALID_STATE,
                error.GetCode(),
                FormatError(error));
        }

        // the other device keeps working
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

        // a failed device is stopped for free
        UNIT_ASSERT_VALUES_EQUAL(1, Devices[0]->StopCount.load());
        UNIT_ASSERT_VALUES_EQUAL(2, Devices[0]->StartCount.load());

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

        // the recreated device does not start until told to, so the format
        // hangs in the middle
        device->DeferStart = true;

        auto request = MakeRequest<NProto::TFormatDeviceRequest>(
            config.DeviceUUID,
            ClientId);
        request.SetWholeDevice(true);

        auto first = Service->FormatDevice(request);
        WaitUntil([&] { return device->IsStarting(); });
        UNIT_ASSERT(!first.HasValue());

        {
            const auto error =
                Service->FormatDevice(request).GetValueSync().GetError();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_STRING_CONTAINS(
                error.GetMessage(),
                "Device " + config.DeviceUUID.Quote() + " is being formatted");
        }

        // only the first format wipes the device
        UNIT_ASSERT_VALUES_EQUAL(1, DeviceManager->ZeroPagesCalls.size());

        device->CompleteStart();

        const auto error = first.GetValueSync().GetError();
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));

        // the device is free to be formatted again
        device->DeferStart = false;
        {
            const auto error = FormatDevice(config.DeviceUUID, true);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }
    }

    Y_UNIT_TEST_F(ShouldStartAndStopDevices, TFixture)
    {
        // A device that fails to start is reported, the others keep working

        Devices[0]->StartError = MakeError(E_IO, "start failed");

        StartService();

        for (const auto& device: Devices) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                1,
                device->StartCount.load(),
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

        UNIT_ASSERT_VALUES_EQUAL(0, Devices[0]->StartCount.load());
        UNIT_ASSERT_VALUES_EQUAL(1, Devices[1]->StartCount.load());

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

        // the format fails the same way and leaves the device failed
        {
            const auto error = FormatDevice(config.DeviceUUID, true);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_VALUES_EQUAL("bad config", error.GetMessage());
        }

        UNIT_ASSERT_VALUES_EQUAL(1, DeviceManager->ZeroPagesCalls.size());
        UNIT_ASSERT_VALUES_EQUAL(0, Devices[0]->StartCount.load());
    }

    Y_UNIT_TEST_F(ShouldRestoreJournalsWithLimitedConcurrency, TFixture)
    {
        constexpr ui32 DeviceCount = 8;
        constexpr ui32 RestoreConcurrency = 3;

        auto [service, devices] =
            CreateDeferredStartService(DeviceCount, RestoreConcurrency);

        TBackgroundStart start(service);

        // Only RestoreConcurrency devices restore at once

        WaitForStarting(devices, RestoreConcurrency);
        UNIT_ASSERT_VALUES_EQUAL(RestoreConcurrency, StartedCount(devices));
        UNIT_ASSERT(!start.Done);

        // Each device that completes lets the next one in

        for (ui32 i = 0; i != DeviceCount; ++i) {
            CompleteAnyStart(devices);

            const ui32 completed = i + 1;
            const ui32 expectedStarted =
                Min(DeviceCount, completed + RestoreConcurrency);

            WaitForStarting(devices, expectedStarted - completed);
            UNIT_ASSERT_VALUES_EQUAL_C(
                expectedStarted,
                StartedCount(devices),
                i);

            if (completed == DeviceCount) {
                start.Wait();
            } else {
                UNIT_ASSERT_C(!start.Done, i);
            }
        }

        for (const auto& device: devices) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                1,
                device->StartCount.load(),
                device->Name);
        }
    }

    Y_UNIT_TEST_F(ShouldRestoreAtLeastOneJournalAtOnce, TFixture)
    {
        // A zero concurrency would never start anything, so it is taken as
        // one

        auto [service, devices] = CreateDeferredStartService(2, 0);

        TBackgroundStart start(service);

        WaitForStarting(devices, 1);
        UNIT_ASSERT_VALUES_EQUAL(1, StartedCount(devices));

        CompleteAnyStart(devices);
        WaitForStarting(devices, 1);
        UNIT_ASSERT_VALUES_EQUAL(2, StartedCount(devices));
        UNIT_ASSERT(!start.Done);

        CompleteAnyStart(devices);
        start.Wait();
    }

    Y_UNIT_TEST_F(ShouldReportDevicesThatFailToRestore, TFixture)
    {
        // A failed restore does not hold the others back: the next device is
        // let in and the start completes successfully

        constexpr ui32 DeviceCount = 3;

        auto [service, devices] = CreateDeferredStartService(DeviceCount, 1);
        devices[0]->StartError = MakeError(E_IO, "restore failed");

        TBackgroundStart start(service);

        for (ui32 i = 0; i != DeviceCount; ++i) {
            WaitForStarting(devices, 1);
            CompleteAnyStart(devices);
        }

        start.Wait();
        UNIT_ASSERT_VALUES_EQUAL(DeviceCount, StartedCount(devices));

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            CriticalEventCount("JournalledDeviceCreationError"));
    }
}

}   // namespace NCloud::NJournalled
