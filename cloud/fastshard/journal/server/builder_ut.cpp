#include "builder.h"

#include <cloud/fastshard/journal/iface/device.h>
#include <cloud/fastshard/journal/impl/memory_device.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>
#include <util/generic/vector.h>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultBlockSize = 4_KB;

////////////////////////////////////////////////////////////////////////////////

struct TCreateDeviceCall
{
    TString DeviceUUID;
    TPageRangeRef Region;
    ui32 BlockSize = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TTestDeviceManager final: public IDeviceManager
{
    TVector<TCreateDeviceCall> CreateDeviceCalls;

    [[nodiscard]] auto AcquireDevices(NProto::TAcquireDevicesRequest /*request*/)
        -> TFuture<NProto::TAcquireDevicesResponse> final
    {
        return MakeFuture<NProto::TAcquireDevicesResponse>();
    }

    [[nodiscard]] auto ReleaseDevices(NProto::TReleaseDevicesRequest /*request*/)
        -> TFuture<NProto::TReleaseDevicesResponse> final
    {
        return MakeFuture<NProto::TReleaseDevicesResponse>();
    }

    [[nodiscard]] NProto::TError AccessDevice(
        const TString& /*deviceUUID*/,
        const TString& /*clientId*/,
        NProto::EAccessMode /*accessMode*/) final
    {
        return {};
    }

    [[nodiscard]] IDevicePtr CreateDevice(
        const TString& deviceUUID,
        TPageRangeRef region,
        ui32 blockSize) final
    {
        CreateDeviceCalls.push_back(
            {.DeviceUUID = deviceUUID,
             .Region = region,
             .BlockSize = blockSize});

        return CreateInMemoryDevice(blockSize);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    NMonitoring::TDynamicCountersPtr Counters;
    ILoggingServicePtr Logging;
    TExecutorPtr Executor;
    std::shared_ptr<TTestDeviceManager> DeviceManager;

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        InitCriticalEventsCounter(Counters);

        Logging = CreateLoggingService(
            "console",
            {.FiltrationLevel = TLOG_RESOURCES});
        Executor = TExecutor::Create("TestExecutor");
        DeviceManager = std::make_shared<TTestDeviceManager>();
    }

    i64 CriticalEventCount(const TString& name) const
    {
        return Counters->GetCounter("AppCriticalEvents/" + name, true)->Val();
    }

    IStartablePtr Build(
        bool journalEnabled,
        TVector<TJournalledDeviceConfig> configs)
    {
        return TServerBuilder(
                   Logging,
                   Executor,
                   DeviceManager,
                   TNetworkAddress{0},   // the server is never started
                   journalEnabled,
                   std::move(configs))
            .Build();
    }

    static TJournalledDeviceConfig MakeConfig(
        TString deviceUUID,
        ui64 blocksCount,
        ui64 logMetaBlockCount,
        ui64 logDataBlockCount)
    {
        return {
            .DeviceUUID = std::move(deviceUUID),
            .BlocksCount = blocksCount,
            .BlockSize = DefaultBlockSize,
            .LogMetaSize = logMetaBlockCount * DefaultBlockSize,
            .LogDataSize = logDataBlockCount * DefaultBlockSize};
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServerBuilderTest)
{
    Y_UNIT_TEST_F(ShouldSplitDeviceIntoJournalParts, TFixture)
    {
        UNIT_ASSERT(Build(true, {MakeConfig("uuid-1", 1024, 4, 64)}));

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            CriticalEventCount("JournalledDeviceCreationError"));

        // the journal metadata, the journal data and the data follow each
        // other
        const std::pair<ui64, ui64> expected[]{
            {0, 4},
            {4, 64},
            {68, 1024 - 68},
        };

        const auto& calls = DeviceManager->CreateDeviceCalls;
        UNIT_ASSERT_VALUES_EQUAL(std::size(expected), calls.size());

        for (size_t i = 0; i != calls.size(); ++i) {
            const auto& [firstPageNo, pageCount] = expected[i];

            UNIT_ASSERT_VALUES_EQUAL_C("uuid-1", calls[i].DeviceUUID, i);
            UNIT_ASSERT_VALUES_EQUAL_C(DefaultBlockSize, calls[i].BlockSize, i);
            UNIT_ASSERT_VALUES_EQUAL_C(
                firstPageNo,
                calls[i].Region.FirstPageNo,
                i);
            UNIT_ASSERT_VALUES_EQUAL_C(pageCount, calls[i].Region.PageCount, i);
        }
    }

    Y_UNIT_TEST_F(ShouldServeWholeDeviceWithoutJournal, TFixture)
    {
        UNIT_ASSERT(Build(false, {MakeConfig("uuid-1", 1024, 0, 0)}));

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            CriticalEventCount("JournalledDeviceCreationError"));

        const auto& calls = DeviceManager->CreateDeviceCalls;
        UNIT_ASSERT_VALUES_EQUAL(1, calls.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid-1", calls[0].DeviceUUID);
        UNIT_ASSERT_VALUES_EQUAL(DefaultBlockSize, calls[0].BlockSize);
        UNIT_ASSERT_VALUES_EQUAL(0, calls[0].Region.FirstPageNo);
        UNIT_ASSERT_VALUES_EQUAL(1024, calls[0].Region.PageCount);
    }

    Y_UNIT_TEST_F(ShouldSkipJournalPartsWhenJournalIsDisabled, TFixture)
    {
        // the journal parts make no sense without the journal, so such devices
        // are reported and skipped
        UNIT_ASSERT(Build(
            false,
            {MakeConfig("uuid-1", 1024, 4, 0),
             MakeConfig("uuid-2", 1024, 0, 64),
             MakeConfig("uuid-3", 1024, 0, 0)}));

        UNIT_ASSERT_VALUES_EQUAL(
            2,
            CriticalEventCount("JournalledDeviceCreationError"));

        const auto& calls = DeviceManager->CreateDeviceCalls;
        UNIT_ASSERT_VALUES_EQUAL(1, calls.size());
        UNIT_ASSERT_VALUES_EQUAL("uuid-3", calls[0].DeviceUUID);
    }

    Y_UNIT_TEST_F(ShouldSkipDevicesWithInvalidConfig, TFixture)
    {
        auto zeroBlockSize = MakeConfig("zero-block-size", 1024, 4, 64);
        zeroBlockSize.BlockSize = 0;

        auto unalignedLogMeta = MakeConfig("unaligned-log-meta", 1024, 4, 64);
        unalignedLogMeta.LogMetaSize += 1;

        auto unalignedLogData = MakeConfig("unaligned-log-data", 1024, 4, 64);
        unalignedLogData.LogDataSize -= 1;

        TVector<TJournalledDeviceConfig> configs{
            zeroBlockSize,
            unalignedLogMeta,
            unalignedLogData,
            // too little journal metadata for the key buffer store
            MakeConfig("small-log-meta", 1024, 2, 64),
            MakeConfig("no-log-data", 1024, 4, 0),
            MakeConfig("no-data", 68, 4, 64),
            // the journal metadata must be smaller than the journal data
            MakeConfig("large-log-meta", 1024, 64, 64),
            // the journal data must be smaller than the data
            MakeConfig("large-log-data", 1024, 4, 510),
            MakeConfig("valid", 1024, 4, 64),
        };

        const auto invalidCount = configs.size() - 1;

        UNIT_ASSERT(Build(true, std::move(configs)));

        UNIT_ASSERT_VALUES_EQUAL(
            invalidCount,
            CriticalEventCount("JournalledDeviceCreationError"));

        // only the valid device is created
        for (const auto& call: DeviceManager->CreateDeviceCalls) {
            UNIT_ASSERT_VALUES_EQUAL("valid", call.DeviceUUID);
        }
        UNIT_ASSERT_VALUES_EQUAL(3, DeviceManager->CreateDeviceCalls.size());
    }

    Y_UNIT_TEST_F(ShouldFailIfNoDeviceCanBeCreated, TFixture)
    {
        auto config = MakeConfig("uuid-1", 1024, 4, 64);
        config.BlockSize = 0;

        try {
            Build(true, {config, config});
            UNIT_FAIL("no exception");
        } catch (const TServiceError& e) {
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, e.GetCode());
            UNIT_ASSERT_STRING_CONTAINS(
                e.GetMessage(),
                "none of the 2 journalled devices could be created");
        }

        UNIT_ASSERT_VALUES_EQUAL(
            2,
            CriticalEventCount("JournalledDeviceCreationError"));
    }
}

}   // namespace NCloud::NJournalled
