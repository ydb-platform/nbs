#include "device_client.h"

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/thread/pool.h>

#include <chrono>

namespace NCloud::NBlockStore::NStorage {

using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TAcquireParamsBuilder
{
    TVector<TString> Uuids;
    TString ClientId;
    TInstant Now = TInstant::Seconds(1);
    NProto::EVolumeAccessMode AccessMode = NProto::VOLUME_ACCESS_READ_WRITE;
    ui64 MountSeqNumber = 1;
    TString DiskId;
    ui32 VolumeGeneration = 0;

    auto& SetUuids(TVector<TString> uuids)
    {
        Uuids = std::move(uuids);
        return *this;
    }

    auto& SetClientId(TString clientId)
    {
        ClientId = std::move(clientId);
        return *this;
    }

    auto& SetNow(TInstant now)
    {
        Now = now;
        return *this;
    }

    auto& SetAccessMode(NProto::EVolumeAccessMode accessMode)
    {
        AccessMode = accessMode;
        return *this;
    }

    auto& SetMountSeqNumber(ui64 mountSeqNumber)
    {
        MountSeqNumber = mountSeqNumber;
        return *this;
    }

    auto& SetDiskId(TString diskId)
    {
        DiskId = std::move(diskId);
        return *this;
    }

    auto& SetVolumeGeneration(ui32 volumeGeneration)
    {
        VolumeGeneration = volumeGeneration;
        return *this;
    }
};

auto AcquireDevices(TDeviceClient& client, const TAcquireParamsBuilder& builder)
{
    return client.AcquireDevices(
        builder.Uuids,
        builder.ClientId,
        builder.Now,
        builder.AccessMode,
        builder.MountSeqNumber,
        builder.DiskId,
        builder.VolumeGeneration).GetError();
}

////////////////////////////////////////////////////////////////////////////////

struct TReleaseParamsBuilder
{
    TVector<TString> Uuids;
    TString ClientId;
    TString DiskId;
    ui32 VolumeGeneration = 0;

    auto& SetUuids(TVector<TString> uuids)
    {
        Uuids = std::move(uuids);
        return *this;
    }

    auto& SetClientId(TString clientId)
    {
        ClientId = std::move(clientId);
        return *this;
    }

    auto& SetDiskId(TString diskId)
    {
        DiskId = std::move(diskId);
        return *this;
    }

    auto& SetVolumeGeneration(ui32 volumeGeneration)
    {
        VolumeGeneration = volumeGeneration;
        return *this;
    }
};

auto ReleaseDevices(TDeviceClient& client, const TReleaseParamsBuilder& builder)
{
    return client.ReleaseDevices(
        builder.Uuids,
        builder.ClientId,
        builder.DiskId,
        builder.VolumeGeneration);
}

struct TDeviceClientParams
{
    TVector<TString> Devices;
};

struct TFixture
    : public NUnitTest::TBaseFixture
    , public IMultiagentWriteHandler
{
    const TDuration ReleaseInactiveSessionsTimeout = 10s;

    ILoggingServicePtr Logging = CreateLoggingService("console");

    TDeviceClient CreateClient(TDeviceClientParams params = {})
    {
        return TDeviceClient(
            ReleaseInactiveSessionsTimeout,
            std::move(params.Devices),
            Logging->CreateLog("BLOCKSTORE_DISK_AGENT"),
            this);
    }

    // Implements IMultiagentWriteHandler
    NThreading::TFuture<TMultiAgentWriteResponsePrivate> PerformMultiAgentWrite(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDeviceBlocksRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        return {};
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDeviceClientTest)
{
    Y_UNIT_TEST_F(TestAcquireReleaseAccess, TFixture)
    {
        auto client = CreateClient({
            .Devices = {"uuid1", "uuid2"}
        });

        auto error = AcquireDevices(
            client,
            TAcquireParamsBuilder().SetUuids({"uuid1"}));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());

        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid3"})
                .SetClientId("client"));
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, error.GetCode());

        error = ReleaseDevices(
            client,
            TReleaseParamsBuilder()
                .SetUuids({"uuid3"})
                .SetClientId("client"));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = client.AccessDevice(
            "uuid3",
            "",
            NProto::VOLUME_ACCESS_READ_WRITE);
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());

        error = client.AccessDevice(
            "uuid3",
            "client",
            NProto::VOLUME_ACCESS_READ_WRITE);
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, error.GetCode());

        error = client.AccessDevice(
            "uuid2",
            "client",
            NProto::VOLUME_ACCESS_READ_WRITE);
        UNIT_ASSERT_VALUES_EQUAL(E_BS_INVALID_SESSION, error.GetCode());

        error = client.AccessDevice(
            "uuid2",
            "client",
            NProto::VOLUME_ACCESS_READ_ONLY);
        UNIT_ASSERT_VALUES_EQUAL(E_BS_INVALID_SESSION, error.GetCode());

        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid2"})
                .SetClientId("client"));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = client.AccessDevice(
            "uuid2",
            "client",
            NProto::VOLUME_ACCESS_READ_WRITE);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = client.AccessDevice(
            "uuid2",
            "client",
            NProto::VOLUME_ACCESS_READ_ONLY);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid2"})
                .SetClientId("another_client"));
        UNIT_ASSERT_VALUES_EQUAL(E_BS_INVALID_SESSION, error.GetCode());

        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid2"})
                .SetClientId("another_client")
                .SetAccessMode(NProto::VOLUME_ACCESS_READ_ONLY));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = client.AccessDevice(
            "uuid2",
            "another_client",
            NProto::VOLUME_ACCESS_READ_WRITE);
        UNIT_ASSERT_VALUES_EQUAL(E_BS_INVALID_SESSION, error.GetCode());

        error = client.AccessDevice(
            "uuid2",
            "another_client",
            NProto::VOLUME_ACCESS_READ_ONLY);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid2"})
                .SetClientId("yet_another_client")
                .SetNow(TInstant::Seconds(12)));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = client.AccessDevice(
            "uuid2",
            "yet_another_client",
            NProto::VOLUME_ACCESS_READ_WRITE);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = client.AccessDevice(
            "uuid2",
            "another_client",
            NProto::VOLUME_ACCESS_READ_WRITE);
        UNIT_ASSERT_VALUES_EQUAL(E_BS_INVALID_SESSION, error.GetCode());

        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid2"})
                .SetClientId("yet_yet_another_client")
                .SetNow(TInstant::Seconds(12))
                .SetMountSeqNumber(2));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = client.AccessDevice(
            "uuid2",
            "yet_yet_another_client",
            NProto::VOLUME_ACCESS_READ_WRITE);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = client.AccessDevice(
            "uuid2",
            "yet_another_client",
            NProto::VOLUME_ACCESS_READ_WRITE);
        UNIT_ASSERT_VALUES_EQUAL(E_BS_INVALID_SESSION, error.GetCode());
    }

    Y_UNIT_TEST_F(TestAcquireAtomicity, TFixture)
    {
        auto client = CreateClient({
            .Devices = {"uuid1", "uuid2"}
        });

        auto error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid2"})
                .SetClientId("client"));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetClientId("another_client"));
        UNIT_ASSERT_VALUES_EQUAL(E_BS_INVALID_SESSION, error.GetCode());

        error = ReleaseDevices(
            client,
            TReleaseParamsBuilder()
                .SetUuids({"uuid1"})
                .SetClientId("another_client"));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = ReleaseDevices(
            client,
            TReleaseParamsBuilder()
                .SetUuids({"uuid2"})
                .SetClientId("client"));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetClientId("client"));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
    }

    Y_UNIT_TEST_F(TestAcquireAccessMultithreaded, TFixture)
    {
        auto client = CreateClient({
            .Devices = {"uuid1", "uuid2"}
        });

        auto threadPool = CreateThreadPool(3);
        const auto runs = 10'000;

        TAtomic completed = 0;
        TManualEvent ev;

        threadPool->SafeAddFunc([&] () {
            for (ui32 i = 0; i < runs; ++i) {
                AcquireDevices(
                    client,
                    TAcquireParamsBuilder()
                        .SetUuids({"uuid1", "uuid2"})
                        .SetClientId("client"));
            }

            AtomicIncrement(completed);
            ev.Signal();
        });

        threadPool->SafeAddFunc([&] () {
            for (ui32 i = 0; i < runs; ++i) {
                client.AccessDevice(
                    "uuid1",
                    "client",
                    NProto::VOLUME_ACCESS_READ_WRITE);
            }

            AtomicIncrement(completed);
            ev.Signal();
        });

        threadPool->SafeAddFunc([&] () {
            for (ui32 i = 0; i < runs; ++i) {
                ReleaseDevices(
                    client,
                    TReleaseParamsBuilder()
                        .SetUuids({"uuid1", "uuid2"})
                        .SetClientId("client"));
            }

            AtomicIncrement(completed);
            ev.Signal();
        });

        while (AtomicGet(completed) < 3) {
            ev.WaitI();
        }

        threadPool->Stop();
    }

    Y_UNIT_TEST_F(TestMigrationAccess, TFixture)
    {
        // TODO: fix current migration access logic
        // NBS-3612
        auto client = CreateClient({
            .Devices = {"uuid1", "uuid2"}
        });

        auto error = client.AccessDevice(
            "uuid1",
            "migration",
            NProto::VOLUME_ACCESS_READ_WRITE);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = client.AccessDevice(
            "uuid1",
            "migration",
            NProto::VOLUME_ACCESS_READ_ONLY);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetClientId("client"));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = client.AccessDevice(
            "uuid1",
            "migration",
            NProto::VOLUME_ACCESS_READ_WRITE);
        UNIT_ASSERT_VALUES_EQUAL(E_BS_INVALID_SESSION, error.GetCode());

        error = client.AccessDevice(
            "uuid1",
            "migration",
            NProto::VOLUME_ACCESS_READ_ONLY);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
    }

    Y_UNIT_TEST_F(TestDiskIdAndVolumeGenerationChecks, TFixture)
    {
        auto client = CreateClient({
            .Devices = {"uuid1", "uuid2"}
        });

        // initial acquire
        auto error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetClientId("client0")
                .SetDiskId("vol0")
                .SetVolumeGeneration(1));
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

        // reacquiring with an increased volume generation
        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetClientId("client1")
                .SetDiskId("vol0")
                .SetNow(TInstant::Seconds(12))
                .SetVolumeGeneration(2));
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

        // attempt to reacquire with previous generation should fail
        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetClientId("client0")
                .SetDiskId("vol0")
                .SetNow(TInstant::Seconds(12))
                .SetVolumeGeneration(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_BS_INVALID_SESSION,
            error.GetCode(),
            error.GetMessage());

        // it should fail even if inactivity timeout passes
        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetClientId("client0")
                .SetDiskId("vol0")
                .SetNow(TInstant::Seconds(23))
                .SetVolumeGeneration(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_BS_INVALID_SESSION,
            error.GetCode(),
            error.GetMessage());

        // release should fail as well
        error = ReleaseDevices(
            client,
            TReleaseParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetClientId(TString(AnyWriterClientId))
                .SetDiskId("vol0")
                .SetVolumeGeneration(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_INVALID_STATE,
            error.GetCode(),
            error.GetMessage());

        // release with current generation should succeed
        error = ReleaseDevices(
            client,
            TReleaseParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetClientId(TString(AnyWriterClientId))
                .SetDiskId("vol0")
                .SetVolumeGeneration(2));
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

        // testing backwards compat (clients that don't send volumeGeneration)
        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetClientId("client2")
                .SetDiskId("vol0")
                .SetNow(TInstant::Seconds(23))
                .SetVolumeGeneration(3));
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

        // acquire without volumeGeneration should succeed
        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetClientId("client3")
                .SetDiskId("vol0")
                .SetNow(TInstant::Seconds(34))
                .SetVolumeGeneration(0));
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

        // acquiring with volumeGeneration
        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetClientId("client2")
                .SetDiskId("vol0")
                .SetNow(TInstant::Seconds(45))
                .SetVolumeGeneration(3));
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

        // release without volumeGeneration should succeed as well
        error = ReleaseDevices(
            client,
            TReleaseParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetClientId(TString(AnyWriterClientId))
                .SetDiskId("vol0")
                .SetVolumeGeneration(0));
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

        // acquiring with volumeGeneration
        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetClientId("client2")
                .SetDiskId("vol0")
                .SetNow(TInstant::Seconds(56))
                .SetVolumeGeneration(3));
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

        // reacquire with a different diskId should fail before inactivity
        // timeout passes
        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetClientId("client0")
                .SetDiskId("vol1")
                .SetNow(TInstant::Seconds(57))
                .SetVolumeGeneration(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_BS_INVALID_SESSION,
            error.GetCode(),
            error.GetMessage());

        // reacquire with a different diskId should succeed after inactivity
        // timeout passes
        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetClientId("client0")
                .SetDiskId("vol1")
                .SetNow(TInstant::Seconds(67))
                .SetVolumeGeneration(1));
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

        // release with a different diskId should succeed
        error = ReleaseDevices(
            client,
            TReleaseParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetClientId(TString(AnyWriterClientId))
                .SetDiskId("vol2")
                .SetVolumeGeneration(1));
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());
    }

    Y_UNIT_TEST_F(TestGetSessions, TFixture)
    {
        auto client = CreateClient({
            .Devices = {"uuid1", "uuid2"}
        });

        UNIT_ASSERT_VALUES_EQUAL(0, client.GetSessions().size());

        const auto acquireWriterTs = TInstant::Seconds(42);

        AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid2", "uuid1"})
                .SetClientId("writer")
                .SetDiskId("vol1")
                .SetNow(acquireWriterTs)
                .SetVolumeGeneration(1));

        {
            auto sessions = client.GetSessions();
            UNIT_ASSERT_VALUES_EQUAL(1, sessions.size());

            auto& session = sessions[0];
            UNIT_ASSERT_VALUES_EQUAL("writer", session.GetClientId());
            UNIT_ASSERT_VALUES_EQUAL(1, session.GetVolumeGeneration());
            UNIT_ASSERT(!session.GetReadOnly());
            UNIT_ASSERT_VALUES_EQUAL(
                acquireWriterTs.MicroSeconds(),
                session.GetLastActivityTs());
            UNIT_ASSERT_VALUES_EQUAL(2, session.DeviceIdsSize());
            UNIT_ASSERT_VALUES_EQUAL("uuid1", session.GetDeviceIds(0));
            UNIT_ASSERT_VALUES_EQUAL("uuid2", session.GetDeviceIds(1));
        }

        const auto acquireReaderTs = TInstant::Seconds(42);

        AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid2", "uuid1"})
                .SetClientId("reader")
                .SetDiskId("vol1")
                .SetAccessMode(NProto::VOLUME_ACCESS_READ_ONLY)
                .SetNow(acquireReaderTs)
                .SetVolumeGeneration(2));

        {
            auto sessions = client.GetSessions();
            UNIT_ASSERT_VALUES_EQUAL(2, sessions.size());
            {
                auto& session = sessions[0];
                UNIT_ASSERT_VALUES_EQUAL("reader", session.GetClientId());
                UNIT_ASSERT_VALUES_EQUAL(2, session.GetVolumeGeneration());
                UNIT_ASSERT(session.GetReadOnly());
                UNIT_ASSERT_VALUES_EQUAL(
                    acquireReaderTs.MicroSeconds(),
                    session.GetLastActivityTs());
                UNIT_ASSERT_VALUES_EQUAL(2, session.DeviceIdsSize());
                UNIT_ASSERT_VALUES_EQUAL("uuid1", session.GetDeviceIds(0));
                UNIT_ASSERT_VALUES_EQUAL("uuid2", session.GetDeviceIds(1));
            }

            {
                auto& session = sessions[1];
                UNIT_ASSERT_VALUES_EQUAL("writer", session.GetClientId());
                UNIT_ASSERT_VALUES_EQUAL(2, session.GetVolumeGeneration());
                UNIT_ASSERT(!session.GetReadOnly());
                UNIT_ASSERT_VALUES_EQUAL(
                    acquireWriterTs.MicroSeconds(),
                    session.GetLastActivityTs());
                UNIT_ASSERT_VALUES_EQUAL(2, session.DeviceIdsSize());
                UNIT_ASSERT_VALUES_EQUAL("uuid1", session.GetDeviceIds(0));
                UNIT_ASSERT_VALUES_EQUAL("uuid2", session.GetDeviceIds(1));
            }
        }
    }

    Y_UNIT_TEST_F(TestShouldAcquire, TFixture)
    {
        auto client = CreateClient({
            .Devices = {"uuid1", "uuid2"}
        });

        UNIT_ASSERT_VALUES_EQUAL(0, client.GetSessions().size());

        TInstant now = Now();

        {
            auto [updated, error] = client.AcquireDevices(
                {"uuid2", "uuid1"},     // uuids
                "writer",               // ClientId
                now,
                NProto::VOLUME_ACCESS_READ_WRITE,
                1,                      // MountSeqNumber
                "vol0",                 // DiskId
                1                       // VolumeGeneration
            );

            UNIT_ASSERT_C(!HasError(error), error);
            UNIT_ASSERT(updated);   // new write session
        }

        // make the writer session stale
        now += ReleaseInactiveSessionsTimeout;

        {
            auto [updated, error] = client.AcquireDevices(
                {"uuid2", "uuid1"},      // uuids
                "writer",                // ClientId
                now,
                NProto::VOLUME_ACCESS_READ_WRITE,
                1,                       // MountSeqNumber
                "vol0",                  // DiskId
                1                        // VolumeGeneration
            );

            UNIT_ASSERT_C(!HasError(error), error);
            // writer session was activated
            UNIT_ASSERT(updated);
        }

        // writer session still active
        now += 5s;

        {
            auto [updated, error] = client.AcquireDevices(
                {"uuid2", "uuid1"},      // uuids
                "writer",                // ClientId
                now,
                NProto::VOLUME_ACCESS_READ_WRITE,
                1,                       // MountSeqNumber
                "vol0",                  // DiskId
                1                        // VolumeGeneration
            );

            UNIT_ASSERT_C(!HasError(error), error);
            // nothing was changed
            UNIT_ASSERT(!updated);
        }

        now += 5s;

        {
            auto [updated, error] = client.AcquireDevices(
                {"uuid2", "uuid1"},      // uuids
                "writer",                // ClientId
                now,
                NProto::VOLUME_ACCESS_READ_WRITE,
                1,                       // MountSeqNumber
                "vol0",                  // DiskId
                2                        // volumeGeneration
            );

            UNIT_ASSERT_C(!HasError(error), error);
            UNIT_ASSERT(updated);   // new volumeGeneration
        }

        now += 5s;

        {
            auto [updated, error] = client.AcquireDevices(
                {"uuid2", "uuid1"},      // uuids
                "reader",                // ClientId
                now,
                NProto::VOLUME_ACCESS_READ_ONLY,
                1,                       // MountSeqNumber
                "vol0",                  // DiskId
                3                        // VolumeGeneration
            );

            UNIT_ASSERT_C(!HasError(error), error);
            UNIT_ASSERT(updated);   // new read session
        }

        now += 5s;

        {
            auto [updated, error] = client.AcquireDevices(
                {"uuid2", "uuid1"},      // uuids
                "reader2",               // ClientId
                now,
                NProto::VOLUME_ACCESS_READ_ONLY,
                1,                       // MountSeqNumber
                "vol0",                  // DiskId
                3                        // VolumeGeneration
            );

            UNIT_ASSERT_C(!HasError(error), error);
            UNIT_ASSERT(updated);   // new read session
        }
    }

    Y_UNIT_TEST_F(TestShouldDisableDevice, TFixture)
    {
        auto client = CreateClient({
            .Devices = {"uuid1", "uuid2"}
        });

        UNIT_ASSERT(!client.IsDeviceDisabled("uuid1"));
        UNIT_ASSERT(!client.IsDeviceDisabled("uuid2"));

        client.DisableDevice("uuid1");
        client.SuspendDevice("uuid2");

        UNIT_ASSERT(client.IsDeviceDisabled("uuid1"));
        UNIT_ASSERT(!client.IsDeviceSuspended("uuid1"));
        UNIT_ASSERT_VALUES_EQUAL(
            E_IO,
            client.GetDeviceIOErrorCode("uuid1").value());

        UNIT_ASSERT(!client.IsDeviceDisabled("uuid2"));
        UNIT_ASSERT(client.IsDeviceSuspended("uuid2"));
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            client.GetDeviceIOErrorCode("uuid2").value());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
