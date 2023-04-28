#include "device_client.h"
#include "public.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/thread/pool.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TAcquireParamsBuilder
{
    TVector<TString> Uuids;
    TString SessionId;
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

    auto& SetSessionId(TString sessionId)
    {
        SessionId = std::move(sessionId);
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
        builder.SessionId,
        builder.Now,
        builder.AccessMode,
        builder.MountSeqNumber,
        builder.DiskId,
        builder.VolumeGeneration);
}

////////////////////////////////////////////////////////////////////////////////

struct TReleaseParamsBuilder
{
    TVector<TString> Uuids;
    TString SessionId;
    TString DiskId;
    ui32 VolumeGeneration = 0;

    auto& SetUuids(TVector<TString> uuids)
    {
        Uuids = std::move(uuids);
        return *this;
    }

    auto& SetSessionId(TString sessionId)
    {
        SessionId = std::move(sessionId);
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
        builder.SessionId,
        builder.DiskId,
        builder.VolumeGeneration);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDeviceClientTest)
{
    Y_UNIT_TEST(TestAcquireReleaseAccess)
    {
        TDeviceClient client(TDuration::Seconds(10), {"uuid1", "uuid2"});

        auto error = AcquireDevices(
            client,
            TAcquireParamsBuilder().SetUuids({"uuid1"}));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());

        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid3"})
                .SetSessionId("session"));
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, error.GetCode());

        error = ReleaseDevices(
            client,
            TReleaseParamsBuilder()
                .SetUuids({"uuid3"})
                .SetSessionId("session"));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = client.AccessDevice(
            "uuid3",
            "",
            NProto::VOLUME_ACCESS_READ_WRITE);
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());

        error = client.AccessDevice(
            "uuid3",
            "session",
            NProto::VOLUME_ACCESS_READ_WRITE);
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, error.GetCode());

        error = client.AccessDevice(
            "uuid2",
            "session",
            NProto::VOLUME_ACCESS_READ_WRITE);
        UNIT_ASSERT_VALUES_EQUAL(E_BS_INVALID_SESSION, error.GetCode());

        error = client.AccessDevice(
            "uuid2",
            "session",
            NProto::VOLUME_ACCESS_READ_ONLY);
        UNIT_ASSERT_VALUES_EQUAL(E_BS_INVALID_SESSION, error.GetCode());

        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid2"})
                .SetSessionId("session"));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = client.AccessDevice(
            "uuid2",
            "session",
            NProto::VOLUME_ACCESS_READ_WRITE);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = client.AccessDevice(
            "uuid2",
            "session",
            NProto::VOLUME_ACCESS_READ_ONLY);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid2"})
                .SetSessionId("another_session"));
        UNIT_ASSERT_VALUES_EQUAL(E_BS_INVALID_SESSION, error.GetCode());

        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid2"})
                .SetSessionId("another_session")
                .SetAccessMode(NProto::VOLUME_ACCESS_READ_ONLY));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = client.AccessDevice(
            "uuid2",
            "another_session",
            NProto::VOLUME_ACCESS_READ_WRITE);
        UNIT_ASSERT_VALUES_EQUAL(E_BS_INVALID_SESSION, error.GetCode());

        error = client.AccessDevice(
            "uuid2",
            "another_session",
            NProto::VOLUME_ACCESS_READ_ONLY);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid2"})
                .SetSessionId("yet_another_session")
                .SetNow(TInstant::Seconds(12)));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = client.AccessDevice(
            "uuid2",
            "yet_another_session",
            NProto::VOLUME_ACCESS_READ_WRITE);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = client.AccessDevice(
            "uuid2",
            "another_session",
            NProto::VOLUME_ACCESS_READ_WRITE);
        UNIT_ASSERT_VALUES_EQUAL(E_BS_INVALID_SESSION, error.GetCode());

        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid2"})
                .SetSessionId("yet_yet_another_session")
                .SetNow(TInstant::Seconds(12))
                .SetMountSeqNumber(2));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = client.AccessDevice(
            "uuid2",
            "yet_yet_another_session",
            NProto::VOLUME_ACCESS_READ_WRITE);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = client.AccessDevice(
            "uuid2",
            "yet_another_session",
            NProto::VOLUME_ACCESS_READ_WRITE);
        UNIT_ASSERT_VALUES_EQUAL(E_BS_INVALID_SESSION, error.GetCode());
    }

    Y_UNIT_TEST(TestAcquireAtomicity)
    {
        TDeviceClient client(TDuration::Seconds(10), {"uuid1", "uuid2"});

        auto error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid2"})
                .SetSessionId("session"));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetSessionId("another_session"));
        UNIT_ASSERT_VALUES_EQUAL(E_BS_INVALID_SESSION, error.GetCode());

        error = ReleaseDevices(
            client,
            TReleaseParamsBuilder()
                .SetUuids({"uuid1"})
                .SetSessionId("another_session"));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = ReleaseDevices(
            client,
            TReleaseParamsBuilder()
                .SetUuids({"uuid2"})
                .SetSessionId("session"));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetSessionId("session"));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
    }

    Y_UNIT_TEST(TestAcquireAccessMultithreaded)
    {
        TDeviceClient client(TDuration::Seconds(10), {"uuid1", "uuid2"});

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
                        .SetSessionId("session"));
            }

            AtomicIncrement(completed);
            ev.Signal();
        });

        threadPool->SafeAddFunc([&] () {
            for (ui32 i = 0; i < runs; ++i) {
                client.AccessDevice(
                    "uuid1",
                    "session",
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
                        .SetSessionId("session"));
            }

            AtomicIncrement(completed);
            ev.Signal();
        });

        while (AtomicGet(completed) < 3) {
            ev.WaitI();
        }

        threadPool->Stop();
    }

    Y_UNIT_TEST(TestMigrationAccess)
    {
        // TODO: fix current migration access logic
        // NBS-3612
        TDeviceClient client(TDuration::Seconds(10), {"uuid1", "uuid2"});

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
                .SetSessionId("session"));
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

    Y_UNIT_TEST(TestDiskIdAndVolumeGenerationChecks)
    {
        TDeviceClient client(TDuration::Seconds(10), {"uuid1", "uuid2"});

        // initial acquire
        auto error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetSessionId("session0")
                .SetDiskId("vol0")
                .SetVolumeGeneration(1));
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

        // reacquiring with an increased volume generation
        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetSessionId("session1")
                .SetDiskId("vol0")
                .SetNow(TInstant::Seconds(12))
                .SetVolumeGeneration(2));
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

        // attempt to reacquire with previous generation should fail
        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetSessionId("session0")
                .SetDiskId("vol0")
                .SetNow(TInstant::Seconds(12))
                .SetVolumeGeneration(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_INVALID_STATE,
            error.GetCode(),
            error.GetMessage());

        // it should fail even if inactivity timeout passes
        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetSessionId("session0")
                .SetDiskId("vol0")
                .SetNow(TInstant::Seconds(23))
                .SetVolumeGeneration(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_INVALID_STATE,
            error.GetCode(),
            error.GetMessage());

        // release should fail as well
        error = ReleaseDevices(
            client,
            TReleaseParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetSessionId(TString(AnyWriterSessionId))
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
                .SetSessionId(TString(AnyWriterSessionId))
                .SetDiskId("vol0")
                .SetVolumeGeneration(2));
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

        // testing backwards compat (clients that don't send volumeGeneration)
        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetSessionId("session2")
                .SetDiskId("vol0")
                .SetNow(TInstant::Seconds(23))
                .SetVolumeGeneration(3));
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

        // acquire without volumeGeneration should succeed
        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetSessionId("session3")
                .SetDiskId("vol0")
                .SetNow(TInstant::Seconds(34))
                .SetVolumeGeneration(0));
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

        // acquiring with volumeGeneration
        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetSessionId("session2")
                .SetDiskId("vol0")
                .SetNow(TInstant::Seconds(45))
                .SetVolumeGeneration(3));
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

        // release without volumeGeneration should succeed as well
        error = ReleaseDevices(
            client,
            TReleaseParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetSessionId(TString(AnyWriterSessionId))
                .SetDiskId("vol0")
                .SetVolumeGeneration(0));
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

        // acquiring with volumeGeneration
        error = AcquireDevices(
            client,
            TAcquireParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetSessionId("session2")
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
                .SetSessionId("session0")
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
                .SetSessionId("session0")
                .SetDiskId("vol1")
                .SetNow(TInstant::Seconds(67))
                .SetVolumeGeneration(1));
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

        // release with a different diskId should succeed
        error = ReleaseDevices(
            client,
            TReleaseParamsBuilder()
                .SetUuids({"uuid1", "uuid2"})
                .SetSessionId(TString(AnyWriterSessionId))
                .SetDiskId("vol2")
                .SetVolumeGeneration(1));
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
