#include "device_manager.h"

#include <cloud/fastshard/libs/journal/iface/device.h>
#include <cloud/fastshard/libs/journal/impl/file_device.h>
#include <cloud/fastshard/libs/journal/server/device_manager.h>
#include <cloud/fastshard/public/api/protos/device.pb.h>

#include <cloud/storage/core/libs/aio/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/file_io_service.h>

#include <util/folder/tempdir.h>
#include <util/generic/size_literals.h>
#include <util/generic/string.h>

#include <gtest/gtest.h>

namespace NCloud::NFastShard::NJdServer {

using namespace NJournalled;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 BlockSize = 4_KB;
constexpr ui64 BlockCount = 16;
const TString DeviceUUID = "uuid-1";

////////////////////////////////////////////////////////////////////////////////

class TSingleDeviceManagerTest: public ::testing::Test
{
protected:
    TTempDir TempDir;
    IFileIOServicePtr FileIO;
    IDeviceManagerPtr Manager;

    void SetUp() override
    {
        FileIO = CreateAIOService();
        FileIO->Start();

        Manager = CreateSingleDeviceManager(
            CreateFileDevice(
                FileIO,
                TempDir.Path() / "device",
                BlockCount,
                BlockSize),
            DeviceUUID);
    }

    void TearDown() override
    {
        Manager.reset();
        FileIO->Stop();
    }

    NProto::TError Acquire(
        const TString& clientId,
        ui32 generation,
        const TString& uuid = DeviceUUID,
        NProto::EAccessMode accessMode = NProto::ACCESS_READ_WRITE)
    {
        NProto::TAcquireDevicesRequest request;
        request.MutableHeaders()->SetClientId(clientId);
        request.AddDeviceUUIDs(uuid);
        request.SetGeneration(generation);
        request.SetAccessMode(accessMode);

        return Manager->AcquireDevices(std::move(request))
            .GetValueSync()
            .GetError();
    }

    NProto::TError Release(const TString& clientId, ui32 generation)
    {
        NProto::TReleaseDevicesRequest request;
        request.MutableHeaders()->SetClientId(clientId);
        request.AddDeviceUUIDs(DeviceUUID);
        request.SetGeneration(generation);

        return Manager->ReleaseDevices(std::move(request))
            .GetValueSync()
            .GetError();
    }

    NProto::TError AcquireForReading(const TString& clientId, ui32 generation)
    {
        return Acquire(
            clientId,
            generation,
            DeviceUUID,
            NProto::ACCESS_READ_ONLY);
    }

    NProto::TError Access(
        const TString& clientId,
        NProto::EAccessMode accessMode = NProto::ACCESS_READ_WRITE)
    {
        return Manager->AccessDevice(DeviceUUID, clientId, accessMode);
    }

    NProto::TError Read(const TString& clientId)
    {
        return Access(clientId, NProto::ACCESS_READ_ONLY);
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSingleDeviceManagerTest, ShouldServeOnlyConfiguredDevice)
{
    EXPECT_EQ(E_NOT_FOUND, Acquire("client-1", 1, "unknown").GetCode());
    EXPECT_EQ(
        E_NOT_FOUND,
        Manager->AccessDevice("unknown", "client-1", NProto::ACCESS_READ_ONLY)
            .GetCode());
}

TEST_F(TSingleDeviceManagerTest, ShouldRejectRequestsWithoutDevices)
{
    NProto::TAcquireDevicesRequest acquire;
    acquire.MutableHeaders()->SetClientId("client-1");
    acquire.SetGeneration(1);
    EXPECT_EQ(
        E_ARGUMENT,
        Manager->AcquireDevices(acquire).GetValueSync().GetError().GetCode());
    EXPECT_EQ(E_BS_INVALID_SESSION, Access("client-1").GetCode());

    EXPECT_EQ(S_OK, Acquire("client-1", 1).GetCode());

    NProto::TReleaseDevicesRequest release;
    release.MutableHeaders()->SetClientId("client-1");
    release.SetGeneration(1);
    EXPECT_EQ(
        E_ARGUMENT,
        Manager->ReleaseDevices(release).GetValueSync().GetError().GetCode());
    EXPECT_EQ(S_OK, Access("client-1").GetCode());
}

TEST_F(TSingleDeviceManagerTest, ShouldGrantAccessToWriterOnly)
{
    EXPECT_EQ(E_BS_INVALID_SESSION, Access("client-1").GetCode());

    EXPECT_EQ(S_OK, Acquire("client-1", 1).GetCode());
    EXPECT_EQ(S_OK, Access("client-1").GetCode());
    EXPECT_EQ(E_BS_INVALID_SESSION, Access("client-2").GetCode());

    // the owner may acquire again
    EXPECT_EQ(S_OK, Acquire("client-1", 1).GetCode());

    // another client with the same generation is rejected
    EXPECT_EQ(E_BS_INVALID_SESSION, Acquire("client-2", 1).GetCode());
    EXPECT_EQ(S_OK, Access("client-1").GetCode());

    EXPECT_EQ(S_OK, Release("client-1", 1).GetCode());
    EXPECT_EQ(E_BS_INVALID_SESSION, Access("client-1").GetCode());
}

TEST_F(TSingleDeviceManagerTest, ShouldLetHigherGenerationTakeOver)
{
    EXPECT_EQ(S_OK, Acquire("client-1", 1).GetCode());
    EXPECT_EQ(S_OK, Acquire("client-2", 2).GetCode());

    EXPECT_EQ(S_OK, Access("client-2").GetCode());
    EXPECT_EQ(E_BS_INVALID_SESSION, Access("client-1").GetCode());

    // a stale generation is rejected, acquire and release alike
    EXPECT_EQ(E_BS_INVALID_SESSION, Acquire("client-1", 1).GetCode());
    EXPECT_EQ(E_BS_INVALID_SESSION, Release("client-2", 1).GetCode());
    EXPECT_EQ(S_OK, Access("client-2").GetCode());

    // a zero generation is not checked
    EXPECT_EQ(S_OK, Acquire("client-2", 0).GetCode());
    EXPECT_EQ(S_OK, Release("client-2", 0).GetCode());
    EXPECT_EQ(E_BS_INVALID_SESSION, Access("client-2").GetCode());
}

TEST_F(TSingleDeviceManagerTest, ShouldIgnoreReleaseByNonOwner)
{
    EXPECT_EQ(S_OK, Acquire("client-1", 1).GetCode());
    EXPECT_EQ(S_OK, Release("client-2", 1).GetCode());
    EXPECT_EQ(S_OK, Access("client-1").GetCode());

    // nor does it raise the generation
    EXPECT_EQ(S_OK, Release("client-2", 7).GetCode());
    EXPECT_EQ(S_OK, Acquire("client-1", 1).GetCode());
    EXPECT_EQ(S_OK, Release("client-1", 1).GetCode());
}

TEST_F(TSingleDeviceManagerTest, ShouldGrantReadersReadAccessOnly)
{
    EXPECT_EQ(S_OK, Acquire("writer", 1).GetCode());

    // a reader does not take the device over, whatever its generation
    EXPECT_EQ(S_OK, AcquireForReading("reader", 2).GetCode());
    EXPECT_EQ(S_OK, Access("writer").GetCode());
    EXPECT_EQ(S_OK, Read("writer").GetCode());
    EXPECT_EQ(S_OK, Read("reader").GetCode());
    EXPECT_EQ(E_BS_INVALID_SESSION, Access("reader").GetCode());
    EXPECT_EQ(E_BS_INVALID_SESSION, Read("client-3").GetCode());

    // readers neither raise nor check the generation
    EXPECT_EQ(S_OK, Acquire("writer", 1).GetCode());
    EXPECT_EQ(S_OK, Acquire("writer", 3).GetCode());
    EXPECT_EQ(S_OK, AcquireForReading("reader", 1).GetCode());

    // a release drops the reader only, whatever its generation
    EXPECT_EQ(S_OK, Release("reader", 1).GetCode());
    EXPECT_EQ(E_BS_INVALID_SESSION, Read("reader").GetCode());
    EXPECT_EQ(S_OK, Access("writer").GetCode());
    EXPECT_EQ(S_OK, Acquire("writer", 3).GetCode());
}

TEST_F(TSingleDeviceManagerTest, ShouldSwitchClientBetweenWriterAndReader)
{
    EXPECT_EQ(S_OK, Acquire("client-1", 1).GetCode());

    // re-acquiring for reading gives the writer role up
    EXPECT_EQ(S_OK, AcquireForReading("client-1", 1).GetCode());
    EXPECT_EQ(E_BS_INVALID_SESSION, Access("client-1").GetCode());
    EXPECT_EQ(S_OK, Read("client-1").GetCode());

    // so another client takes the device at the same generation
    EXPECT_EQ(S_OK, Acquire("client-2", 1).GetCode());
    EXPECT_EQ(S_OK, Access("client-2").GetCode());
    EXPECT_EQ(S_OK, Read("client-1").GetCode());

    // re-acquiring for writing makes the reader the writer again
    EXPECT_EQ(S_OK, Acquire("client-1", 2).GetCode());
    EXPECT_EQ(S_OK, Access("client-1").GetCode());
    EXPECT_EQ(E_BS_INVALID_SESSION, Access("client-2").GetCode());
    EXPECT_EQ(E_BS_INVALID_SESSION, Read("client-2").GetCode());

    // and a single release drops it completely
    EXPECT_EQ(S_OK, Release("client-1", 2).GetCode());
    EXPECT_EQ(E_BS_INVALID_SESSION, Read("client-1").GetCode());
}

}   // namespace

}   // namespace NCloud::NFastShard::NJdServer
