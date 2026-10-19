#include "device_manager.h"

#include <cloud/fastshard/journal/iface/device.h>
#include <cloud/fastshard/journal/server/device_manager.h>
#include <cloud/fastshard/protos/device.pb.h>

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

class TFileDeviceManagerTest: public ::testing::Test
{
protected:
    TTempDir TempDir;
    IFileIOServicePtr FileIO;
    IDeviceManagerPtr Manager;

    void SetUp() override
    {
        FileIO = CreateAIOService();
        FileIO->Start();

        Manager = CreateFileDeviceManager(
            FileIO,
            DeviceUUID,
            TempDir.Path() / "device",
            BlockCount,
            BlockSize);
    }

    void TearDown() override
    {
        Manager.reset();
        FileIO->Stop();
    }

    NProto::TError Acquire(
        const TString& clientId,
        ui32 generation,
        const TString& uuid = DeviceUUID)
    {
        NProto::TAcquireDevicesRequest request;
        request.MutableHeaders()->SetClientId(clientId);
        request.AddDeviceUUIDs(uuid);
        request.SetGeneration(generation);

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

    NProto::TError Access(const TString& clientId)
    {
        return Manager->AccessDevice(
            DeviceUUID,
            clientId,
            NProto::ACCESS_READ_WRITE);
    }

    static TPageRange MakeRange(ui64 firstPageNo, char fill)
    {
        TPageRange range;
        range.FirstPageNo = firstPageNo;
        range.Pages.emplace_back(TString(BlockSize, fill).data(), BlockSize);
        return range;
    }

    static TString ReadPage(const IDevicePtr& device, ui64 pageNo)
    {
        auto result = device->ReadPages({{pageNo, 1}}).GetValueSync();
        EXPECT_EQ(S_OK, result.GetError().GetCode())
            << result.GetError().GetMessage();
        if (HasError(result)) {
            return {};
        }

        const auto& page = result.GetResult()[0];
        return TString(page.Data(), page.Size());
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TFileDeviceManagerTest, ShouldServeOnlyTheConfiguredDevice)
{
    EXPECT_EQ(E_NOT_FOUND, Acquire("client-1", 1, "unknown").GetCode());
    EXPECT_EQ(
        E_NOT_FOUND,
        Manager->AccessDevice("unknown", "client-1", NProto::ACCESS_READ_ONLY)
            .GetCode());
}

TEST_F(TFileDeviceManagerTest, ShouldGrantAccessToTheOwnerOnly)
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

TEST_F(TFileDeviceManagerTest, ShouldLetAHigherGenerationTakeOver)
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

TEST_F(TFileDeviceManagerTest, ShouldIgnoreAReleaseByANonOwner)
{
    EXPECT_EQ(S_OK, Acquire("client-1", 1).GetCode());
    EXPECT_EQ(S_OK, Release("client-2", 1).GetCode());
    EXPECT_EQ(S_OK, Access("client-1").GetCode());
}

TEST_F(TFileDeviceManagerTest, ShouldAddressTheRegionFromItsFirstPage)
{
    auto whole = Manager->CreateDevice(DeviceUUID, {0, BlockCount}, BlockSize);
    auto region = Manager->CreateDevice(DeviceUUID, {4, 8}, BlockSize);

    EXPECT_EQ(
        S_OK,
        region->WritePages({MakeRange(0, 'a'), MakeRange(7, 'b')})
            .GetValueSync()
            .GetCode());

    EXPECT_EQ(TString(BlockSize, 'a'), ReadPage(whole, 4));
    EXPECT_EQ(TString(BlockSize, 'b'), ReadPage(whole, 11));
    EXPECT_EQ(TString(BlockSize, 'a'), ReadPage(region, 0));
    EXPECT_EQ(TString(BlockSize, 'b'), ReadPage(region, 7));

    EXPECT_EQ(S_OK, region->ZeroPages({{0, 8}}).GetValueSync().GetCode());
    EXPECT_EQ(TString(BlockSize, '\0'), ReadPage(whole, 4));
    EXPECT_EQ(TString(BlockSize, '\0'), ReadPage(whole, 11));
}

TEST_F(TFileDeviceManagerTest, ShouldRejectPagesOutsideTheRegion)
{
    auto region = Manager->CreateDevice(DeviceUUID, {4, 8}, BlockSize);

    EXPECT_EQ(
        E_ARGUMENT,
        region->ReadPages({{7, 2}}).GetValueSync().GetError().GetCode());
    EXPECT_EQ(
        E_ARGUMENT,
        region->WritePages({MakeRange(8, 'a')}).GetValueSync().GetCode());
    EXPECT_EQ(
        E_ARGUMENT,
        region->ZeroPages({{0, 9}}).GetValueSync().GetCode());
    EXPECT_EQ(
        E_ARGUMENT,
        region->ZeroPages({{Max<ui64>(), 1}}).GetValueSync().GetCode());
}

}   // namespace

}   // namespace NCloud::NFastShard::NJdServer
