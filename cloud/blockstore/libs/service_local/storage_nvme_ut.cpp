#include "storage_nvme.h"

#include <cloud/blockstore/libs/nvme/nvme_stub.h>
#include <cloud/blockstore/libs/service/storage_provider.h>
#include <cloud/blockstore/libs/service_local/storage_null.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TFixture: NUnitTest::TBaseFixture
{
    ILoggingServicePtr Logging;
    NNvme::INvmeManagerPtr NvmeManager;

    void SetUp(NUnitTest::TTestContext& /*testContext*/) override
    {
        Logging = CreateLoggingService("console");
        NvmeManager = NNvme::CreateNvmeManagerStub();
    }
};

struct TTestStorageProvider: IStorageProvider
{
    IStoragePtr Storage = CreateNullStorage();

    TFuture<IStoragePtr> CreateStorage(
        const NProto::TVolume& volume,
        const TString& clientId,
        NProto::EVolumeAccessMode accessMode) final
    {
        Y_UNUSED(volume, clientId, accessMode);

        return MakeFuture(Storage);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TNvmeStorageTest)
{
    Y_UNIT_TEST_F(ShouldFallback, TFixture)
    {
        auto fallback = std::make_shared<TTestStorageProvider>();
        auto service =
            CreateNvmeStorageProvider(Logging, fallback, NvmeManager);

        NProto::TVolume volume;
        volume.SetDiskId("path");
        volume.SetBlocksCount(93_MB);

        auto storage = service->CreateStorage(
            volume,
            "client",
            NProto::VOLUME_ACCESS_READ_WRITE);

        UNIT_ASSERT_EQUAL(fallback->Storage, storage.GetValueSync());
    }

    Y_UNIT_TEST_F(ShouldLocalReadWriteZero, TFixture)
    {
        // TODO
    }
}

}   // namespace NCloud::NBlockStore::NServer
