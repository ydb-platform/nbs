#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/durable.h>
#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/encryption/encryption_client.h>
#include <cloud/blockstore/libs/encryption/encryption_key.h>
#include <cloud/blockstore/libs/encryption/encryption_test.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service_test.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/sglist_test.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/benchmark/bench.h>

namespace NCloud::NBlockStore {

using namespace NClient;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
{
    const ui32 BlockSize = 4*1024;

    ILoggingServicePtr Logging = CreateLoggingService(
        "console",
        {
            .FiltrationLevel = TLOG_ERR,
        });
    TClientAppConfigPtr Config = std::make_shared<TClientAppConfig>();
    ITimerPtr Timer = CreateWallClockTimer();
    ISchedulerPtr Scheduler = CreateScheduler();
    IRequestStatsPtr RequestStats = CreateRequestStatsStub();
    IVolumeStatsPtr VolumeStats = CreateVolumeStatsStub();
    IEncryptionClientFactoryPtr EncryptionClientFactory;

    TBootstrap()
    {
        EncryptionClientFactory = CreateEncryptionClientFactory(
            Logging,
            CreateDefaultEncryptionKeyProvider(),
            NProto::EZP_WRITE_ENCRYPTED_ZEROS);
    }

    IBlockStorePtr CreateClient()
    {
        auto client = std::make_shared<TTestService>();

        client->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                Y_UNUSED(request);

                NProto::TMountVolumeResponse response;
                response.SetSessionId("test");

                auto& volume = *response.MutableVolume();
                volume.SetDiskId("disk");
                volume.SetBlockSize(BlockSize);
                volume.SetBlocksCount(1024);

                return MakeFuture(response);
            };

        client->WriteBlocksHandler =
            [&] (std::shared_ptr<NProto::TWriteBlocksRequest> request) {
                Y_UNUSED(request);
                return MakeFuture(NProto::TWriteBlocksResponse());
            };

        client->WriteBlocksLocalHandler =
            [&] (std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) {
                Y_UNUSED(request);
                return MakeFuture(NProto::TWriteBlocksLocalResponse());
            };

        return client;
    }

    IBlockStorePtr CreateDurableClient(IBlockStorePtr client)
    {
        return NClient::CreateDurableClient(
            Config,
            std::move(client),
            CreateRetryPolicy(Config, std::nullopt),
            Logging,
            Timer,
            Scheduler,
            RequestStats,
            VolumeStats);
    }

    IBlockStorePtr CreateEncryptionClient(IBlockStorePtr client)
    {
        TString diskId("diskid");
        TEncryptionKeyFile keyFile("01234567890123456789012345678901");
        NProto::TEncryptionSpec encryptionSpec;
        encryptionSpec.SetMode(NProto::ENCRYPTION_AES_XTS);
        encryptionSpec.MutableKeyPath()->SetFilePath(keyFile.GetPath());

        auto future = EncryptionClientFactory->CreateEncryptionClient(
            std::move(client),
            encryptionSpec,
            diskId);

        auto clientOrError = future.GetValue();
        if (HasError(clientOrError)) {
            const auto& error = clientOrError.GetError();
            ythrow TServiceError(error.GetCode()) << error.GetMessage();
        }

        return clientOrError.GetResult();
    }

    ISessionPtr CreateSession(IBlockStorePtr client)
    {
        return NClient::CreateSession(
            Timer,
            Scheduler,
            Logging,
            RequestStats,
            VolumeStats,
            std::move(client),
            Config,
            TSessionConfig());
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void CheckError(TFuture<T> future)
{
    const auto& response = future.GetValueSync();
    if (HasError(response)) {
        auto error = FormatError(response.GetError());
        Y_ABORT("Request failed with error: %s", error.c_str());
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_CPU_BENCHMARK(ClientTest, iface)
{
    TBootstrap bootstrap;

    auto client = bootstrap.CreateClient();

    auto context = MakeIntrusive<TCallContext>();
    auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
    request->BlocksCount = 32;
    request->BlockSize = bootstrap.BlockSize;

    TVector<TString> blocks;
    request->Sglist = TGuardedSgList(ResizeBlocks(
        blocks,
        request->BlocksCount,
        TString(bootstrap.BlockSize, 42)));

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        CheckError(client->WriteBlocksLocal(context, request));
    }
}

Y_CPU_BENCHMARK(DurableClientTest, iface)
{
    TBootstrap bootstrap;

    auto client = bootstrap.CreateClient();
    auto durable = bootstrap.CreateDurableClient(client);

    auto context = MakeIntrusive<TCallContext>();
    auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
    request->BlocksCount = 32;
    request->BlockSize = bootstrap.BlockSize;

    TVector<TString> blocks;
    request->Sglist = TGuardedSgList(ResizeBlocks(
        blocks,
        request->BlocksCount,
        TString(bootstrap.BlockSize, 42)));

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        CheckError(durable->WriteBlocksLocal(context, request));
    }
}

Y_CPU_BENCHMARK(SessionDurableClientTest, iface)
{
    TBootstrap bootstrap;

    auto client = bootstrap.CreateClient();
    auto durable = bootstrap.CreateDurableClient(client);
    auto session = bootstrap.CreateSession(durable);

    CheckError(session->MountVolume());

    auto context = MakeIntrusive<TCallContext>();
    auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
    request->BlocksCount = 32;
    request->BlockSize = bootstrap.BlockSize;

    TVector<TString> blocks;
    request->Sglist = TGuardedSgList(ResizeBlocks(
        blocks,
        request->BlocksCount,
        TString(bootstrap.BlockSize, 42)));

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        CheckError(session->WriteBlocksLocal(context, request));
    }
}

Y_CPU_BENCHMARK(SessionClientTest, iface)
{
    TBootstrap bootstrap;

    auto client = bootstrap.CreateClient();
    auto session = bootstrap.CreateSession(client);

    CheckError(session->MountVolume());

    auto context = MakeIntrusive<TCallContext>();
    auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
    request->BlocksCount = 32;
    request->BlockSize = bootstrap.BlockSize;

    TVector<TString> blocks;
    request->Sglist = TGuardedSgList(ResizeBlocks(
        blocks,
        request->BlocksCount,
        TString(bootstrap.BlockSize, 42)));

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        CheckError(session->WriteBlocksLocal(context, request));
    }
}

Y_CPU_BENCHMARK(EncryptionClientTest, iface)
{
    TBootstrap bootstrap;

    auto client = bootstrap.CreateClient();
    auto encryption = bootstrap.CreateEncryptionClient(client);

    CheckError(encryption->MountVolume(
        MakeIntrusive<TCallContext>(),
        std::make_shared<NProto::TMountVolumeRequest>()));

    auto context = MakeIntrusive<TCallContext>();
    auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
    request->BlocksCount = 32;
    request->BlockSize = bootstrap.BlockSize;

    TVector<TString> blocks;
    request->Sglist = TGuardedSgList(ResizeBlocks(
        blocks,
        request->BlocksCount,
        TString(bootstrap.BlockSize, 42)));

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        CheckError(encryption->WriteBlocksLocal(context, request));
    }
}

}   // namespace NCloud::NBlockStore
