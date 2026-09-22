#include "service_local.h"

#include <cloud/blockstore/config/server.pb.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/storage_provider.h>
#include <cloud/blockstore/libs/service/storage_test.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/sglist.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/vector.h>

#include <functional>
#include <memory>

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 BlockSize = 4096;
constexpr ui64 BlocksCount = 262144;   // 1 GiB

struct TTestStorageProvider final: IStorageProvider
{
    std::function<IStoragePtr(const NProto::TVolume&)> Handler;

    TFuture<IStoragePtr> CreateStorage(
        const NProto::TVolume& volume,
        const TString& clientId,
        NProto::EVolumeAccessMode accessMode) override
    {
        UNIT_ASSERT_VALUES_EQUAL(clientId, "client");
        UNIT_ASSERT_EQUAL(accessMode, NProto::VOLUME_ACCESS_READ_WRITE);
        return MakeFuture(Handler(volume));
    }
};

struct TTestEnv
{
    TTempDir DataDir;
    IBlockStorePtr Service;
    TString SessionId;

    explicit TTestEnv(IStorageProviderPtr storageProvider)
    {
        NProto::TLocalServiceConfig config;
        config.SetDataDir(DataDir.Name());
        config.SetShutdownTimeout(0);
        Service =
            CreateLocalService(config, nullptr, std::move(storageProvider));

        auto create = std::make_shared<NProto::TCreateVolumeRequest>();
        create->SetDiskId("disk");
        create->SetBlockSize(BlockSize);
        create->SetBlocksCount(BlocksCount);
        const auto createResponse =
            Service
                ->CreateVolume(MakeIntrusive<TCallContext>(), std::move(create))
                .GetValue();
        UNIT_ASSERT_C(
            !HasError(createResponse),
            FormatError(createResponse.GetError()));

        auto mount = std::make_shared<NProto::TMountVolumeRequest>();
        mount->SetDiskId("disk");
        mount->MutableHeaders()->SetClientId("client");
        mount->SetVolumeAccessMode(NProto::VOLUME_ACCESS_READ_WRITE);
        const auto mountResponse =
            Service
                ->MountVolume(MakeIntrusive<TCallContext>(), std::move(mount))
                .GetValue();
        UNIT_ASSERT_C(
            !HasError(mountResponse),
            FormatError(mountResponse.GetError()));
        SessionId = mountResponse.GetSessionId();
    }

    template <typename TRequest>
    std::shared_ptr<TRequest> CreateIORequest() const
    {
        auto request = std::make_shared<TRequest>();
        request->SetDiskId("disk");
        request->SetSessionId(SessionId);
        request->MutableHeaders()->SetClientId("client");
        return request;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLocalServiceTest)
{
    Y_UNIT_TEST(ShouldValidateWriteBoundsUsingVolumeBlockSize)
    {
        auto storage = std::make_shared<TTestStorage>();
        ui32 writeCount = 0;
        ui64 lastStartIndex = 0;
        ui32 lastBlocksCount = 0;
        storage->WriteBlocksLocalHandler =
            [&](TCallContextPtr,
                std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
        {
            ++writeCount;
            lastStartIndex = request->GetStartIndex();
            lastBlocksCount = request->BlocksCount;
            UNIT_ASSERT_VALUES_EQUAL(request->GetBlockSize(), BlockSize);
            auto guard = request->Sglist.Acquire();
            UNIT_ASSERT(guard);
            UNIT_ASSERT_VALUES_EQUAL(
                SgListGetSize(guard.Get()),
                request->BlocksCount * BlockSize);
            return MakeFuture(NProto::TWriteBlocksLocalResponse());
        };

        auto provider = std::make_shared<TTestStorageProvider>();
        provider->Handler = [storage](const NProto::TVolume&)
        {
            return storage;
        };
        TTestEnv env(provider);

        struct TTestCase
        {
            ui64 StartIndex;
            ui32 BytesCount;
            ui32 ErrorCode;
        };

        const TTestCase testCases[] = {
            {0, BlockSize, S_OK},
            {BlocksCount - 1, BlockSize, S_OK},
            {BlocksCount - 2, 2 * BlockSize, S_OK},
            {BlocksCount - 1, 2 * BlockSize, E_ARGUMENT},
            {BlocksCount, BlockSize, E_ARGUMENT},
        };

        for (const auto& testCase: testCases) {
            auto request = env.CreateIORequest<NProto::TWriteBlocksRequest>();
            request->SetStartIndex(testCase.StartIndex);
            // Public clients may omit BlockSize; use the volume's block size.
            UNIT_ASSERT_VALUES_EQUAL(request->GetBlockSize(), 0);
            request->MutableBlocks()->AddBuffers()->assign(
                testCase.BytesCount,
                'x');

            const auto response = env.Service
                                      ->WriteBlocks(
                                          MakeIntrusive<TCallContext>(),
                                          std::move(request))
                                      .GetValue();
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetError().GetCode(),
                testCase.ErrorCode);
            if (testCase.ErrorCode == S_OK) {
                UNIT_ASSERT_VALUES_EQUAL(lastStartIndex, testCase.StartIndex);
                UNIT_ASSERT_VALUES_EQUAL(
                    lastBlocksCount,
                    testCase.BytesCount / BlockSize);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(writeCount, 3);
    }

    Y_UNIT_TEST(ShouldKeepOldStorageAliveUntilLocalIOCompletesAfterResize)
    {
        auto readPromise = NewPromise<NProto::TReadBlocksLocalResponse>();
        auto writePromise = NewPromise<NProto::TWriteBlocksLocalResponse>();
        TVector<std::weak_ptr<TTestStorage>> storages;
        ui32 writeCounts[2] = {};
        auto provider = std::make_shared<TTestStorageProvider>();
        provider->Handler = [&](const NProto::TVolume& volume)
        {
            const auto generation = storages.size();
            UNIT_ASSERT(generation < 2);
            UNIT_ASSERT_VALUES_EQUAL(
                volume.GetBlocksCount(),
                generation == 0 ? BlocksCount : 2 * BlocksCount);

            auto storage = std::make_shared<TTestStorage>();
            storage->ReadBlocksLocalHandler =
                [readPromise](TCallContextPtr, auto)
            {
                return readPromise.GetFuture();
            };
            storage->WriteBlocksLocalHandler =
                [&, generation, writePromise](TCallContextPtr, auto request)
            {
                ++writeCounts[generation];
                UNIT_ASSERT_VALUES_EQUAL(
                    request->GetStartIndex(),
                    generation == 0 ? 0 : BlocksCount);
                if (generation == 0) {
                    return writePromise.GetFuture();
                }
                return MakeFuture(NProto::TWriteBlocksLocalResponse());
            };
            // The provider must not keep the old storage alive itself.
            storages.push_back(storage);
            return storage;
        };
        TTestEnv env(provider);

        TString readBuffer(BlockSize, 0);
        auto read = env.CreateIORequest<NProto::TReadBlocksLocalRequest>();
        read->SetBlocksCount(1);
        read->SetBlockSize(BlockSize);
        read->Sglist = TGuardedSgList({{readBuffer.data(), readBuffer.size()}});
        auto readFuture = env.Service->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(read));

        TString writeBuffer(BlockSize, 'x');
        auto write = env.CreateIORequest<NProto::TWriteBlocksLocalRequest>();
        write->BlocksCount = 1;
        write->SetBlockSize(BlockSize);
        write->Sglist =
            TGuardedSgList({{writeBuffer.data(), writeBuffer.size()}});
        auto writeFuture = env.Service->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(write));
        UNIT_ASSERT(!readFuture.IsReady());
        UNIT_ASSERT(!writeFuture.IsReady());

        auto resize = std::make_shared<NProto::TResizeVolumeRequest>();
        resize->SetDiskId("disk");
        resize->SetBlocksCount(2 * BlocksCount);
        const auto resizeResponse =
            env.Service
                ->ResizeVolume(MakeIntrusive<TCallContext>(), std::move(resize))
                .GetValue();
        UNIT_ASSERT_C(
            !HasError(resizeResponse),
            FormatError(resizeResponse.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(storages.size(), 2);
        UNIT_ASSERT(!storages[0].expired());

        // The original session ID must now route writes to the expanded
        // storage.
        auto newWrite = env.CreateIORequest<NProto::TWriteBlocksLocalRequest>();
        newWrite->SetStartIndex(BlocksCount);
        newWrite->BlocksCount = 1;
        newWrite->SetBlockSize(BlockSize);
        newWrite->Sglist =
            TGuardedSgList({{writeBuffer.data(), writeBuffer.size()}});
        const auto newWriteResponse = env.Service
                                          ->WriteBlocksLocal(
                                              MakeIntrusive<TCallContext>(),
                                              std::move(newWrite))
                                          .GetValue();
        UNIT_ASSERT_C(
            !HasError(newWriteResponse),
            FormatError(newWriteResponse.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(writeCounts[0], 1);
        UNIT_ASSERT_VALUES_EQUAL(writeCounts[1], 1);

        readPromise.SetValue(NProto::TReadBlocksLocalResponse());
        UNIT_ASSERT(!HasError(readFuture.GetValue()));
        UNIT_ASSERT(!storages[0].expired());
        UNIT_ASSERT(!writeFuture.IsReady());

        writePromise.SetValue(NProto::TWriteBlocksLocalResponse());
        UNIT_ASSERT(!HasError(writeFuture.GetValue()));
        UNIT_ASSERT(storages[0].expired());
        UNIT_ASSERT(!storages[1].expired());
    }
}

}   // namespace NCloud::NBlockStore::NServer
