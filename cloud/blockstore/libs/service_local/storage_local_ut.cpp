#include "storage_local.h"

#include "file_io_service_provider.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/service/storage_provider.h>
#include <cloud/blockstore/libs/nvme/nvme_stub.h>

#include <cloud/storage/core/libs/aio/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/file_io_service.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/folder/dirut.h>
#include <util/folder/tempdir.h>
#include <util/generic/scope.h>
#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/system/file.h>

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;
using namespace NNvme;

namespace {

////////////////////////////////////////////////////////////////////////////////

#define UNIT_ASSERT_SUCCEEDED(e) \
    UNIT_ASSERT_C(SUCCEEDED(e.GetCode()), e.GetMessage())

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration WaitTimeout = TDuration::Minutes(5);

TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
    IStorage& storage,
    ui64 startIndex,
    ui32 blockCount,
    ui32 blockSize,
    TGuardedSgList sglist)
{
    auto context = MakeIntrusive<TCallContext>();

    auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
    request->SetStartIndex(startIndex);
    request->SetBlocksCount(blockCount);
    request->BlockSize = blockSize;
    request->Sglist = std::move(sglist);

    return storage.ReadBlocksLocal(
        std::move(context),
        std::move(request));
}

TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
    IStorage& storage,
    ui64 startIndex,
    ui32 blockCount,
    ui32 blockSize,
    TGuardedSgList sglist)
{
    auto context = MakeIntrusive<TCallContext>();

    auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
    request->SetStartIndex(startIndex);
    request->BlocksCount = blockCount;
    request->BlockSize = blockSize;
    request->Sglist = std::move(sglist);

    return storage.WriteBlocksLocal(
        std::move(context),
        std::move(request));
}

TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
    IStorage& storage,
    ui64 startIndex,
    ui32 blockCount)
{
    auto context = MakeIntrusive<TCallContext>();

    auto request = std::make_shared<NProto::TZeroBlocksRequest>();
    request->SetStartIndex(startIndex);
    request->SetBlocksCount(blockCount);

    return storage.ZeroBlocks(
        std::move(context),
        std::move(request));

}

TFsPath TryGetRamDrivePath()
{
    auto p = GetRamDrivePath();
    return !p
        ? GetSystemTempDir()
        : p;
}

auto CreateAndStartAIOServiceProvider()
{
    auto provider = CreateSingleFileIOServiceProvider(CreateAIOService());
    provider->Start();

    return provider;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLocalStorageTest)
{
    void ShouldHandleLocalReadWriteRequestsImpl(
        ui32 blockSize,
        ELocalSubmitQueueOpt submitQueueOpt)
    {
        const ui64 blockCount = 1024;
        const ui64 startIndex = 10;
        const auto filePath = TryGetRamDrivePath() / "test";

        TFile fileData(filePath, EOpenModeFlag::CreateAlways);
        fileData.Resize(blockSize * (blockCount + startIndex));

        auto fileIOServiceProvider = CreateAndStartAIOServiceProvider();
        Y_DEFER { fileIOServiceProvider->Stop(); };

        auto provider = CreateLocalStorageProvider(
            fileIOServiceProvider,
            CreateNvmeManagerStub(),
            false,  // directIO
            submitQueueOpt
        );

        NProto::TVolume volume;
        volume.SetDiskId(filePath);
        volume.SetBlockSize(blockSize);
        volume.SetBlocksCount(blockCount);
        volume.SetStartIndex(startIndex);

        auto future = provider->CreateStorage(
            volume,
            "",
            NProto::VOLUME_ACCESS_READ_WRITE);
        auto storage = future.GetValue();

        auto writeBuffer = storage->AllocateBuffer(blockSize);
        memset(writeBuffer.get(), 'a', blockSize);
        auto writeSgList = TGuardedSgList({{writeBuffer.get(), blockSize}});

        {
            auto writeResponse = WriteBlocksLocal(
                *storage,
                0,
                1,
                blockSize,
                writeSgList
            );
            UNIT_ASSERT_NO_EXCEPTION(writeResponse.Wait(WaitTimeout));
            UNIT_ASSERT_SUCCEEDED(writeResponse.GetValue().GetError());
        }

        {
            auto writeResponse = WriteBlocksLocal(
                *storage,
                blockCount,
                1,
                blockSize,
                writeSgList
            );
            UNIT_ASSERT_NO_EXCEPTION(writeResponse.Wait(WaitTimeout));
            UNIT_ASSERT_VALUES_EQUAL(
                E_ARGUMENT,
                writeResponse.GetValue().GetError().GetCode());
        }

        auto readBuffer = storage->AllocateBuffer(blockSize);
        auto readSgList = TGuardedSgList({{readBuffer.get(), blockSize}});

        {
            auto readResponse = ReadBlocksLocal(
                *storage,
                0,
                1,
                blockSize,
                readSgList
            );
            UNIT_ASSERT_NO_EXCEPTION(readResponse.Wait(WaitTimeout));
            UNIT_ASSERT_SUCCEEDED(readResponse.GetValue().GetError());
        }

        UNIT_ASSERT(memcmp(writeBuffer.get(), readBuffer.get(), blockSize) == 0);

        {
            auto readResponse = ReadBlocksLocal(
                *storage,
                blockCount,
                1,
                blockSize,
                readSgList
            );
            UNIT_ASSERT_NO_EXCEPTION(readResponse.Wait(WaitTimeout));
            UNIT_ASSERT_VALUES_EQUAL(
                E_ARGUMENT,
                readResponse.GetValue().GetError().GetCode());
        }

        TString buffer(blockSize, 0);
        size_t bytes = fileData.Pread(
            buffer.begin(),
            blockSize,
            blockSize * startIndex);
        UNIT_ASSERT_VALUES_EQUAL(blockSize, bytes);
        UNIT_ASSERT_VALUES_EQUAL(TString(blockSize, 'a'), buffer);

        bytes = fileData.Pread(
            buffer.begin(),
            blockSize,
            blockSize * (startIndex - 1));
        UNIT_ASSERT_VALUES_EQUAL(blockSize, bytes);
        UNIT_ASSERT_VALUES_EQUAL(TString(blockSize, 0), buffer);
    }

    void ShouldHandleZeroBlocksRequestsImpl(ELocalSubmitQueueOpt submitQueueOpt)
    {
        const ui32 blockSize = 4096;
        const ui64 blockCount = 32_MB / blockSize;
        const auto filePath = TryGetRamDrivePath() / "test";

        TFile fileData(filePath, EOpenModeFlag::CreateAlways);

        {
            TVector<char> buffer(blockSize, 'X');
            for (ui32 i = 0; i != blockCount; ++i) {
                fileData.Write(buffer.data(), blockSize);
            }

            fileData.Flush();
        }

        auto fileIOServiceProvider = CreateAndStartAIOServiceProvider();
        Y_DEFER { fileIOServiceProvider->Stop(); };

        auto provider = CreateLocalStorageProvider(
            fileIOServiceProvider,
            CreateNvmeManagerStub(),
            false,  // directIO
            submitQueueOpt
        );

        NProto::TVolume volume;
        volume.SetDiskId(filePath);
        volume.SetBlockSize(blockSize);
        volume.SetBlocksCount(blockCount);

        auto future = provider->CreateStorage(
            volume,
            "",
            NProto::VOLUME_ACCESS_READ_WRITE);
        auto storage = future.GetValue();

        auto readBuffer = storage->AllocateBuffer(blockSize);
        auto readSgList = TGuardedSgList({{readBuffer.get(), blockSize}});

        auto verifyData = [&] (char c) {
            for (ui64 i = 0; i < blockCount; ++i) {
                auto response = ReadBlocksLocal(
                    *storage,
                    i,
                    1,
                    blockSize,
                    readSgList
                );
                UNIT_ASSERT_NO_EXCEPTION_C(response.Wait(WaitTimeout), c);
                UNIT_ASSERT_SUCCEEDED(response.GetValue().GetError());

                UNIT_ASSERT_VALUES_EQUAL(
                    blockSize,
                    std::count(
                        readBuffer.get(),
                        readBuffer.get() + blockSize,
                        c));
            }
        };

        verifyData('X');

        auto zeroResponse = ZeroBlocks(*storage, 0, blockCount);
        UNIT_ASSERT_NO_EXCEPTION(zeroResponse.Wait(WaitTimeout));
        UNIT_ASSERT_SUCCEEDED(zeroResponse.GetValue().GetError());

        verifyData('\0');
    }


    Y_UNIT_TEST(ShouldHandleLocalReadWriteRequests_512)
    {
        ShouldHandleLocalReadWriteRequestsImpl(
            512,
            ELocalSubmitQueueOpt::DontUse);
    }

    Y_UNIT_TEST(ShouldHandleLocalReadWriteRequests_512_withSubmitQueue)
    {
        ShouldHandleLocalReadWriteRequestsImpl(512, ELocalSubmitQueueOpt::Use);
    }

    Y_UNIT_TEST(ShouldHandleLocalReadWriteRequests_1024)
    {
        ShouldHandleLocalReadWriteRequestsImpl(
            1024,
            ELocalSubmitQueueOpt::DontUse);
    }

    Y_UNIT_TEST(ShouldHandleLocalReadWriteRequests_4096)
    {
        ShouldHandleLocalReadWriteRequestsImpl(
            DefaultBlockSize,
            ELocalSubmitQueueOpt::DontUse);
    }

    Y_UNIT_TEST(ShouldHandleZeroBlocksRequests)
    {
        ShouldHandleZeroBlocksRequestsImpl(ELocalSubmitQueueOpt::DontUse);
    }

    Y_UNIT_TEST(ShouldHandleZeroBlocksRequests_withSubmitQueue)
    {
        ShouldHandleZeroBlocksRequestsImpl(ELocalSubmitQueueOpt::Use);
    }

    Y_UNIT_TEST(ShouldHandleZeroBlocksRequestsForBigFiles)
    {
        const ui32 blockSize = 4_KB;
        const ui64 blockCount = 8_GB / blockSize;
        const auto filePath = TryGetRamDrivePath() / "test";

        TFile fileData(filePath, EOpenModeFlag::CreateAlways);
        fileData.Resize(blockSize * blockCount);

        auto fileIOServiceProvider = CreateAndStartAIOServiceProvider();
        Y_DEFER { fileIOServiceProvider->Stop(); };

        auto provider = CreateLocalStorageProvider(
            fileIOServiceProvider,
            CreateNvmeManagerStub(),
            false,  // directIO
            ELocalSubmitQueueOpt::DontUse
        );

        NProto::TVolume volume;
        volume.SetDiskId(filePath);
        volume.SetBlockSize(blockSize);
        volume.SetBlocksCount(blockCount);

        auto future = provider->CreateStorage(
            volume,
            "",
            NProto::VOLUME_ACCESS_READ_WRITE);
        auto storage = future.GetValue();

        auto writeBuffer = storage->AllocateBuffer(blockSize);
        memset(writeBuffer.get(), 'a', blockSize);
        auto writeSgList = TGuardedSgList({{writeBuffer.get(), blockSize}});

        const ui64 startIndex = 7_GB / blockSize;

        {
            auto writeResponse = WriteBlocksLocal(
                *storage,
                startIndex,
                1,
                blockSize,
                writeSgList
            );
            UNIT_ASSERT_NO_EXCEPTION(writeResponse.Wait(WaitTimeout));
            UNIT_ASSERT_SUCCEEDED(writeResponse.GetValue().GetError());
        }

        auto readBuffer = storage->AllocateBuffer(blockSize);
        auto readSgList = TGuardedSgList({{readBuffer.get(), blockSize}});

        // verify data

        {
            auto response = ReadBlocksLocal(
                *storage,
                startIndex,
                1,
                blockSize,
                readSgList
            );
            UNIT_ASSERT_NO_EXCEPTION(response.Wait(WaitTimeout));
            UNIT_ASSERT_SUCCEEDED(response.GetValue().GetError());

            UNIT_ASSERT_VALUES_EQUAL(
                0,
                memcmp(writeBuffer.get(), readBuffer.get(), blockSize)
            );
        }

        // erase 10241 blocks (40MB + 4KB)
        {
            auto response = ZeroBlocks(*storage, startIndex, 10241);
            UNIT_ASSERT_NO_EXCEPTION(response.Wait(WaitTimeout));
            UNIT_ASSERT_SUCCEEDED(response.GetValue().GetError());
        }

        // verify

        {
            auto response = ReadBlocksLocal(
                *storage,
                startIndex,
                1,
                blockSize,
                readSgList
            );
            UNIT_ASSERT_NO_EXCEPTION(response.Wait(WaitTimeout));
            UNIT_ASSERT_SUCCEEDED(response.GetValue().GetError());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            blockSize,
            std::count(
                readBuffer.get(),
                readBuffer.get() + blockSize,
                '\0'));

        // should reject too big requests

        {
            const ui32 len = 64_MB;
            auto tooBigBuffer = storage->AllocateBuffer(len);
            auto sgList = TGuardedSgList({{ tooBigBuffer.get(), len }});

            auto readResponse = ReadBlocksLocal(
                *storage,
                startIndex,
                len / blockSize,
                blockSize,
                sgList
            );
            UNIT_ASSERT_NO_EXCEPTION(readResponse.Wait(WaitTimeout));
            UNIT_ASSERT_EQUAL(
                E_ARGUMENT,
                readResponse.GetValue().GetError().GetCode()
            );

            auto writeResponse = WriteBlocksLocal(
                *storage,
                startIndex,
                len / blockSize,
                blockSize,
                sgList
            );
            UNIT_ASSERT_NO_EXCEPTION(writeResponse.Wait(WaitTimeout));
            UNIT_ASSERT_EQUAL(
                E_ARGUMENT,
                writeResponse.GetValue().GetError().GetCode()
            );
        }
    }

    Y_UNIT_TEST(ShouldValidateDeallocatedBlocksInEraseDeviceUsingDeallocate)
    {
        const ui32 blockSize = 4_KB;
        const ui64 blockCount = 32_MB / blockSize;
        const auto filePath = TryGetRamDrivePath() / "test";

        TFile fileData(filePath, EOpenModeFlag::CreateAlways);
        fileData.Resize(blockSize * blockCount);

        auto fileIOServiceProvider = CreateAndStartAIOServiceProvider();
        Y_DEFER { fileIOServiceProvider->Stop(); };

        auto provider = CreateLocalStorageProvider(
            fileIOServiceProvider,
            CreateNvmeManagerStub(),
            true,  // directIO
            ELocalSubmitQueueOpt::DontUse
        );

        NProto::TVolume volume;
        volume.SetDiskId(filePath);
        volume.SetBlockSize(blockSize);
        volume.SetBlocksCount(blockCount);

        auto future = provider->CreateStorage(
            volume,
            "",
            NProto::VOLUME_ACCESS_READ_WRITE);
        auto storage = future.GetValue();

        auto fillWithPattern = [&] (char p) {
            fileData.Seek(0, sSet);
            TVector<char> buffer(blockSize, p);
            for (ui32 i = 0; i != blockCount; ++i) {
                fileData.Write(buffer.data(), blockSize);
            }

            fileData.Flush();
        };


        // disk filled with 0x0 validates ok
        fillWithPattern(0x0);
        auto response =
            storage->EraseDevice(NProto::DEVICE_ERASE_METHOD_DEALLOCATE);
        UNIT_ASSERT_NO_EXCEPTION(response.Wait(WaitTimeout));
        UNIT_ASSERT_SUCCEEDED(response.GetValue());

        // disk filled with 0xff validates ok
        fillWithPattern(0xff);
        response =
            storage->EraseDevice(NProto::DEVICE_ERASE_METHOD_DEALLOCATE);
        UNIT_ASSERT_NO_EXCEPTION(response.Wait(WaitTimeout));
        UNIT_ASSERT_SUCCEEDED(response.GetValue());

        // disk filled with other pattern fails validation
        fillWithPattern(0x12);
        response =
            storage->EraseDevice(NProto::DEVICE_ERASE_METHOD_DEALLOCATE);
        UNIT_ASSERT_NO_EXCEPTION(response.Wait(WaitTimeout));
        UNIT_ASSERT_EQUAL(E_IO, response.GetValue().GetCode());

    }

    Y_UNIT_TEST(ShouldZeroFillIfDeviceNonSsdInEraseDeviceUsingDeallocate)
    {
        const ui32 blockSize = 4_KB;
        const ui64 blockCount = 32_MB / blockSize;
        const auto filePath = TryGetRamDrivePath() / "test";

        TFile fileData(filePath, EOpenModeFlag::CreateAlways);
        fileData.Resize(blockSize * blockCount);

        auto fileIOServiceProvider = CreateAndStartAIOServiceProvider();
        Y_DEFER { fileIOServiceProvider->Stop(); };

        auto provider = CreateLocalStorageProvider(
            fileIOServiceProvider,
            CreateNvmeManagerStub(false /* not ssd */),
            true,  // directIO
            ELocalSubmitQueueOpt::DontUse
        );

        NProto::TVolume volume;
        volume.SetDiskId(filePath);
        volume.SetBlockSize(blockSize);
        volume.SetBlocksCount(blockCount);

        auto future = provider->CreateStorage(
            volume,
            "",
            NProto::VOLUME_ACCESS_READ_WRITE);
        auto storage = future.GetValue();

        auto fillWithPattern = [&] (char p) {
            fileData.Seek(0, sSet);
            TVector<char> buffer(blockSize, p);
            for (ui32 i = 0; i != blockCount; ++i) {
                fileData.Write(buffer.data(), blockSize);
            }

            fileData.Flush();
        };

        auto validatePattern = [&] (char p) {
            fileData.Seek(0, sSet);
            TVector<char> patternBuffer(blockSize, p);
            TVector<char> readBuffer(blockSize, 0);
            for (ui32 i = 0; i != blockCount; ++i) {
                fileData.Read(readBuffer.data(), blockSize);
            }

            UNIT_ASSERT_EQUAL(0,
                memcmp(patternBuffer.data(), readBuffer.data(), blockSize));
        };


        fillWithPattern(0x12);
        auto response =
            storage->EraseDevice(NProto::DEVICE_ERASE_METHOD_DEALLOCATE);
        UNIT_ASSERT_NO_EXCEPTION(response.Wait(WaitTimeout));
        UNIT_ASSERT_SUCCEEDED(response.GetValue());

        validatePattern(0x0);
    }

    Y_UNIT_TEST(ShouldZeroFillIfNotDirectIoInEraseDeviceUsingDeallocate)
    {
        const ui32 blockSize = 4_KB;
        const ui64 blockCount = 32_MB / blockSize;
        const auto filePath = TryGetRamDrivePath() / "test";

        TFile fileData(filePath, EOpenModeFlag::CreateAlways);
        fileData.Resize(blockSize * blockCount);

        auto fileIOServiceProvider = CreateAndStartAIOServiceProvider();
        Y_DEFER { fileIOServiceProvider->Stop(); };

        auto provider = CreateLocalStorageProvider(
            fileIOServiceProvider,
            CreateNvmeManagerStub(),
            false,  // directIO
            ELocalSubmitQueueOpt::DontUse
        );

        NProto::TVolume volume;
        volume.SetDiskId(filePath);
        volume.SetBlockSize(blockSize);
        volume.SetBlocksCount(blockCount);

        auto future = provider->CreateStorage(
            volume,
            "",
            NProto::VOLUME_ACCESS_READ_WRITE);
        auto storage = future.GetValue();

        auto fillWithPattern = [&] (char p) {
            fileData.Seek(0, sSet);
            TVector<char> buffer(blockSize, p);
            for (ui32 i = 0; i != blockCount; ++i) {
                fileData.Write(buffer.data(), blockSize);
            }

            fileData.Flush();
        };

        auto validatePattern = [&] (char p) {
            fileData.Seek(0, sSet);
            TVector<char> patternBuffer(blockSize, p);
            TVector<char> readBuffer(blockSize, 0);
            for (ui32 i = 0; i != blockCount; ++i) {
                fileData.Read(readBuffer.data(), blockSize);
            }

            UNIT_ASSERT_EQUAL(0,
                memcmp(patternBuffer.data(), readBuffer.data(), blockSize));
        };


        fillWithPattern(0x12);
        auto response =
            storage->EraseDevice(NProto::DEVICE_ERASE_METHOD_DEALLOCATE);
        UNIT_ASSERT_NO_EXCEPTION(response.Wait(WaitTimeout));
        UNIT_ASSERT_SUCCEEDED(response.GetValue());

        validatePattern(0x0);
    }

    Y_UNIT_TEST(ShouldDeallocate1GBChunkInEraseDeviceUsingDeallocate)
    {
        const ui32 blockSize = 4_KB;
        const ui64 blockCount = 3_GB / blockSize + 10;
        const auto filePath = TryGetRamDrivePath() / "test";

        TFile fileData(filePath, EOpenModeFlag::CreateAlways);
        fileData.Resize(blockSize * blockCount);

        auto fileIOServiceProvider = CreateAndStartAIOServiceProvider();
        Y_DEFER { fileIOServiceProvider->Stop(); };

        auto deallocateHistory = std::make_shared<TNvmeDeallocateHistory>();

        auto provider = CreateLocalStorageProvider(
            fileIOServiceProvider,
            CreateNvmeManagerStub(true, deallocateHistory),
            true,   // directIO
            ELocalSubmitQueueOpt::DontUse);

        NProto::TVolume volume;
        volume.SetDiskId(filePath);
        volume.SetBlockSize(blockSize);
        volume.SetBlocksCount(blockCount);

        auto future = provider->CreateStorage(
            volume,
            "",
            NProto::VOLUME_ACCESS_READ_WRITE);
        auto storage = future.GetValue();

        auto fillWithPattern = [&] (char p) {
            fileData.Seek(0, sSet);
            TVector<char> buffer(blockSize, p);
            for (ui32 i = 0; i != blockCount; ++i) {
                fileData.Write(buffer.data(), blockSize);
            }

            fileData.Flush();
        };


        // disk filled with 0x0 validates ok
        fillWithPattern(0x0);
        auto response =
            storage->EraseDevice(NProto::DEVICE_ERASE_METHOD_DEALLOCATE);
        UNIT_ASSERT_NO_EXCEPTION(response.Wait(WaitTimeout));
        UNIT_ASSERT_SUCCEEDED(response.GetValue());

        UNIT_ASSERT_EQUAL(4, deallocateHistory->size());
        UNIT_ASSERT_EQUAL(TDeallocateReq(0, 1_GB), deallocateHistory->at(0));
        UNIT_ASSERT_EQUAL(
            TDeallocateReq(1_GB, 1_GB),
            deallocateHistory->at(1));
        UNIT_ASSERT_EQUAL(
            TDeallocateReq(2_GB, 1_GB),
            deallocateHistory->at(2));
        UNIT_ASSERT_EQUAL(
            TDeallocateReq(3_GB, 10 * 4_KB),
            deallocateHistory->at(3));
    }
}

}   // namespace NCloud::NBlockStore::NServer
