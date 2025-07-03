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

////////////////////////////////////////////////////////////////////////////////

#define UNIT_ASSERT_SUCCEEDED(e) \
    UNIT_ASSERT_C(SUCCEEDED(e.GetCode()), e.GetMessage())

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLocalStorageTest)
{
    Y_UNIT_TEST(ShouldZeroWholeHugeFile)
    {
        const ui32 blockSize = 4_KB;
        const ui64 totalBlockCount = 5_GB / blockSize;
        const auto filePath = TFsPath(GetSystemTempDir()) / "test";

        TFile fileData(filePath, EOpenModeFlag::CreateAlways);
        fileData.Resize(blockSize * totalBlockCount);

        auto fileIOServiceProvider =
            CreateSingleFileIOServiceProvider(CreateAIOService());

        fileIOServiceProvider->Start();
        Y_DEFER { fileIOServiceProvider->Stop(); };

        auto provider = CreateLocalStorageProvider(
            fileIOServiceProvider,
            CreateNvmeManagerStub(),
            true,   // directIO
            ELocalSubmitQueueOpt::DontUse
        );

        NProto::TVolume volume;
        volume.SetDiskId(filePath);
        volume.SetBlockSize(blockSize);
        volume.SetBlocksCount(totalBlockCount);

        auto future = provider->CreateStorage(
            volume,
            "",
            NProto::VOLUME_ACCESS_READ_WRITE);
        auto storage = future.GetValue();

        const ui32 chunkSize = 8_MB;

        auto invokeIO = [&] (auto op) {
            for (ui64 i = 0; i < totalBlockCount; i += 1_GB / blockSize) {
                op(i, Min<ui64>(totalBlockCount - i, chunkSize / blockSize));
            }
        };

        auto writeBuffer = storage->AllocateBuffer(chunkSize);
        memset(writeBuffer.get(), 'a', chunkSize);
        auto writeSgList = TGuardedSgList({{writeBuffer.get(), chunkSize}});

        auto write = [&] (ui64 startIndex, ui64 blockCount) {
            auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
            request->SetStartIndex(startIndex);
            request->BlocksCount = blockCount;
            request->BlockSize = blockSize;
            request->Sglist = writeSgList;

            auto response = storage->WriteBlocksLocal(
                MakeIntrusive<TCallContext>(),
                std::move(request)).GetValueSync();

            UNIT_ASSERT_SUCCEEDED(response.GetError());
        };

        auto readBuffer = storage->AllocateBuffer(chunkSize);
        auto readSgList = TGuardedSgList({{readBuffer.get(), chunkSize}});

        auto read = [&] (ui64 startIndex, ui64 blockCount) {
            UNIT_ASSERT_EQUAL(blockCount, chunkSize / blockSize);

            auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
            request->SetStartIndex(startIndex);
            request->SetBlocksCount(blockCount);
            request->BlockSize = blockSize;
            request->Sglist = readSgList;

            auto response = storage->ReadBlocksLocal(
                MakeIntrusive<TCallContext>(),
                std::move(request)).GetValueSync();
            UNIT_ASSERT_SUCCEEDED(response.GetError());

            return TStringBuf(readBuffer.get(), chunkSize);
        };

        // write data

        invokeIO(write);

        // verify data

        invokeIO([&] (ui64 startIndex, ui64 blockCount) {
            auto buf = read(startIndex, blockCount);

            UNIT_ASSERT_VALUES_EQUAL(
                chunkSize,
                std::count(buf.begin(), buf.end(), 'a'));
        });

        // erase

        {
            auto request = std::make_shared<NProto::TZeroBlocksRequest>();
            request->SetStartIndex(0);
            request->SetBlocksCount(totalBlockCount);

            auto response = storage->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request)).GetValueSync();

            UNIT_ASSERT_SUCCEEDED(response.GetError());
        }

        // verify whole file

        for (ui64 i = 0; i < totalBlockCount; i += chunkSize / blockSize) {
            auto buf = read(i, chunkSize / blockSize);
            UNIT_ASSERT_VALUES_EQUAL(
                chunkSize,
                std::count(buf.begin(), buf.end(), '\0'));
        }
    }
}

}   // namespace NCloud::NBlockStore::NServer
