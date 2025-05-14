#include "storage_spdk.h"

#include <cloud/blockstore/libs/common/caching_allocator.h>
#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/server/config.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/service/storage_provider.h>
#include <cloud/blockstore/libs/spdk/iface/env_stub.h>
#include <cloud/blockstore/libs/spdk/iface/env_test.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/sglist_test.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/folder/tempdir.h>
#include <util/generic/scope.h>
#include <util/generic/string.h>
#include <util/system/file.h>
#include <util/thread/pool.h>

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestSpdkEnv final
    : public NSpdk::TTestSpdkEnv
{
    using TOpenDeviceHandler = std::function<
        NThreading::TFuture<NSpdk::ISpdkDevicePtr>(
            const TString& name,
            bool write)>;

    using TRegisterNVMeDevicesHandler = std::function<
        NThreading::TFuture<TVector<TString>>(
        const TString& baseName,
        const TString& transportId)>;

    TOpenDeviceHandler OpenDeviceHandler;
    TRegisterNVMeDevicesHandler RegisterNVMeDevicesHandler;

    TFuture<NSpdk::ISpdkDevicePtr> OpenDevice(
        const TString& name,
        bool write) override
    {
        return OpenDeviceHandler(name, write);
    }

    TFuture<TVector<TString>> RegisterNVMeDevices(
        const TString& baseName,
        const TString& transportId) override
    {
        return RegisterNVMeDevicesHandler(baseName, transportId);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TCompositeDevice
    : ITaskQueue
{
    const ui32 BlockSize;
    const ui32 BlocksPerDevice;
    TVector<std::shared_ptr<NSpdk::TTestSpdkDevice>> Devs;
    TVector<char> Data;

    ITimerPtr Timer;
    ISchedulerPtr Scheduler;

    TCompositeDevice(ui32 blockSize, ui32 deviceCount, ui32 blocksPerDevice)
        : BlockSize(blockSize)
        , BlocksPerDevice(blocksPerDevice)
        , Devs(deviceCount)
        , Data(deviceCount * blocksPerDevice * BlockSize, 'Z')
        , Timer(CreateWallClockTimer())
        , Scheduler(CreateScheduler())
    {
        for (ui32 d = 0; d < deviceCount; ++d) {
            const auto off = d * BlocksPerDevice * BlockSize;

            Devs[d] = std::make_shared<NSpdk::TTestSpdkDevice>();

            Devs[d]->ReadBufferHandler = // fast path
                [=, this] (void* buffer, ui64 fileOffset, ui32 bytes)
            {
                return Execute([=, this] {
                    UNIT_ASSERT(fileOffset < BlocksPerDevice * BlockSize);
                    UNIT_ASSERT(bytes <= BlocksPerDevice * BlockSize - fileOffset);

                    const auto relOff = off + fileOffset;
                    memcpy(buffer, Data.data() + relOff, bytes);

                    return NProto::TError();
                });
            };

            Devs[d]->ReadSgListHandler =
                [=, this] (TSgList sglist, ui64 fileOffset, ui32 bytes)
            {
                return Execute([=, this] {
                    UNIT_ASSERT_VALUES_EQUAL(bytes, SgListGetSize(sglist));
                    UNIT_ASSERT(fileOffset < BlocksPerDevice * BlockSize);
                    UNIT_ASSERT(bytes <= BlocksPerDevice * BlockSize - fileOffset);

                    const auto relOff = off + fileOffset;
                    TSgList src = {
                        { Data.data() + relOff, bytes }
                    };

                    auto size = SgListCopy(src, sglist);
                    UNIT_ASSERT_VALUES_EQUAL(bytes, size);

                    return NProto::TError();
                });
            };

            Devs[d]->WriteBufferHandler = // fast path
                [=, this] (void* buffer, ui64 fileOffset, ui32 bytes)
            {
                return Execute([=, this] {
                    UNIT_ASSERT(fileOffset < BlocksPerDevice * BlockSize);
                    UNIT_ASSERT(bytes <= BlocksPerDevice * BlockSize - fileOffset);

                    const auto relOff = off + fileOffset;
                    memcpy(Data.data() + relOff, buffer, bytes);

                    return NProto::TError();
                });
            };

            Devs[d]->WriteSgListHandler =
                [=, this] (TSgList sglist, ui64 fileOffset, ui32 bytes)
            {
                return Execute([=, this] {
                    UNIT_ASSERT_VALUES_EQUAL(bytes, SgListGetSize(sglist));
                    UNIT_ASSERT(fileOffset < BlocksPerDevice * BlockSize);
                    UNIT_ASSERT(bytes <= BlocksPerDevice * BlockSize - fileOffset);

                    const auto relOff = off + fileOffset;
                    TSgList dst = {
                        { Data.data() + relOff, bytes }
                    };

                    auto size = SgListCopy(sglist, dst);
                    UNIT_ASSERT_VALUES_EQUAL(bytes, size);

                    return NProto::TError();
                });
            };

            Devs[d]->WriteZeroesHandler = [=, this] (ui64 fileOffset, ui32 bytes)
            {
                return Execute([=, this] {
                    UNIT_ASSERT(fileOffset < BlocksPerDevice * BlockSize);
                    UNIT_ASSERT(bytes <= BlocksPerDevice * BlockSize - fileOffset);

                    const auto relOff = off + fileOffset;
                    for (ui32 i = 0; i < bytes; ++i) {
                        Data[relOff + i] = 'Z';
                    }

                    return NProto::TError();
                });
            };
        }
    }

    void Start() override
    {
        Scheduler->Start();
    }

    void Stop() override
    {
        Scheduler->Stop();
    }

    std::shared_ptr<NSpdk::ISpdkDevice> ByName(const TString& name) const
    {
        TStringBuf sb;
        UNIT_ASSERT(TStringBuf(name).AfterPrefix("nvme", sb));
        ui32 idx;
        UNIT_ASSERT(TryFromString(sb, idx));
        UNIT_ASSERT(idx < Devs.size());
        return Devs[idx];
    }

    void Enqueue(ITaskPtr task) override
    {
        std::shared_ptr<ITask> p(task.release());

        Scheduler->Schedule(Timer->Now(), [=] () mutable {
            p->Execute();
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

IStoragePtr CreateSpdkStorage(
    const TCompositeDevice& cd,
    ui32 blockSize,
    IMonitoringServicePtr monitoring,
    NProto::EVolumeIOMode ioMode = NProto::VOLUME_IO_OK)
{
    auto spdk = std::make_shared<TTestSpdkEnv>();
    spdk->OpenDeviceHandler = [&] (const TString& name, bool write) {
        UNIT_ASSERT(write);

        return MakeFuture(cd.ByName(name));
    };

    spdk->RegisterNVMeDevicesHandler = [=] (
        const TString& baseName,
        const TString& transportId)
    {
        UNIT_ASSERT_VALUES_EQUAL(baseName, "nvme");

        return MakeFuture(TVector<TString>{ baseName + transportId });
    };

    auto allocator = CreateCachingAllocator(
        TDefaultAllocator::Instance(), 0, 0, 0);

    auto serverGroup = monitoring->GetCounters()
        ->GetSubgroup("counters", "blockstore")
        ->GetSubgroup("component", "server");

    auto serverStats = CreateServerStats(
        std::make_shared<TServerAppConfig>(),
        std::make_shared<TDiagnosticsConfig>(),
        monitoring,
        CreateProfileLogStub(),
        CreateServerRequestStats(
            serverGroup,
            CreateWallClockTimer(),
            EHistogramCounterOption::ReportMultipleCounters),
        CreateVolumeStatsStub());

    auto storageProvider = CreateSpdkStorageProvider(
        std::move(spdk),
        std::move(allocator),
        std::move(serverStats));

    NProto::TVolume volume;
    volume.SetBlockSize(blockSize);
    volume.SetIOMode(ioMode);
    volume.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
    auto& devs = *volume.MutableDevices();
    for (ui32 i = 0; i < cd.Devs.size(); ++i) {
        auto& dev = *devs.Add();
        dev.SetBaseName("nvme");
        dev.SetTransportId(ToString(i));
        dev.SetBlockCount(cd.BlocksPerDevice);
    }

    auto storage = storageProvider->CreateStorage(
        volume,
        "",
        NProto::VOLUME_ACCESS_READ_WRITE);
    return storage.GetValue(TDuration::Seconds(30));
}

TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
    IStorage& storage,
    ui64 startIndex,
    ui32 blocksCount,
    ui32 blockSize,
    TGuardedSgList sglist)
{
    auto context = MakeIntrusive<TCallContext>();

    auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
    request->SetStartIndex(startIndex);
    request->SetBlocksCount(blocksCount);
    request->BlockSize = blockSize;
    request->Sglist = std::move(sglist);

    return storage.ReadBlocksLocal(
        std::move(context),
        std::move(request));
}

TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
    IStorage& storage,
    ui64 startIndex,
    ui32 blocksCount,
    ui32 blockSize,
    TGuardedSgList sglist)
{
    auto context = MakeIntrusive<TCallContext>();

    auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
    request->SetStartIndex(startIndex);
    request->BlocksCount = blocksCount;
    request->BlockSize = blockSize;
    request->Sglist = std::move(sglist);

    return storage.WriteBlocksLocal(
        std::move(context),
        std::move(request));
}

TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
    IStorage& storage,
    ui64 startIndex,
    ui32 blocksCount)
{
    auto context = MakeIntrusive<TCallContext>();

    auto request = std::make_shared<NProto::TZeroBlocksRequest>();
    request->SetStartIndex(startIndex);
    request->SetBlocksCount(blocksCount);

    return storage.ZeroBlocks(
        std::move(context),
        std::move(request));
}

auto GetCounter(IMonitoringServicePtr monitoring, const TString& request)
{
    return monitoring->GetCounters()
        ->GetSubgroup("counters", "blockstore")
        ->GetSubgroup("component", "server")
        ->GetSubgroup("request", request)
        ->GetCounter("FastPathHits");
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSpdkStorageTest)
{
    Y_UNIT_TEST(ShouldReadWriteZeroLocal)
    {
        TCompositeDevice device(5, 3, 10);
        device.Start();

        auto monitoring = CreateMonitoringServiceStub();
        auto storage = CreateSpdkStorage(device, 5, monitoring);

        Y_DEFER { device.Stop(); };

        {
            TString blockContent = "00000";
            TVector<TString> blocks;
            auto sglist = ResizeBlocks(
                blocks,
                30,
                blockContent
            );

            auto future = ReadBlocksLocal(
                *storage,
                0,
                30,
                blockContent.size(),
                TGuardedSgList(sglist)
            );

            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(error));

            for (const auto& buffer: sglist) {
                UNIT_ASSERT_VALUES_EQUAL("ZZZZZ", buffer.AsStringBuf());
            }
        }

        {
            TString blockContent = "XXXXX";
            TVector<TString> blocks;
            auto sglist = ResizeBlocks(
                blocks,
                20,
                blockContent
            );

            auto future = WriteBlocksLocal(
                *storage,
                5,
                20,
                blockContent.size(),
                TGuardedSgList(sglist)
            );

            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(error));
        }

        {
            TString blockContent = "00000";
            TVector<TString> blocks;
            auto sglist = ResizeBlocks(
                blocks,
                22,
                blockContent
            );

            auto future = ReadBlocksLocal(
                *storage,
                4,
                22,
                blockContent.size(),
                TGuardedSgList(sglist));

            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(error));

            for (ui32 i = 1; i < sglist.size() - 1; ++i) {
                UNIT_ASSERT_VALUES_EQUAL("XXXXX", sglist[i].AsStringBuf());
            }

            UNIT_ASSERT_VALUES_EQUAL("ZZZZZ", sglist[0].AsStringBuf());
            UNIT_ASSERT_VALUES_EQUAL("ZZZZZ", sglist[21].AsStringBuf());
        }

        auto future = ZeroBlocks(
            *storage,
            6,
            18
        );

        const auto& error = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(!HasError(error));

        {
            TString blockContent = "00000";
            TVector<TString> blocks;
            auto sglist = ResizeBlocks(
                blocks,
                20,
                blockContent
            );

            auto future = ReadBlocksLocal(
                *storage,
                5,
                20,
                blockContent.size(),
                TGuardedSgList(sglist)
            );

            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(error));

            for (ui32 i = 1; i < sglist.size() - 1; ++i) {
                UNIT_ASSERT_VALUES_EQUAL("ZZZZZ", sglist[i].AsStringBuf());
            }

            UNIT_ASSERT_VALUES_EQUAL("XXXXX", sglist[0].AsStringBuf());
            UNIT_ASSERT_VALUES_EQUAL("XXXXX", sglist[19].AsStringBuf());
        }

        auto readRequestFastPathHits = GetCounter(monitoring, "ReadBlocks");
        auto writeRequestFastPathHits = GetCounter(monitoring, "WriteBlocks");
        auto zeroRequestFastPathHits = GetCounter(monitoring, "ZeroBlocks");

        UNIT_ASSERT_VALUES_EQUAL(0, readRequestFastPathHits->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, writeRequestFastPathHits->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, zeroRequestFastPathHits->Val());
    }

    Y_UNIT_TEST(ShouldReadWriteZeroLocalFastPath)
    {
        TCompositeDevice device(5, 3, 10);
        device.Start();

        auto monitoring = CreateMonitoringServiceStub();
        auto storage = CreateSpdkStorage(device, 5, monitoring);

        Y_DEFER { device.Stop(); };

        {
            TString blockContent = "XXXXX";
            TVector<TString> blocks;
            auto sglist = ResizeBlocks(
                blocks,
                10,
                blockContent
            );

            auto future = WriteBlocksLocal(
                *storage,
                10,
                10,
                blockContent.size(),
                TGuardedSgList(sglist)
            );

            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(error));
        }

        auto readRequestFastPathHits = GetCounter(monitoring, "ReadBlocks");
        auto writeRequestFastPathHits = GetCounter(monitoring, "WriteBlocks");
        auto zeroRequestFastPathHits = GetCounter(monitoring, "ZeroBlocks");

        UNIT_ASSERT_VALUES_EQUAL(0, readRequestFastPathHits->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, writeRequestFastPathHits->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, zeroRequestFastPathHits->Val());

        {
            TString blockContent = "00000";
            TVector<TString> blocks;
            auto sglist = ResizeBlocks(
                blocks,
                10,
                blockContent
            );

            auto future = ReadBlocksLocal(
                *storage,
                10,
                10,
                blockContent.size(),
                TGuardedSgList(sglist));

            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(error));

            for (const auto buffer: sglist) {
                UNIT_ASSERT_VALUES_EQUAL("XXXXX", buffer.AsStringBuf());
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(1, readRequestFastPathHits->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, writeRequestFastPathHits->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, zeroRequestFastPathHits->Val());

        auto future = ZeroBlocks(
            *storage,
            10,
            10
        );

        UNIT_ASSERT_VALUES_EQUAL(1, readRequestFastPathHits->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, writeRequestFastPathHits->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, zeroRequestFastPathHits->Val());

        const auto& error = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(!HasError(error));

        {
            TString blockContent = "00000";
            TVector<TString> blocks;
            auto sglist = ResizeBlocks(
                blocks,
                10,
                blockContent
            );

            auto future = ReadBlocksLocal(
                *storage,
                10,
                10,
                blockContent.size(),
                TGuardedSgList(sglist)
            );

            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(error));

            for (const auto& buffer: sglist) {
                UNIT_ASSERT_VALUES_EQUAL("ZZZZZ", buffer.AsStringBuf());
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(2, readRequestFastPathHits->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, writeRequestFastPathHits->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, zeroRequestFastPathHits->Val());
    }

    Y_UNIT_TEST(ShouldReadWriteSingleDevice)
    {
        const ui32 blockSize = 512;
        const ui32 blocksCount = 100;

        TCompositeDevice device(blockSize, 1, blocksCount);
        device.Start();

        auto monitoring = CreateMonitoringServiceStub();

        auto storage = CreateSpdkStorage(device, blockSize, monitoring);

        Y_DEFER { device.Stop(); };

        {
            TVector<TString> blocks;
            auto sglist = ResizeBlocks(blocks, 8, TString(blockSize, 'A'));

            auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
            request->SetStartIndex(8);
            request->BlocksCount = sglist.size();
            request->BlockSize = blockSize;
            request->Sglist = TGuardedSgList(std::move(sglist));

            auto future = storage->WriteBlocksLocal(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }

        {
            TVector<TString> blocks;
            auto sglist = ResizeBlocks(blocks, 8, TString(blockSize, '0'));

            auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
            request->SetStartIndex(8);
            request->SetBlocksCount(sglist.size());
            request->BlockSize = blockSize;
            request->Sglist = TGuardedSgList(sglist);

            auto future = storage->ReadBlocksLocal(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));

            const TString expected(blockSize, 'A');

            for (auto& s: sglist) {
                UNIT_ASSERT_VALUES_EQUAL(expected, s.AsStringBuf());
            }
        }
    }

    Y_UNIT_TEST(ShouldNonLocalReadWriteSingleDevice)
    {
        const ui32 blockSize = 512;
        const ui32 blocksCount = 100;

        TCompositeDevice device(blockSize, 1, blocksCount);
        device.Start();

        auto storage = CreateSpdkStorage(
            device,
            blockSize,
            CreateMonitoringServiceStub());

        TStorageAdapter storageAdapter(
            std::move(storage),
            blockSize,
            true,                // normalize,
            TDuration::Zero(),   // maxRequestDuration
            TDuration::Zero()    // shutdownTimeout
        );

        Y_DEFER { device.Stop(); };

        {
            auto request = std::make_shared<NProto::TWriteBlocksRequest>();
            request->SetStartIndex(8);
            auto sglist = ResizeIOVector(*request->MutableBlocks(), 8, blockSize);
            for (auto buf: sglist) {
                memset(const_cast<char*>(buf.Data()), 'A', blockSize);
            }

            auto future = storageAdapter.WriteBlocks(
                Now(),
                MakeIntrusive<TCallContext>(),
                std::move(request),
                blockSize,
                {}   // no data buffer
            );

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }

        {
            auto request = std::make_shared<NProto::TReadBlocksRequest>();
            request->SetStartIndex(8);
            request->SetBlocksCount(8);

            auto future = storageAdapter.ReadBlocks(
                Now(),
                MakeIntrusive<TCallContext>(),
                std::move(request),
                blockSize,
                {}   // no data buffer
            );

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));

            auto sgListOrError = GetSgList(response, blockSize);
            UNIT_ASSERT(!HasError(sgListOrError));
            auto sglist = sgListOrError.ExtractResult();

            UNIT_ASSERT(!sglist.empty());

            const TString expected(blockSize, 'A');

            for (auto buf: sglist) {
                UNIT_ASSERT_VALUES_EQUAL(expected, buf.AsStringBuf());
            }
        }
    }

    Y_UNIT_TEST(ShouldSupportReadOnlyMode)
    {
        TCompositeDevice device(5, 3, 10);
        device.Start();

        auto monitoring = CreateMonitoringServiceStub();
        auto storage = CreateSpdkStorage(
            device,
            5,
            monitoring,
            NProto::VOLUME_IO_ERROR_READ_ONLY);

        Y_DEFER { device.Stop(); };

        {
            TString blockContent = "00000";
            TVector<TString> blocks;
            auto sglist = ResizeBlocks(blocks, 30, blockContent);

            auto future = ReadBlocksLocal(
                *storage,
                0,
                30,
                blockContent.size(),
                TGuardedSgList(sglist)
            );

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response.GetError().GetCode());

            for (const auto& buffer: sglist) {
                UNIT_ASSERT_VALUES_EQUAL("ZZZZZ", buffer.AsStringBuf());
            }
        }

        {
            TString blockContent = "XXXXX";
            TVector<TString> blocks;
            auto sglist = ResizeBlocks(blocks, 20, blockContent);

            auto future = WriteBlocksLocal(
                *storage,
                5,
                20,
                blockContent.size(),
                TGuardedSgList(sglist)
            );

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_VALUES_EQUAL(E_IO, response.GetError().GetCode());
        }

        {
            auto future = ZeroBlocks(*storage, 6, 18 );
            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_VALUES_EQUAL(E_IO, response.GetError().GetCode());
        }

        {
            TString blockContent = "00000";
            TVector<TString> blocks;
            auto sglist = ResizeBlocks(blocks, 30, blockContent);

            auto future = ReadBlocksLocal(
                *storage,
                0,
                30,
                blockContent.size(),
                TGuardedSgList(sglist)
            );

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response.GetError().GetCode());

            for (const auto& buffer: sglist) {
                UNIT_ASSERT_VALUES_EQUAL("ZZZZZ", buffer.AsStringBuf());
            }
        }
    }
}

}   // namespace NCloud::NBlockStore::NServer
