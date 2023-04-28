#include "device.h"

#include "config.h"

#include "address.h"
#include "alloc.h"
#include "env.h"
#include "env_test.h"

#include <cloud/blockstore/libs/common/sglist_test.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/folder/tempdir.h>
#include <util/generic/flags.h>
#include <util/generic/string.h>
#include <util/system/file.h>

#include <functional>

namespace NCloud::NBlockStore::NSpdk {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

static constexpr size_t DefaultBlocksCount = 1024;

static constexpr TDuration WaitTimeout = TDuration::Seconds(5);

////////////////////////////////////////////////////////////////////////////////

#define UNIT_ASSERT_SUCCEEDED(e) \
    UNIT_ASSERT_C(SUCCEEDED(e.GetCode()), e.GetMessage())

////////////////////////////////////////////////////////////////////////////////

enum class EReadWriteOp
{
    Read    = (1 << 0),
    Write   = (1 << 1),
};

Y_DECLARE_FLAGS(EReadWriteOps, EReadWriteOp);
Y_DECLARE_OPERATORS_FOR_FLAGS(EReadWriteOps);

////////////////////////////////////////////////////////////////////////////////

ISpdkEnvPtr InitEnv(const NProto::TSpdkEnvConfig& config = {})
{
    auto logging = CreateLoggingService("console");
    InitLogging(logging->CreateLog("SPDK"));

    return CreateEnv(std::make_shared<TSpdkEnvConfig>(config));
}

void TestReadWrite(
    ISpdkEnvPtr env,
    const TString& name,
    EReadWriteOps ops = EReadWriteOp::Read | EReadWriteOp::Write)
{
    auto device = env->OpenDevice(name, ops & EReadWriteOp::Write)
        .GetValue(WaitTimeout);

    if (ops & EReadWriteOp::Write) {
        auto buffer = AllocateUninitialized(DefaultBlockSize);
        memset(buffer.get(), 'A', DefaultBlockSize);

        auto error = device->Write(buffer.get(), 0, DefaultBlockSize)
            .GetValue(WaitTimeout);
        UNIT_ASSERT_SUCCEEDED(error);
    }

    if (ops & EReadWriteOp::Read) {
        auto buffer = AllocateZero(DefaultBlockSize);

        auto error = device->Read(buffer.get(), 0, DefaultBlockSize)
            .GetValue(WaitTimeout);
        UNIT_ASSERT_SUCCEEDED(error);

        char* ptr = buffer.get();
        for (size_t i = 0; i < DefaultBlockSize; ++i) {
            UNIT_ASSERT(ptr[i] == 'A');
        }
    }

    ui64 blocksCount = 5;

    if (ops & EReadWriteOp::Write) {
        TVector<TString> blocks;
        auto sglist = ResizeBlocks(
            blocks,
            blocksCount,
            TString(DefaultBlockSize, 'A'));
        auto size = SgListGetSize(sglist);

        auto error = device->Write(sglist, 0, size)
            .GetValue(WaitTimeout);
        UNIT_ASSERT_SUCCEEDED(error);
    }

    if (ops & EReadWriteOp::Read) {
        TVector<TString> blocks;
        auto sglist = ResizeBlocks(
            blocks,
            blocksCount,
            TString(DefaultBlockSize, 0));
        auto size = SgListGetSize(sglist);

        auto error = device->Read(sglist, 0, size)
            .GetValue(WaitTimeout);
        UNIT_ASSERT_SUCCEEDED(error);

        for (const auto& buffer: sglist) {
            const char* ptr = buffer.Data();
            for (size_t i = 0; i < buffer.Size(); ++i) {
                UNIT_ASSERT(ptr[i] == 'A');
            }
        }
    }

    if (ops & EReadWriteOp::Write) {
        auto error = device->WriteZeroes(0, DefaultBlockSize)
            .GetValue(WaitTimeout);
        UNIT_ASSERT_SUCCEEDED(error);
    }

    device->Stop();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSpdkDeviceTest)
{
    Y_UNIT_TEST(ShouldHandleReadWriteRequests_Malloc)
    {
        auto env = InitEnv();

        env->Start();

        auto name = env->RegisterMemoryDevice("test", DefaultBlocksCount, DefaultBlockSize)
            .GetValue(WaitTimeout);

        TestReadWrite(env, name);

        env->UnregisterDevice(name).GetValue(WaitTimeout);

        env->Stop();
    }

    Y_UNIT_TEST(ShouldHandleReadWriteRequests_File)
    {
        auto env = InitEnv();

        env->Start();

        TTempDir workingDir;
        auto filePath = workingDir.Path() / "test";

        TFile fileData(filePath, EOpenModeFlag::CreateNew);
        fileData.Resize(DefaultBlockSize * DefaultBlocksCount);

        auto name = env->RegisterFileDevice(filePath, filePath, DefaultBlockSize)
            .GetValue(WaitTimeout);

        TestReadWrite(env, name);

        env->UnregisterDevice(name).GetValue(WaitTimeout);

        env->Stop();
    }
/*
    Y_UNIT_TEST(ShouldRegisterNVMeDevices)
    {
        auto env = InitEnv();

        env->Start();

        auto transportId = CreatePCIeDeviceTransportId();

        auto devices = env->RegisterNVMeDevices("nvme", transportId)
            .GetValue(WaitTimeout);

        for (const auto& name: devices) {
            TestReadWrite(env, name, EReadWriteOp::Read);
        }

        env->Stop();
    }
*/
    Y_UNIT_TEST(ShouldQueryDeviceStats)
    {
        auto env = InitEnv();

        env->Start();

        auto name = env->RegisterMemoryDevice("test", DefaultBlocksCount, DefaultBlockSize)
            .GetValue(WaitTimeout);

        auto stats = env->QueryDeviceStats(name)
            .GetValue(WaitTimeout);

        UNIT_ASSERT_EQUAL(stats.BlockSize, DefaultBlockSize);
        UNIT_ASSERT_EQUAL(stats.BlocksCount, DefaultBlocksCount);

        env->UnregisterDevice(name).GetValue(WaitTimeout);

        env->Stop();
    }

    Y_UNIT_TEST(ShouldUseMultipleCores)
    {
        NProto::TSpdkEnvConfig config;
        config.SetCpuMask("[1, 2]");

        auto env = InitEnv(config);

        env->Start();

        auto name = env->RegisterMemoryDevice("test", DefaultBlocksCount, DefaultBlockSize)
            .GetValue(WaitTimeout);

        TestReadWrite(env, name);

        env->UnregisterDevice(name).GetValue(WaitTimeout);

        env->Stop();
    }

    Y_UNIT_TEST(ShouldWorkThroughProxy)
    {
        auto env = InitEnv();

        env->Start();

        auto name1 = env->RegisterMemoryDevice("test1", DefaultBlocksCount, DefaultBlockSize)
            .GetValue(WaitTimeout);

        auto name2 = env->RegisterDeviceProxy("test1", "test2")
            .GetValue(WaitTimeout);

        auto stats = env->QueryDeviceStats(name2)
            .GetValue(WaitTimeout);

        UNIT_ASSERT_EQUAL(stats.BlockSize, DefaultBlockSize);
        UNIT_ASSERT_EQUAL(stats.BlocksCount, DefaultBlocksCount);

        TestReadWrite(env, name2);

        env->UnregisterDevice(name2).GetValue(WaitTimeout);
        env->UnregisterDevice(name1).GetValue(WaitTimeout);

        env->Stop();
    }

    Y_UNIT_TEST(ShouldWrapDevice)
    {
        auto env = InitEnv();

        env->Start();

        auto name1 = env->RegisterMemoryDevice("test1", DefaultBlocksCount, DefaultBlockSize)
            .GetValue(WaitTimeout);

        auto device1 = env->OpenDevice(name1, true)
            .GetValue(WaitTimeout);

        auto device2 = std::make_shared<TTestSpdkDevice>();
        device2->ReadBufferHandler =
            [=] (void* buf, ui64 fileOffset, ui32 bytesCount) {
                return device1->Read(buf, fileOffset, bytesCount);
            };
        device2->ReadSgListHandler =
            [=] (TSgList sglist, ui64 fileOffset, ui32 bytesCount) {
                return device1->Read(std::move(sglist), fileOffset, bytesCount);
            };
        device2->WriteBufferHandler =
            [=] (void* buf, ui64 fileOffset, ui32 bytesCount) {
                return device1->Write(buf, fileOffset, bytesCount);
            };
        device2->WriteSgListHandler =
            [=] (TSgList sglist, ui64 fileOffset, ui32 bytesCount) {
                return device1->Write(std::move(sglist), fileOffset, bytesCount);
            };
        device2->WriteZeroesHandler =
            [=] (ui64 fileOffset, ui32 bytesCount) {
                return device1->WriteZeroes(fileOffset, bytesCount);
            };

        TAtomic deviceIsStopped = 0;
        device2->StopHandler =
            [&] () {
                AtomicSet(deviceIsStopped, 1);
                return MakeFuture();
            };

        auto name2 = env->RegisterDeviceWrapper(device2, "test2", DefaultBlocksCount, DefaultBlockSize)
            .GetValue(WaitTimeout);

        auto stats = env->QueryDeviceStats(name2)
            .GetValue(WaitTimeout);

        UNIT_ASSERT_EQUAL(stats.BlockSize, DefaultBlockSize);
        UNIT_ASSERT_EQUAL(stats.BlocksCount, DefaultBlocksCount);

        TestReadWrite(env, name2);

        device1->Stop();

        env->UnregisterDevice(name2).GetValue(WaitTimeout);
        env->UnregisterDevice(name1).GetValue(WaitTimeout);

        env->Stop();
        UNIT_ASSERT(AtomicGet(deviceIsStopped));
    }

    Y_UNIT_TEST(ShouldGetIoStats)
    {
        auto env = InitEnv();

        env->Start();

        auto name = env->RegisterMemoryDevice("test", DefaultBlocksCount, DefaultBlockSize)
            .GetValue(WaitTimeout);

        auto device = env->OpenDevice(name, true)
            .GetValue(WaitTimeout);

        auto buffer = AllocateUninitialized(DefaultBlockSize);

        device->Write(buffer.get(), 0, DefaultBlockSize)
            .GetValue(WaitTimeout);

        device->Read(buffer.get(), 0, DefaultBlockSize)
            .GetValue(WaitTimeout);

        device->WriteZeroes(0, DefaultBlockSize)
            .GetValue(WaitTimeout);

        auto stats = env->GetDeviceIoStats(name).GetValue(WaitTimeout);

        UNIT_ASSERT_EQUAL(stats.BytesRead, DefaultBlockSize);
        UNIT_ASSERT_EQUAL(stats.NumReadOps, 1);

        UNIT_ASSERT_EQUAL(stats.BytesWritten, DefaultBlockSize);
        UNIT_ASSERT_EQUAL(stats.NumWriteOps, 1); // write zeroes are not counted

        device->Stop();

        env->UnregisterDevice(name).GetValue(WaitTimeout);
        env->Stop();
    }

    Y_UNIT_TEST(ShouldSetLimits)
    {
        auto env = InitEnv();

        env->Start();

        auto name = env->RegisterMemoryDevice("test", DefaultBlocksCount, DefaultBlockSize)
            .GetValue(WaitTimeout);

        const TDeviceRateLimits limits {
            .IopsLimit = 100000,
            .BandwidthLimit = 100,
            .ReadBandwidthLimit = 50,
            .WriteBandwidthLimit = 50
        };

        env->SetRateLimits(name, limits).GetValue(WaitTimeout);

        auto actualLimits = env->GetRateLimits(name).GetValue(WaitTimeout);

        UNIT_ASSERT_EQUAL(actualLimits.IopsLimit, limits.IopsLimit);
        UNIT_ASSERT_EQUAL(actualLimits.BandwidthLimit, limits.BandwidthLimit);
        UNIT_ASSERT_EQUAL(actualLimits.ReadBandwidthLimit, limits.ReadBandwidthLimit);
        UNIT_ASSERT_EQUAL(actualLimits.WriteBandwidthLimit, limits.WriteBandwidthLimit);

        env->UnregisterDevice(name).GetValue(WaitTimeout);
        env->Stop();
    }

    Y_UNIT_TEST(ShouldEnableHistogram)
    {
        auto env = InitEnv();

        env->Start();

        auto name = env->RegisterMemoryDevice("test", DefaultBlocksCount, DefaultBlockSize)
            .GetValue(WaitTimeout);

        env->EnableHistogram(name, true).GetValue(WaitTimeout);
        env->EnableHistogram(name, false).GetValue(WaitTimeout);

        env->UnregisterDevice(name).GetValue(WaitTimeout);
        env->Stop();
    }

    Y_UNIT_TEST(ShouldGetHistogramBuckets)
    {
        auto env = InitEnv();

        env->Start();

        auto name = env->RegisterMemoryDevice("test", DefaultBlocksCount, DefaultBlockSize)
            .GetValue(WaitTimeout);

        env->EnableHistogram(name, true).GetValue(WaitTimeout);

        auto device = env->OpenDevice(name, true)
            .GetValue(WaitTimeout);

        {
            const auto buckets = env->GetHistogramBuckets(name)
                .GetValue(WaitTimeout);

            UNIT_ASSERT_EQUAL(buckets.size(), 0);
        }

        auto buffer = AllocateUninitialized(DefaultBlockSize);
        device->Write(buffer.get(), 0, DefaultBlockSize).GetValue(WaitTimeout);

        {
            const auto buckets = env->GetHistogramBuckets(name)
                .GetValue(WaitTimeout);

            UNIT_ASSERT_EQUAL(buckets.size(), 1);

            auto [mcs, count] = buckets.front();
            UNIT_ASSERT_EQUAL(count, 1);
            UNIT_ASSERT_UNEQUAL(mcs, 0);
        }

        device->Stop();

        env->UnregisterDevice(name).GetValue(WaitTimeout);
        env->Stop();
    }
}

}   // namespace NCloud::NBlockStore::NSpdk
