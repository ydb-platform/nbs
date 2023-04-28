#include "target.h"

#include "address.h"
#include "alloc.h"
#include "config.h"
#include "device.h"
#include "env.h"

#include <cloud/blockstore/libs/common/sglist_test.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

#include <sys/wait.h>
#include <unistd.h>

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

////////////////////////////////////////////////////////////////////////////////

void TestNVMeDevices(ui16 port, const TString& nqn, size_t expectedDeviceCount)
{
    auto logging = CreateLoggingService("console");
    InitLogging(logging->CreateLog("SPDK"));

    auto env = CreateEnv(std::make_shared<TSpdkEnvConfig>());

    env->Start();

    auto transportId = CreateNVMeDeviceTransportId(
        "TCP", "IPv4", "127.0.0.1",
        port,
        nqn);

    auto remoteDevices = env->RegisterNVMeDevices("nvme", transportId)
        .GetValue(WaitTimeout);
    UNIT_ASSERT_VALUES_EQUAL(expectedDeviceCount, remoteDevices.size());

    for (const auto& name: remoteDevices) {
        TestReadWrite(env, name);
    }

    for (const auto& name: remoteDevices) {
        env->UnregisterDevice(name).GetValue(WaitTimeout);
    }

    env->Stop();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSpdkTargetTest_NVMe)
{
    Y_UNIT_TEST(ShoudHandleRemoteRequests)
    {
        TGUID uuid1;
        CreateGuid(&uuid1);
        auto nqn1 = CreateNqnFromUuid(uuid1);

        TGUID uuid2;
        CreateGuid(&uuid2);
        auto nqn2 = CreateNqnFromUuid(uuid2);

        int pipefd[2];
        auto res = pipe(pipefd);
        UNIT_ASSERT(res != -1);

        auto pid = fork();
        UNIT_ASSERT(pid != -1);

        if (pid != 0) {
            close(pipefd[0]);   // Close unused read end

            auto logging = CreateLoggingService("console");
            InitLogging(logging->CreateLog("SPDK"));

            auto env = CreateEnv(std::make_shared<TSpdkEnvConfig>());

            env->Start();

            auto device1 = env->RegisterMemoryDevice("test1", DefaultBlocksCount, DefaultBlockSize)
                .GetValue(WaitTimeout);

            auto device2 = env->RegisterMemoryDevice("test2", DefaultBlocksCount, DefaultBlockSize)
                .GetValue(WaitTimeout);

            auto device3 = env->RegisterMemoryDevice("test3", DefaultBlocksCount, DefaultBlockSize)
                .GetValue(WaitTimeout);

            TPortManager portManager;
            auto port = portManager.GetPort(1234);

            auto transportId = CreateNVMeDeviceTransportId(
                "TCP", "IPv4", "127.0.0.1",
                port);

            env->AddTransport(transportId).GetValue(WaitTimeout);
            env->StartListen(transportId).GetValue(WaitTimeout);

            TVector<TString> transportIds = { transportId };

            auto target1 = env->CreateNVMeTarget(nqn1, { device1, device2 }, transportIds)
                .GetValue(WaitTimeout);

            auto target2 = env->CreateNVMeTarget(nqn2, { device3 }, transportIds)
                .GetValue(WaitTimeout);

            target1->Start();
            target2->Start();

            auto localDevice1 = target1->GetDevice(device1);
            UNIT_ASSERT(localDevice1);

            auto localDevice2 = target1->GetDevice(device2);
            UNIT_ASSERT(localDevice2);

            auto localDevice3 = target2->GetDevice(device3);
            UNIT_ASSERT(localDevice3);

            auto res = write(pipefd[1], &port, sizeof(port));
            UNIT_ASSERT(res == sizeof(port));
            close(pipefd[1]);

            int status = 0;
            wait(&status);  // wait child proccess

            localDevice1 = nullptr;
            localDevice2 = nullptr;
            localDevice3 = nullptr;

            target1->Stop();
            target2->Stop();

            env->Stop();
        } else {
            close(pipefd[1]);   // Close unused write end

            ui16 port;
            auto res = read(pipefd[0], &port, sizeof(port));
            UNIT_ASSERT(res == sizeof(port));
            close(pipefd[0]);

            auto pid2 = fork();
            UNIT_ASSERT(pid2 != -1);

            if (pid2 != 0) {
                TestNVMeDevices(port, nqn1, 2);

                int status = 0;
                wait(&status);  // wait child proccess
            } else {
                TestNVMeDevices(port, nqn2, 1);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSpdkTargetTest_iSCSI)
{
    Y_UNIT_TEST(ShoudHandleRemoteRequests)
    {
        auto logging = CreateLoggingService("console");
        InitLogging(logging->CreateLog("SPDK"));

        auto env = CreateEnv(std::make_shared<TSpdkEnvConfig>());

        env->Start();

        auto device1 = env->RegisterMemoryDevice("test1", DefaultBlocksCount, DefaultBlockSize)
            .GetValue(WaitTimeout);

        auto device2 = env->RegisterMemoryDevice("test2", DefaultBlocksCount, DefaultBlockSize)
            .GetValue(WaitTimeout);

        auto device3 = env->RegisterMemoryDevice("test3", DefaultBlocksCount, DefaultBlockSize)
            .GetValue(WaitTimeout);

        TString ident1 = "target1";
        TString ident2 = "target2";

        auto targetIqn1 = CreateScsiIqn("2016-06.io.spdk", ident1);
        auto targetIqn2 = CreateScsiIqn("2016-06.io.spdk", ident2);

        auto initiatorIqn = CreateScsiIqn("2016-06.io.spdk", "initiator");

        TPortManager portManager;
        auto portal = ISpdkEnv::TPortal("127.0.0.1", portManager.GetPort(1234));
        auto initiator = ISpdkEnv::TInitiator("ANY", "ANY");

        env->CreatePortalGroup(1, { portal }).GetValue(WaitTimeout);
        env->CreateInitiatorGroup(1, { initiator }).GetValue(WaitTimeout);

        TVector<ISpdkEnv::TDevice> devices1 = {
            { device1, 0 },
            { device2, 1 },
        };
        TVector<ISpdkEnv::TDevice> devices2 = {
            { device3, 0 },
        };
        TVector<ISpdkEnv::TGroupMapping> groups = {
            { 1, 1 },
        };

        auto target1 = env->CreateSCSITarget(ident1, devices1, groups)
            .GetValue(WaitTimeout);
        auto target2 = env->CreateSCSITarget(ident2, devices2, groups)
            .GetValue(WaitTimeout);

        target1->Start();
        target2->Start();

        auto localDevice1 = target1->GetDevice(device1);
        UNIT_ASSERT(localDevice1);

        auto localDevice2 = target1->GetDevice(device2);
        UNIT_ASSERT(localDevice2);

        auto localDevice3 = target2->GetDevice(device3);
        UNIT_ASSERT(localDevice3);

        auto targetUrl1 = CreateScsiUrl(portal.first, portal.second, targetIqn1, 0);
        auto remoteDevice1 = env->RegisterSCSIDevice("scsi1", targetUrl1, initiatorIqn)
            .GetValue(WaitTimeout);

        auto targetUrl2 = CreateScsiUrl(portal.first, portal.second, targetIqn1, 1);
        auto remoteDevice2 = env->RegisterSCSIDevice("scsi2", targetUrl2, initiatorIqn)
            .GetValue(WaitTimeout);

        auto targetUrl3 = CreateScsiUrl(portal.first, portal.second, targetIqn2, 0);
        auto remoteDevice3 = env->RegisterSCSIDevice("scsi3", targetUrl3, initiatorIqn)
            .GetValue(WaitTimeout);

        auto remoteDevices = { remoteDevice1, remoteDevice2, remoteDevice3 };

        for (const auto& name: remoteDevices) {
            TestReadWrite(env, name);
        }

        localDevice1 = nullptr;
        localDevice2 = nullptr;
        localDevice3 = nullptr;

        for (const auto& name: remoteDevices) {
            env->UnregisterDevice(name).GetValue(WaitTimeout);
        }

        target1->Stop();
        target2->Stop();

        env->Stop();
    }
}

}   // namespace NCloud::NBlockStore::NSpdk
