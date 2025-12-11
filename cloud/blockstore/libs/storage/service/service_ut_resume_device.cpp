#include "service_ut.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceResumeDeviceTest)
{
    Y_UNIT_TEST(ShouldResumeDevice)
    {
        constexpr ui32 blockSize = 512;
        constexpr ui64 blockCount = 100'000'000'000 / blockSize;
        const TString agentId = "agent.yandex.cloud.net";
        const TString diskId = "local0";

        auto createDevice = [&](TString name, TString uuid)
        {
            NProto::TDeviceConfig device;

            device.SetAgentId(agentId);
            device.SetDeviceName(std::move(name));
            device.SetDeviceUUID(std::move(uuid));
            device.SetBlocksCount(blockCount);
            device.SetBlockSize(blockSize);

            return device;
        };

        auto diskRegistryState = MakeIntrusive<TDiskRegistryState>();

        diskRegistryState->Devices = {
            createDevice("/dev/nvme3n1", "uuid-1"),
            createDevice("/dev/nvme3n2", "uuid-2"),
            createDevice("/dev/nvme3n3", "uuid-3"),
            createDevice("/dev/nvme3n4", "uuid-4")};

        TTestEnv env{1, 1, 4, 1, {diskRegistryState}};

        TServiceClient service{env.GetRuntime(), SetupTestEnv(env)};

        {
            auto response = service.ResumeDevice(agentId, "/dev/nvme3n3");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        {
            service.SendResumeDeviceRequest(agentId, "unknown");
            auto response = service.RecvResumeDeviceResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
