#include "ban.h"

#include "config.h"

#include <cloud/blockstore/config/discovery.pb.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>

namespace NCloud::NBlockStore::NDiscovery {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TEnv
{
    TTempDir TempDir;
    TDiscoveryConfigPtr Config;

    TEnv()
    {
        NProto::TDiscoveryServiceConfig c;
        c.SetBannedInstanceListFile(TempDir.Path() / "banned_instances.txt");
        Config = std::make_shared<TDiscoveryConfig>(std::move(c));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(BanListTest)
{
    Y_UNIT_TEST(ShouldUpdate)
    {
        TEnv env;
        {
            TOFStream of(env.Config->GetBannedInstanceListFile());
            of << "host1\t1" << Endl;
            of << "host2\t2" << Endl;
            of << "host3\t3" << Endl;
        }

        auto banList = CreateBanList(
            env.Config,
            CreateLoggingService("console"),
            CreateMonitoringServiceStub());
        banList->Start();

        UNIT_ASSERT(!banList->IsBanned("host1", 1));
        UNIT_ASSERT(!banList->IsBanned("host2", 2));
        UNIT_ASSERT(!banList->IsBanned("host3", 3));

        banList->Update();
        UNIT_ASSERT(banList->IsBanned("host1", 1));
        UNIT_ASSERT(banList->IsBanned("host2", 2));
        UNIT_ASSERT(banList->IsBanned("host3", 3));
        UNIT_ASSERT(!banList->IsBanned("host4", 4));

        {
            TOFStream of(env.Config->GetBannedInstanceListFile());
            of << "host1\t1" << Endl;
            of << "broken entry" << Endl;
            of << "host2\t2" << Endl;
            of << "host4\t4" << Endl;
        }

        banList->Update();
        UNIT_ASSERT(banList->IsBanned("host1", 1));
        UNIT_ASSERT(banList->IsBanned("host2", 2));
        UNIT_ASSERT(!banList->IsBanned("host3", 3));
        UNIT_ASSERT(banList->IsBanned("host4", 4));

        banList->Stop();
    }
}

}   // namespace NCloud::NBlockStore::NDiscovery
