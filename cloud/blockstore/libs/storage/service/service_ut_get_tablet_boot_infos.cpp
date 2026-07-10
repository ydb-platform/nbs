#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/hive_proxy/hive_proxy.h>
#include <cloud/storage/core/libs/hive_proxy/tablet_boot_info.h>

#include <contrib/ydb/core/testlib/basics/appdata.h>
#include <contrib/ydb/core/testlib/basics/helpers.h>
#include <contrib/ydb/core/testlib/basics/runtime.h>
#include <contrib/ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/util/json_util.h>

#include <util/folder/tempdir.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TGetSetTestEnv
{
    TTempDir TempDir;
    TTestBasicRuntime Runtime;

    TGetSetTestEnv()
    {
        NKikimr::TAppPrepare app;
        SetupTabletServices(Runtime, &app, true);
    }

    void SetupHiveProxy()
    {
        TString backupFilePath = TempDir.Path() / "tablet_boot_info_backup.txt";

        THiveProxyConfig config{
            .PipeClientRetryCount = 4,
            .PipeClientMinRetryTime = TDuration::Seconds(1),
            .HiveLockExpireTimeout = TDuration::Seconds(30),
            .LogComponent = 0,
            .TabletBootInfoBackupFilePath = backupFilePath,
        };
        auto actorId =
            Runtime.Register(CreateHiveProxy(std::move(config)).release());
        Runtime.EnableScheduleForActor(actorId);
        Runtime.RegisterService(MakeHiveProxyServiceId(), actorId);
    }

    TEvHiveProxy::TEvGetTabletBootInfosResponse::TPtr GetViaHiveProxy(
        const TActorId& sender)
    {
        Runtime.Send(new IEventHandle(
            MakeHiveProxyServiceId(),
            sender,
            new TEvHiveProxy::TEvGetTabletBootInfosRequest()));

        auto ev =
            Runtime.GrabEdgeEvent<TEvHiveProxy::TEvGetTabletBootInfosResponse>(
                sender);
        UNIT_ASSERT(ev);

        return ev;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TGetTabletBootInfosTest)
{
    Y_UNIT_TEST(ShouldGetEmptyBootInfos)
    {
        TGetSetTestEnv env;
        env.SetupHiveProxy();

        auto sender = env.Runtime.AllocateEdgeActor();

        auto ev = env.GetViaHiveProxy(sender);
        UNIT_ASSERT_VALUES_EQUAL(0, ev->Get()->TabletBootInfos.size());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
