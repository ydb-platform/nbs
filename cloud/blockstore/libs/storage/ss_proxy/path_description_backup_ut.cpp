#include "path_description_backup.h"

#include <cloud/blockstore/libs/storage/api/partition.h>
#include <cloud/blockstore/libs/storage/core/config.h>

#include <contrib/ydb/core/testlib/basics/runtime.h>
#include <contrib/ydb/core/testlib/tablet_helpers.h>
#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TMyTestEnv final
{
private:
    TTestBasicRuntime Runtime;
    TActorId Sender;

public:
    TMyTestEnv()
    {
        SetupLogging();
        NKikimr::SetupTabletServices(Runtime);

        Sender = Runtime.AllocateEdgeActor();
    }

    void SetupLogging()
    {
        Runtime.AppendToLogSettings(
            TBlockStoreComponents::START,
            TBlockStoreComponents::END,
            GetComponentName);

        for (ui32 i = TBlockStoreComponents::START;
             i < TBlockStoreComponents::END;
             ++i)
        {
            Runtime.SetLogPriority(i, NLog::PRI_TRACE);
        }
    }

    TActorId Register(IActorPtr actor)
    {
        auto actorId = Runtime.Register(actor.release());
        Runtime.EnableScheduleForActor(actorId);

        return actorId;
    }

    void Send(const TActorId& recipient, IEventBasePtr event)
    {
        Runtime.Send(new IEventHandle(recipient, Sender, event.release()));
    }

    void DispatchEvents()
    {
        Runtime.DispatchEvents(TDispatchOptions(), TDuration());
    }

    THolder<TEvSSProxyPrivate::TEvReadPathDescriptionBackupResponse>
    GrabReadPathDescriptionBackupResponse()
    {
        return Runtime.GrabEdgeEvent<
            TEvSSProxyPrivate::TEvReadPathDescriptionBackupResponse>(
            TDuration());
    }
};

TString GetTestFilePath(const TString& fileName)
{
    return JoinFsPaths(
        ArcadiaSourceRoot(),
        "cloud/blockstore/libs/storage/ss_proxy/ut/test_backups",
        fileName);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPathDescriptionBackupTest)
{
    Y_UNIT_TEST(ShouldReplyNotFoundForUnknownVolume)
    {
        TMyTestEnv testEnv;

        auto actorId = testEnv.Register(
            std::make_unique<TPathDescriptionBackup>(
                TBlockStoreComponents::SS_PROXY,
                "",
                false,
                true));
        testEnv.Send(
            actorId,
            std::make_unique<NActors::TEvents::TEvBootstrap>());

        testEnv.Send(
            actorId,
            std::make_unique<
                TEvSSProxyPrivate::TEvReadPathDescriptionBackupRequest>(
                "unknown"));

        auto response = testEnv.GrabReadPathDescriptionBackupResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetStatus());
    }

    void DoShouldLoadFromBackup(const TString& fileName)
    {
        TMyTestEnv testEnv;

        auto actorId = testEnv.Register(
            std::make_unique<TPathDescriptionBackup>(
                TBlockStoreComponents::SS_PROXY,
                GetTestFilePath(fileName),
                false,
                true));
        testEnv.Send(
            actorId,
            std::make_unique<NActors::TEvents::TEvBootstrap>());

        testEnv.Send(
            actorId,
            std::make_unique<
                TEvSSProxyPrivate::TEvReadPathDescriptionBackupRequest>(
                "/Root/NBS/_1E7/vol0"));

        auto response = testEnv.GrabReadPathDescriptionBackupResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
    }

    Y_UNIT_TEST(ShouldLoadFromTextFormat)
    {
        DoShouldLoadFromBackup("backup.txt");
    }

    Y_UNIT_TEST(ShouldLoadFromBinaryFormat)
    {
        DoShouldLoadFromBackup("backup.proto");
    }

    Y_UNIT_TEST(ShouldLoadFromChunkedBinaryFormat)
    {
        DoShouldLoadFromBackup("backup.chunked.proto");
    }
}

}   // namespace NCloud::NBlockStore::NStorage
