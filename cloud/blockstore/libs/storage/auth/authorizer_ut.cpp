#include "authorizer.h"

#include "auth_counters.h"

#include <cloud/blockstore/config/storage.pb.h>
#include <cloud/blockstore/libs/storage/api/authorizer.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/testlib/test_env.h>

#include <ydb/core/base/ticket_parser.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString AuthToken1 = "TestAuthToken1";
const TString AuthToken2 = "TestAuthToken2";
const TString FolderId = "TestFolderName";

////////////////////////////////////////////////////////////////////////////////

TEvTicketParser::TError FatalError()
{
    return TEvTicketParser::TError{"Fatal error", false};
}

TEvTicketParser::TError RetriableError()
{
    return TEvTicketParser::TError{"Retriable error", true};
}

////////////////////////////////////////////////////////////////////////////////

class TTestTicketParser final
    : public TActor<TTestTicketParser>
{
public:
    std::function<void(const TEvTicketParser::TEvAuthorizeTicket::TPtr&)>
        AuthorizeTicketHandler;

public:
    TTestTicketParser()
        : TActor(&TThis::StateWork)
    {}

private:
    STRICT_STFUNC(
        StateWork,
        HFunc(TEvTicketParser::TEvAuthorizeTicket, HandleAuthorizeTicket);
    )

    void HandleAuthorizeTicket(
        const TEvTicketParser::TEvAuthorizeTicket::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ctx);
        Y_VERIFY(AuthorizeTicketHandler);

        AuthorizeTicketHandler(ev);
    }
};

////////////////////////////////////////////////////////////////////////////////

using TParseTicketResultPtr =
    std::unique_ptr<TEvTicketParser::TEvAuthorizeTicketResult>;

TParseTicketResultPtr CreateSuccessfulParseTicketResult(
    const TString& token,
    TVector<TString> sids)
{
    auto userToken = MakeIntrusive<NACLib::TUserToken>(TString(), std::move(sids));
    userToken->SaveSerializationInfo();
    return std::make_unique<TEvTicketParser::TEvAuthorizeTicketResult>(
        token,
        userToken);
}

////////////////////////////////////////////////////////////////////////////////

class TAuthorizerTestEnv final
{
private:
    TTestEnv TestEnv;
    TActorId Sender;

public:
    TAuthorizerTestEnv()
    {
        Sender = TestEnv.GetRuntime().AllocateEdgeActor();
    }

    TActorId Register(IActorPtr actor)
    {
        auto actorId = TestEnv.GetRuntime().Register(actor.release());
        TestEnv.GetRuntime().EnableScheduleForActor(actorId);

        return actorId;
    }

    void Send(const TActorId &recipient, IEventBasePtr event)
    {
        TestEnv.GetRuntime().Send(new IEventHandle(recipient, Sender, event.release()));
    }

    void DispatchEvents()
    {
        TestEnv.GetRuntime().DispatchEvents(TDispatchOptions(), TDuration());
    }

    void RegisterTestTicketParser(IActorPtr ticketParser)
    {
        TestEnv.GetRuntime().RegisterService(
            NKikimr::MakeTicketParserID(),
            Register(std::move(ticketParser)));
    }

    THolder<TEvAuth::TEvAuthorizationResponse> GrabAuthorizationResponse()
    {
        return TestEnv.GetRuntime().
            GrabEdgeEvent<TEvAuth::TEvAuthorizationResponse>(TDuration());
    }

    const TDynamicCountersPtr& GetCounters()
    {
        return TestEnv.GetRuntime().GetAppData(0).Counters;
    }
};

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateAuthorizerActor(
    bool checkAuthorization,
    NProto::EAuthorizationMode mode,
    TString folderId)
{
    NProto::TStorageServiceConfig storageConfig;
    storageConfig.SetAuthorizationMode(mode);
    storageConfig.SetFolderId(std::move(folderId));
    return CreateAuthorizerActor(
        std::make_shared<TStorageConfig>(
            storageConfig,
            std::make_shared<TFeaturesConfig>(NProto::TFeaturesConfig())
        ),
        checkAuthorization);
}

IActorPtr CreateAuthorizerActor()
{
    return CreateAuthorizerActor(
        true,
        NProto::AUTHORIZATION_REQUIRE,
        FolderId);
}

////////////////////////////////////////////////////////////////////////////////

void AssertAuthCounters(
    const TDynamicCountersPtr& actualCounters,
    std::map<EAuthorizationStatus, int> expectedCounters)
{
    for (int i = 0; i < (int)EAuthorizationStatus::MAX; ++i)
    {
        const EAuthorizationStatus status = (EAuthorizationStatus)i;
        const TAtomicBase actualCounter = actualCounters
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "auth")
            ->GetCounter(ToString(status))
            ->Val();
        const TAtomicBase expectedCounter = expectedCounters[status];
        UNIT_ASSERT_VALUES_EQUAL(expectedCounter, actualCounter);
    }
}

}  // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TAuthorizerActorTest)
{
    Y_UNIT_TEST(AuthorizeRequest)
    {
        TAuthorizerTestEnv testEnv;

        std::vector<TEvTicketParser::TEvAuthorizeTicket::TPtr> authorizeEvents;
        auto ticketParser = std::make_unique<TTestTicketParser>();
        ticketParser->AuthorizeTicketHandler =
            [&](const TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
                authorizeEvents.push_back(ev);
            };
        testEnv.RegisterTestTicketParser(std::move(ticketParser));

        auto authorizerActorID = testEnv.Register(CreateAuthorizerActor(
            true,
            NProto::AUTHORIZATION_REQUIRE,
            FolderId));

        testEnv.DispatchEvents();

        testEnv.Send(
            authorizerActorID,
            std::make_unique<TEvAuth::TEvAuthorizationRequest>(
                MakeIntrusive<TCallContext>(1ul),
                AuthToken1,
                CreatePermissionList({
                    EPermission::Read,
                    EPermission::Write})));

        testEnv.DispatchEvents();

        UNIT_ASSERT_EQUAL(authorizeEvents.size(), 1ul);
        const auto& event = *(authorizeEvents[0]->Get());
        UNIT_ASSERT_EQUAL(event.Ticket, AuthToken1);
        UNIT_ASSERT_EQUAL(event.Entries.size(), 1);
        UNIT_ASSERT_EQUAL(event.Entries[0].Attributes.size(), 2);
        UNIT_ASSERT_EQUAL(event.Entries[0].Attributes[0].first, "folder_id");
        UNIT_ASSERT_EQUAL(event.Entries[0].Attributes[0].second, FolderId);
        UNIT_ASSERT_EQUAL(event.Entries[0].Attributes[1].first, "database_id");
        UNIT_ASSERT_EQUAL(event.Entries[0].Attributes[1].second, "NBS");
        UNIT_ASSERT_EQUAL(event.Entries[0].Permissions.size(), 2);
        UNIT_ASSERT_EQUAL(event.Entries[0].Permissions[0].Permission, "nbsInternal.disks.read");
        UNIT_ASSERT_EQUAL(event.Entries[0].Permissions[1].Permission, "nbsInternal.disks.write");
    }

    Y_UNIT_TEST(AuthorizeWithAuthorizerDisabledWhenIgnoring)
    {
        TAuthorizerTestEnv testEnv;

        std::vector<TEvTicketParser::TEvAuthorizeTicket::TPtr> authorizeEvents;
        auto ticketParser = std::make_unique<TTestTicketParser>();
        ticketParser->AuthorizeTicketHandler =
            [&](const TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
                authorizeEvents.push_back(ev);
            };
        testEnv.RegisterTestTicketParser(std::move(ticketParser));

        auto authorizerActorID = testEnv.Register(CreateAuthorizerActor(
            false,
            NProto::AUTHORIZATION_IGNORE,
            FolderId));

        testEnv.DispatchEvents();

        testEnv.Send(
            authorizerActorID,
            std::make_unique<TEvAuth::TEvAuthorizationRequest>(
                MakeIntrusive<TCallContext>(1ul),
                AuthToken1,
                CreatePermissionList({
                    EPermission::Read,
                    EPermission::Write})));

        auto event = testEnv.GrabAuthorizationResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, event->GetStatus());

        UNIT_ASSERT_EQUAL(authorizeEvents.size(), 0ul);

        AssertAuthCounters(testEnv.GetCounters(), {});
    }

    Y_UNIT_TEST(AuthorizeWithAuthorizerDisabledWhenAccepting)
    {
        TAuthorizerTestEnv testEnv;

        std::vector<TEvTicketParser::TEvAuthorizeTicket::TPtr> authorizeEvents;
        auto ticketParser = std::make_unique<TTestTicketParser>();
        ticketParser->AuthorizeTicketHandler =
            [&](const TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
                authorizeEvents.push_back(ev);
            };
        testEnv.RegisterTestTicketParser(std::move(ticketParser));

        auto authorizerActorID = testEnv.Register(CreateAuthorizerActor(
            false,
            NProto::AUTHORIZATION_ACCEPT,
            FolderId));

        testEnv.DispatchEvents();

        testEnv.Send(
            authorizerActorID,
            std::make_unique<TEvAuth::TEvAuthorizationRequest>(
                MakeIntrusive<TCallContext>(1ul),
                AuthToken1,
                CreatePermissionList({
                    EPermission::Read,
                    EPermission::Write})));

        auto event = testEnv.GrabAuthorizationResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, event->GetStatus());

        UNIT_ASSERT_EQUAL(authorizeEvents.size(), 0ul);

        AssertAuthCounters(
            testEnv.GetCounters(),
            {{EAuthorizationStatus::PermissionsGrantedWhenDisabled, 1}});
    }

    Y_UNIT_TEST(DoNotAuthorizeWithAuthorizerDisabledAndRequiring)
    {
        TAuthorizerTestEnv testEnv;

        std::vector<TEvTicketParser::TEvAuthorizeTicket::TPtr> authorizeEvents;
        auto ticketParser = std::make_unique<TTestTicketParser>();
        ticketParser->AuthorizeTicketHandler =
            [&](const TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
                authorizeEvents.push_back(ev);
            };
        testEnv.RegisterTestTicketParser(std::move(ticketParser));

        auto authorizerActorID = testEnv.Register(CreateAuthorizerActor(
            false,
            NProto::AUTHORIZATION_REQUIRE,
            FolderId));

        testEnv.DispatchEvents();

        testEnv.Send(
            authorizerActorID,
            std::make_unique<TEvAuth::TEvAuthorizationRequest>(
                MakeIntrusive<TCallContext>(1ul),
                AuthToken1,
                CreatePermissionList({
                    EPermission::Read,
                    EPermission::Write})));

        auto event = testEnv.GrabAuthorizationResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_UNAUTHORIZED, event->GetStatus());

        UNIT_ASSERT_EQUAL(authorizeEvents.size(), 0ul);

        AssertAuthCounters(
            testEnv.GetCounters(),
            {{EAuthorizationStatus::PermissionsDeniedWhenDisabled, 1}});
    }

    Y_UNIT_TEST(AuthorizeWithEmptyTokenWhenIgnoring)
    {
        TAuthorizerTestEnv testEnv;

        std::vector<TEvTicketParser::TEvAuthorizeTicket::TPtr> authorizeEvents;
        auto ticketParser = std::make_unique<TTestTicketParser>();
        ticketParser->AuthorizeTicketHandler =
            [&](const TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
                authorizeEvents.push_back(ev);
            };
        testEnv.RegisterTestTicketParser(std::move(ticketParser));

        auto authorizerActorID = testEnv.Register(CreateAuthorizerActor(
            true,
            NProto::AUTHORIZATION_IGNORE,
            FolderId));

        testEnv.DispatchEvents();

        testEnv.Send(
            authorizerActorID,
            std::make_unique<TEvAuth::TEvAuthorizationRequest>(
                MakeIntrusive<TCallContext>(1ul),
                TString(),
                CreatePermissionList({
                    EPermission::Read,
                    EPermission::Write})));

        auto event = testEnv.GrabAuthorizationResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, event->GetStatus());

        UNIT_ASSERT_EQUAL(authorizeEvents.size(), 0ul);

        AssertAuthCounters(testEnv.GetCounters(), {});
    }

    Y_UNIT_TEST(AuthorizeWithEmptyTokenWhenAccepting)
    {
        TAuthorizerTestEnv testEnv;

        std::vector<TEvTicketParser::TEvAuthorizeTicket::TPtr> authorizeEvents;
        auto ticketParser = std::make_unique<TTestTicketParser>();
        ticketParser->AuthorizeTicketHandler =
            [&](const TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
                authorizeEvents.push_back(ev);
            };
        testEnv.RegisterTestTicketParser(std::move(ticketParser));

        auto authorizerActorID = testEnv.Register(CreateAuthorizerActor(
            true,
            NProto::AUTHORIZATION_ACCEPT,
            FolderId));

        testEnv.DispatchEvents();

        testEnv.Send(
            authorizerActorID,
            std::make_unique<TEvAuth::TEvAuthorizationRequest>(
                MakeIntrusive<TCallContext>(1ul),
                TString(),
                CreatePermissionList({
                    EPermission::Read,
                    EPermission::Write})));

        auto event = testEnv.GrabAuthorizationResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, event->GetStatus());

        UNIT_ASSERT_EQUAL(authorizeEvents.size(), 0ul);

        AssertAuthCounters(
            testEnv.GetCounters(),
            {{EAuthorizationStatus::PermissionsGrantedWithEmptyToken, 1}});
    }

    Y_UNIT_TEST(DoNotAuthorizeWithEmptyTokenWhenRequiring)
    {
        TAuthorizerTestEnv testEnv;

        std::vector<TEvTicketParser::TEvAuthorizeTicket::TPtr> authorizeEvents;
        auto ticketParser = std::make_unique<TTestTicketParser>();
        ticketParser->AuthorizeTicketHandler =
            [&](const TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
                authorizeEvents.push_back(ev);
            };
        testEnv.RegisterTestTicketParser(std::move(ticketParser));

        auto authorizerActorID = testEnv.Register(CreateAuthorizerActor(
            true,
            NProto::AUTHORIZATION_REQUIRE,
            FolderId));

        testEnv.DispatchEvents();

        testEnv.Send(
            authorizerActorID,
            std::make_unique<TEvAuth::TEvAuthorizationRequest>(
                MakeIntrusive<TCallContext>(1ul),
                TString(),
                CreatePermissionList({
                    EPermission::Read,
                    EPermission::Write})));

        auto event = testEnv.GrabAuthorizationResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_UNAUTHORIZED, event->GetStatus());

        UNIT_ASSERT_EQUAL(authorizeEvents.size(), 0ul);

        AssertAuthCounters(
            testEnv.GetCounters(),
            {{EAuthorizationStatus::PermissionsDeniedWithEmptyToken, 1}});
    }

    Y_UNIT_TEST(AuthorizeWithoutFolderIdWhenIgnoring)
    {
        TAuthorizerTestEnv testEnv;

        std::vector<TEvTicketParser::TEvAuthorizeTicket::TPtr> authorizeEvents;
        auto ticketParser = std::make_unique<TTestTicketParser>();
        ticketParser->AuthorizeTicketHandler =
            [&](const TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
                authorizeEvents.push_back(ev);
            };
        testEnv.RegisterTestTicketParser(std::move(ticketParser));

        auto authorizerActorID = testEnv.Register(CreateAuthorizerActor(
            true,
            NProto::AUTHORIZATION_IGNORE,
            {}));

        testEnv.DispatchEvents();

        testEnv.Send(
            authorizerActorID,
            std::make_unique<TEvAuth::TEvAuthorizationRequest>(
                MakeIntrusive<TCallContext>(1ul),
                AuthToken1,
                CreatePermissionList({
                    EPermission::Read,
                    EPermission::Write})));

        auto event = testEnv.GrabAuthorizationResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, event->GetStatus());

        UNIT_ASSERT_EQUAL(authorizeEvents.size(), 0ul);

        AssertAuthCounters(testEnv.GetCounters(), {});
    }

    Y_UNIT_TEST(DoNotAuthorizeWithoutFolderIdWhenAccepting)
    {
        TAuthorizerTestEnv testEnv;

        std::vector<TEvTicketParser::TEvAuthorizeTicket::TPtr> authorizeEvents;
        auto ticketParser = std::make_unique<TTestTicketParser>();
        ticketParser->AuthorizeTicketHandler =
            [&](const TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
                authorizeEvents.push_back(ev);
            };
        testEnv.RegisterTestTicketParser(std::move(ticketParser));

        auto authorizerActorID = testEnv.Register(CreateAuthorizerActor(
            true,
            NProto::AUTHORIZATION_ACCEPT,
            {}));

        testEnv.DispatchEvents();

        testEnv.Send(
            authorizerActorID,
            std::make_unique<TEvAuth::TEvAuthorizationRequest>(
                MakeIntrusive<TCallContext>(1ul),
                AuthToken1,
                CreatePermissionList({
                    EPermission::Read,
                    EPermission::Write})));

        auto event = testEnv.GrabAuthorizationResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_UNAUTHORIZED, event->GetStatus());

        UNIT_ASSERT_EQUAL(authorizeEvents.size(), 0ul);

        AssertAuthCounters(
            testEnv.GetCounters(),
            {{EAuthorizationStatus::PermissionsDeniedWithoutFolderId, 1}});
    }

    Y_UNIT_TEST(DoNotAuthorizeWithoutFolderIdWhenRequiring)
    {
        TAuthorizerTestEnv testEnv;

        std::vector<TEvTicketParser::TEvAuthorizeTicket::TPtr> authorizeEvents;
        auto ticketParser = std::make_unique<TTestTicketParser>();
        ticketParser->AuthorizeTicketHandler =
            [&](const TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
                authorizeEvents.push_back(ev);
            };
        testEnv.RegisterTestTicketParser(std::move(ticketParser));

        auto authorizerActorID = testEnv.Register(CreateAuthorizerActor(
            true,
            NProto::AUTHORIZATION_REQUIRE,
            {}));

        testEnv.DispatchEvents();

        testEnv.Send(
            authorizerActorID,
            std::make_unique<TEvAuth::TEvAuthorizationRequest>(
                MakeIntrusive<TCallContext>(1ul),
                AuthToken1,
                CreatePermissionList({
                    EPermission::Read,
                    EPermission::Write})));

        auto event = testEnv.GrabAuthorizationResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_UNAUTHORIZED, event->GetStatus());

        UNIT_ASSERT_EQUAL(authorizeEvents.size(), 0ul);

        AssertAuthCounters(
            testEnv.GetCounters(),
            {{EAuthorizationStatus::PermissionsDeniedWithoutFolderId, 1}});
    }

    Y_UNIT_TEST(ReplyWithFailureWhenRequiring)
    {
        TAuthorizerTestEnv testEnv;

        std::vector<TEvTicketParser::TEvAuthorizeTicket::TPtr> authorizeEvents;
        auto ticketParser = std::make_unique<TTestTicketParser>();
        ticketParser->AuthorizeTicketHandler =
            [&](const TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
                authorizeEvents.push_back(ev);
            };
        testEnv.RegisterTestTicketParser(std::move(ticketParser));

        auto authorizerActorID = testEnv.Register(CreateAuthorizerActor(
            true,
            NProto::AUTHORIZATION_REQUIRE,
            FolderId));

        testEnv.DispatchEvents();

        testEnv.Send(
            authorizerActorID,
            std::make_unique<TEvAuth::TEvAuthorizationRequest>(
                MakeIntrusive<TCallContext>(1ul),
                AuthToken1,
                CreatePermissionList({
                    EPermission::Read,
                    EPermission::Write})));

        testEnv.DispatchEvents();

        UNIT_ASSERT_EQUAL(authorizeEvents.size(), 1ul);

        testEnv.Send(
            authorizeEvents[0]->Sender,
            std::make_unique<TEvTicketParser::TEvAuthorizeTicketResult>(
                AuthToken1,
                FatalError()));

        auto event = testEnv.GrabAuthorizationResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_UNAUTHORIZED, event->GetStatus());

        AssertAuthCounters(
            testEnv.GetCounters(),
            {{EAuthorizationStatus::PermissionsDenied, 1}});
    }

    Y_UNIT_TEST(ReplyWithFailureWhenAccepting)
    {
        TAuthorizerTestEnv testEnv;

        std::vector<TEvTicketParser::TEvAuthorizeTicket::TPtr> authorizeEvents;
        auto ticketParser = std::make_unique<TTestTicketParser>();
        ticketParser->AuthorizeTicketHandler =
            [&](const TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
                authorizeEvents.push_back(ev);
            };
        testEnv.RegisterTestTicketParser(std::move(ticketParser));

        auto authorizerActorID = testEnv.Register(CreateAuthorizerActor(
            true,
            NProto::AUTHORIZATION_ACCEPT,
            FolderId));

        testEnv.DispatchEvents();

        testEnv.Send(
            authorizerActorID,
            std::make_unique<TEvAuth::TEvAuthorizationRequest>(
                MakeIntrusive<TCallContext>(1ul),
                AuthToken1,
                CreatePermissionList({
                    EPermission::Read,
                    EPermission::Write})));

        testEnv.DispatchEvents();

        UNIT_ASSERT_EQUAL(authorizeEvents.size(), 1ul);

        testEnv.Send(
            authorizeEvents[0]->Sender,
            std::make_unique<TEvTicketParser::TEvAuthorizeTicketResult>(
                AuthToken1,
                FatalError()));

        auto event = testEnv.GrabAuthorizationResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_UNAUTHORIZED, event->GetStatus());

        AssertAuthCounters(
            testEnv.GetCounters(),
            {{EAuthorizationStatus::PermissionsDenied, 1}});
    }

    Y_UNIT_TEST(ReplyWithAllPermissions)
    {
        TAuthorizerTestEnv testEnv;

        std::vector<TEvTicketParser::TEvAuthorizeTicket::TPtr> authorizeEvents;
        auto ticketParser = std::make_unique<TTestTicketParser>();
        ticketParser->AuthorizeTicketHandler =
            [&](const TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
                authorizeEvents.push_back(ev);
            };
        testEnv.RegisterTestTicketParser(std::move(ticketParser));

        auto authorizerActorID = testEnv.Register(
            CreateAuthorizerActor());

        testEnv.DispatchEvents();

        testEnv.Send(
            authorizerActorID,
            std::make_unique<TEvAuth::TEvAuthorizationRequest>(
                MakeIntrusive<TCallContext>(1ul),
                AuthToken1,
                CreatePermissionList({
                    EPermission::Read,
                    EPermission::Write})));

        testEnv.DispatchEvents();

        UNIT_ASSERT_EQUAL(authorizeEvents.size(), 1ul);

        testEnv.Send(
            authorizeEvents[0]->Sender,
            CreateSuccessfulParseTicketResult(
                AuthToken1,
                TVector<TString>{
                    "nbsInternal.disks.read-NBS@as",
                    "nbsInternal.disks.write-NBS@as"}));

        auto event = testEnv.GrabAuthorizationResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, event->GetStatus());

        AssertAuthCounters(
            testEnv.GetCounters(),
            {{EAuthorizationStatus::PermissionsGranted, 1}});
    }

    Y_UNIT_TEST(ReplyWithAllPermissionsInReverseOrder)
    {
        TAuthorizerTestEnv testEnv;

        std::vector<TEvTicketParser::TEvAuthorizeTicket::TPtr> authorizeEvents;
        auto ticketParser = std::make_unique<TTestTicketParser>();
        ticketParser->AuthorizeTicketHandler =
            [&](const TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
                authorizeEvents.push_back(ev);
            };
        testEnv.RegisterTestTicketParser(std::move(ticketParser));

        auto authorizerActorID = testEnv.Register(
            CreateAuthorizerActor());

        testEnv.DispatchEvents();

        testEnv.Send(
            authorizerActorID,
            std::make_unique<TEvAuth::TEvAuthorizationRequest>(
                MakeIntrusive<TCallContext>(1ul),
                AuthToken1,
                CreatePermissionList({
                    EPermission::Read,
                    EPermission::Write})));

        testEnv.DispatchEvents();

        UNIT_ASSERT_EQUAL(authorizeEvents.size(), 1ul);

        testEnv.Send(
            authorizeEvents[0]->Sender,
            CreateSuccessfulParseTicketResult(
                AuthToken1,
                TVector<TString>{
                    "nbsInternal.disks.write-NBS@as",
                    "nbsInternal.disks.read-NBS@as"}));

        auto event = testEnv.GrabAuthorizationResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, event->GetStatus());

        AssertAuthCounters(
            testEnv.GetCounters(),
            {{EAuthorizationStatus::PermissionsGranted, 1}});
    }

    Y_UNIT_TEST(ReplyWithExtraPermissions)
    {
        TAuthorizerTestEnv testEnv;

        std::vector<TEvTicketParser::TEvAuthorizeTicket::TPtr> authorizeEvents;
        auto ticketParser = std::make_unique<TTestTicketParser>();
        ticketParser->AuthorizeTicketHandler =
            [&](const TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
                authorizeEvents.push_back(ev);
            };
        testEnv.RegisterTestTicketParser(std::move(ticketParser));

        auto authorizerActorID = testEnv.Register(
            CreateAuthorizerActor());

        testEnv.DispatchEvents();

        testEnv.Send(
            authorizerActorID,
            std::make_unique<TEvAuth::TEvAuthorizationRequest>(
                MakeIntrusive<TCallContext>(1ul),
                AuthToken1,
                CreatePermissionList({
                    EPermission::Read,
                    EPermission::Write})));

        testEnv.DispatchEvents();

        UNIT_ASSERT_EQUAL(authorizeEvents.size(), 1ul);

        testEnv.Send(
            authorizeEvents[0]->Sender,
            CreateSuccessfulParseTicketResult(
                AuthToken1,
                TVector<TString>{
                    "nbsInternal.disks.create-NBS@as",
                    "nbsInternal.disks.read-NBS@as",
                    "nbsInternal.disks.write-NBS@as"}));

        auto event = testEnv.GrabAuthorizationResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, event->GetStatus());

        AssertAuthCounters(
            testEnv.GetCounters(),
            {{EAuthorizationStatus::PermissionsGranted, 1}});
    }

    Y_UNIT_TEST(ReplyWithNoPermissionsWhenRequiring)
    {
        TAuthorizerTestEnv testEnv;

        std::vector<TEvTicketParser::TEvAuthorizeTicket::TPtr> authorizeEvents;
        auto ticketParser = std::make_unique<TTestTicketParser>();
        ticketParser->AuthorizeTicketHandler =
            [&](const TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
                authorizeEvents.push_back(ev);
            };
        testEnv.RegisterTestTicketParser(std::move(ticketParser));

        auto authorizerActorID = testEnv.Register(CreateAuthorizerActor(
            true,
            NProto::AUTHORIZATION_REQUIRE,
            FolderId));

        testEnv.DispatchEvents();

        testEnv.Send(
            authorizerActorID,
            std::make_unique<TEvAuth::TEvAuthorizationRequest>(
                MakeIntrusive<TCallContext>(1ul),
                AuthToken1,
                CreatePermissionList({
                    EPermission::Read,
                    EPermission::Write})));

        testEnv.DispatchEvents();

        UNIT_ASSERT_EQUAL(authorizeEvents.size(), 1ul);

        testEnv.Send(
            authorizeEvents[0]->Sender,
            CreateSuccessfulParseTicketResult(AuthToken1, TVector<TString>{}));

        auto event = testEnv.GrabAuthorizationResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_UNAUTHORIZED, event->GetStatus());

        AssertAuthCounters(
            testEnv.GetCounters(),
            {{EAuthorizationStatus::PermissionsDenied, 1}});
    }

    Y_UNIT_TEST(ReplyWithNoPermissionsWhenAccepting)
    {
        TAuthorizerTestEnv testEnv;

        std::vector<TEvTicketParser::TEvAuthorizeTicket::TPtr> authorizeEvents;
        auto ticketParser = std::make_unique<TTestTicketParser>();
        ticketParser->AuthorizeTicketHandler =
            [&](const TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
                authorizeEvents.push_back(ev);
            };
        testEnv.RegisterTestTicketParser(std::move(ticketParser));

        auto authorizerActorID = testEnv.Register(CreateAuthorizerActor(
            true,
            NProto::AUTHORIZATION_ACCEPT,
            FolderId));

        testEnv.DispatchEvents();

        testEnv.Send(
            authorizerActorID,
            std::make_unique<TEvAuth::TEvAuthorizationRequest>(
                MakeIntrusive<TCallContext>(1ul),
                AuthToken1,
                CreatePermissionList({
                    EPermission::Read,
                    EPermission::Write})));

        testEnv.DispatchEvents();

        UNIT_ASSERT_EQUAL(authorizeEvents.size(), 1ul);

        testEnv.Send(
            authorizeEvents[0]->Sender,
            CreateSuccessfulParseTicketResult(AuthToken1, TVector<TString>{}));

        auto event = testEnv.GrabAuthorizationResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_UNAUTHORIZED, event->GetStatus());

        AssertAuthCounters(
            testEnv.GetCounters(),
            {{EAuthorizationStatus::PermissionsDenied, 1}});
    }

    Y_UNIT_TEST(ReplyWithOnePermissionWhenRequiring)
    {
        TAuthorizerTestEnv testEnv;

        std::vector<TEvTicketParser::TEvAuthorizeTicket::TPtr> authorizeEvents;
        auto ticketParser = std::make_unique<TTestTicketParser>();
        ticketParser->AuthorizeTicketHandler =
            [&](const TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
                authorizeEvents.push_back(ev);
            };
        testEnv.RegisterTestTicketParser(std::move(ticketParser));

        auto authorizerActorID = testEnv.Register(CreateAuthorizerActor(
            true,
            NProto::AUTHORIZATION_REQUIRE,
            FolderId));

        testEnv.DispatchEvents();

        testEnv.Send(
            authorizerActorID,
            std::make_unique<TEvAuth::TEvAuthorizationRequest>(
                MakeIntrusive<TCallContext>(1ul),
                AuthToken1,
                CreatePermissionList({
                    EPermission::Read,
                    EPermission::Write})));

        testEnv.DispatchEvents();

        UNIT_ASSERT_EQUAL(authorizeEvents.size(), 1ul);

        testEnv.Send(
            authorizeEvents[0]->Sender,
            CreateSuccessfulParseTicketResult(
                AuthToken1,
                TVector<TString>{"nbsInternal.disks.write-NBS@as"}));

        auto event = testEnv.GrabAuthorizationResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_UNAUTHORIZED, event->GetStatus());

        AssertAuthCounters(
            testEnv.GetCounters(),
            {{EAuthorizationStatus::PermissionsDenied, 1}});
    }

    Y_UNIT_TEST(ReplyWithOnePermissionWhenAccepting)
    {
        TAuthorizerTestEnv testEnv;

        std::vector<TEvTicketParser::TEvAuthorizeTicket::TPtr> authorizeEvents;
        auto ticketParser = std::make_unique<TTestTicketParser>();
        ticketParser->AuthorizeTicketHandler =
            [&](const TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
                authorizeEvents.push_back(ev);
            };
        testEnv.RegisterTestTicketParser(std::move(ticketParser));

        auto authorizerActorID = testEnv.Register(CreateAuthorizerActor(
            true,
            NProto::AUTHORIZATION_ACCEPT,
            FolderId));

        testEnv.DispatchEvents();

        testEnv.Send(
            authorizerActorID,
            std::make_unique<TEvAuth::TEvAuthorizationRequest>(
                MakeIntrusive<TCallContext>(1ul),
                AuthToken1,
                CreatePermissionList({
                    EPermission::Read,
                    EPermission::Write})));

        testEnv.DispatchEvents();

        UNIT_ASSERT_EQUAL(authorizeEvents.size(), 1ul);

        testEnv.Send(
            authorizeEvents[0]->Sender,
            CreateSuccessfulParseTicketResult(
                AuthToken1,
                TVector<TString>{"nbsInternal.disks.write-NBS@as"}));

        auto event = testEnv.GrabAuthorizationResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_UNAUTHORIZED, event->GetStatus());

        AssertAuthCounters(
            testEnv.GetCounters(),
            {{EAuthorizationStatus::PermissionsDenied, 1}});
    }

    Y_UNIT_TEST(ReplyWithDifferentSIDWhenRequiring)
    {
        TAuthorizerTestEnv testEnv;

        std::vector<TEvTicketParser::TEvAuthorizeTicket::TPtr> authorizeEvents;
        auto ticketParser = std::make_unique<TTestTicketParser>();
        ticketParser->AuthorizeTicketHandler =
            [&](const TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
                authorizeEvents.push_back(ev);
            };
        testEnv.RegisterTestTicketParser(std::move(ticketParser));

        auto authorizerActorID = testEnv.Register(CreateAuthorizerActor(
            true,
            NProto::AUTHORIZATION_REQUIRE,
            FolderId));

        testEnv.DispatchEvents();

        testEnv.Send(
            authorizerActorID,
            std::make_unique<TEvAuth::TEvAuthorizationRequest>(
                MakeIntrusive<TCallContext>(1ul),
                AuthToken1,
                CreatePermissionList({
                    EPermission::Read,
                    EPermission::Write})));

        testEnv.DispatchEvents();

        UNIT_ASSERT_EQUAL(authorizeEvents.size(), 1ul);

        testEnv.Send(
            authorizeEvents[0]->Sender,
            CreateSuccessfulParseTicketResult(
                AuthToken1,
                TVector<TString>{
                    "nbsInternal.disks.read-AnotherDatabaseID@as",
                    "nbsInternal.disks.write-AnotherDatabaseID@as"}));

        auto event = testEnv.GrabAuthorizationResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_UNAUTHORIZED, event->GetStatus());

        AssertAuthCounters(
            testEnv.GetCounters(),
            {{EAuthorizationStatus::PermissionsDenied, 1}});
    }

    Y_UNIT_TEST(ReplyWithDifferentSIDWhenAccepting)
    {
        TAuthorizerTestEnv testEnv;

        std::vector<TEvTicketParser::TEvAuthorizeTicket::TPtr> authorizeEvents;
        auto ticketParser = std::make_unique<TTestTicketParser>();
        ticketParser->AuthorizeTicketHandler =
            [&](const TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
                authorizeEvents.push_back(ev);
            };
        testEnv.RegisterTestTicketParser(std::move(ticketParser));

        auto authorizerActorID = testEnv.Register(CreateAuthorizerActor(
            true,
            NProto::AUTHORIZATION_ACCEPT,
            FolderId));

        testEnv.DispatchEvents();

        testEnv.Send(
            authorizerActorID,
            std::make_unique<TEvAuth::TEvAuthorizationRequest>(
                MakeIntrusive<TCallContext>(1ul),
                AuthToken1,
                CreatePermissionList({
                    EPermission::Read,
                    EPermission::Write})));

        testEnv.DispatchEvents();

        UNIT_ASSERT_EQUAL(authorizeEvents.size(), 1ul);

        testEnv.Send(
            authorizeEvents[0]->Sender,
            CreateSuccessfulParseTicketResult(
                AuthToken1,
                TVector<TString>{
                    "nbsInternal.disks.read-AnotherDatabaseID@as",
                    "nbsInternal.disks.write-AnotherDatabaseID@as"}));

        auto event = testEnv.GrabAuthorizationResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_UNAUTHORIZED, event->GetStatus());

        AssertAuthCounters(
            testEnv.GetCounters(),
            {{EAuthorizationStatus::PermissionsDenied, 1}});
    }

    Y_UNIT_TEST(ConcurrentRequests)
    {
        TAuthorizerTestEnv testEnv;

        std::vector<TEvTicketParser::TEvAuthorizeTicket::TPtr> authorizeEvents;
        auto ticketParser = std::make_unique<TTestTicketParser>();
        ticketParser->AuthorizeTicketHandler =
            [&](const TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
                authorizeEvents.push_back(ev);
            };
        testEnv.RegisterTestTicketParser(std::move(ticketParser));

        auto authorizerActorID = testEnv.Register(
            CreateAuthorizerActor());

        testEnv.DispatchEvents();

        testEnv.Send(
            authorizerActorID,
            std::make_unique<TEvAuth::TEvAuthorizationRequest>(
                MakeIntrusive<TCallContext>(1ul),
                AuthToken1,
                CreatePermissionList({EPermission::Read})));
        testEnv.Send(
            authorizerActorID,
            std::make_unique<TEvAuth::TEvAuthorizationRequest>(
                MakeIntrusive<TCallContext>(2ul),
                AuthToken2,
                CreatePermissionList({EPermission::Create})));
        testEnv.Send(
            authorizerActorID,
            std::make_unique<TEvAuth::TEvAuthorizationRequest>(
                MakeIntrusive<TCallContext>(3ul),
                AuthToken1,
                CreatePermissionList({EPermission::Write})));

        testEnv.DispatchEvents();

        UNIT_ASSERT_EQUAL(authorizeEvents.size(), 3ul);

        testEnv.Send(
            authorizeEvents[0]->Sender,
            CreateSuccessfulParseTicketResult(
                AuthToken1,
                TVector<TString>{"nbsInternal.disks.read-NBS@as"}));
        testEnv.Send(
            authorizeEvents[1]->Sender,
            CreateSuccessfulParseTicketResult(
                AuthToken2,
                TVector<TString>{"nbsInternal.disks.create-NBS@as"}));
        testEnv.Send(
            authorizeEvents[2]->Sender,
            CreateSuccessfulParseTicketResult(
                AuthToken1,
                TVector<TString>{ "nbsInternal.disks.write-NBS@as"}));

        auto event1 = testEnv.GrabAuthorizationResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, event1->GetStatus());

        auto event2 = testEnv.GrabAuthorizationResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, event2->GetStatus());

        auto event3 = testEnv.GrabAuthorizationResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, event3->GetStatus());
    }

    Y_UNIT_TEST(ReplyWithRetriableError)
    {
        TAuthorizerTestEnv testEnv;

        std::vector<TEvTicketParser::TEvAuthorizeTicket::TPtr> authorizeEvents;
        auto ticketParser = std::make_unique<TTestTicketParser>();
        ticketParser->AuthorizeTicketHandler =
            [&](const TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
                authorizeEvents.push_back(ev);
            };
        testEnv.RegisterTestTicketParser(std::move(ticketParser));

        auto authorizerActorID = testEnv.Register(CreateAuthorizerActor(
            true,
            NProto::AUTHORIZATION_REQUIRE,
            FolderId));

        testEnv.DispatchEvents();

        testEnv.Send(
            authorizerActorID,
            std::make_unique<TEvAuth::TEvAuthorizationRequest>(
                MakeIntrusive<TCallContext>(1ul),
                AuthToken1,
                CreatePermissionList({
                    EPermission::Read,
                    EPermission::Write})));

        testEnv.DispatchEvents();

        UNIT_ASSERT_EQUAL(authorizeEvents.size(), 1ul);

        testEnv.Send(
            authorizeEvents[0]->Sender,
            std::make_unique<TEvTicketParser::TEvAuthorizeTicketResult>(
                AuthToken1,
                RetriableError()));

        auto event = testEnv.GrabAuthorizationResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, event->GetStatus());

        AssertAuthCounters(testEnv.GetCounters(), {});
    }
}

}   // namespace NCloud::NBlockStore::NStorage
