#include "authorizer.h"

#include "auth_counters.h"

#include <cloud/blockstore/config/server.pb.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/service/auth_scheme.h>
#include <cloud/blockstore/libs/storage/api/authorizer.h>
#include <cloud/blockstore/libs/storage/core/config.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/ticket_parser.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/generic/deque.h>
#include <util/generic/ptr.h>
#include <util/stream/output.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

static const TString DatabaseId = "NBS";

////////////////////////////////////////////////////////////////////////////////

TString MaskSecret(TStringBuf secret)
{
    TStringBuilder mask;
    if (secret.size() >= 16) {
        mask << secret.substr(0, 4);
        mask << "****";
        mask << secret.substr(secret.size() - 4, 4);
    } else {
        mask << "****";
    }

    mask << " (";
    mask << Sprintf("CRC-32c: %08X", Crc32c(secret.data(), secret.size()));
    mask << ")";
    return mask;
}

////////////////////////////////////////////////////////////////////////////////

struct TRequestPermissionsInfo
{
    ui64 RequestId;
    const TVector<TString>& Permissions;
    const TVector<std::pair<TString, TString>>& Attributes;
    const TString& Token;

    TRequestPermissionsInfo(
            ui64 requestId,
            const TVector<TString>& permissions,
            const TVector<std::pair<TString, TString>>& attributes,
            const TString& token)
        : RequestId(requestId)
        , Permissions(permissions)
        , Attributes(attributes)
        , Token(token)
    {}
};

IOutputStream& operator <<(
    IOutputStream& out,
    const TRequestPermissionsInfo& info)
{
    out << "{ ";
    out << "RequestId = " << info.RequestId << ", ";
    out << "Permissions = [";
    for (const auto& permission : info.Permissions) {
        out << "'" << permission << "', ";
    }
    out << "], ";
    for (const auto& attribute : info.Attributes) {
        if (attribute.first == "folder_id") {
            out << "folder_id = '" << attribute.second << "', ";
            break;
        }
    }
    out << "} Token = '" << MaskSecret(info.Token) << "'";
    return out;
}

////////////////////////////////////////////////////////////////////////////////

struct TResponsePermissionsInfo
{
    const ui64 RequestId;
    const TEvTicketParser::TEvAuthorizeTicketResult& Response;
    const bool Allowed;

    TResponsePermissionsInfo(
            ui64 requestId,
            const TEvTicketParser::TEvAuthorizeTicketResult& response,
            bool allowed)
        : RequestId(requestId)
        , Response(response)
        , Allowed(allowed)
    {}
};

IOutputStream& operator <<(
    IOutputStream& out,
    const TResponsePermissionsInfo& info)
{
    out << "{ ";
    out << "RequestId = " << info.RequestId << ", ";
    if (info.Response.Error) {
        out << "Error = '" << info.Response.Error << "', ";
    }
    out << "Ticket = '" << MaskSecret(info.Response.Ticket) << "', ";
    if (const auto& token = info.Response.Token) {
        out << "GroupSIDs = [";
        for (const auto& sid : token->GetGroupSIDs()) {
            out << "'" << sid << "', ";
        }
        out << "], ";
    }
    out << "Verdict = " << (info.Allowed ? "allow" : "deny") << ", ";
    out << "}";
    return out;
}

////////////////////////////////////////////////////////////////////////////////

bool PermissionsMatch(
    const TVector<TString>& requestedPermissions,
    const TEvTicketParser::TEvAuthorizeTicketResult& parseTicketResult)
{
    if (parseTicketResult.Error) {
        return false;
    }
    const auto& userToken = parseTicketResult.Token;
    if (!userToken) {
        return false;
    }
    for (const auto& permission : requestedPermissions) {
        TString sid = TStringBuilder()
            << permission << "-" << DatabaseId << "@as";
        if (!userToken->IsExist(sid)) {
            return false;
        }
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TVector<std::pair<TString, TString>> GetAttributesFromConfig(
    const TStorageConfigPtr& storageConfig)
{
    TVector<std::pair<TString, TString>> result;
    if (const auto& value = storageConfig->GetFolderId()) {
        result.emplace_back("folder_id", value);
    }
    result.emplace_back("database_id", DatabaseId);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TRequestPermissionsActor final
    : public TActorBootstrapped<TRequestPermissionsActor>
{
private:
    const ui64 RequestId;
    const TString Token;
    const TVector<TString> Permissions;
    const TVector<std::pair<TString, TString>> Attributes;
    IEventHandlePtr OriginalRequest;
    const TAuthCountersPtr Counters;

public:
    TRequestPermissionsActor(
            ui64 requestId,
            TString token,
            TVector<TString> permissions,
            TVector<std::pair<TString, TString>> attributes,
            IEventHandlePtr originalRequest,
            TAuthCountersPtr counters)
        : RequestId(requestId)
        , Token(std::move(token))
        , Permissions(std::move(permissions))
        , Attributes(std::move(attributes))
        , OriginalRequest(std::move(originalRequest))
        , Counters(std::move(counters))
    {
        TThis::ActivityType = TBlockStoreActivities::AUTH;
    }

    void Bootstrap(const TActorContext& ctx)
    {
        LOG_DEBUG_S(ctx, TBlockStoreComponents::AUTH,
            "Requesting permissions: "
            << TRequestPermissionsInfo(RequestId, Permissions, Attributes, Token));

        NCloud::Send(
            ctx,
            MakeTicketParserID(),
            std::make_unique<TEvTicketParser::TEvAuthorizeTicket>(
                Token,
                Attributes,
                Permissions));

        TThis::Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(
                TEvTicketParser::TEvAuthorizeTicketResult,
                HandleParseTicketResult);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::AUTH);
                break;
        }
    }

    void HandleParseTicketResult(
        const TEvTicketParser::TEvAuthorizeTicketResult::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();

        if (msg->Error && msg->Error.Retryable) {
            LOG_WARN_S(ctx, TBlockStoreComponents::AUTH,
                "Permissions response: "
                << TResponsePermissionsInfo(RequestId, *msg, false));

            NProto::TError error;
            // Need to indicate that request should be retried on the client side.
            error.SetCode(E_REJECTED);
            NCloud::Reply(
                ctx,
                *(OriginalRequest),
                std::make_unique<TEvAuth::TEvAuthorizationResponse>(error));
            Die(ctx);
            return;
        }

        Y_VERIFY(Token == msg->Ticket);

        const bool allow = PermissionsMatch(Permissions, *msg);

        auto logLevel = allow ? NLog::PRI_DEBUG : NLog::PRI_WARN;
        LOG_LOG_S(ctx, logLevel, TBlockStoreComponents::AUTH,
            "Permissions response: "
            << TResponsePermissionsInfo(RequestId, *msg, allow));

        Counters->ReportAuthorizationStatus(allow
            ? EAuthorizationStatus::PermissionsGranted
            : EAuthorizationStatus::PermissionsDenied);

        NProto::TError error;
        if (!allow) {
            error.SetCode(E_UNAUTHORIZED);
        }

        NCloud::Reply(
            ctx,
            *(OriginalRequest),
            std::make_unique<TEvAuth::TEvAuthorizationResponse>(error));
        Die(ctx);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TAuthorizerActor final
    : public TActorBootstrapped<TAuthorizerActor>
{
private:
    const TStorageConfigPtr StorageConfig;
    const bool CheckAuthorization;
    TAuthCountersPtr Counters;

public:
    TAuthorizerActor(TStorageConfigPtr storageConfig, bool checkAuthorization)
        : StorageConfig(std::move(storageConfig))
        , CheckAuthorization(checkAuthorization)
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        Counters = MakeIntrusive<TAuthCounters>(AppData(ctx)->Counters);

        TThis::Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(
                TEvAuth::TEvAuthorizationRequest,
                HandleAuthorizationRequest);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::AUTH);
                break;
        }
    }

    void HandleAuthorizationRequest(
        const TEvAuth::TEvAuthorizationRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto authMode = StorageConfig->GetAuthorizationMode();

        if (authMode == NProto::AUTHORIZATION_IGNORE) {
            // Skipping authorization completely.
            NCloud::Reply(
                ctx,
                *ev,
                std::make_unique<TEvAuth::TEvAuthorizationResponse>());
            return;
        }

        const auto* msg = ev->Get();
        const auto requestId = GetRequestId(ev->TraceId);
        const bool requireAuthorization =
            authMode == NProto::AUTHORIZATION_REQUIRE;

        if (msg->Token.Empty()) {
            if (requireAuthorization) {
                LOG_ERROR_S(ctx, TBlockStoreComponents::AUTH,
                    "Request for authorization with empty token: "
                    << requestId);

                Counters->ReportAuthorizationStatus(
                    EAuthorizationStatus::PermissionsDeniedWithEmptyToken);

                NCloud::Reply(
                    ctx,
                    *ev,
                    std::make_unique<TEvAuth::TEvAuthorizationResponse>(MakeError(E_UNAUTHORIZED)));
            } else {
                LOG_DEBUG_S(ctx, TBlockStoreComponents::AUTH,
                    "Authorization is skipped for request with empty token: "
                    << requestId);

                Counters->ReportAuthorizationStatus(
                    EAuthorizationStatus::PermissionsGrantedWithEmptyToken);

                NCloud::Reply(
                    ctx,
                    *ev,
                    std::make_unique<TEvAuth::TEvAuthorizationResponse>());
            }

            return;
        }

        if (!CheckAuthorization) {
            if (requireAuthorization) {
                LOG_ERROR_S(ctx, TBlockStoreComponents::AUTH,
                    "Authorization is disabled but enforced. Failing request: "
                    << requestId);

                Counters->ReportAuthorizationStatus(
                    EAuthorizationStatus::PermissionsDeniedWhenDisabled);

                NCloud::Reply(
                    ctx,
                    *ev,
                    std::make_unique<TEvAuth::TEvAuthorizationResponse>(MakeError(E_UNAUTHORIZED)));
            } else {
                LOG_WARN_S(ctx, TBlockStoreComponents::AUTH,
                    "Request for authorization with authorization disabled: "
                    << requestId);

                Counters->ReportAuthorizationStatus(
                    EAuthorizationStatus::PermissionsGrantedWhenDisabled);

                NCloud::Reply(
                    ctx,
                    *ev,
                    std::make_unique<TEvAuth::TEvAuthorizationResponse>());
            }

            return;
        }

        if (StorageConfig->GetFolderId().empty()) {
            LOG_ERROR_S(ctx, TBlockStoreComponents::AUTH,
                "Authorization is enabled but FolderId is not set on server");

            Counters->ReportAuthorizationStatus(
                EAuthorizationStatus::PermissionsDeniedWithoutFolderId);

            NCloud::Reply(
                ctx,
                *ev,
                std::make_unique<TEvAuth::TEvAuthorizationResponse>(MakeError(E_UNAUTHORIZED)));
        }

        NCloud::RegisterLocal(
            ctx,
            std::make_unique<TRequestPermissionsActor>(
                requestId,
                msg->Token,
                GetPermissionStrings(msg->Permissions),
                GetAttributesFromConfig(StorageConfig),
                IEventHandlePtr(ev.Release()),
                Counters));
    }
};

}  // namespace

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateAuthorizerActor(
    TStorageConfigPtr storageConfig,
    bool checkAuthorization)
{
    return std::make_unique<TAuthorizerActor>(
        std::move(storageConfig),
        checkAuthorization);
}

}   // namespace NCloud::NBlockStore::NStorage
