#include "ss_proxy_actor.h"

#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NSchemeShard;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError GetErrorFromPreconditionFailed(const NProto::TError& error)
{
    NProto::TError result = error;
    const auto& msg = error.GetMessage();

    if (msg.Contains("Wrong version in")) {
        // ConfigVersion is different from current one in SchemeShard
        // return E_ABORTED to client to read
        // updated config (Stat FS) and issue new request
        result.SetCode(E_ABORTED);
        result.SetMessage("Config version mismatch");
    } else if (msg.Contains("path version mistmach")) {
        // Just path version mismatch. Return E_REJECTED
        // so durable client will retry request
        result.SetCode(E_REJECTED);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TModifySchemeActor final
    : public TActorBootstrapped<TModifySchemeActor>
{
private:
    const int LogComponent;
    const TSSProxyActor::TRequestInfo RequestInfo;
    const TActorId Owner;
    const NKikimrSchemeOp::TModifyScheme ModifyScheme;

    ui64 TxId = 0;
    ui64 SchemeShardTabletId = 0;
    NKikimrScheme::EStatus SchemeShardStatus = NKikimrScheme::StatusSuccess;
    TString SchemeShardReason;

public:
    TModifySchemeActor(
        int logComponent,
        TSSProxyActor::TRequestInfo requestInfo,
        const TActorId& owner,
        NKikimrSchemeOp::TModifyScheme modifyScheme);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleStatus(
        const TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev,
        const TActorContext& ctx);

    void HandleTxDone(
        const TEvSSProxy::TEvWaitSchemeTxResponse::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        NProto::TError error = NProto::TError());
};

////////////////////////////////////////////////////////////////////////////////

TModifySchemeActor::TModifySchemeActor(
        int logComponent,
        TSSProxyActor::TRequestInfo requestInfo,
        const TActorId& owner,
        NKikimrSchemeOp::TModifyScheme modifyScheme)
    : LogComponent(logComponent)
    , RequestInfo(std::move(requestInfo))
    , Owner(owner)
    , ModifyScheme(std::move(modifyScheme))
{}

void TModifySchemeActor::Bootstrap(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();

    auto* tx = request->Record.MutableTransaction();
    tx->MutableModifyScheme()->CopyFrom(ModifyScheme);

    NCloud::Send(ctx, MakeTxProxyID(), std::move(request));

    Become(&TThis::StateWork);
}

void TModifySchemeActor::HandleStatus(
    const TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& record = ev->Get()->Record;

    TxId = record.GetTxId();
    SchemeShardTabletId = record.GetSchemeShardTabletId();
    SchemeShardStatus = (NKikimrScheme::EStatus) record.GetSchemeShardStatus();
    SchemeShardReason = record.GetSchemeShardReason();

    auto status = (TEvTxUserProxy::TEvProposeTransactionStatus::EStatus) record.GetStatus();
    switch (status) {
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete:
            LOG_DEBUG(ctx, LogComponent,
                "Request %s with TxId# %lu completed immediately",
                NKikimrSchemeOp::EOperationType_Name(ModifyScheme.GetOperationType()).c_str(),
                TxId);

            ReplyAndDie(ctx);
            break;

        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress: {
            LOG_DEBUG(ctx, LogComponent,
                "Request %s with TxId# %lu in progress, waiting for completion",
                NKikimrSchemeOp::EOperationType_Name(ModifyScheme.GetOperationType()).c_str(),
                TxId);

            auto request = std::make_unique<TEvSSProxy::TEvWaitSchemeTxRequest>(
                SchemeShardTabletId,
                TxId);

            NCloud::Send(ctx, Owner, std::move(request));
            break;
        }

        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError:
            LOG_DEBUG(ctx, LogComponent,
                "Request %s with TxId# %lu failed with status %s",
                NKikimrSchemeOp::EOperationType_Name(ModifyScheme.GetOperationType()).c_str(),
                TxId,
                NKikimrScheme::EStatus_Name(SchemeShardStatus).c_str());

            if (SchemeShardStatus == NKikimrScheme::StatusMultipleModifications &&
                (record.GetPathCreateTxId() != 0 || record.GetPathDropTxId() != 0))
            {
                ui64 txId = record.GetPathCreateTxId() != 0 ? record.GetPathCreateTxId() : record.GetPathDropTxId();
                LOG_DEBUG(ctx, LogComponent,
                    "Waiting for a different TxId# %lu", txId);

                auto request = std::make_unique<TEvSSProxy::TEvWaitSchemeTxRequest>(
                    SchemeShardTabletId,
                    txId);

                NCloud::Send(ctx, Owner, std::move(request));
                break;
            }

            ReplyAndDie(
                ctx,
                MakeError(
                    MAKE_SCHEMESHARD_ERROR(SchemeShardStatus),
                    (TStringBuilder()
                        << NKikimrSchemeOp::EOperationType_Name(ModifyScheme.GetOperationType()).c_str()
                        << " failed with reason: "
                        << (SchemeShardReason.empty() ?
                            NKikimrScheme::EStatus_Name(SchemeShardStatus).c_str()
                            :SchemeShardReason))));
            break;

        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError:
            if (SchemeShardStatus == NKikimrScheme::StatusPathDoesNotExist) {
                LOG_DEBUG(ctx, LogComponent,
                    "Request %s failed to resolve parent path",
                    NKikimrSchemeOp::EOperationType_Name(ModifyScheme.GetOperationType()).c_str());

                ReplyAndDie(
                    ctx,
                    MakeError(
                        MAKE_SCHEMESHARD_ERROR(SchemeShardStatus),
                        (TStringBuilder()
                            << NKikimrSchemeOp::EOperationType_Name(ModifyScheme.GetOperationType()).c_str()
                            << " failed with reason: "
                            << (SchemeShardReason.empty() ?
                                NKikimrScheme::EStatus_Name(SchemeShardStatus).c_str()
                                                          :SchemeShardReason))));
                break;
            }

            /* fall through */

        default:
            LOG_DEBUG(ctx, LogComponent,
                "Request %s to tx_proxy failed with code %u",
                NKikimrSchemeOp::EOperationType_Name(ModifyScheme.GetOperationType()).c_str(),
                status);

            ReplyAndDie(
                ctx,
                MakeError(MAKE_TXPROXY_ERROR(status), TStringBuilder()
                    << "TxProxy failed: " << status));
            break;
    }
}

void TModifySchemeActor::HandleTxDone(
    const TEvSSProxy::TEvWaitSchemeTxResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(ctx, LogComponent,
        "TModifySchemeActor received TEvWaitSchemeTxResponse");

    ReplyAndDie(ctx, msg->GetError());
}

void TModifySchemeActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    if (SchemeShardStatus == NKikimrScheme::StatusPreconditionFailed) {
        error = GetErrorFromPreconditionFailed(error);
    }

    auto response = std::make_unique<TEvSSProxy::TEvModifySchemeResponse>(
        error,
        SchemeShardTabletId,
        SchemeShardStatus,
        SchemeShardReason);

    NCloud::Reply(ctx, RequestInfo, std::move(response));
    Die(ctx);
}

STFUNC(TModifySchemeActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvTxUserProxy::TEvProposeTransactionStatus, HandleStatus);
        HFunc(TEvSSProxy::TEvWaitSchemeTxResponse, HandleTxDone);

        default:
            HandleUnexpectedEvent(ev, LogComponent, __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TSSProxyActor::HandleModifyScheme(
    const TEvSSProxy::TEvModifySchemeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    NCloud::Register(
        ctx,
        std::make_unique<TModifySchemeActor>(
            Config.LogComponent,
            TRequestInfo(ev->Sender, ev->Cookie),
            ctx.SelfID,
            msg->ModifyScheme));
}

}   // namespace NCloud::NStorage
