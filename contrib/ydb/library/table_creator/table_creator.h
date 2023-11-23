#pragma once

#include <contrib/ydb/core/base/events.h>
#include <contrib/ydb/core/protos/flat_scheme_op.pb.h>
#include <contrib/ydb/library/services/services.pb.h>

#include <library/cpp/actors/core/actor.h>

namespace NKikimr {

struct TEvTableCreator {
    enum EEv {
        EvBegin = EventSpaceBegin(TKikimrEvents::ES_TABLE_CREATOR),
        EvCreateTableResponse,
    };

    struct TEvCreateTableResponse : public TEventLocal<TEvCreateTableResponse, EvCreateTableResponse> {
    };
};

NActors::IActor* CreateTableCreator(
    TVector<TString> pathComponents,
    TVector<NKikimrSchemeOp::TColumnDescription> columns,
    TVector<TString> keyColumns,
    NKikimrServices::EServiceKikimr logService,
    TMaybe<NKikimrSchemeOp::TTTLSettings> ttlSettings = Nothing());

} // namespace NKikimr
