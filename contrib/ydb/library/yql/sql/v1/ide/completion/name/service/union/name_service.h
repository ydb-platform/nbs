#pragma once

#include <contrib/ydb/library/yql/sql/v1/ide/completion/name/service/ranking/ranking.h>
#include <contrib/ydb/library/yql/sql/v1/ide/completion/name/service/name_service.h>

namespace NSQLComplete {

INameService::TPtr MakeUnionNameService(
    TVector<INameService::TPtr> children,
    IRanking::TPtr ranking);

} // namespace NSQLComplete
