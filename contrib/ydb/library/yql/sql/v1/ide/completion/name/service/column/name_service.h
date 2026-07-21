#pragma once

#include <contrib/ydb/library/yql/sql/v1/ide/completion/analysis/global/global.h>
#include <contrib/ydb/library/yql/sql/v1/ide/completion/name/service/name_service.h>

namespace NSQLComplete {

INameService::TPtr MakeColumnNameService(TVector<TColumnId> columns);

} // namespace NSQLComplete
