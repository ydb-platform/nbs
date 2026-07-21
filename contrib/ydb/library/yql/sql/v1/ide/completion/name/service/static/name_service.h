#pragma once

#include "name_set.h"

#include <contrib/ydb/library/yql/sql/v1/ide/completion/name/service/ranking/frequency.h>
#include <contrib/ydb/library/yql/sql/v1/ide/completion/name/service/ranking/ranking.h>
#include <contrib/ydb/library/yql/sql/v1/ide/completion/name/service/name_service.h>

namespace NSQLComplete {

INameService::TPtr MakeStaticNameService(TNameSet names, TFrequencyData frequency);

INameService::TPtr MakeStaticNameService(TNameSet names, IRanking::TPtr ranking);

} // namespace NSQLComplete
