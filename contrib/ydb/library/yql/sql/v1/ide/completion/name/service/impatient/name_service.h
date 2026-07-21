#pragma once

#include <contrib/ydb/library/yql/sql/v1/ide/completion/name/service/name_service.h>

namespace NSQLComplete {

INameService::TPtr MakeImpatientNameService(INameService::TPtr origin);

} // namespace NSQLComplete
