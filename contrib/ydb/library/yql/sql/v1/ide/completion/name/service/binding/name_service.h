#pragma once

#pragma once

#include <contrib/ydb/library/yql/sql/v1/ide/completion/name/service/name_service.h>

namespace NSQLComplete {

INameService::TPtr MakeBindingNameService(TVector<TString> names);

} // namespace NSQLComplete
