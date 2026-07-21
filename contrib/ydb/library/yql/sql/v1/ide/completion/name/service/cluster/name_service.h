#pragma once

#include <contrib/ydb/library/yql/sql/v1/ide/completion/name/cluster/discovery.h>
#include <contrib/ydb/library/yql/sql/v1/ide/completion/name/service/name_service.h>

namespace NSQLComplete {

INameService::TPtr MakeClusterNameService(IClusterDiscovery::TPtr discovery);

} // namespace NSQLComplete
