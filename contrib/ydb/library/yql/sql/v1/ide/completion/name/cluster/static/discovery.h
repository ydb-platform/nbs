#pragma once

#include <contrib/ydb/library/yql/sql/v1/ide/completion/name/cluster/discovery.h>

namespace NSQLComplete {

IClusterDiscovery::TPtr MakeStaticClusterDiscovery(TVector<TString> instances);

} // namespace NSQLComplete
