#pragma once

#include <contrib/ydb/public/lib/ydb_cli/common/command.h>
#include <contrib/ydb/public/lib/ydb_cli/common/lazy_driver.h>

#include <contrib/ydb/library/yql/sql/v1/ide/completion/name/object/simple/schema.h>

namespace NYdb::NConsoleClient {

    NSQLComplete::ISimpleSchema::TPtr MakeYDBSchema(TLazyDriver::TPtr lazyDriver, TString database, bool isVerbose);

} // namespace NYdb::NConsoleClient
