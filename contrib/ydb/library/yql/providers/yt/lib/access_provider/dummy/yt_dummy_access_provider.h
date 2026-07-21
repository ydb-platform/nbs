#pragma once

#include <contrib/ydb/library/yql/providers/yt/lib/access_provider/yt_access_provider.h>

namespace NYql {

IYtAccessProvider::TPtr CreateYtDummyAccessProvider();

}; // namespace NYql
