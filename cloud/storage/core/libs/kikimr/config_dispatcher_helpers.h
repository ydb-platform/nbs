#pragma once

#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/core/config/init/init.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

TResultOrError<NKikimr::NConfig::TAllowList> ParseConfigDispatcherItems(
    const TVector<TString>& items);

}   // namespace NCloud::NStorage
