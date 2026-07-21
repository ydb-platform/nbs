#pragma once

#include <contrib/ydb/library/yql/core/url_lister/interface/url_lister_manager.h>

#include <contrib/ydb/library/yql/core/qplayer/storage/interface/yql_qstorage.h>

namespace NYql::NCommon {
IUrlListerManagerPtr WrapUrlListerManagerWithQContext(IUrlListerManagerPtr underlying, const TQContext& qContext);
} // namespace NYql::NCommon
