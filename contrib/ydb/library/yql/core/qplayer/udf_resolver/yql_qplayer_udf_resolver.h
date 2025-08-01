#pragma once
#include <contrib/ydb/library/yql/core/yql_udf_resolver.h>
#include <contrib/ydb/library/yql/core/qplayer/storage/interface/yql_qstorage.h>

namespace NYql::NCommon {

IUdfResolver::TPtr WrapUdfResolverWithQContext(IUdfResolver::TPtr inner, const TQContext& qContext);

}
