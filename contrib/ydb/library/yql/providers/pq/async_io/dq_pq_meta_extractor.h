#pragma once

#include <functional>

#include <contrib/ydb/library/yql/public/udf/udf_data_type.h>
#include <contrib/ydb/library/yql/public/udf/udf_value.h>

#include <contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/string.h>

namespace NKikimr::NMiniKQL {
class THolderFactory;
class TTypeEnvironment;
}

namespace NYql::NDq {
using TPqMetaExtractorLambda = std::function<std::pair<NYql::NUdf::TUnboxedValuePod, i64>(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage&, const TString& /*cluster*/)>;

TPqMetaExtractorLambda CreatePqMetaExtractorLambda(
    const TString& columnName,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv);
}
