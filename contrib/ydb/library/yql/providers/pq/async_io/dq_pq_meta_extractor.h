#pragma once

#include "contrib/ydb/library/yql/minikql/mkql_string_util.h"
#include <optional>

#include <contrib/ydb/library/yql/public/udf/udf_data_type.h>
#include <contrib/ydb/library/yql/public/udf/udf_value.h>

#include <contrib/ydb/public/sdk/cpp/client/ydb_persqueue_core/persqueue.h>

#include <util/generic/string.h>

namespace NYql::NDq {
    struct TPqMetaExtractor {
        using TPqMetaExtractorLambda = std::function<std::pair<NYql::NUdf::TUnboxedValuePod, i64>(const NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent::TMessage&)>;

    public:
        TPqMetaExtractor();
        TPqMetaExtractorLambda FindExtractorLambda(TString sysColumn) const;
    };
}
