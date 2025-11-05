#pragma once
#include <contrib/ydb/library/yql/public/udf/udf_value.h>

namespace NYql {

template<typename ValueType>
NUdf::TUnboxedValuePod ScalarValueToPod(const ValueType value);

}
