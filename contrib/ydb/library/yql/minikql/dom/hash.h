#pragma once

#include <contrib/ydb/library/yql/public/udf/udf_types.h>
#include <contrib/ydb/library/yql/public/udf/udf_type_ops.h>

namespace NYql::NDom {

NUdf::THashType HashDom(NUdf::TUnboxedValuePod value);

bool EquateDoms(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs);

} // namespace NYql::NDom
