#pragma once

#include <contrib/ydb/library/yql/minikql/mkql_node.h>

#include <memory>

namespace arrow {
struct ArrayData;
}

namespace NKikimr::NMiniKQL {

std::shared_ptr<arrow::ArrayData> DoSerializerRoundtrip(
    const std::shared_ptr<arrow::ArrayData>& arrayData, TType* itemType, TType* blockType);

} // namespace NKikimr::NMiniKQL
