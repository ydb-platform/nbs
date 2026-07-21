#pragma once
#include <contrib/ydb/library/yql/providers/common/codec/yt_arrow_converter_interface/yt_arrow_converter.h>
#include <contrib/ydb/library/yql/public/udf/arrow/block_builder.h>

namespace NKikimr::NMiniKQL {
class TType;
}

namespace NYql {
std::unique_ptr<IYtColumnConverter> MakeYtColumnConverter(NKikimr::NMiniKQL::TType* type, const NUdf::IPgBuilder* pgBuilder, arrow::MemoryPool& pool, ui64 nativeYtTypeFlags);
}
