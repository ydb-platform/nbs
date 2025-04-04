#include "column_record.h"
#include <contrib/ydb/core/formats/arrow/arrow_helpers.h>
#include <contrib/ydb/core/tx/columnshard/common/scalars.h>
#include <contrib/ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <contrib/ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap {

TChunkMeta::TChunkMeta(const TColumnChunkLoadContext& context, const TIndexInfo& indexInfo) {
    auto field = indexInfo.ArrowColumnFieldOptional(context.GetAddress().GetColumnId());
    if (context.GetMetaProto().HasNumRows()) {
        NumRows = context.GetMetaProto().GetNumRows();
    }
    if (context.GetMetaProto().HasRawBytes()) {
        RawBytes = context.GetMetaProto().GetRawBytes();
    }
    if (context.GetMetaProto().HasMaxValue()) {
        AFL_VERIFY(field)("field_id", context.GetAddress().GetColumnId())("field_name", indexInfo.GetColumnName(context.GetAddress().GetColumnId()));
        Max = ConstantToScalar(context.GetMetaProto().GetMaxValue(), field->type());
    }
}

TChunkMeta::TChunkMeta(const std::shared_ptr<arrow::Array>& column, const ui32 columnId, const TIndexInfo& indexInfo)
    : TBase(column, indexInfo.GetMinMaxIdxColumns().contains(columnId), indexInfo.IsSortedColumn(columnId))
{
}

NKikimrTxColumnShard::TIndexColumnMeta TChunkMeta::SerializeToProto() const {
    NKikimrTxColumnShard::TIndexColumnMeta meta;
    if (NumRows) {
        meta.SetNumRows(*NumRows);
    }
    if (RawBytes) {
        meta.SetRawBytes(*RawBytes);
    }
    if (HasMax()) {
        ScalarToConstant(*Max, *meta.MutableMaxValue());
        ScalarToConstant(*Max, *meta.MutableMinValue());
    }
    return meta;
}

TColumnRecord::TColumnRecord(const TColumnChunkLoadContext& loadContext, const TIndexInfo& info)
    : Meta(loadContext, info)
    , ColumnId(loadContext.GetAddress().GetColumnId())
    , Chunk(loadContext.GetAddress().GetChunk())
    , BlobRange(loadContext.GetBlobRange())
{
}

TColumnRecord::TColumnRecord(const TChunkAddress& address, const std::shared_ptr<arrow::Array>& column, const TIndexInfo& info)
    : Meta(column, address.GetColumnId(), info)
    , ColumnId(address.GetColumnId())
    , Chunk(address.GetChunk())
{
}

}
