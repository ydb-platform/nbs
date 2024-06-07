#pragma once
#include <contrib/ydb/core/tx/columnshard/engines/insert_table/meta.h>

namespace NKikimr::NOlap::NEngines::NTest {

class TLocalHelper {
public:
    static NKikimrTxColumnShard::TLogicalMetadata GetMetaProto();
};

};