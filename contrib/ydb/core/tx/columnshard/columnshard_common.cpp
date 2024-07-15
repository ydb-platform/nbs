#include "columnshard_common.h"
#include <contrib/ydb/core/formats/arrow/arrow_batch_builder.h>

namespace NKikimr::NColumnShard {

namespace {

using EOperation = NArrow::EOperation;
using EAggregate = NArrow::EAggregate;
using TAssign = NSsa::TAssign;
using TAggregateAssign = NSsa::TAggregateAssign;

}

using EOperation = NArrow::EOperation;
using TPredicate = NOlap::TPredicate;

}
