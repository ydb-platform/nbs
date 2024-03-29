#pragma once

#include <contrib/ydb/public/sdk/cpp/client/impl/ydb_internal/internal_header.h>

#include <contrib/ydb/public/api/protos/ydb_value.pb.h>

namespace NYdb {

bool TypesEqual(const Ydb::Type& t1, const Ydb::Type& t2);

} // namespace NYdb
