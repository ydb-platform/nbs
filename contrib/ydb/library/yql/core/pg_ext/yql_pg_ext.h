#pragma once

#include <contrib/ydb/library/yql/protos/pg_ext.pb.h>
#include <contrib/ydb/library/yql/parser/pg_catalog/catalog.h>

namespace NYql {

void PgExtensionsFromProto(const NYql::NProto::TPgExtensions& proto,
                           TVector<NPg::TExtensionDesc>& extensions);

} // namespace NYql
