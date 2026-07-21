#pragma once

#include "linter.h"

#include <contrib/ydb/library/yql/sql/settings/translation_settings.h>

namespace NYql::NFastCheck {

std::unique_ptr<IUdfMeta> LoadUdfMeta(TStringBuf json);
void FillClusters(const TChecksRequest& request, NSQLTranslation::TTranslationSettings& settings);

} // namespace NYql::NFastCheck
