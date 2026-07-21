#include "ansi.h"

#include <contrib/ydb/library/yql/public/issue/yql_issue.h>
#include <contrib/ydb/library/yql/sql/settings/translation_settings.h>

namespace NSQLComplete {

using NSQLTranslation::ParseTranslationSettings;
using NSQLTranslation::TTranslationSettings;
using NYql::TIssues;

bool IsAnsiQuery(const TString& query) {
    TTranslationSettings settings;
    TIssues issues;
    ParseTranslationSettings(query, settings, issues);
    return settings.AnsiLexer;
}

} // namespace NSQLComplete
