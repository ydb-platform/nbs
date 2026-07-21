#pragma once

#include <contrib/ydb/library/yql/ast/yql_ast.h>
#include <contrib/ydb/library/yql/public/issue/yql_issue.h>
#include <contrib/ydb/library/yql/sql/settings/translation_settings.h>

namespace NSQLFormat {

enum class EConvergenceRequirement: ui8 {
    None,
    Triple, // format(format(input)) == format(format(format(input)))
    Double, // .      format(input)  ==        format(format(input))
};

TMaybe<TString> CheckedFormat(
    const TString& query,
    TMaybe<const NYql::TAstNode*> ast,
    NSQLTranslation::TTranslationSettings settings,
    NYql::TIssues& issues,
    EConvergenceRequirement convergence = EConvergenceRequirement::Double);

} // namespace NSQLFormat
