#include "utils.h"
#include <contrib/ydb/library/yql/ast/yql_expr.h>

namespace NSQLTranslationPG {

TString NormalizeName(TStringBuf name) {
    return NYql::NormalizeName(name);
}

} // namespace NSQLTranslationPG
