#pragma once

#include <contrib/ydb/library/yql/sql/v1/reflect/sql_reflect.h>

#include <util/generic/hash.h>

namespace NSQLTranslationV1 {

// Makes regexes only for tokens from OtherNames,
// as keywords and punctuation are trivially matched.
TVector<std::tuple<TString, TString>> MakeRegexByOtherName(
    const NSQLReflect::TLexerGrammar& grammar, bool ansi);

} // namespace NSQLTranslationV1
