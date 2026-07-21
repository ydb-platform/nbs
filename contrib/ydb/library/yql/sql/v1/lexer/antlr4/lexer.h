#pragma once
#include <contrib/ydb/library/yql/parser/lexer_common/lexer.h>

namespace NSQLTranslationV1 {

NSQLTranslation::TLexerFactoryPtr MakeAntlr4LexerFactory();

} // namespace NSQLTranslationV1
