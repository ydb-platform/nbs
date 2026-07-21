#pragma once

#include <contrib/ydb/library/yql/ast/yql_ast.h>
#include <contrib/ydb/library/yql/parser/lexer_common/lexer.h>
#include <contrib/ydb/library/yql/parser/lexer_common/hints.h>
#include <contrib/ydb/library/yql/parser/proto_ast/common.h>
#include <contrib/ydb/library/yql/parser/proto_ast/gen/v1_proto_split_antlr4/SQLv1Antlr4Parser.pb.main.h>
#include <contrib/ydb/library/yql/public/issue/yql_warning.h>
#include <contrib/ydb/library/yql/public/issue/yql_issue_manager.h>
#include <contrib/ydb/library/yql/sql/settings/translation_settings.h>
#include <contrib/ydb/library/yql/sql/settings/translator.h>
#include <contrib/ydb/library/yql/sql/v1/lexer/lexer.h>
#include <contrib/ydb/library/yql/sql/v1/proto_parser/proto_parser.h>

#include <google/protobuf/message.h>

namespace NSQLTranslation {
struct TTranslationSettings;
} // namespace NSQLTranslation

namespace NSQLTranslationV1 {

NYql::TAstParseResult SqlToYql(const TLexers& lexers, const TParsers& parsers, const TString& query, const NSQLTranslation::TTranslationSettings& settings, NYql::TWarningRules* warningRules = nullptr);

NYql::TAstParseResult SqlASTToYql(const TLexers& lexers, const TParsers& parsers, const TString& query, const google::protobuf::Message& protoAst, const NSQLTranslation::TSQLHints& hints, const NSQLTranslation::TTranslationSettings& settings);

TVector<NYql::TAstParseResult> SqlToAstStatements(const TLexers& lexers, const TParsers& parsers, const TString& query, const NSQLTranslation::TTranslationSettings& settings, NYql::TWarningRules* warningRules, TVector<NYql::TStmtParseInfo>* stmtParseInfo = nullptr);

bool NeedUseForAllStatements(const NSQLv1Generated::TRule_sql_stmt_core::AltCase& subquery);

bool SplitQueryToStatements(const TLexers& lexers, const TParsers& parsers, const TString& query, TVector<TString>& statements, NYql::TIssues& issues,
                            const NSQLTranslation::TTranslationSettings& settings);

NSQLTranslation::TTranslatorPtr MakeTranslator(const TLexers& lexers, const TParsers& parsers);
} // namespace NSQLTranslationV1
