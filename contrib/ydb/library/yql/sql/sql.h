#pragma once

#include <contrib/ydb/library/yql/parser/lexer_common/hints.h>
#include <contrib/ydb/library/yql/parser/lexer_common/lexer.h>
#include <contrib/ydb/library/yql/parser/proto_ast/common.h>
#include <contrib/ydb/library/yql/public/issue/yql_warning.h>
#include <contrib/ydb/library/yql/public/issue/yql_issue_manager.h>
#include <contrib/ydb/library/yql/ast/yql_ast.h>
#include <contrib/ydb/library/yql/sql/settings/translation_settings.h>
#include <contrib/ydb/library/yql/sql/settings/translator.h>

namespace NSQLTranslation {

struct TTranslators {
    TTranslatorPtr const V0;
    TTranslatorPtr const V1;
    TTranslatorPtr const PG;

    TTranslators(TTranslatorPtr v0, TTranslatorPtr v1, TTranslatorPtr pg);
};

NYql::TAstParseResult SqlToYql(const TTranslators& translators, const TString& query, const TTranslationSettings& settings,
                               NYql::TWarningRules* warningRules = nullptr, NYql::TStmtParseInfo* stmtParseInfo = nullptr,
                               TTranslationSettings* effectiveSettings = nullptr);

google::protobuf::Message* SqlAST(const TTranslators& translators, const TString& query, const TString& queryName, NYql::TIssues& issues, size_t maxErrors,
                                  const TTranslationSettings& settings = {}, ui16* actualSyntaxVersion = nullptr);

ILexer::TPtr SqlLexer(const TTranslators& translators, const TString& query, NYql::TIssues& issues, const TTranslationSettings& settings = {}, ui16* actualSyntaxVersion = nullptr);

NYql::TAstParseResult SqlASTToYql(const TTranslators& translators, const TString& query, const google::protobuf::Message& protoAst, const TSQLHints& hints, const TTranslationSettings& settings);

TVector<NYql::TAstParseResult> SqlToAstStatements(const TTranslators& translators, const TString& query, const TTranslationSettings& settings,
                                                  NYql::TWarningRules* warningRules = nullptr, ui16* actualSyntaxVersion = nullptr, TVector<NYql::TStmtParseInfo>* stmtParseInfo = nullptr);
} // namespace NSQLTranslation
