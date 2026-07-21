#pragma once

#include <contrib/ydb/library/yql/parser/proto_ast/common.h>
#include <contrib/ydb/library/yql/public/issue/yql_warning.h>
#include <contrib/ydb/library/yql/public/issue/yql_issue_manager.h>
#include <contrib/ydb/library/yql/sql/settings/translation_settings.h>

#include <google/protobuf/message.h>

namespace NSQLTranslation {
struct TTranslationSettings;
} // namespace NSQLTranslation

namespace NSQLTranslationV1 {

struct TParsers {
    NSQLTranslation::TParserFactoryPtr Antlr4;
    NSQLTranslation::TParserFactoryPtr Antlr4Ansi;
};

google::protobuf::Message* SqlAST(const TParsers& parsers, const TString& query, const TString& queryName,
                                  NYql::TIssues& err, size_t maxErrors, bool ansiLexer, google::protobuf::Arena* arena);

google::protobuf::Message* SqlAST(const TParsers& parsers, const TString& query, const TString& queryName,
                                  NAST::IErrorCollector& err, bool ansiLexer, google::protobuf::Arena* arena);

// TODO(YQL-19017): remove.
google::protobuf::Message* SqlAST(const TParsers& parsers, const TString& query, const TString& queryName,
                                  NYql::TIssues& err, size_t maxErrors, bool ansiLexer, bool antlr4, google::protobuf::Arena* arena);

// TODO(YQL-19017): remove.
google::protobuf::Message* SqlAST(const TParsers& parsers, const TString& query, const TString& queryName,
                                  NAST::IErrorCollector& err, bool ansiLexer, bool antlr4, google::protobuf::Arena* arena);

} // namespace NSQLTranslationV1
