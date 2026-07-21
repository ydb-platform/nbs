#pragma once
#include "translation_settings.h"

#include <contrib/ydb/library/yql/parser/lexer_common/lexer.h>
#include <contrib/ydb/library/yql/parser/lexer_common/hints.h>
#include <contrib/ydb/library/yql/public/issue/yql_warning.h>
#include <contrib/ydb/library/yql/ast/yql_ast.h>

namespace NSQLTranslation {

class ITranslator: public TThrRefBase {
public:
    ~ITranslator() override = default;

    virtual ILexer::TPtr MakeLexer(const TTranslationSettings& settings) = 0;
    virtual NYql::TAstParseResult TextToAst(const TString& query, const TTranslationSettings& settings,
                                            NYql::TWarningRules* warningRules, NYql::TStmtParseInfo* stmtParseInfo) = 0;
    virtual google::protobuf::Message* TextToMessage(const TString& query, const TString& queryName,
                                                     NYql::TIssues& issues, size_t maxErrors, const TTranslationSettings& settings) = 0;
    virtual NYql::TAstParseResult TextAndMessageToAst(const TString& query, const google::protobuf::Message& protoAst,
                                                      const TSQLHints& hints, const TTranslationSettings& settings) = 0;
    virtual TVector<NYql::TAstParseResult> TextToManyAst(const TString& query, const TTranslationSettings& settings,
                                                         NYql::TWarningRules* warningRules, TVector<NYql::TStmtParseInfo>* stmtParseInfo) = 0;
};

using TTranslatorPtr = TIntrusivePtr<ITranslator>;

TTranslatorPtr MakeDummyTranslator(const TString& name);

} // namespace NSQLTranslation
