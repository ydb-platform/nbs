#pragma once

#include "context.h"
#include "node.h"

#include <contrib/ydb/library/yql/sql/settings/translation_settings.h>
#include <contrib/ydb/library/yql/sql/v1/sql_translation.h>

#include <contrib/ydb/library/yql/parser/proto_ast/gen/v1_proto_split_antlr4/SQLv1Antlr4Parser.pb.main.h>

namespace NSQLTranslationV1 {

NYql::TLangVersion YqlSelectLangVersion();

std::unexpected<ESQLError> YqlSelectUnsupported(TContext& ctx, TStringBuf message);

TNodeResult BuildYqlSelectStatement(
    TSqlTranslation& that,
    const NSQLv1Generated::TRule_select_stmt& rule);

TNodeResult BuildYqlSelectStatement(
    TSqlTranslation& that,
    const NSQLv1Generated::TRule_values_stmt& rule);

TNodeResult BuildYqlSelectSubExpr(
    TSqlTranslation& that,
    const NSQLv1Generated::TRule_select_subexpr& rule,
    EColumnRefState state,
    ESmartParenthesis smartParenthesis);

TNodeResult BuildYqlSelectSubExpr(
    TSqlTranslation& that,
    const NSQLv1Generated::TRule_select_unparenthesized_stmt& rule);

TNodeResult BuildYqlExists(
    TSqlTranslation& that,
    const NSQLv1Generated::TRule_exists_expr& rule);

} // namespace NSQLTranslationV1
