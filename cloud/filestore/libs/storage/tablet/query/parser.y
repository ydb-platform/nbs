%{
#include "parser.h"

#include <memory>

namespace NCloud::NFileStore::NStorage::NQuery {

////////////////////////////////////////////////////////////////////////////////

static TExpression* BinaryOperatorExpression(
    TBinaryOperatorNode* operatorNode)
{
    auto expression = new TExpression;
    expression->Node = std::move(*operatorNode);
    delete operatorNode;
    return expression;
}

template <typename TPredicateType>
static TBinaryOperatorNode* MakeBinaryOperatorNode(TString column, TValue value)
{
    TPredicateType predicate;
    predicate.Column = std::move(column);
    predicate.Value = std::move(value);
    auto node = new TBinaryOperatorNode;
    node->Operator = std::move(predicate);
    return node;
}

static TExpression* MultiOperatorExpression(TMultiOperatorNode* operatorNode)
{
    auto expression = new TExpression;
    expression->Node = std::move(*operatorNode);
    delete operatorNode;
    return expression;
}

static TExpression* LogicalExpression(
    ELogicalOperator op,
    TExpression* left,
    TExpression* right)
{
    TLogicalExpression logical;
    logical.Operator = op;
    logical.Left.reset(left);
    logical.Right.reset(right);

    auto node = new TMultiOperatorNode;
    node->Operator = std::move(logical);
    return MultiOperatorExpression(node);
}

static void SetError(TParseContext& context, const char* message)
{
    context.Error.Message = message;
    context.Error.Offset = context.CurrentTokenOffset;
}

}   // namespace NCloud::NFileStore::NStorage::NQuery

using namespace NCloud::NFileStore::NStorage::NQuery;
%}

%define api.pure full
%define parse.error verbose

%parse-param { NCloud::NFileStore::NStorage::NQuery::TParseContext& context }
%lex-param { NCloud::NFileStore::NStorage::NQuery::TParseContext& context }

%code requires {
#include <cloud/filestore/libs/storage/tablet/query/query.h>

namespace NCloud::NFileStore::NStorage::NQuery {

struct TParseContext
{
    TSelect Query;
    TParseError Error;
    size_t CurrentTokenOffset = 0;
    size_t ScannedInputBytes = 0;
};

}   // namespace NCloud::NFileStore::NStorage::NQuery
}

%code provides {
int yylex(
    YYSTYPE* value,
    NCloud::NFileStore::NStorage::NQuery::TParseContext& context);
void yyerror(
    NCloud::NFileStore::NStorage::NQuery::TParseContext& context,
    const char* message);
}

%union {
    TString* text;
    ui64 number;
    NCloud::NFileStore::NStorage::NQuery::TValue* value;
    NCloud::NFileStore::NStorage::NQuery::TBinaryOperatorNode* binaryOperator;
    NCloud::NFileStore::NStorage::NQuery::TMultiOperatorNode* multiOperator;
    NCloud::NFileStore::NStorage::NQuery::TExpression* expression;
    TVector<TString>* columns;
    TVector<NCloud::NFileStore::NStorage::NQuery::TValue>* values;
}

%destructor { delete $$; } <text> <value> <binaryOperator> <multiOperator>
%destructor { delete $$; } <expression> <columns> <values>

%token SELECT FROM WHERE AND OR IN SUBSTR LIMIT INVALID
%token EQ NE GT GE LT LE
%token <text> IDENTIFIER STRING
%token <number> NUMBER

%type <columns> columns identifier_list
%type <expression> where_clause expr primary
%type <binaryOperator> binary_predicate
%type <multiOperator> multi_predicate
%type <value> value
%type <values> value_list

%left OR
%left AND

%%

query:
    SELECT columns FROM IDENTIFIER where_clause limit_clause
    {
        context.Query.Columns = std::move(*$2);
        context.Query.Table = std::move(*$4);
        if ($5) {
            context.Query.Where = std::move(*$5);
            delete $5;
        }
        delete $2;
        delete $4;
    }
;

columns:
    '*'
    {
        $$ = new TVector<TString>;
    }
  | identifier_list
    {
        $$ = $1;
    }
;

identifier_list:
    IDENTIFIER
    {
        $$ = new TVector<TString>;
        $$->push_back(std::move(*$1));
        delete $1;
    }
  | identifier_list ',' IDENTIFIER
    {
        $$ = $1;
        $$->push_back(std::move(*$3));
        delete $3;
    }
;

where_clause:
    %empty
    {
        $$ = nullptr;
    }
  | WHERE expr
    {
        $$ = $2;
    }
;

limit_clause:
    %empty
  | LIMIT NUMBER
    {
        context.Query.Limit = $2;
    }
;

expr:
    primary
    {
        $$ = $1;
    }
  | expr AND expr
    {
        $$ = LogicalExpression(ELogicalOperator::And, $1, $3);
    }
  | expr OR expr
    {
        $$ = LogicalExpression(ELogicalOperator::Or, $1, $3);
    }
;

primary:
    binary_predicate
    {
        $$ = BinaryOperatorExpression($1);
    }
  | multi_predicate
    {
        $$ = MultiOperatorExpression($1);
    }
  | '(' expr ')'
    {
        $$ = $2;
    }
;

binary_predicate:
    IDENTIFIER EQ value
    {
        $$ = SingleValuePredicate<TEqualPredicate>(
            std::move(*$1), std::move(*$3));
        delete $1;
        delete $3;
    }
  | IDENTIFIER NE value
    {
        $$ = SingleValuePredicate<TNotEqualPredicate>(
            std::move(*$1), std::move(*$3));
        delete $1;
        delete $3;
    }
  | IDENTIFIER SUBSTR value
    {
        $$ = SingleValuePredicate<TSubstrPredicate>(
            std::move(*$1), std::move(*$3));
        delete $1;
        delete $3;
    }
  | IDENTIFIER GT value
    {
        $$ = SingleValuePredicate<TGreaterPredicate>(
            std::move(*$1), std::move(*$3));
        delete $1;
        delete $3;
    }
  | IDENTIFIER GE value
    {
        $$ = SingleValuePredicate<TGreaterOrEqualPredicate>(
            std::move(*$1), std::move(*$3));
        delete $1;
        delete $3;
    }
  | IDENTIFIER LT value
    {
        $$ = SingleValuePredicate<TLessPredicate>(
            std::move(*$1), std::move(*$3));
        delete $1;
        delete $3;
    }
  | IDENTIFIER LE value
    {
        $$ = SingleValuePredicate<TLessOrEqualPredicate>(
            std::move(*$1), std::move(*$3));
        delete $1;
        delete $3;
    }
;

multi_predicate:
    IDENTIFIER IN '{' value_list '}'
    {
        TInPredicate predicate;
        predicate.Column = std::move(*$1);
        predicate.Values = std::move(*$4);
        $$ = new TMultiOperatorNode;
        $$->Operator = std::move(predicate);
        delete $1;
        delete $4;
    }
;

value_list:
    value
    {
        $$ = new TVector<TValue>;
        $$->push_back(std::move(*$1));
        delete $1;
    }
  | value_list ',' value
    {
        $$ = $1;
        $$->push_back(std::move(*$3));
        delete $3;
    }
;

value:
    NUMBER
    {
        $$ = new TValue($1);
    }
  | STRING
    {
        $$ = new TValue(std::move(*$1));
        delete $1;
    }
  | IDENTIFIER
    {
        $$ = new TValue(TColumnRef{std::move(*$1)});
        delete $1;
    }
;

%%

void yyerror(
    NCloud::NFileStore::NStorage::NQuery::TParseContext& context,
    const char* message)
{
    NCloud::NFileStore::NStorage::NQuery::SetError(context, message);
}
