%{
#include <cloud/filestore/libs/storage/tablet/query/query.h>

#include <memory>

namespace NCloud::NFileStore::NStorage::NQuery {

////////////////////////////////////////////////////////////////////////////////

struct TParseContext
{
    TSelect Query;
    TString Error;
};

extern TParseContext CurrentParseContext;

static TExpression* PredicateExpression(TPredicate* predicate)
{
    auto expression = new TExpression;
    expression->Node = std::move(*predicate);
    delete predicate;
    return expression;
}

template <typename TPredicateType>
static TPredicate* SingleValuePredicate(TString column, TValue value)
{
    TPredicateType predicate;
    predicate.Column = std::move(column);
    predicate.Value = std::move(value);
    return new TPredicate(std::move(predicate));
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

    auto expression = new TExpression;
    expression->Node = std::move(logical);
    return expression;
}

static void SetError(const char* message)
{
    CurrentParseContext.Error = message;
}

}   // namespace NCloud::NFileStore::NStorage::NQuery

using namespace NCloud::NFileStore::NStorage::NQuery;
%}

%define parse.error verbose

%code requires {
#include <cloud/filestore/libs/storage/tablet/query/query.h>
}

%code provides {
int yylex(void);
void yyerror(const char* message);
}

%union {
    TString* text;
    ui64 number;
    NCloud::NFileStore::NStorage::NQuery::TValue* value;
    NCloud::NFileStore::NStorage::NQuery::TPredicate* predicate;
    NCloud::NFileStore::NStorage::NQuery::TExpression* expression;
    TVector<TString>* columns;
    TVector<NCloud::NFileStore::NStorage::NQuery::TValue>* values;
}

%token SELECT FROM WHERE AND OR IN SUBSTR LIMIT INVALID
%token EQ NE GT GE LT LE
%token <text> IDENTIFIER STRING
%token <number> NUMBER

%type <columns> columns identifier_list
%type <expression> where_clause expr primary
%type <predicate> predicate
%type <value> value
%type <values> value_list

%left OR
%left AND

%%

query:
    SELECT columns FROM IDENTIFIER where_clause limit_clause
    {
        CurrentParseContext.Query.Columns = std::move(*$2);
        CurrentParseContext.Query.Table = std::move(*$4);
        if ($5) {
            CurrentParseContext.Query.Where = std::move(*$5);
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
        CurrentParseContext.Query.Limit = $2;
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
    predicate
    {
        $$ = PredicateExpression($1);
    }
  | '(' expr ')'
    {
        $$ = $2;
    }
;

predicate:
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
  | IDENTIFIER IN '{' value_list '}'
    {
        TInPredicate predicate;
        predicate.Column = std::move(*$1);
        predicate.Values = std::move(*$4);
        $$ = new TPredicate(std::move(predicate));
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

void yyerror(const char* message)
{
    NCloud::NFileStore::NStorage::NQuery::SetError(message);
}
