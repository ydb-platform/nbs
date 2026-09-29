%{
#include "parser.h"

#include <memory>

namespace NCloud::NFileStore::NClient::NQuery {

struct TParseContext
{
    TSelect Query;
    TString Error;
};

extern TParseContext* CurrentParseContext;

static TExpression* PredicateExpression(TCondition* condition)
{
    auto expression = new TExpression;
    expression->Predicate = std::move(*condition);
    delete condition;
    return expression;
}

static TExpression* LogicalExpression(
    ELogicalOperator op,
    TExpression* left,
    TExpression* right)
{
    auto expression = new TExpression;
    expression->Kind = TExpression::EKind::Logical;
    expression->Operator = op;
    expression->Left.reset(left);
    expression->Right.reset(right);
    return expression;
}

static void SetError(const char* message)
{
    if (CurrentParseContext) {
        CurrentParseContext->Error = message;
    }
}

}   // namespace NCloud::NFileStore::NClient::NQuery

using namespace NCloud::NFileStore::NClient::NQuery;
%}

%defines "parser_generated.h"
%define parse.error verbose

%code requires {
#include "parser.h"
}

%code provides {
int yylex(void);
void yyerror(const char* message);
}

%union {
    TString* text;
    ui64 number;
    NCloud::NFileStore::NClient::NQuery::TValue* value;
    NCloud::NFileStore::NClient::NQuery::TCondition* condition;
    NCloud::NFileStore::NClient::NQuery::TExpression* expression;
    TVector<TString>* columns;
    TVector<NCloud::NFileStore::NClient::NQuery::TValue>* values;
}

%token SELECT FROM WHERE AND OR IN SUBSTR LIMIT INVALID
%token EQ NE
%token <text> IDENTIFIER STRING
%token <number> NUMBER

%type <columns> columns identifier_list
%type <expression> where_clause expr primary
%type <condition> predicate
%type <value> value
%type <values> value_list

%left OR
%left AND

%%

query:
    SELECT columns FROM IDENTIFIER where_clause limit_clause
    {
        CurrentParseContext->Query.Columns = std::move(*$2);
        CurrentParseContext->Query.Table = std::move(*$4);
        CurrentParseContext->Query.Where.reset($5);
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
        CurrentParseContext->Query.Limit = $2;
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
        CurrentParseContext->Query.Conditions.push_back(*$1);
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
        $$ = new TCondition;
        $$->Column = std::move(*$1);
        $$->Predicate = EPredicate::Equal;
        $$->Values.push_back(std::move(*$3));
        delete $1;
        delete $3;
    }
  | IDENTIFIER NE value
    {
        $$ = new TCondition;
        $$->Column = std::move(*$1);
        $$->Predicate = EPredicate::NotEqual;
        $$->Values.push_back(std::move(*$3));
        delete $1;
        delete $3;
    }
  | IDENTIFIER SUBSTR value
    {
        $$ = new TCondition;
        $$->Column = std::move(*$1);
        $$->Predicate = EPredicate::Substr;
        $$->Values.push_back(std::move(*$3));
        delete $1;
        delete $3;
    }
  | IDENTIFIER IN '{' value_list '}'
    {
        $$ = new TCondition;
        $$->Column = std::move(*$1);
        $$->Predicate = EPredicate::In;
        $$->Values = std::move(*$4);
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
    NCloud::NFileStore::NClient::NQuery::SetError(message);
}
