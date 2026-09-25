#include <cloud/filestore/libs/storage/query/parser.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage::NQuery {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TQueryParserTest)
{
    Y_UNIT_TEST(ShouldParseSelectAll)
    {
        auto query = Parse(
            "SELECT * FROM NodeRefs WHERE child_id IN {1, 2, 3} LIMIT 10");

        UNIT_ASSERT(query);
        UNIT_ASSERT_VALUES_EQUAL(query->Table, "NodeRefs");
        UNIT_ASSERT(query->Columns.empty());
        UNIT_ASSERT(query->Where);
        UNIT_ASSERT(query->Where->Kind == TExpression::EKind::Predicate);
        UNIT_ASSERT_VALUES_EQUAL("child_id", query->Where->Predicate.Column);
        UNIT_ASSERT_VALUES_EQUAL(3, query->Where->Predicate.Values.size());
        UNIT_ASSERT_VALUES_EQUAL(10, *query->Limit);
    }

    Y_UNIT_TEST(ShouldParseProjectionAndString)
    {
        auto query = Parse(
            "select name, child_id from NodeRefs "
            "where name = 'hello' and node_id = 42");

        UNIT_ASSERT(query);
        UNIT_ASSERT_VALUES_EQUAL(query->Columns.size(), 2);
        UNIT_ASSERT(query->Where);
        UNIT_ASSERT(query->Where->Kind == TExpression::EKind::Logical);
        UNIT_ASSERT(query->Where->Left);
        UNIT_ASSERT(query->Where->Right);
        UNIT_ASSERT(query->Where->Operator == ELogicalOperator::And);
        UNIT_ASSERT(
            std::holds_alternative<TString>(
                query->Where->Left->Predicate.Values[0]));
    }

    Y_UNIT_TEST(ShouldRejectEmptyIn)
    {
        TParseError error;
        UNIT_ASSERT(
            !Parse("SELECT * FROM NodeRefs WHERE child_id IN {}", &error));
        UNIT_ASSERT(!error.Message.empty());
    }

    Y_UNIT_TEST(ShouldRejectUnknownCharacters)
    {
        TParseError error;
        UNIT_ASSERT(
            !Parse("SELECT * FROM NodeRefs @ WHERE child_id = 1", &error));
        UNIT_ASSERT(!error.Message.empty());
    }

    Y_UNIT_TEST(ShouldRejectUnterminatedString)
    {
        TParseError error;
        UNIT_ASSERT(
            !Parse("SELECT * FROM NodeRefs WHERE name = 'hello", &error));
        UNIT_ASSERT(!error.Message.empty());
    }

    Y_UNIT_TEST(ShouldRejectNumericOverflow)
    {
        TParseError error;
        UNIT_ASSERT(!Parse(
            "SELECT * FROM NodeRefs WHERE child_id = "
            "18446744073709551616",
            &error));
        UNIT_ASSERT(!error.Message.empty());

        UNIT_ASSERT(!Parse(
            "SELECT * FROM NodeRefs LIMIT 18446744073709551616",
            &error));
        UNIT_ASSERT(!error.Message.empty());
    }

    Y_UNIT_TEST(ShouldParseEscapedStringsAndLimit)
    {
        auto query =
            Parse("select name from NodeRefs where name = 'it\\'s' limit 0");

        UNIT_ASSERT(query);
        UNIT_ASSERT_VALUES_EQUAL(1, query->Columns.size());
        UNIT_ASSERT(query->Where);
        UNIT_ASSERT(query->Where->Kind == TExpression::EKind::Predicate);
        UNIT_ASSERT_VALUES_EQUAL(
            "it's",
            std::get<TString>(query->Where->Predicate.Values[0]));
        UNIT_ASSERT_VALUES_EQUAL(*query->Limit, 0);
    }

    Y_UNIT_TEST(ShouldParsePrecedenceAndColumnReferences)
    {
        auto query = Parse(
            "SELECT name FROM NodeRefs "
            "WHERE a == b OR c != 'x' AND d SUBSTR \"y\"");

        UNIT_ASSERT(query);
        UNIT_ASSERT(query->Where);
        UNIT_ASSERT(query->Where->Kind == TExpression::EKind::Logical);
        UNIT_ASSERT(query->Where->Operator == ELogicalOperator::Or);
        UNIT_ASSERT(query->Where->Left->Predicate.Operator == EOperator::Equal);
        UNIT_ASSERT_VALUES_EQUAL(
            "b",
            std::get<TColumnRef>(query->Where->Left->Predicate.Values[0]).Name);
        UNIT_ASSERT(query->Where->Right->Operator == ELogicalOperator::And);
        UNIT_ASSERT(
            query->Where->Right->Left->Predicate.Operator ==
            EOperator::NotEqual);
        UNIT_ASSERT(
            query->Where->Right->Right->Predicate.Operator ==
            EOperator::Substr);
    }

    Y_UNIT_TEST(ShouldParseNestedParentheses)
    {
        auto query = Parse(
            "SELECT * FROM NodeRefs WHERE "
            "((a == b OR c != 'x') AND d SUBSTR 'y') LIMIT 5");

        UNIT_ASSERT(query);
        UNIT_ASSERT(query->Where);
        UNIT_ASSERT(query->Where->Operator == ELogicalOperator::And);
        UNIT_ASSERT(query->Where->Left->Operator == ELogicalOperator::Or);
        UNIT_ASSERT_VALUES_EQUAL(5, *query->Limit);
    }

    Y_UNIT_TEST(ShouldRejectMalformedQueries)
    {
        TParseError error;
        for (const auto* input:
             {
                 "SELECT * FROM NodeRefs WHERE",
                 "SELECT * FROM NodeRefs WHERE child_id IN {1,}",
                 "SELECT * FROM NodeRefs WHERE child_id = 1 AND OR name = 'x'",
                 "SELECT * FROM NodeRefs WHERE (child_id == other_id",
                 "SELECT * FROM NodeRefs WHERE child_id == other_id)",
                 "SELECT * FROM NodeRefs WHERE ()",
                 "SELECT * FROM NodeRefs LIMIT -1",
                 "SELECT * FROM NodeRefs LIMIT 1 trailing",
             })
        {
            UNIT_ASSERT_C(!Parse(input, &error), input);
            UNIT_ASSERT_C(!error.Message.empty(), input);
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage::NQuery
