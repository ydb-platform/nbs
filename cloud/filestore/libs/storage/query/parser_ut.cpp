#include <cloud/filestore/libs/storage/query/parser.h>

#include <library/cpp/testing/unittest/registar.h>

#include <type_traits>

namespace NCloud::NFileStore::NStorage::NQuery {

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString& PredicateColumn(const TPredicate& predicate)
{
    return std::visit(
        [](const auto& pred) -> const TString& { return pred.Column; },
        predicate);
}

const TValue& PredicateValue(const TPredicate& predicate, size_t index = 0)
{
    return std::visit(
        [index](const auto& value) -> const TValue&
        {
            if constexpr (
                std::is_same_v<std::decay_t<decltype(value)>, TInPredicate>)
            {
                return value.Values[index];
            } else {
                return value.Value;
            }
        },
        predicate);
}

size_t PredicateValuesSize(const TPredicate& predicate)
{
    return std::visit(
        [](const auto& value) -> size_t
        {
            if constexpr (
                std::is_same_v<std::decay_t<decltype(value)>, TInPredicate>)
            {
                return value.Values.size();
            } else {
                return 1;
            }
        },
        predicate);
}

}   // namespace

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
        UNIT_ASSERT(std::holds_alternative<TPredicate>(query->Where->Node));
        UNIT_ASSERT_VALUES_EQUAL(
            "child_id",
            PredicateColumn(std::get<TPredicate>(query->Where->Node)));
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            PredicateValuesSize(std::get<TPredicate>(query->Where->Node)));
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
        UNIT_ASSERT(
            std::holds_alternative<TLogicalExpression>(query->Where->Node));
        UNIT_ASSERT(std::get<TLogicalExpression>(query->Where->Node).Left);
        UNIT_ASSERT(std::get<TLogicalExpression>(query->Where->Node).Right);
        UNIT_ASSERT(
            std::get<TLogicalExpression>(query->Where->Node).Operator ==
            ELogicalOperator::And);
        UNIT_ASSERT(
            std::holds_alternative<TString>(PredicateValue(
                std::get<TPredicate>(
                    std::get<TLogicalExpression>(query->Where->Node)
                        .Left->Node))));
    }

    Y_UNIT_TEST(ShouldParseComparisonOperators)
    {
        const auto assertComparison = [] (TStringBuf op, auto expectedTag) {
            auto query = Parse(
                TString("SELECT * FROM t WHERE a ") + TString(op) + " 42");

            UNIT_ASSERT(query);
            UNIT_ASSERT(query->Where);
            UNIT_ASSERT(
                std::holds_alternative<TPredicate>(query->Where->Node));

            const auto& predicate = std::get<TPredicate>(query->Where->Node);
            UNIT_ASSERT(
                std::holds_alternative<decltype(expectedTag)>(predicate));
            UNIT_ASSERT_VALUES_EQUAL("a", PredicateColumn(predicate));
            UNIT_ASSERT_VALUES_EQUAL(
                42,
                std::get<ui64>(PredicateValue(predicate)));
        };

        assertComparison(">", TGreaterPredicate{});
        assertComparison(">=", TGreaterOrEqualPredicate{});
        assertComparison("<", TLessPredicate{});
        assertComparison("<=", TLessOrEqualPredicate{});
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
        UNIT_ASSERT(std::holds_alternative<TPredicate>(query->Where->Node));
        UNIT_ASSERT_VALUES_EQUAL(
            "it's",
            std::get<TString>(
                PredicateValue(std::get<TPredicate>(query->Where->Node))));
        UNIT_ASSERT_VALUES_EQUAL(*query->Limit, 0);
    }

    Y_UNIT_TEST(ShouldParsePrecedenceAndColumnReferences)
    {
        auto query = Parse(
            "SELECT name FROM NodeRefs "
            "WHERE a == b OR c != 'x' AND d SUBSTR \"y\"");

        UNIT_ASSERT(query);
        UNIT_ASSERT(query->Where);
        UNIT_ASSERT(
            std::holds_alternative<TLogicalExpression>(query->Where->Node));
        UNIT_ASSERT(
            std::get<TLogicalExpression>(query->Where->Node).Operator ==
            ELogicalOperator::Or);
        UNIT_ASSERT(
            std::holds_alternative<TEqualPredicate>(std::get<TPredicate>(
                std::get<TLogicalExpression>(query->Where->Node).Left->Node)));
        UNIT_ASSERT_VALUES_EQUAL(
            "b",
            std::get<TColumnRef>(
                PredicateValue(
                    std::get<TPredicate>(
                        std::get<TLogicalExpression>(query->Where->Node)
                            .Left->Node)))
                .Name);
        UNIT_ASSERT(
            std::get<TLogicalExpression>(
                std::get<TLogicalExpression>(query->Where->Node).Right->Node)
                .Operator == ELogicalOperator::And);
        UNIT_ASSERT(
            std::holds_alternative<TNotEqualPredicate>(std::get<TPredicate>(
                std::get<TLogicalExpression>(
                    std::get<TLogicalExpression>(query->Where->Node)
                        .Right->Node)
                    .Left->Node)));
        UNIT_ASSERT(
            std::holds_alternative<TSubstrPredicate>(std::get<TPredicate>(
                std::get<TLogicalExpression>(
                    std::get<TLogicalExpression>(query->Where->Node)
                        .Right->Node)
                    .Right->Node)));
    }

    Y_UNIT_TEST(ShouldParseNestedParentheses)
    {
        auto query = Parse(
            "SELECT * FROM NodeRefs WHERE "
            "((a == b OR c != 'x') AND d SUBSTR 'y') LIMIT 5");

        UNIT_ASSERT(query);
        UNIT_ASSERT(query->Where);
        UNIT_ASSERT(
            std::get<TLogicalExpression>(query->Where->Node).Operator ==
            ELogicalOperator::And);
        UNIT_ASSERT(
            std::get<TLogicalExpression>(
                std::get<TLogicalExpression>(query->Where->Node).Left->Node)
                .Operator == ELogicalOperator::Or);
        UNIT_ASSERT_VALUES_EQUAL(5, *query->Limit);
    }

    Y_UNIT_TEST(ShouldNotTreatKeywordsInsideIdentifiersAsOperators)
    {
        auto candy = Parse("SELECT a FROM t WHERE a == candy");
        UNIT_ASSERT(candy);
        UNIT_ASSERT(candy->Where);
        UNIT_ASSERT(std::holds_alternative<TPredicate>(candy->Where->Node));
        UNIT_ASSERT_VALUES_EQUAL(
            "candy",
            std::get<TColumnRef>(
                PredicateValue(std::get<TPredicate>(candy->Where->Node)))
                .Name);

        auto logical = Parse("SELECT a FROM t WHERE a == floor OR  c == d");
        UNIT_ASSERT(logical);
        UNIT_ASSERT(logical->Where);
        UNIT_ASSERT(
            std::holds_alternative<TLogicalExpression>(logical->Where->Node));
        UNIT_ASSERT(
            std::get<TLogicalExpression>(logical->Where->Node).Operator ==
            ELogicalOperator::Or);
        UNIT_ASSERT_VALUES_EQUAL(
            "floor",
            std::get<TColumnRef>(
                PredicateValue(
                    std::get<TPredicate>(
                        std::get<TLogicalExpression>(logical->Where->Node)
                            .Left->Node)))
                .Name);
        UNIT_ASSERT_VALUES_EQUAL(
            "c",
            PredicateColumn(
                std::get<TPredicate>(
                    std::get<TLogicalExpression>(logical->Where->Node)
                        .Right->Node)));
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
