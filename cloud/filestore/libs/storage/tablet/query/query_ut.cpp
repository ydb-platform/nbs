#include <cloud/filestore/libs/storage/tablet/query/query.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage::NQuery {

////////////////////////////////////////////////////////////////////////////////

namespace {

const TBinaryOperator& AsBinaryOperator(const TExpression& expression)
{
    return std::get<TBinaryOperatorNode>(expression.Node).Operator;
}

const TLogicalExpression& AsLogical(const TExpression& expression)
{
    const auto& multi = std::get<TMultiOperatorNode>(expression.Node);
    return std::get<TLogicalExpression>(multi.Operator);
}

const TInPredicate& AsInPredicate(const TExpression& expression)
{
    const auto& multi = std::get<TMultiOperatorNode>(expression.Node);
    return std::get<TInPredicate>(multi.Operator);
}

const TString& PredicateColumn(const TBinaryOperator& predicate)
{
    return std::visit(
        [](const auto& value) -> const TString& { return value.Column; },
        predicate);
}

const TValue& PredicateValue(const TBinaryOperator& predicate)
{
    return std::visit(
        [](const auto& value) -> const TValue& {
            return value.Value;
        },
        predicate);
}

TSelect ParseQuery(TStringBuf input)
{
    auto result = Parse(input);
    UNIT_ASSERT_C(!HasError(result.GetError()), result.GetError().GetMessage());
    return result.ExtractResult();
}

NProto::TError GetParseError(TStringBuf input)
{
    auto result = Parse(input);
    UNIT_ASSERT(HasError(result.GetError()));
    return result.GetError();
}

}   // namespace

Y_UNIT_TEST_SUITE(TQueryParserTest)
{
    Y_UNIT_TEST(ShouldParseSelectAll)
    {
        auto query = ParseQuery(
            "SELECT * FROM NodeRefs WHERE child_id IN {1, 2, 3} LIMIT 10");

        UNIT_ASSERT_VALUES_EQUAL(query.Table, "NodeRefs");
        UNIT_ASSERT(query.Columns.empty());
        UNIT_ASSERT(query.Where);
        UNIT_ASSERT(std::holds_alternative<TMultiOperatorNode>(
            query.Where->Node));
        const auto& predicate = AsInPredicate(*query.Where);
        UNIT_ASSERT_VALUES_EQUAL("child_id", predicate.Column);
        UNIT_ASSERT_VALUES_EQUAL(3, predicate.Values.size());
        UNIT_ASSERT_VALUES_EQUAL(10, *query.Limit);
    }

    Y_UNIT_TEST(ShouldParseProjectionAndString)
    {
        auto query = ParseQuery(
            "select name, child_id from NodeRefs "
            "where name = 'hello' and node_id = 42");

        UNIT_ASSERT_VALUES_EQUAL(query.Columns.size(), 2);
        UNIT_ASSERT(query.Where);
        const auto& logical = AsLogical(*query.Where);
        UNIT_ASSERT(logical.Left);
        UNIT_ASSERT(logical.Right);
        UNIT_ASSERT(logical.Operator == ELogicalOperator::And);
        UNIT_ASSERT(
            std::holds_alternative<TString>(
                PredicateValue(AsBinaryOperator(*logical.Left))));
    }

    Y_UNIT_TEST(ShouldRejectEmptyIn)
    {
        const auto error =
            GetParseError("SELECT * FROM NodeRefs WHERE child_id IN {}");
        UNIT_ASSERT(!error.GetMessage().empty());
    }

    Y_UNIT_TEST(ShouldRejectUnknownCharacters)
    {
        const auto error =
            GetParseError("SELECT * FROM NodeRefs @ WHERE child_id = 1");
        UNIT_ASSERT(!error.GetMessage().empty());
    }

    Y_UNIT_TEST(ShouldRejectUnterminatedString)
    {
        const auto error =
            GetParseError("SELECT * FROM NodeRefs WHERE name = 'hello");
        UNIT_ASSERT(!error.GetMessage().empty());
    }

    Y_UNIT_TEST(ShouldRejectNumericOverflow)
    {
        auto error = GetParseError(
            "SELECT * FROM NodeRefs WHERE child_id = "
            "18446744073709551616");
        UNIT_ASSERT(!error.GetMessage().empty());

        error = GetParseError(
            "SELECT * FROM NodeRefs LIMIT 18446744073709551616");
        UNIT_ASSERT(!error.GetMessage().empty());
    }

    Y_UNIT_TEST(ShouldParseEscapedStringsAndLimit)
    {
        auto query = ParseQuery(
            "select name from NodeRefs where name = 'it\\'s' limit 0");

        UNIT_ASSERT_VALUES_EQUAL(1, query.Columns.size());
        UNIT_ASSERT(query.Where);
        UNIT_ASSERT_VALUES_EQUAL(
            "it's",
            std::get<TString>(PredicateValue(AsBinaryOperator(*query.Where))));
        UNIT_ASSERT_VALUES_EQUAL(*query.Limit, 0);
    }

    Y_UNIT_TEST(ShouldParsePrecedenceAndColumnReferences)
    {
        auto query = ParseQuery(
            "SELECT name FROM NodeRefs "
            "WHERE a == b OR c != 'x' AND d SUBSTR \"y\"");

        UNIT_ASSERT(query.Where);
        const auto& logical = AsLogical(*query.Where);
        UNIT_ASSERT(logical.Operator == ELogicalOperator::Or);
        const auto& equal = AsBinaryOperator(*logical.Left);
        UNIT_ASSERT(std::holds_alternative<TEqualPredicate>(equal));
        UNIT_ASSERT_VALUES_EQUAL(
            "b",
            std::get<TColumnRef>(PredicateValue(equal)).Name);
        const auto& andExpression = AsLogical(*logical.Right);
        UNIT_ASSERT(andExpression.Operator == ELogicalOperator::And);
        UNIT_ASSERT(std::holds_alternative<TNotEqualPredicate>(
            AsBinaryOperator(*andExpression.Left)));
        UNIT_ASSERT(std::holds_alternative<TSubstrPredicate>(
            AsBinaryOperator(*andExpression.Right)));
    }

    Y_UNIT_TEST(ShouldParseNestedParentheses)
    {
        auto query = ParseQuery(
            "SELECT * FROM NodeRefs WHERE "
            "((a == b OR c != 'x') AND d SUBSTR 'y') LIMIT 5");

        UNIT_ASSERT(query.Where);
        const auto& logical = AsLogical(*query.Where);
        UNIT_ASSERT(logical.Operator == ELogicalOperator::And);
        UNIT_ASSERT(AsLogical(*logical.Left).Operator == ELogicalOperator::Or);
        UNIT_ASSERT_VALUES_EQUAL(5, *query.Limit);
    }

    Y_UNIT_TEST(ShouldParseComparisonOperators)
    {
        const auto assertComparison = [] (TStringBuf op, auto expectedTag) {
            auto query = ParseQuery(
                TString("SELECT * FROM t WHERE a ") + TString(op) + " 42");

            UNIT_ASSERT(query.Where);
            const auto& predicate = AsBinaryOperator(*query.Where);
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

    Y_UNIT_TEST(ShouldNotTreatKeywordsInsideIdentifiersAsOperators)
    {
        auto candy = ParseQuery("SELECT a FROM t WHERE a == candy");
        UNIT_ASSERT(candy.Where);
        const auto& equal = AsBinaryOperator(*candy.Where);
        UNIT_ASSERT(std::holds_alternative<TEqualPredicate>(equal));
        UNIT_ASSERT_VALUES_EQUAL(
            "candy",
            std::get<TColumnRef>(PredicateValue(equal)).Name);

        auto logical =
            ParseQuery("SELECT a FROM t WHERE a == floor OR  c == d");
        UNIT_ASSERT(logical.Where);
        const auto& logicalExpression = AsLogical(*logical.Where);
        UNIT_ASSERT(logicalExpression.Operator == ELogicalOperator::Or);
        UNIT_ASSERT_VALUES_EQUAL(
            "floor",
            std::get<TColumnRef>(
                PredicateValue(AsBinaryOperator(*logicalExpression.Left))).Name);
        UNIT_ASSERT_VALUES_EQUAL(
            "c",
            PredicateColumn(AsBinaryOperator(*logicalExpression.Right)));
    }

    Y_UNIT_TEST(ShouldRejectMalformedQueries)
    {
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
            const auto error = GetParseError(input);
            UNIT_ASSERT_C(!error.GetMessage().empty(), input);
        }
    }

    Y_UNIT_TEST(ShouldReportParseErrorOffset)
    {
        const TString unexpectedToken =
            "SELECT * FROM NodeRefs @ WHERE child_id = 1";
        const auto tokenError = GetParseError(unexpectedToken);
        const TString tokenOffset = TStringBuilder()
            << "at byte " << unexpectedToken.find('@');
        UNIT_ASSERT(tokenError.GetMessage().find(tokenOffset.c_str()) !=
                    std::string::npos);

        const TString unexpectedEof =
            "SELECT * FROM NodeRefs WHERE child_id =";
        const auto eofError = GetParseError(unexpectedEof);
        const TString eofOffset = TStringBuilder()
            << "at byte " << unexpectedEof.size();
        UNIT_ASSERT(eofError.GetMessage().find(eofOffset.c_str()) !=
                    std::string::npos);
    }
}

}   // namespace NCloud::NFileStore::NStorage::NQuery
