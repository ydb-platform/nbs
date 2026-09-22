#include "query.h"

#include <util/string/cast.h>

#include <cctype>

namespace NCloud::NFileStore::NStorage::NQuery {

namespace {

////////////////////////////////////////////////////////////////////////////////

enum class EToken
{
    End,
    Word,
    Number,
    String,
    Comma,
    Equal,
    Star,
    OpenBrace,
    CloseBrace,
};

struct TToken
{
    EToken Type = EToken::End;
    TString Text;
    size_t Offset = 0;
};

class TLexer
{
private:
    TStringBuf Input;
    size_t Position = 0;

public:
    explicit TLexer(TStringBuf input)
        : Input(input)
    {}

    TToken Next()
    {
        while (Position < Input.size() &&
               std::isspace(static_cast<unsigned char>(Input[Position])))
        {
            ++Position;
        }

        const size_t offset = Position;
        if (Position == Input.size()) {
            return {EToken::End, {}, offset};
        }

        const char ch = Input[Position++];
        switch (ch) {
            case ',':
                return {EToken::Comma, {}, offset};
            case '=':
                return {EToken::Equal, {}, offset};
            case '*':
                return {EToken::Star, {}, offset};
            case '{':
                return {EToken::OpenBrace, {}, offset};
            case '}':
                return {EToken::CloseBrace, {}, offset};
            case '\'':
            case '"': {
                const char quote = ch;
                TString value;
                while (Position < Input.size() && Input[Position] != quote) {
                    if (Input[Position] == '\\' && Position + 1 < Input.size())
                    {
                        ++Position;
                    }
                    value += Input[Position++];
                }
                if (Position == Input.size()) {
                    return {EToken::String, {}, offset};
                }
                ++Position;
                return {EToken::String, std::move(value), offset};
            }
            default:
                if (std::isdigit(static_cast<unsigned char>(ch))) {
                    while (Position < Input.size() &&
                           std::isdigit(
                               static_cast<unsigned char>(Input[Position])))
                    {
                        ++Position;
                    }
                    return {
                        EToken::Number,
                        TString(Input.substr(offset, Position - offset)),
                        offset};
                }
                if (std::isalpha(static_cast<unsigned char>(ch)) || ch == '_') {
                    while (Position < Input.size() &&
                           (std::isalnum(
                                static_cast<unsigned char>(Input[Position])) ||
                            Input[Position] == '_'))
                    {
                        ++Position;
                    }
                    return {
                        EToken::Word,
                        TString(Input.substr(offset, Position - offset)),
                        offset};
                }
                return {EToken::End, {}, offset};
        }
    }
};

bool EqualsIgnoreCase(TStringBuf lhs, TStringBuf rhs)
{
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (size_t i = 0; i < lhs.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(lhs[i])) !=
            std::tolower(static_cast<unsigned char>(rhs[i])))
        {
            return false;
        }
    }
    return true;
}

class TParser
{
private:
    TLexer Lexer;
    TToken Current;
    TParseError* Error;

public:
    TParser(TStringBuf input, TParseError* error)
        : Lexer(input)
        , Error(error)
    {
        Next();
    }

    TMaybe<TSelect> ParseQuery()
    {
        if (!AcceptWord("SELECT")) {
            return Fail("expected SELECT");
        }

        TSelect result;
        if (Accept(EToken::Star)) {
            // SELECT * keeps columns empty
        } else {
            if (!ParseNames(result.Columns)) {
                return Nothing();
            }
        }

        if (!AcceptWord("FROM")) {
            return Fail("expected FROM");
        }
        if (Current.Type != EToken::Word) {
            return Fail("expected table name");
        }
        result.Table = Current.Text;
        Next();

        if (AcceptWord("WHERE") && !ParseConditions(result.Conditions)) {
            return Nothing();
        }

        if (AcceptWord("LIMIT")) {
            if (Current.Type != EToken::Number) {
                return Fail("expected non-negative LIMIT");
            }
            result.Limit = FromString<ui64>(Current.Text);
            Next();
        }

        if (Current.Type != EToken::End) {
            return Fail("unexpected token");
        }
        return result;
    }

private:
    void Next()
    {
        Current = Lexer.Next();
    }

    bool Accept(EToken type, TStringBuf text = {})
    {
        if (Current.Type != type || (!text.empty() && Current.Text != text)) {
            return false;
        }
        Next();
        return true;
    }

    bool AcceptWord(TStringBuf word)
    {
        if (Current.Type != EToken::Word ||
            !EqualsIgnoreCase(Current.Text, word))
        {
            return false;
        }
        Next();
        return true;
    }

    bool ParseNames(TVector<TString>& names)
    {
        while (true) {
            if (Current.Type != EToken::Word) {
                Fail("expected column name");
                return false;
            }
            names.push_back(Current.Text);
            Next();
            if (!Accept(EToken::Comma)) {
                return true;
            }
        }
    }

    bool ParseConditions(TVector<TCondition>& conditions)
    {
        while (true) {
            TCondition condition;
            if (Current.Type != EToken::Word) {
                Fail("expected column name in WHERE");
                return false;
            }
            condition.Column = Current.Text;
            Next();

            if (Accept(EToken::Equal)) {
                condition.Predicate = EPredicate::Equal;
                if (!ParseValue(condition.Values)) {
                    return false;
                }
            } else if (AcceptWord("IN")) {
                condition.Predicate = EPredicate::In;
                if (!Accept(EToken::OpenBrace)) {
                    Fail("expected '{' after IN");
                    return false;
                }
                if (Current.Type == EToken::CloseBrace) {
                    Fail("IN list must not be empty");
                    return false;
                }
                if (!ParseValue(condition.Values)) {
                    return false;
                }
                while (Accept(EToken::Comma)) {
                    if (!ParseValue(condition.Values)) {
                        return false;
                    }
                }
                if (!Accept(EToken::CloseBrace)) {
                    Fail("expected '}' after IN list");
                    return false;
                }
            } else {
                Fail("expected '=' or IN");
                return false;
            }

            conditions.push_back(std::move(condition));
            if (!AcceptWord("AND")) {
                return true;
            }
        }
    }

    bool ParseValue(TVector<TValue>& values)
    {
        if (Current.Type == EToken::Number) {
            values.emplace_back(FromString<ui64>(Current.Text));
            Next();
            return true;
        }
        if (Current.Type == EToken::String) {
            values.emplace_back(Current.Text);
            Next();
            return true;
        }
        Fail("expected numeric or quoted string value");
        return false;
    }

    TMaybe<TSelect> Fail(TString message)
    {
        if (Error) {
            Error->Offset = Current.Offset;
            Error->Message = std::move(message);
        }
        return Nothing();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TMaybe<TSelect> Parse(TStringBuf input, TParseError* error)
{
    if (error) {
        *error = {};
    }
    return TParser(input, error).ParseQuery();
}

}   // namespace NCloud::NFileStore::NStorage::NQuery
