#include "simple_template.h"

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf TokenBegin = "{{ ";
constexpr TStringBuf TokenEnd = " }}";
constexpr TStringBuf ForPrefix = "for ";
constexpr TStringBuf EndForToken = "endfor";

struct TToken
{
    // Token text between the delimiters; empty if no token was found.
    TStringBuf Text;

    // Offset of "{{ " in the content; npos if no token was found.
    size_t Begin = TStringBuf::npos;

    // Offset just past " }}".
    size_t End = TStringBuf::npos;
};

TToken FindToken(TStringBuf content, size_t from)
{
    TToken token;
    const size_t idx = content.find(TokenBegin, from);
    if (idx == TStringBuf::npos) {
        return token;
    }

    const size_t nameIdx = idx + TokenBegin.size();
    const size_t endIdx = content.find(TokenEnd, nameIdx);
    if (endIdx == TStringBuf::npos) {
        return token;
    }

    token.Text = content.substr(nameIdx, endIdx - nameIdx);
    token.Begin = idx;
    token.End = endIdx + TokenEnd.size();
    return token;
}

void Render(
    TStringBuf content,
    const TTemplateVars& vars,
    const TTemplateArrays& arrays,
    IOutputStream& out)
{
    size_t prevIdx = 0;
    while (true) {
        const TToken token = FindToken(content, prevIdx);
        if (token.Begin == TStringBuf::npos) {
            out << content.substr(prevIdx);
            break;
        }

        out << content.substr(prevIdx, token.Begin - prevIdx);

        if (token.Text.StartsWith(ForPrefix)) {
            const TStringBuf name = token.Text.substr(ForPrefix.size());

            //
            // Find the matching endfor, counting nested loops.
            //

            size_t depth = 1;
            size_t scanIdx = token.End;
            size_t bodyEndIdx = TStringBuf::npos;
            size_t loopEndIdx = TStringBuf::npos;
            while (depth) {
                const TToken t = FindToken(content, scanIdx);
                if (t.Begin == TStringBuf::npos) {
                    break;
                }
                if (t.Text.StartsWith(ForPrefix)) {
                    ++depth;
                } else if (t.Text == EndForToken) {
                    if (!--depth) {
                        bodyEndIdx = t.Begin;
                        loopEndIdx = t.End;
                    }
                }
                scanIdx = t.End;
            }

            if (bodyEndIdx == TStringBuf::npos) {
                //
                // Unterminated loop - drop the for token and render the
                // rest as plain text.
                //

                prevIdx = token.End;
                continue;
            }

            const TStringBuf body =
                content.substr(token.End, bodyEndIdx - token.End);
            if (const auto* items = arrays.FindPtr(name)) {
                for (const auto& item: *items) {
                    TTemplateVars merged = vars;
                    for (const auto& [k, v]: item) {
                        merged[k] = v;
                    }
                    Render(body, merged, arrays, out);
                }
            }

            prevIdx = loopEndIdx;
            continue;
        }

        if (const auto* varValue = vars.FindPtr(token.Text)) {
            out << *varValue;
        }

        prevIdx = token.End;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void OutputTemplate(
    const TString& templateData,
    const TTemplateVars& vars,
    IOutputStream& out)
{
    OutputTemplate(templateData, vars, {} /* arrays */, out);
}

void OutputTemplate(
    const TString& templateData,
    const TTemplateVars& vars,
    const TTemplateArrays& arrays,
    IOutputStream& out)
{
    Render(templateData, vars, arrays, out);
}

}   // namespace NCloud
