#include "simple_template.h"

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

void OutputTemplate(
    const TString& templateData,
    const THashMap<TString, TString>& vars,
    IOutputStream& out)
{
    TStringBuf contentRef(templateData);
    const TStringBuf b = "{{ ";
    const TStringBuf e = " }}";
    ui64 prevIdx = 0;
    while (true) {
        const ui64 idx = contentRef.find(b, prevIdx);
        if (idx == TString::npos) {
            out << contentRef.substr(prevIdx);
            break;
        }

        out << contentRef.substr(prevIdx, idx - prevIdx);
        const ui64 nextIdx = contentRef.find(e, idx + b.size());
        if (nextIdx == TString::npos) {
            out << contentRef.substr(idx);
            break;
        }

        auto varName = contentRef.substr(
            idx + b.size(),
            nextIdx - idx - b.size());
        if (const auto* varValue = vars.FindPtr(varName)) {
            out << *varValue;
        }

        prevIdx = nextIdx + e.size();
    }
}

}   // namespace NCloud::NFileStore
