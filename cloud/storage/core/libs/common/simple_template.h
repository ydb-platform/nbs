#pragma once

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

using TTemplateVars = THashMap<TString, TString>;
using TTemplateArrays = THashMap<TString, TVector<TTemplateVars>>;

/**
 * Renders a trivial text template: every "{{ name }}" occurrence is
 * replaced with vars["name"]; unknown names are replaced with nothing.
 * Everything else is copied verbatim.
 *
 * @param templateData - Template text.
 * @param vars - Variable name to substitution value mapping.
 * @param out - (out) Stream the rendered text is written to.
 */
void OutputTemplate(
    const TString& templateData,
    const TTemplateVars& vars,
    IOutputStream& out);

/**
 * Same as above plus loops. A "{{ for name }} body {{ endfor }}" block
 * renders its body once per element of arrays["name"]; inside the body
 * the element's variables are visible and shadow the outer vars. Loops
 * nest. A block whose name is not in arrays renders nothing; an
 * unterminated block is rendered as if the for token were absent.
 *
 * @param templateData - Template text.
 * @param vars - Variable name to substitution value mapping.
 * @param arrays - Loop name to element variable mappings.
 * @param out - (out) Stream the rendered text is written to.
 */
void OutputTemplate(
    const TString& templateData,
    const TTemplateVars& vars,
    const TTemplateArrays& arrays,
    IOutputStream& out);

}   // namespace NCloud
