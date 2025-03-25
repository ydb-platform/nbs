#include "message.h"

namespace NCloud::NNetlink {

void ValidateAttribute(const ::nlattr& attribute, ui16 expectedAttribute)
{
    if (attribute.nla_type != expectedAttribute) {
        throw TIoException() << "Invalid attribute type: " << attribute.nla_type
                           << " Expected attribute type: " << expectedAttribute;
    }
}

}   // namespace NCloud::NNetlink
