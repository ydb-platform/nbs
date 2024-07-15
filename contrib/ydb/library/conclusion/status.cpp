#include "status.h"
#include <contrib/ydb/library/actors/core/log.h>

namespace NKikimr {

void TConclusionStatus::Validate() const {
    AFL_VERIFY(Ok())("problem", GetErrorMessage());
}

}
