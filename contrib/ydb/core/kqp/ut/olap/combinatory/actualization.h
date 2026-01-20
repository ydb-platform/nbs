#pragma once
#include "abstract.h"

#include <contrib/ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <contrib/ydb/library/conclusion/status.h>

namespace NKikimr::NKqp {

class TOneActualizationCommand: public ICommand {
private:
    virtual TConclusionStatus DoExecute(TKikimrRunner& kikimr) override;
};

}   // namespace NKikimr::NKqp
