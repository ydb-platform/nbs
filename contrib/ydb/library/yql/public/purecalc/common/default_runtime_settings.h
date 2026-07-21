#pragma once

#include <contrib/ydb/library/yql/minikql/runtime_settings/runtime_settings.h>

namespace NYql::NPureCalc::NPrivate {

NYql::TRuntimeSettings::TConstPtr GetDefaultRuntimeSettings();

} // namespace NYql::NPureCalc::NPrivate
