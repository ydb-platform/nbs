
#include "diagnostics.h"

namespace NCloud::NBlockStore {

inline TDiagnosticsConfigPtr CreateDiagnosticsConfig()
{
    NProto::TDiagnosticsConfig diagnosticsConfig;
    diagnosticsConfig.SetReportHistogramAsMultipleCounters(true);
    diagnosticsConfig.SetReportHistogramAsSingleCounter(false);
    return std::make_shared<TDiagnosticsConfig>(diagnosticsConfig);
}

} // namespace NCloud::NBlockStore
