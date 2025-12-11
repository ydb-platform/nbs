#pragma once

#include <memory>

namespace NMonitoring {

////////////////////////////////////////////////////////////////////////////////

class IMetricSupplier;

}   // namespace NMonitoring

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

using IUserMetricsSupplierPtr = std::shared_ptr<NMonitoring::IMetricSupplier>;

}   // namespace NCloud::NStorage
