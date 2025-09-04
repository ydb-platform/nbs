#include "vector_workload_params.h"
#include <contrib/ydb/library/workload/abstract/workload_factory.h>

namespace NYdbWorkload {

TWorkloadFactory::TRegistrator<TVectorWorkloadParams> VectorRegistrar("vector");

}
