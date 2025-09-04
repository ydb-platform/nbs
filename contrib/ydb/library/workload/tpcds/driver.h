extern "C" {

#include <contrib/ydb/library/benchmarks/gen/tpcds-dbgen/config.h>
#include <contrib/ydb/library/benchmarks/gen/tpcds-dbgen/porting.h>
#include <contrib/ydb/library/benchmarks/gen/tpcds-dbgen/constants.h>
#include <contrib/ydb/library/benchmarks/gen/tpcds-dbgen/date.h>
#include <contrib/ydb/library/benchmarks/gen/tpcds-dbgen/scaling.h>
#include <contrib/ydb/library/benchmarks/gen/tpcds-dbgen/tables.h>
#include <contrib/ydb/library/benchmarks/gen/tpcds-dbgen/tdefs.h>
#include <contrib/ydb/library/benchmarks/gen/tpcds-dbgen/tdef_functions.h>

void InitTpcdsGen(int scale, int processCount, int processIndex);

}