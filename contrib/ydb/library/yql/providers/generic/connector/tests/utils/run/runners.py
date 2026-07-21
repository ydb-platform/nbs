import yatest.common as yat

from contrib.ydb.library.contrib.ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings

from contrib.ydb.library.contrib.ydb.library.yql.providers.generic.connector.tests.utils.run.parent import Runner
from contrib.ydb.library.contrib.ydb.library.yql.providers.generic.connector.tests.utils.run.kqprun import KqpRunner


# used in every test.py
def configure_runner(settings: Settings) -> Runner:
    return KqpRunner(
        kqprun_path=yat.build_path("contrib/ydb/tests/tools/kqprun/kqprun"),
        settings=settings,
        udf_dir=yat.build_path("contrib/ydb/library/yql/udfs/common/json2"),
    )
