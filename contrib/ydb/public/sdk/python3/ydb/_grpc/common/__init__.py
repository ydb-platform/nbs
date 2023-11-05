import sys

from contrib.ydb.public.api.grpc import *  # noqa
sys.modules["ydb._grpc.common"] = sys.modules["contrib.ydb.public.api.grpc"]
from contrib.ydb.public.api import protos  # noqa
sys.modules["ydb._grpc.common.protos"] = sys.modules["contrib.ydb.public.api.protos"]
