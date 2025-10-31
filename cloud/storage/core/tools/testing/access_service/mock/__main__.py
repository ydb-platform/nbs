import argparse
from concurrent import futures

import grpc

from .mock_service import AccessServiceMockState, AccessServiceMockServicer
from .control_service import ControlServer

from ydb.public.api.client.yc_private.servicecontrol import access_service_pb2_grpc


def parse_args():
    parser = argparse.ArgumentParser(description="Access Service Mock Server")
    parser.add_argument("--host", dest="host", default="0.0.0.0",
                        help="Host to listen on")
    parser.add_argument("--max-workers", dest="max_workers", type=int, default=1,
                        help="The maximum number of threads that can be used to execute the given calls")
    parser.add_argument("--port", dest="port", type=int, default=4284,
                        help="Mock server port")
    parser.add_argument("--control-server-port", dest="control_server_port", type=int, default=2484,
                        help="Control server port")
    return parser.parse_args()


def run(host="0.0.0.0", port=4284, control_server_port=2484, max_workers=1):
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=max_workers),
        options=(
            ("grpc.keepalive_time_ms", 1000),
            ("grpc.keepalive_permit_without_calls", True),
            ('grpc.http2.min_ping_interval_without_data_ms', 0),
        )
    )

    mock_state = AccessServiceMockState()
    access_service = AccessServiceMockServicer(mock_state)
    access_service_pb2_grpc.add_AccessServiceServicer_to_server(access_service, server)

    server.add_insecure_port("{0}:{1}".format(host, port))
    server.start()

    control_server = ControlServer(mock_state, "AccessServiceMockControl")
    control_server.run(host=host, port=control_server_port, threaded=True)


if __name__ == "__main__":
    args = parse_args()
    run(
        host=args.host,
        port=args.port,
        control_server_port=args.control_server_port,
        max_workers=args.max_workers,
    )
