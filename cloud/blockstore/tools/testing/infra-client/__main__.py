import argparse
import logging

from cloud.blockstore.public.sdk.python.client import CreateClient
import cloud.blockstore.public.sdk.python.protos as protos

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter(fmt="%(message)s", datefmt='%m/%d/%Y %I:%M:%S')
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', help='nbs server endpoint', default='localhost')
    parser.add_argument('--port', help='nbs grpc port', default=9766)
    parser.add_argument('--cms-host', help='cms request host', required=True)

    args = parser.parse_args()

    client = CreateClient('{}:{}'.format(args.host, args.port), log=logging)
    request = protos.TCmsActionRequest()
    action = request.Actions.add()
    action.Type = protos.TAction.GET_DEPENDENT_DISKS
    action.Host = args.cms_host
    print(client.cms_action(request))


run()
