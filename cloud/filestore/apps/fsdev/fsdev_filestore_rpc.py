from spdk.rpc.client import print_json


def fsdev_filestore_create(args):
    params = {"name": args.name}
    print_json(args.client.call("fsdev_filestore_create", params))


def fsdev_filestore_delete(args):
    params = {"name": args.name}
    print_json(args.client.call("fsdev_filestore_create", params))


def spdk_rpc_plugin_initialize(subparsers):
    p = subparsers.add_parser("fsdev_filestore_create", help="Add a filestore fsdev")
    p.add_argument("name", help="Name of the filestore fsdev")
    p.set_defaults(func=fsdev_filestore_create)

    p = subparsers.add_parser("fsdev_filestore_delete", help="Delete a filestore fsdev")
    p.add_argument("name", help="Name of the filestore fsdev")
    p.set_defaults(func=fsdev_filestore_delete)
