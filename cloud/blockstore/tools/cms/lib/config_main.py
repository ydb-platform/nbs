import argparse
import json
import logging
import sys

from cloud.blockstore.tools.cms.lib.cms import Cms, CmsEngine
from cloud.blockstore.tools.cms.lib.conductor import get_dc_host
from cloud.blockstore.tools.cms.lib.config import CONFIG_NAMES
from cloud.blockstore.tools.cms.lib.proto import to_json, get_proto
from cloud.blockstore.tools.cms.lib.tools import parse_cms_configs, get_config_files, find_items, Scp


def get_node_type(args):
    if args.node_type is not None:
        return args.node_type
    return 'nbs_control' if args.control else 'nbs'


def get_dc_configs(conductor, pssh, dc, args):
    cms = Cms(dc, args.cluster, get_node_type(args), CmsEngine(pssh), conductor)

    dc_configs = parse_cms_configs(cms.get_tenant_config_items())

    host = get_dc_host(conductor, dc, args.cluster, args.control)

    fs_configs = get_config_files(host, Scp(pssh), args.host_cfg_path)

    if args.split:
        return {
            "files": to_json(fs_configs),
            "global_cms": to_json(dc_configs)
        }

    for k, v in fs_configs.items():
        dc_configs.setdefault(k, v)

    return to_json(dc_configs)


def get_host_configs(cms, pssh, host, args):
    if args.only_host:
        return to_json(parse_cms_configs(cms.get_host_config_items(host)))

    fs_configs = get_config_files(host, Scp(pssh), args.host_cfg_path)

    if args.split:
        host_configs = parse_cms_configs(cms.get_host_config_items(host))
        dc_configs = parse_cms_configs(cms.get_tenant_config_items())

        return {
            "files": to_json(fs_configs),
            "global_cms": to_json(dc_configs),
            "host_cms": to_json(host_configs)
        }

    host_configs = parse_cms_configs(cms.get_config_items(host))

    for k, v in fs_configs.items():
        host_configs.setdefault(k, v)

    return to_json(host_configs)


def view_config(conductor, pssh, args):
    result = {}

    if args.host:
        for dc in set([conductor.get_dc(host, args.cluster) for host in args.host]):
            cms = Cms(dc, args.cluster, get_node_type(args), CmsEngine(pssh), conductor)
            for host in [host for host in args.host if dc == conductor.get_dc(host, args.cluster)]:
                result[host] = get_host_configs(cms, pssh, host, args)
    else:
        for dc in set(args.dc or conductor.get_dc_list()):
            result[dc] = get_dc_configs(conductor, pssh, dc, args)

    json.dump(result, sys.stdout, indent=4)


def update_hosts_config(conductor, pssh, args, message):
    for dc in set([conductor.get_dc(host, args.cluster) for host in args.host]):
        cms = Cms(dc, args.cluster, get_node_type(args), CmsEngine(pssh), conductor)
        for host in [host for host in args.host if dc == conductor.get_dc(host, args.cluster)]:
            current = cms.get_host_config_items(host)
            items = find_items(current, set([args.config_name]))
            cms.update_host_config_items(host, [message], args.cookie)
            cms.delete_config_items(items)


def update_dc_config(conductor, pssh, dc, args, message):
    cms = Cms(dc, args.cluster, get_node_type(args), CmsEngine(pssh), conductor)

    current = cms.get_tenant_config_items()
    items = find_items(current, set([args.config_name]))
    cms.update_dc_config_items(message, args.cookie)
    cms.delete_config_items(items)


def update_config(conductor, pssh, args):
    message = get_proto(args.proto, args.json, args.config_name)

    if args.host:
        update_hosts_config(conductor, pssh, args, message)
        return

    for dc in set(args.dc or conductor.get_dc_list()):
        update_dc_config(conductor, pssh, dc, args, message)


def remove_config(conductor, pssh, args):
    to_delete = set(args.config_name)

    if args.host:
        for dc in set([conductor.get_dc(host, args.cluster) for host in args.host]):
            cms = Cms(dc, args.cluster, get_node_type(args), CmsEngine(pssh), conductor)
            items = []
            for host in set([host for host in args.host if dc == conductor.get_dc(host, args.cluster)]):
                current = cms.get_host_config_items(host)
                items += find_items(current, to_delete)
            cms.delete_config_items(items)
        return

    for dc in set(args.dc or conductor.get_dc_list()):
        cms = Cms(dc, args.cluster, get_node_type(args), CmsEngine(pssh), conductor)
        current = cms.get_tenant_config_items()
        items = find_items(current, to_delete)
        cms.delete_config_items(items)


def prepare_logging(args):
    log_level = logging.ERROR

    if args.silent:
        log_level = logging.INFO
    elif args.verbose:
        log_level = max(0, logging.ERROR - 10 * int(args.verbose))

    logging.basicConfig(
        stream=sys.stderr,
        level=log_level,
        format="[%(levelname)s] [%(asctime)s] %(message)s")


def do_main(conductor, pssh, default_host_cfg_path: str):

    def add_common(p):
        clusters = conductor.get_cluster_list()
        p.add_argument('--cluster', type=str, choices=clusters, default=clusters[0])
        p.add_argument('--control', action='store_true')
        p.add_argument('--node-type', type=str, default=None)

        target = p.add_mutually_exclusive_group()
        target.add_argument('--dc', metavar="DC", nargs='*', choices=conductor.get_dc_list())
        target.add_argument("--host", metavar="HOST", nargs='*')

    parser = argparse.ArgumentParser()

    parser.add_argument("-s", '--silent', help="silent mode", default=0, action='count')
    parser.add_argument("-v", '--verbose', help="verbose mode", default=0, action='count')

    subparser = parser.add_subparsers(title='actions', dest="subparser_name", required=True)

    view = subparser.add_parser('view')
    update = subparser.add_parser('update')
    remove = subparser.add_parser('remove')

    view.add_argument('--split', action='store_true', help='split output by sources (filesystem, cms)')
    view.add_argument('--only-host', action='store_true', help='get only host-specific configs (cms only)')
    view.add_argument('--host-cfg-path', type=str, default=default_host_cfg_path, help='configs path on hosts')
    add_common(view)

    update.add_argument("--config-name", type=str, choices=CONFIG_NAMES.keys(), required=True)
    update.add_argument("--proto", type=str, required=True)
    # update.add_argument('--merge', action='store_true')
    update.add_argument('--json', action='store_true')
    update.add_argument("--cookie", type=str, default=None)
    add_common(update)

    remove.add_argument("--config-name", metavar="NAME", nargs='+', type=str, choices=CONFIG_NAMES.keys(), required=True)
    add_common(remove)

    args = parser.parse_args()

    prepare_logging(args)

    # TODO: resolve dc list from host group
    dc_list = conductor.get_dc_list(args.cluster)
    if len(dc_list) == 1:
        args.dc = dc_list[0]

    if args.subparser_name == "view":
        return view_config(conductor, pssh, args)

    if args.subparser_name == "update":
        return update_config(conductor, pssh, args)

    if args.subparser_name == "remove":
        return remove_config(conductor, pssh, args)
