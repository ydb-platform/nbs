import cloud.blockstore.tools.cms.lib.cms as libcms
import cloud.blockstore.tools.cms.lib.conductor as libconductor
import cloud.blockstore.tools.cms.lib.proto as libproto
import cloud.blockstore.tools.cms.lib.tools as libtools

import jsondiff

import argparse
import copy
import json
import logging
import os
import sys


class ModuleFactories:

    def __init__(self, conductor_maker, pssh_maker):
        self._conductor_maker = conductor_maker
        self._pssh_maker = pssh_maker

    def make_conductor(self, test_data_dir: str):
        return self._conductor_maker(test_data_dir)

    def make_pssh(self, robot: bool, cluster: str):
        return self._pssh_maker(robot, cluster)


class BgColors:
    White = '\033[97m'
    End = '\033[0m'


class ClusterConfig(object):

    def __init__(self, d):
        self.cluster = d['cluster']
        self.dc = d['dc']


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


def load_hosts(path):
    hosts = []
    with open(path) as f:
        for line in f.readlines():
            line = line.strip()
            if not line:
                continue
            hosts.append(line)
    return hosts


def do_main(module_factories: ModuleFactories, default_host_cfg_path: str):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c',
        '--cluster-config',
        help='cluster config file path',
        type=str,
        required=True
    )
    parser.add_argument(
        '-C',
        '--control',
        help='nbs control flag',
        action='store_true'
    )
    parser.add_argument(
        '-p',
        '--patch',
        help='patch file path',
        type=str,
        required=True
    )
    parser.add_argument(
        '-m',
        '--commit-message',
        help='commit message',
        type=str,
        required=True
    )
    parser.add_argument(
        '-d',
        '--dry-run',
        help='do not apply any changes',
        action='store_true'
    )
    parser.add_argument(
        '-t',
        '--test-mode',
        help='use mocks instead of pssh and cms',
        action='store_true'
    )
    parser.add_argument(
        '-T',
        '--test-data-dir',
        help='test data dir',
        type=str
    )
    parser.add_argument(
        '-V',
        '--file-config-verification-host-count',
        help='the number of hosts from which file configs will be downloaded and compared',
        type=int,
        default=3
    )
    parser.add_argument(
        '-P',
        '--only-patched-hosts',
        help='process only the hosts present in the latest patch',
        action='store_true'
    )
    parser.add_argument(
        '--robot',
        action='store_true'
    )
    parser.add_argument(
        '-s',
        '--silent',
        help='silent mode',
        default=0,
        action='count'
    )
    parser.add_argument(
        '-v',
        '--verbose',
        help="verbose mode",
        default=0,
        action='count'
    )
    parser.add_argument(
        '-H',
        '--hosts',
        action='append',
        help='host filter'
    )
    parser.add_argument(
        '--host-cfg-path',
        type=str,
        default=default_host_cfg_path,
        help='configs path on hosts'
    )

    args = parser.parse_args()

    prepare_logging(args)

    with open(args.cluster_config) as f:
        cluster_config = ClusterConfig(json.load(f))

    if args.test_mode:
        cms_engine = libcms.CmsEngineMock(args.test_data_dir)
        scp = libtools.ScpMock(args.test_data_dir)
        conductor = libconductor.ConductorMock(args.test_data_dir)
    else:
        pssh = module_factories.make_pssh(args.robot, cluster_config.cluster)
        cms_engine = libcms.CmsEngine(pssh, args.test_data_dir)
        scp = libtools.Scp(pssh, args.test_data_dir)
        conductor = module_factories.make_conductor(args.test_data_dir)

    cms = libcms.Cms(
        cluster_config.dc,
        cluster_config.cluster,
        'nbs_control' if args.control else 'nbs',
        cms_engine,
        conductor)

    logging.info('fetching cluster/dc configs from cms')
    cms_configs = libproto.to_json(
        libtools.parse_cms_configs(cms.get_tenant_config_items())
    )
    logging.debug('fetched configs from cms: %s' % json.dumps(cms_configs))

    hosts = conductor.get_dc_hosts(
        cluster_config.dc,
        cluster_config.cluster,
        args.control,
        0,
        False
    )

    logging.debug(f'Total hosts: {len(hosts)}')

    if args.hosts:
        host_filter = None
        if len(args.hosts) == 1 and os.path.exists(args.hosts[0]):
            host_filter = set(load_hosts(args.hosts[0]))
        else:
            host_filter = set(args.hosts)

        logging.debug(f'Allowed hosts: {len(host_filter)}')

        hosts = [h for h in hosts if h in host_filter]

        logging.debug(f'Filtered hosts: {len(hosts)}')

    with open(args.patch) as f:
        patch = json.load(f)

    if args.only_patched_hosts:
        patched_hosts = set()
        for patch_item in patch:
            for host in patch_item['hosts']:
                patched_hosts.add(host)

        filtered_hosts = []
        for host in hosts:
            if host in patched_hosts:
                filtered_hosts.append(host)
        hosts = filtered_hosts

    logging.info('fetched %s hosts from conductor' % len(hosts))
    file_config_hosts = hosts[0:args.file_config_verification_host_count]
    logging.info('fetching file configs from hosts: %s' % file_config_hosts)

    file_configs = {}
    for host in file_config_hosts:
        host_file_configs = libproto.to_json(
            libtools.get_config_files(host, scp, args.host_cfg_path)
        )
        logging.debug('host %s file configs: %s' % (host, host_file_configs))
        for config_name, config_data in host_file_configs.items():
            if config_name in file_configs:
                if config_data != file_configs[config_name]:
                    raise Exception(
                        'config data for config %s differ for hosts %s'
                        % (config_name, hosts)
                    )
            else:
                file_configs[config_name] = config_data

    logging.debug('file configs: %s' % json.dumps(file_configs))

    individual_configs = {}
    new_individual_configs = {}
    host2current_cms = {}
    for i, host in enumerate(hosts):
        print(f"{BgColors.White}Collecting... {i}/{len(hosts)} "
              f"{i*100/len(hosts):.2f} %{BgColors.End}")
        c = copy.deepcopy(file_configs)
        c.update(copy.deepcopy(cms_configs))
        new_individual_configs[host] = c
        logging.debug('host %s default config: %s' % (host, c))

        cc = copy.deepcopy(c)
        host2current_cms[host] = cms.get_host_config_items(host)
        host_cms_config = libproto.to_json(
            libtools.parse_cms_configs(host2current_cms[host])
        )
        logging.debug('host %s cms config: %s' % (host, host_cms_config))
        cc.update(host_cms_config)
        logging.debug('host %s current config: %s' % (host, cc))
        individual_configs[host] = cc

    host2patched_configs = {}
    for patch_item in patch:
        for host in patch_item['hosts']:
            c = new_individual_configs.get(host)
            if c is None:
                continue

            patched_configs = []
            for config_name, props in patch_item['patch'].items():
                if config_name not in c:
                    c[config_name] = {}
                    c[config_name].update(copy.deepcopy(props))
                else:
                    for key, value in props.items():
                        c[config_name][key] = copy.deepcopy(value)
                patched_configs.append(config_name)

            host2patched_configs[host] = patched_configs
            logging.debug('patched config for host %s: %s' % (host, json.dumps(c)))

    print()

    zerodiff_hosts = set()
    have_diff = False
    for host in hosts:
        c = new_individual_configs[host]
        cc = individual_configs[host]
        diff = jsondiff.diff(cc, c)
        # logging.debug("XXX\t%s\t%s\t%s" % (host, json.dumps(cc), json.dumps(c)))
        if len(diff):
            print("%s\t%s" % (host, diff))
            have_diff = True
        else:
            zerodiff_hosts.add(host)

    if not have_diff:
        print('no diff, exiting')
        return 0

    reaction = 'Y' if args.test_mode else None
    while reaction not in ['Y', 'n']:
        if reaction is not None:
            print('unsupported reaction, try again')
        reaction = input("Apply changes (Y/n)?")

    if reaction == 'Y':
        print('applying changes')

        for i, host in enumerate(hosts):
            print(f"{BgColors.White}Applying... {i}/{len(hosts)} "
                  f"{i*100/len(hosts):.2f} %{BgColors.End}")

            current = host2current_cms[host]

            if host in zerodiff_hosts:
                if len(current):
                    logging.warning("no diff for host %s, skipping" % host)
                continue

            if len(current):
                logging.warning("deleting host %s cms config" % host)
                if not args.dry_run:
                    cms.delete_config_items(current)

            patched_configs = host2patched_configs.get(host)
            if patched_configs is not None:
                messages = []

                logging.warning("uploading configs for host %s, patched configs: %s" % (
                    host,
                    patched_configs
                ))

                for config_name in patched_configs:
                    config_json = json.dumps(
                        new_individual_configs[host][config_name]
                    )
                    logging.debug("uploading config %s, data: %s" % (
                        config_name,
                        config_json
                    ))
                    messages.append(libproto.get_proto(
                        config_json,
                        True,
                        config_name
                    ))

                if not args.dry_run:
                    cms.update_host_config_items(
                        host,
                        messages,
                        args.commit_message
                    )
    else:
        print('aborting changes')

    return 0
