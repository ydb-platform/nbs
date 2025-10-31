from cloud.blockstore.tools.cms.lib.conductor import get_dc_host

from ydb.core.protos import msgbus_pb2 as msgbus
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

import getpass
import logging
import os
import os.path
import socket
import sys
import tempfile

from datetime import datetime
from google.protobuf.text_format import MessageToString, Parse as ProtoParse

CFG_PREFIX = 'Cloud.NBS.'
NAMED_CONFIG_KIND = 100


class CmsEngine(object):

    def __init__(self, pssh, test_data_dir=None):
        self.__pssh = pssh
        self.__dir = test_data_dir
        self.__request_id = 0
        self.__chowns = set()

    def execute(self, target, proto):
        logging.info(f'[cms.execute] {target} proto: {proto}')

        tmp_file = tempfile.NamedTemporaryFile(suffix=".txt")
        tmp_file.write(proto.encode("utf8"))
        tmp_file.flush()

        target_path = '~/cms_request_%s' % os.path.basename(tmp_file.name)

        # to be able to copy files to hosts we need to invoke 'chown' on home
        # folder
        if target not in self.__chowns:
            logging.info('[cms.execute] chown...')
            output = self.__pssh.run("sudo chown $(whoami) .", target)
            logging.info(f'[cms.execute] chown: {output}')
            self.__chowns.add(target)

        self.__pssh.scp_to(target, tmp_file.name, target_path)

        cmd = "kikimr admin console execute " + target_path
        output = self.__pssh.run(cmd, target)

        logging.debug(f'[cms.execute] output: {output}')

        if self.__dir is not None:
            d = os.path.join(self.__dir, 'cms')
            os.makedirs(d, exist_ok=True)
            with open(os.path.join(d, 'req%s.req.txt' % self.__request_id), "w") as f:
                f.write(proto)
            with open(os.path.join(d, 'req%s.resp.txt' % self.__request_id), "w") as f:
                f.write(output)

            self.__request_id += 1

        return output


class CmsEngineMock(object):

    def __init__(self, test_data_dir):
        d = os.path.join(test_data_dir, 'cms')
        data = {}
        for f in os.listdir(d):
            parts = f.split('.')
            if len(parts) != 3 or parts[2] != 'txt':
                raise Exception('bad file spotted: %s' % f)

            with open(os.path.join(d, f)) as stream:
                content = stream.read()

            if parts[0] not in data:
                data[parts[0]] = [None, None]
            if parts[1] == 'req':
                data[parts[0]][0] = content
            elif parts[1] == 'resp':
                data[parts[0]][1] = content

        self.__data = {}
        for k, v in data.items():
            if v[0] is None or v[1] is None:
                raise Exception('req or resp missing for %s: %s' % (k, v))

            m = self.__normalize_req(v[0])
            self.__data[MessageToString(m)] = \
                self.__normalize_resp(v[1])

    def __normalize_cookie(self, cookie):
        m = cookie.split(';')
        if len(m) < 4:
            return cookie

        m[0] = 'USERNAME'
        m[1] = 'HOSTNAME'
        m[2] = 'TIMESTAMP'

        return ";".join(m)

    def __normalize_resp(self, proto):
        return MessageToString(ProtoParse(proto, msgbus.TConsoleResponse()))

    def __normalize_req(self, proto):
        m = ProtoParse(proto, msgbus.TConsoleRequest())

        if m.HasField('ConfigureRequest'):
            for a in m.ConfigureRequest.Actions:
                if a.HasField('AddConfigItem'):
                    a.AddConfigItem.ConfigItem.Cookie = self.__normalize_cookie(
                        a.AddConfigItem.ConfigItem.Cookie)

        return m

    def execute(self, target, proto):
        proto = self.__normalize_req(proto)
        print(proto)
        r = MessageToString(proto)
        output = self.__data.get(r, '')

        if not output:
            logging.error(f'no data for {r}')

        return output


class Cms(object):

    def __init__(self, dc, cluster, node_type, engine, conductor, count=0):
        self.__dc = dc
        self.__cluster = cluster
        self.__target = None
        self.__tenant_config = None
        self.__node_type = node_type
        self.__engine = engine
        self.__conductor = conductor
        self.__username = getpass.getuser()
        self.__hostname = socket.gethostname()
        self.__timestamp = datetime.now().strftime('%d-%b-%Y %H:%M:%S')
        self.__ua = os.path.basename(sys.argv[0])
        self.__count = count

    @property
    def target(self):
        if not self.__target:
            self.__target = get_dc_host(
                self.__conductor,
                self.__dc,
                self.__cluster,
                False)

        return self.__target

    @property
    def tenant_name(self):
        return self.__conductor.get_tenant_name(self.__cluster, self.__dc)

    @property
    def node_type(self):
        return self.__node_type

    def execute(self, proto):
        output = self.__engine.execute(self.target, proto)

        assert output

        r = ProtoParse(output, msgbus.TConsoleResponse())
        assert r.Status.Code == StatusIds.SUCCESS

        return r

    def __load_config_items(self, request):
        items = []
        for item in self.execute(request).GetConfigItemsResponse.ConfigItems:
            if item.Kind != NAMED_CONFIG_KIND:
                continue

            for config in item.Config.NamedConfigs:
                if not config.Name.startswith(CFG_PREFIX):
                    continue
                config.Name = config.Name[len(CFG_PREFIX):]
                items.append(item)
        return items

    def get_host_config_items(self, host):
        request = msgbus.TConsoleRequest()
        request.GetConfigItemsRequest.HostFilter.Hosts.append(host)

        proto = MessageToString(request, as_one_line=True)

        return self.__load_config_items(proto)

    def get_tenant_config_items(self):
        if not self.__tenant_config:
            self.__tenant_config = self.get_config_items()
        return self.__tenant_config

    def get_config_items(self, host=None):
        if not self.__tenant_config:
            request = msgbus.TConsoleRequest()

            f = request.GetConfigItemsRequest.TenantAndNodeTypeFilter.TenantAndNodeTypes.add()
            f.Tenant = self.tenant_name
            f.NodeType = self.node_type

            if host:
                request.GetConfigItemsRequest.HostFilter.Hosts.append(host)

            proto = MessageToString(request, as_one_line=True)
            self.__tenant_config = self.__load_config_items(proto)
        return self.__tenant_config

    def __create_cookie(self, cookie):
        m = [
            self.__username,
            self.__hostname,
            self.__timestamp,
            self.__ua
        ]
        if cookie:
            m.append(cookie)
        return ";".join(m)

    def __update_config_items(self, messages, cookie, setup_scope):
        configure_request = msgbus.TConsoleRequest()
        action = configure_request.ConfigureRequest.Actions.add()

        for message in messages:
            custom_cfg = action.AddConfigItem.ConfigItem.Config.NamedConfigs.add()
            custom_cfg.Name = CFG_PREFIX + message.__class__.__name__[1:]
            custom_cfg.Config = MessageToString(message, as_one_line=True).encode()

        setup_scope(action.AddConfigItem.ConfigItem.UsageScope)

        action.AddConfigItem.ConfigItem.MergeStrategy = 2  # merge
        action.AddConfigItem.ConfigItem.Cookie = self.__create_cookie(cookie)

        proto = MessageToString(configure_request, as_one_line=True)

        self.execute(proto)

    def update_host_config_items(self, host, messages, cookie):
        def setup_hosts(s):
            s.HostFilter.Hosts.append(host)

        self.__update_config_items(messages, cookie, setup_hosts)

    def update_dc_config_items(self, message, cookie):
        def setup_tenant(s):
            s.TenantAndNodeTypeFilter.Tenant = self.tenant_name
            s.TenantAndNodeTypeFilter.NodeType = self.node_type

        self.__update_config_items([message], cookie, setup_tenant)

    def delete_config_items(self, items):
        if not items:
            return

        logging.debug(f'[cms.delete_config_items] {items}')

        configure_request = msgbus.TConsoleRequest()

        for item in items:
            action = configure_request.ConfigureRequest.Actions.add()
            config_item_id = action.RemoveConfigItem.ConfigItemId
            config_item_id.Id = item.Id.Id
            config_item_id.Generation = item.Id.Generation

        proto = MessageToString(configure_request, as_one_line=True)

        self.execute(proto)
