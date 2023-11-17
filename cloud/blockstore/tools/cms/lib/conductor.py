import os
import random


class ConductorMock(object):

    def __init__(self, test_data_dir):
        self.__dir = test_data_dir

    def get_cluster_list(self):
        return ["prod", "preprod", "dev"]

    def get_dc_list(self, cluster=None):
        return ["dca", "dcb", "dcc"]

    def get_dc(self, host, cluster):
        return "dca"

    def get_tenant_name(self, cluster, dc):
        return "/dev_global/NBS"

    def get_dc_hosts(self, dc, cluster, control, count, az):
        group = f'cloud_{cluster}_compute'
        with open(os.path.join(self.__dir, group + '.txt')) as f:
            hosts = [m.rstrip() for m in f.readlines()]

        return hosts if count == 0 else hosts[0:count]


def get_dc_host(conductor, dc, cluster, control):
    return random.choice(conductor.get_dc_hosts(dc, cluster, control, 0, True))
