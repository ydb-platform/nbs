import sys
import uuid

from cloud.blockstore.libs.storage.protos import disk_pb2
from cloud.blockstore.public.api.protos.disk_pb2 import TDescribeDiskRegistryConfigResponse
from google.protobuf.text_format import Parse


def main():
    agents = [{
        "AgentId": "fake-agent-%d" % (i+1),
        "NodeId": (3000+i),
        "Devices": [
            {
                "DeviceUUID": str(uuid.uuid4()),
                "DeviceName": "fake-device-%s" % (j+1)
            } for j in range(15)
        ]
    } for i in range(100)]

    dr_config = disk_pb2.TDiskRegistryConfig()

    if len(sys.argv) > 1:
        with open(sys.argv[1]) as f:
            x = Parse(f.read(), TDescribeDiskRegistryConfigResponse())
            dr_config.Version = x.Version
            for agent in x.KnownAgents:
                a = dr_config.KnownAgents.add()
                a.AgentId = agent.AgentId
                for device in agent.Devices:
                    d = a.Devices.add()
                    d.DeviceUUID = device

    dr_config.Version += 1

    for agent in agents:
        a = dr_config.KnownAgents.add()
        a.AgentId = agent["AgentId"]
        for device in agent["Devices"]:
            d = a.Devices.add()
            d.DeviceUUID = device["DeviceUUID"]

    let = []
    upd = []
    row = []

    for i, agent in enumerate(agents):
        config = disk_pb2.TAgentConfig()
        config.NodeId = agent["NodeId"]
        config.AgentId = agent["AgentId"]

        for device in agent["Devices"]:
            d = config.Devices.add()
            d.DeviceName = device["DeviceName"]
            d.DeviceUUID = device["DeviceUUID"]
            d.BlockSize = 4096
            d.BlocksCount = 262144
            d.NodeId = config.NodeId
            d.Rack = "rack-%d" % i

        let.append("""
    (let key%d '('('NodeId (Uint32 '%d)) ))
        """ % (i, config.NodeId))

        upd.append("""
    (let updAgent%d '( '('Config (String '"%s")) ))
        """ % (i, config.SerializeToString().encode('string-escape').replace('"', '\\"')))

        row.append("""
        (UpdateRow 'Agents key%d updAgent%d)
        """ % (i, i))

    req = """
    (
    (let drCfgKey '('('Id (Uint32 '1))))
    (let updDrCfg '( '('Config (String '"%s")) ))
    %s
    %s
    (let pgmReturn (AsList
        (UpdateRow 'DiskRegistryConfig drCfgKey updDrCfg)
        %s
    ))
    (return pgmReturn)
    )
    """ % (
        dr_config.SerializeToString().encode('string-escape').replace('"', '\\"'),
        "".join(let),
        "".join(upd),
        "".join(row))

    print(req)


if __name__ == '__main__':
    main()
