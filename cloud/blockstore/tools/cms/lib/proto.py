import cloud.blockstore.tools.cms.lib.config as libconfig

from google.protobuf.json_format import MessageToJson, Parse as JsonParse
from google.protobuf.text_format import Parse as ProtoParse

import base64
import json
import os


def to_json(configs):
    result = {}
    for name, message in configs.items():
        value = json.loads(MessageToJson(message))

        # fix Component
        if name == 'LogConfig':
            for e in value.get('Entry', []):
                e['Component'] = \
                    base64.b64decode(e['Component']).decode("utf-8")

        result[name] = value

    return result


def get_proto(data, is_json, config_name):
    if not data:
        return None

    if os.path.exists(data):
        if not is_json:
            is_json = os.path.splitext(data)[1] in ['.json', '.js']
        with open(data, 'r') as f:
            data = f.read()

    if is_json:
        # fix Component
        if config_name == 'LogConfig':
            value = json.loads(data)
            for e in value.get('Entry', []):
                e['Component'] = \
                    base64.b64encode(e['Component'].encode()).decode("utf-8")
                data = json.dumps(value)

        return JsonParse(data, libconfig.CONFIGS[config_name]())

    return ProtoParse(data, libconfig.CONFIGS[config_name]())
