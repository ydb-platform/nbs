import re

import yatest.common as common


def analyze_profile_log(profile_tool_bin_path, profile_log_path, fs_name, node_name=""):
    proc = common.execute(
        [profile_tool_bin_path, "dumpevents",
         "--profile-log", profile_log_path,
         "--fs-id", fs_name])

    type_dict = {}
    for line in proc.stdout.decode("utf-8").splitlines():
        request_type = line.rstrip().split("\t")[2]

        if node_name and f"node_name={node_name}" not in line:
            continue

        type_dict[request_type] = type_dict.get(request_type, 0) + 1

    return type_dict


def get_profile_log_events(profile_tool_bin_path, profile_log_path, fs_name):
    proc = common.execute(
        [profile_tool_bin_path, "dumpevents",
         "--profile-log", profile_log_path,
         "--fs-id", fs_name])

    events = []
    for line in proc.stdout.decode("utf-8").splitlines():
        parts = line.rstrip().split("\t")
        request_type = parts[2]
        body_str = re.sub(r"[{}\[\]]", "", parts[-1])
        body_parts = body_str.split(", ")
        body_dict = {}
        for body_part in body_parts:
            kv = body_part.split("=", 2)
            body_dict[kv[0]] = kv[1] if len(kv) == 2 else None

        events.append((request_type, body_dict))

    return events


def dump_profile_log(profile_tool_bin_path,
                     profile_log_path,
                     fs_name,
                     dump_title,
                     results_path):
    with open(results_path, 'a') as results:
        type_dict = analyze_profile_log(profile_tool_bin_path,
                                        profile_log_path,
                                        fs_name)

        results.write('{}:\n'.format(dump_title))
        results.writelines('{}\n'.format(key)
                           for key
                           in sorted(type_dict.keys()))
