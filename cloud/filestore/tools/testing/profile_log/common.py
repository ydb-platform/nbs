import re

import yatest.common as common


def analyze_profile_log(profile_tool_bin_path, profile_log_path, fs_name):
    proc = common.execute(
        [profile_tool_bin_path, "dumpevents",
         "--profile-log", profile_log_path,
         "--fs-id", fs_name])

    type_dict = {}
    for line in proc.stdout.decode('utf-8').splitlines():
        request_type = re.split(r'\t+', line.rstrip())[2]
        type_dict[request_type] = type_dict.get(request_type, 0) + 1

    return type_dict


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
