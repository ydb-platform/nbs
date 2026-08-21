import re
import tempfile

import yatest.common as common


def _iter_profile_log_lines(profile_tool_bin_path,
                            profile_log_path,
                            fs_name):
    # yatest.common.execute captures stdout in a file and then reads the whole
    # file into memory.  Supplying an open file marks stdout as caller-owned,
    # so the execution helper leaves it on disk and does not materialize it.
    with tempfile.TemporaryFile(mode="w+b") as output:
        common.execute(
            [profile_tool_bin_path, "dumpevents",
             "--profile-log", profile_log_path,
             "--fs-id", fs_name],
            stdout=output)

        output.flush()
        output.seek(0)
        for line in output:
            yield line.decode("utf-8")


def _parse_profile_log_event(line):
    parts = line.rstrip().split("\t")
    request_type = parts[2]
    body_dict = {}
    for i in range(5, len(parts)):
        body_str = re.sub(r"[{}\[\]]", "", parts[i])
        body_parts = body_str.split(", ")
        for body_part in body_parts:
            kv = body_part.split("=", 2)
            body_dict[kv[0]] = kv[1] if len(kv) == 2 else None

    return request_type, body_dict


def analyze_profile_log(profile_tool_bin_path,
                        profile_log_path,
                        fs_name,
                        node_name_filter=None):
    type_dict = {}
    for line in _iter_profile_log_lines(
            profile_tool_bin_path,
            profile_log_path,
            fs_name):
        request_type = line.rstrip().split("\t")[2]

        if node_name_filter and f"node_name={node_name_filter}" not in line:
            continue

        type_dict[request_type] = type_dict.get(request_type, 0) + 1

    return type_dict


def iter_profile_log_events(profile_tool_bin_path,
                            profile_log_path,
                            fs_name):
    for line in _iter_profile_log_lines(
            profile_tool_bin_path,
            profile_log_path,
            fs_name):
        yield _parse_profile_log_event(line)


def get_profile_log_events(profile_tool_bin_path,
                           profile_log_path,
                           fs_name):
    """Return all profile events as a list.

    Prefer iter_profile_log_events for large profile logs so parsed events are
    not retained in memory.
    """

    return list(iter_profile_log_events(
        profile_tool_bin_path,
        profile_log_path,
        fs_name))


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
