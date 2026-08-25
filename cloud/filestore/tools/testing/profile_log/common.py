import os
import re
import tempfile

import yatest.common as common


def _iter_profile_log_lines(profile_tool_bin_path,
                            profile_log_path,
                            fs_name):
    # yatest.common.execute captures stdout in a file and then reads the whole
    # file into memory.  Supplying an open file marks stdout as caller-owned,
    # so the execution helper leaves it on disk and does not materialize it.
    # Keep the name until the command succeeds so failed output is preserved.
    with tempfile.NamedTemporaryFile(
            mode="w+b",
            dir=common.output_path(),
            prefix="filestore-profile-tool.dumpevents.",
            suffix=".out",
            delete=False) as output:
        common.execute(
            [profile_tool_bin_path, "dumpevents",
             "--profile-log", profile_log_path,
             "--fs-id", fs_name],
            stdout=output)

        # Continue reading through the descriptor without keeping an artifact.
        os.unlink(output.name)
        output.seek(0)
        for line in output:
            yield line.decode("utf-8", errors="replace")


def _split_profile_log_event(line):
    parts = line.rstrip().split("\t")
    if len(parts) < 3:
        return None

    return parts


def _parse_profile_log_event(line):
    parts = _split_profile_log_event(line)
    if parts is None:
        return None

    request_type = parts[2]
    body_dict = {}
    for i in range(5, len(parts)):
        body_str = re.sub(r"[{}\[\]]", "", parts[i])
        body_parts = body_str.split(", ")
        for body_part in body_parts:
            kv = body_part.split("=", 1)
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
        parts = _split_profile_log_event(line)
        if parts is None:
            continue

        if node_name_filter and f"node_name={node_name_filter}" not in line:
            continue

        request_type = parts[2]
        type_dict[request_type] = type_dict.get(request_type, 0) + 1

    return type_dict


def iter_profile_log_events(profile_tool_bin_path,
                            profile_log_path,
                            fs_name):
    """Yield parsed events without retaining them in memory.

    The returned iterator is single-pass; it has no length and cannot be
    rewound. Use get_profile_log_events when list semantics are required.
    """

    for line in _iter_profile_log_lines(
            profile_tool_bin_path,
            profile_log_path,
            fs_name):
        event = _parse_profile_log_event(line)
        if event is not None:
            yield event


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
