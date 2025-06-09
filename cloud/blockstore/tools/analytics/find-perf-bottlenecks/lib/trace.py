import json


TAGS = ["AllRequests", "SlowRequests"]


def percentile(vals, p):
    idx = min(int(len(vals) * (p / 100.)), len(vals) - 1)
    return vals[idx]


class Probe(object):

    def __init__(self, x):
        self.name = x[0]
        self.ts = x[1]
        self.startBlock = None
        self.requestSize = None
        self.deviceUUID = None

        if self.name == "Fork":
            self.span = x[2]

        elif self.name == "Join":
            self.span = x[2]
            self.length = x[3]

        if len(x) > 2:
            self.request = x[2]
        else:
            self.request = None

        if self.name == "RequestStarted" and len(x) > 7:
            self.startBlock = x[6]
            self.requestSize = x[7]

        if self.name == "RequestReceived_NonreplPartitionWorker" and len(x) > 4:
            self.deviceUUID = x[4]

    def describe(self):
        x = self.name
        if self.request:
            x += ":" + self.request
        additions = []
        if self.startBlock:
            additions.append("startBlock:" + self.startBlock)
        if self.requestSize:
            additions.append("size:" + self.requestSize)
        if self.deviceUUID:
            additions.append("device:" + self.deviceUUID)
        params = ", ".join(additions)
        if len(params):
            x += " [{}]".format(params)
        return x

    def __str__(self):
        x = self.describe()
        if hasattr(self, "length"):
            x += ":" + self.length
        return x


class Trace(object):

    def __init__(self, j):
        self.probes = [Probe(x) for x in j]
        self.request = self.probes[0].request if len(j) else None
        self.media_kind = None
        if len(j):
            self.media_kind = j[0][3]
            # there is bug with arguments order for some tracing events, where media kind and reqid are swapped
            # example (correct one):
            # ["RequestStarted",0,"ReadBlocks","1","12249921191883969784","a7lffa924b2p569p98eo","12823","65536"]
            # example (bad one)
            # ["BackgroundTaskStarted_Partition",0,"Compaction","10161783872880318072","1","c8rgmvrh9q3sv277aptm"]
            # This behaviour will be fixed in NBS-3069 and we need to add backward compatibility for this case
            # So we added check for "big" number to detect this issue
            if int(j[0][3]) > 10000:
                self.media_kind = j[0][4]


_LOG_COMPONENTS = ["BLOCKSTORE_TRACE", "NFS_TRACE"]


def extract_trace_from_log_message(message, tag=None):
    try:
        items = json.loads(message)
    except Exception:
        parts = []
        for log_comp in _LOG_COMPONENTS:
            parts = message.split(":%s " % log_comp)

            if len(parts) == 2:
                break

        if len(parts) != 2:
            return None

        parts = parts[1].split(" ")

        if len(parts) != 2:
            return None

        try:
            items = json.loads(parts[1])
        except Exception:
            return None

    if items and items[-1]:
        if items[-1][0] in TAGS:
            if items[-1][0] == tag or tag is None:
                items.pop()
                return Trace(items)

        # accept logs without a tag for backwards compatibility
        elif tag in ["SlowRequests", None]:
            return Trace(items)

    return None


def extract_trace(row, tag=None):
    assert row["component"] in _LOG_COMPONENTS

    message = row["message"]
    return extract_trace_from_log_message(message, tag)


def describe_trace(trace):
    probes = []
    for probe in trace.probes:
        probes.append(probe.describe())
    return " ".join(probes)


# https://github.com/ydb-platform/nbs/blob/main/cloud/storage/core/protos/media.proto#L10
def media_kind2str(media_kind):
    if media_kind == "0" or media_kind == "2" or media_kind == "3":
        return "HDD"
    elif media_kind == "1":
        return "SSD"
    elif media_kind == "4":
        return "SSD_NONREPLICATED"
    elif media_kind == "5":
        return "SSD_MIRROR2"
    elif media_kind == "6":
        return "SSD_MIRROR3"
    elif media_kind == "7":
        return "SSD_LOCAL"
    else:
        return "UNKNOWN"


def filter_postponed_interval(trace):
    filtered_trace = Trace([])
    filtered_trace.request = trace.request
    filtered_trace.media_kind = trace.media_kind

    inside = False
    postponed_ts = 0
    postponed_interval = 0
    for probe in trace.probes:
        if inside:
            if probe.name == "RequestAdvanced_Volume":
                inside = False
                postponed_interval += probe.ts - postponed_ts
        else:
            if probe.name == "RequestPostponed_Volume":
                postponed_ts = probe.ts
                inside = True
            else:
                probe.ts -= postponed_interval
                filtered_trace.probes.append(probe)
    return filtered_trace


# collapse probes between matching send/recv
def collapse_probes(probes, requests):
    result = []

    collapsed = set()
    active = None
    count = 0
    drop = 0

    for probe in probes:
        # drop subtrack probes
        if drop > 0:
            drop -= 1

        # save span in case corresponding join will be outside collapsed range
        elif probe.name == "Fork":
            if active:
                collapsed.add(probe.span)
            else:
                result.append(probe)

        # drop subtrack forked inside collapsed range
        elif probe.name == "Join":
            if probe.span in collapsed:
                collapsed.discard(probe.span)
                drop = int(probe.length)
            else:
                result.append(probe)

        elif probe.name == "RequestReceived_Partition":
            if active == probe.request:
                count += 1

            if not active:
                result.append(probe)

                if probe.request in requests:
                    active = probe.request
                    count = 1

        elif probe.name == "ResponseSent_Partition":
            if active == probe.request:
                count -= 1

                if count == 0:
                    active = None

            if not active:
                result.append(probe)

        elif not active:
            result.append(probe)

    return result


def extract_minimized_trace(row, tag=None):
    trace = extract_trace(row, tag)
    if trace is None:
        return None

    trace = filter_postponed_interval(trace)

    if trace.request in ["Compaction", "Flush"]:
        trace.probes = collapse_probes(trace.probes, ["ReadBlob", "WriteBlob"])

    return trace
