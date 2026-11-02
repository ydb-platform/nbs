#!/usr/bin/env python3
#
# Generates filesystem_events_animation.svg - a looping CSS-animated SVG (no
# JavaScript, so that it can be embedded into Markdown) that shows the read /
# modification / invalidation exchange for a single NodeRef + Node.
#
# The scenario is the same as in filesystem_events_animation.html.
#
# Usage: ./filesystem_events_animation_svg.py > filesystem_events_animation.svg
#

from collections import defaultdict
from html import escape

FADE = 0.4  # seconds
CLIENTS = 4
WIDTH = 760
HEIGHT = 490

SHARDS = [
    {"id": "dir", "title": "Directory shard", "y": 160},
    {"id": "node", "title": "Node shard", "y": 340},
]

CLIENT_X = 110
SHARD_X = 520
SHARD_W = 200
SHARD_H = 80


def client_y(c):
    return 110 + c * 100


################################################################################


class Timeline:
    def __init__(self):
        self.t = 0.0
        # element id -> list of (time, {property: value})
        self.tracks = defaultdict(list)
        # list of (start, end, text)
        self.captions = []
        self.caption_start = None
        self.caption_text = None
        # list of (start, end, text) for the Node shard version line
        self.versions = []
        self.version_start = 0.0
        self.version = 1

    def set(self, element, **props):
        self.tracks[element].append((self.t, props))

    def wait(self, seconds):
        self.t += seconds

    def caption(self, text):
        self.close_caption()
        self.caption_start = self.t
        self.caption_text = text

    def close_caption(self):
        if self.caption_text is not None:
            self.captions.append(
                (self.caption_start, self.t, self.caption_text))
            self.caption_text = None

    def bump_version(self):
        self.versions.append((self.version_start, self.t, self.version))
        self.version += 1
        self.version_start = self.t

    def finish(self):
        self.close_caption()
        self.versions.append((self.version_start, self.t, self.version))
        return self.t


################################################################################


def sub(c, s):
    return "sub-%d-%s" % (c, s)


def read_req(c, s):
    return "read-%d-%s" % (c, s)


def mod_req(c, s):
    return "mod-%d-%s" % (c, s)


def client(c):
    return "client-%d" % c


def shard_title(s):
    return next(x["title"] for x in SHARDS if x["id"] == s).lower()


class Scenario:
    def __init__(self, tl):
        self.tl = tl
        self.cached = [set() for _ in range(CLIENTS)]
        self.subscribed = [set() for _ in range(CLIENTS)]
        for c in range(CLIENTS):
            tl.set(client(c), fill="var(--panel)")
            for s in SHARDS:
                tl.set(sub(c, s["id"]), opacity=0, stroke="var(--sub)")
                tl.set(read_req(c, s["id"]), opacity=0)
                tl.set(mod_req(c, s["id"]), opacity=0)

    def render_client(self, c):
        full = "dir" in self.cached[c] and "node" in self.cached[c]
        self.tl.set(
            client(c),
            fill="var(--cached)" if full else "var(--panel)")

    def read(self, c, shards, what):
        tl = self.tl
        tl.caption("client %d: %s - read requests (green) to %s." % (
            c + 1, what, " and ".join(map(shard_title, shards))))
        tl.wait(0.6)
        for s in shards:
            tl.set(read_req(c, s), opacity=1)
            tl.wait(0.5)
        tl.wait(0.5)
        for s in shards:
            tl.set(read_req(c, s), opacity=0)
            tl.set(sub(c, s), opacity=1)
            self.subscribed[c].add(s)
            self.cached[c].add(s)
        self.render_client(c)
        tl.wait(1.2)

    def modify(self, c, shards, what, invalidates):
        tl = self.tl
        tl.caption("client %d: %s - modification (blue) to %s." % (
            c + 1, what, " and ".join(map(shard_title, shards))))
        tl.wait(0.6)
        for s in shards:
            tl.set(mod_req(c, s), opacity=1)
        for s in invalidates:
            self.cached[c].discard(s)
        self.render_client(c)
        tl.wait(1.2)
        for s in shards:
            tl.set(mod_req(c, s), opacity=0)
        if "node" in invalidates:
            tl.bump_version()

        tl.caption(
            "The shards send invalidations (red) to the subscribers and drop "
            "the subscriptions.")
        tl.wait(0.6)
        targets = [
            (other, s)
            for other in range(CLIENTS)
            for s in invalidates
            if s in self.subscribed[other]
        ]
        for other, s in targets:
            tl.set(sub(other, s), stroke="var(--inval)")
        tl.wait(1.0)
        for other, s in targets:
            tl.set(sub(other, s), opacity=0)
            self.subscribed[other].discard(s)
            self.cached[other].discard(s)
        for other in range(CLIENTS):
            self.render_client(other)
        tl.wait(FADE + 0.1)
        for other, s in targets:
            tl.set(sub(other, s), stroke="var(--sub)")
        tl.wait(3.0)

    def pause(self, text, seconds):
        self.tl.caption(text)
        self.tl.wait(seconds)


def run_scenario(tl):
    sc = Scenario(tl)
    lookup = 'Lookup(root, "f")'
    sc.read(0, ["dir", "node"], lookup)
    sc.read(1, ["dir", "node"], lookup)
    sc.read(2, ["dir", "node"], lookup)
    sc.pause(
        "Clients 1-3 reply to Lookup and GetAttr from their caches - no "
        "requests to the shards.",
        4)
    sc.modify(3, ["node"], "SetAttr(node)", ["node"])
    sc.pause(
        "Clients 1-3 still have the NodeRef cached, but not the Node - "
        "Lookup goes to the shards again.",
        4)
    sc.read(0, ["dir", "node"], lookup)
    sc.modify(2, ["dir", "node"], 'Unlink(root, "f")', ["dir", "node"])
    sc.pause("Nothing is cached anymore. The scenario restarts.", 4)
    return tl.finish()


################################################################################


def pct(t, total):
    return "%.3f%%" % (100.0 * min(t, total) / total)


def keyframes(name, events, total):
    # events: list of (time, {prop: value}) sorted by time. Each change is
    # animated during FADE seconds after its time, the values are held
    # between the changes.
    props = sorted({p for _, e in events for p in e})
    current = {}
    frames = defaultdict(dict)  # time -> {prop: value}
    for t, e in events:
        for p, v in e.items():
            if p not in current:
                frames[0.0][p] = v
            elif current[p] != v:
                frames[t][p] = current[p]
                frames[min(t + FADE, total)][p] = v
            current[p] = v
    for p in props:
        frames[total][p] = current[p]

    # CSS animates every property independently between the keyframes that
    # specify it, so a frame lists only the properties that change there.
    lines = ["@keyframes %s {" % name]
    for t in sorted(frames):
        body = " ".join("%s: %s;" % kv for kv in sorted(frames[t].items()))
        lines.append("  %s { %s }" % (pct(t, total), body))
    lines.append("}")
    return "\n".join(lines)


def visibility_keyframes(name, start, end, total):
    # Visible during [start, end), fades in and out during FADE / 2.
    half = FADE / 2
    points = [
        (0.0, 0),
        (max(start - half, 0.0), 0),
        (start, 1),
        (max(end - half, start), 1),
        (end, 0),
        (total, 0),
    ]
    if start == 0.0:
        points = [(0.0, 1)] + points[3:]
    lines = ["@keyframes %s {" % name]
    for t, v in points:
        lines.append("  %s { opacity: %d; }" % (pct(t, total), v))
    lines.append("}")
    return "\n".join(lines)


def generate():
    tl = Timeline()
    total = run_scenario(tl)

    css = []
    body = []

    css.append("""
:root, svg {
  --bg: #fafaf7;
  --fg: #222;
  --muted: #777;
  --panel: #fff;
  --read: #2e9d4f;
  --sub: #222;
  --mod: #2a62d4;
  --inval: #d8382f;
  --cached: #4cc06a;
}
@media (prefers-color-scheme: dark) {
  :root, svg {
    --bg: #1c1c1e;
    --fg: #e6e6e6;
    --muted: #999;
    --panel: #26262a;
    --sub: #e6e6e6;
  }
}
text { font-family: system-ui, -apple-system, "Segoe UI", sans-serif; }
.label { fill: var(--fg); font-size: 15px; }
.small { fill: var(--muted); font-size: 13px; }
.caption { fill: var(--fg); font-size: 14px; }
.edge { stroke-width: 3; fill: none; }
.read { stroke: var(--read); }
.mod { stroke: var(--mod); stroke-dasharray: 8 5; }
.client { stroke: var(--fg); stroke-width: 2; }
.shard { stroke: var(--fg); stroke-width: 2; fill: var(--panel); }
""")

    def animate(element, cls, tag, attrs):
        name = "k-" + element
        css.append(keyframes(name, tl.tracks[element], total))
        css.append("#%s { animation: %s %.2fs linear infinite; }" % (
            element, name, total))
        attr_text = " ".join('%s="%s"' % kv for kv in attrs.items())
        body.append('<%s id="%s" class="%s" %s/>' % (
            tag, element, cls, attr_text))

    body.append('<rect width="%d" height="%d" fill="var(--bg)"/>' % (
        WIDTH, HEIGHT))

    # Legend.
    legend = [
        ("var(--read)", "read"),
        ("var(--sub)", "subscription"),
        ("var(--mod)", "modification"),
        ("var(--inval)", "invalidation"),
    ]
    x = 20
    for color, text in legend:
        body.append(
            '<rect x="%d" y="16" width="18" height="4" fill="%s"/>' % (
                x, color))
        body.append(
            '<text x="%d" y="23" class="small">%s</text>' % (x + 24, text))
        x += 130
    body.append(
        '<text x="20" y="44" class="small">filled client = cached '
        'NodeRef + Node</text>')

    # Edges.
    for c in range(CLIENTS):
        for s in SHARDS:
            x1 = CLIENT_X + 28
            y1 = client_y(c)
            x2 = SHARD_X
            y2 = s["y"] + SHARD_H / 2
            line = {"x1": x1, "y1": y1, "x2": x2, "y2": y2}
            offset = dict(line, y1=y1 + 6, y2=y2 + 6)
            animate(sub(c, s["id"]), "edge", "line", line)
            animate(read_req(c, s["id"]), "edge read", "line", offset)
            animate(mod_req(c, s["id"]), "edge mod", "line", offset)

    # Clients.
    for c in range(CLIENTS):
        animate(client(c), "client", "circle",
                {"cx": CLIENT_X, "cy": client_y(c), "r": 28})
        body.append(
            '<text x="%d" y="%d" class="label">client %d</text>' % (
                CLIENT_X - 92, client_y(c) + 5, c + 1))

    # Shards.
    for s in SHARDS:
        body.append(
            '<rect x="%d" y="%d" width="%d" height="%d" rx="8" '
            'class="shard"/>' % (SHARD_X, s["y"], SHARD_W, SHARD_H))
        body.append(
            '<text x="%d" y="%d" class="label">%s</text>' % (
                SHARD_X + 12, s["y"] + 30, s["title"]))
    body.append(
        '<text x="%d" y="%d" class="small">NodeRef (root, "f") '
        '&#8594; node</text>' % (SHARD_X + 12, SHARDS[0]["y"] + 56))
    for i, (start, end, version) in enumerate(tl.versions):
        name = "v%d" % i
        css.append(visibility_keyframes("k-" + name, start, end, total))
        css.append("#%s { animation: k-%s %.2fs linear infinite; }" % (
            name, name, total))
        body.append(
            '<text id="%s" x="%d" y="%d" class="small">Node attrs v%d'
            '</text>' % (name, SHARD_X + 12, SHARDS[1]["y"] + 56, version))

    # Captions.
    for i, (start, end, text) in enumerate(tl.captions):
        name = "c%d" % i
        css.append(visibility_keyframes("k-" + name, start, end, total))
        css.append("#%s { animation: k-%s %.2fs linear infinite; }" % (
            name, name, total))
        body.append(
            '<text id="%s" x="20" y="70" class="caption">%s</text>' % (
                name, escape(text)))

    return (
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" '
        'width="%d" height="%d" role="img" aria-label="FileSystemEvents: '
        'reads, modifications, invalidations">\n'
        '<style>\n%s\n</style>\n%s\n</svg>\n' % (
            WIDTH, HEIGHT, WIDTH, HEIGHT, "\n".join(css), "\n".join(body)))


if __name__ == "__main__":
    print(generate(), end="")
