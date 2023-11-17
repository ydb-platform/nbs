import logging
import re


def get_unused_blocks_ntfs(fs, bs):
    logging.debug(f"[unused_blocks] bs: {bs} path: {fs['ntfswipe']}")

    path = fs['ntfswipe']

    unused_blocks = []

    unused_re = re.compile(r'=== \[unused\] pos: (\d+) count: (\d+)')

    p = (-1, 0)

    with open(path) as file:
        for line in file:
            if not line.startswith('==='):
                continue
            m = unused_re.search(line)
            assert m

            offset = int(m.group(1))
            count = int(m.group(2))

            assert offset % bs == 0
            assert count % bs == 0

            b = offset // bs
            n = count // bs
            e = b + n

            assert n != 0

            if p[0] == -1:
                p = (b, e)
                continue

            if b == p[1]:
                p = (p[0], e)
                continue

            unused_blocks.append(p)
            p = (b, e)

    if p[0] != -1:
        unused_blocks.append(p)

    return unused_blocks
