import argparse


def run():
    parser = argparse.ArgumentParser()

    parser.add_argument('--iops', required=True, type=float, help='iops for 4KiB requests')
    parser.add_argument('--bw', required=True, type=float, help='bandwidth for 4MiB requests')

    args = parser.parse_args()

    i1 = args.iops
    i2 = args.bw / 4.
    r1 = 4096
    r2 = 4096 * 1024

    c2 = i1 * i2 * (r1 - r2) / (i2 - i1)
    c1 = i1 * c2 / (c2 - i1 * r1)

    print("iops = {}".format(int(c1)))
    print("bandwidth = {}".format(int(c2)))


run()
