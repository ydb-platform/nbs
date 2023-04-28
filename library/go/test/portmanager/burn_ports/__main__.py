from yatest.common.network import PortManager


if __name__ == "__main__":
    pm = PortManager()

    for _ in range(10000):
        print(pm.get_port())
