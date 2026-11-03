"""Run only on a provisioned disposable VM, never on a service VM."""

import argparse
import fcntl
import signal
import sys
import time

from .cloud import Cloud
from .config import Blocked, load
from .devices import Devices
from .runner import reconcile_known, run_cycle, serve
from .state import State, metrics
from .transport import interrupt_action


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, help="Private JSON file; see deploy/config.example.json")
    parser.add_argument("action", choices=("preflight", "once", "serve", "cleanup", "metrics", "ack-failure"))
    args = parser.parse_args()
    if args.action != "serve":
        signal.signal(signal.SIGTERM, interrupt_action)
        signal.signal(signal.SIGINT, interrupt_action)
    try:
        config = load(args.config)
        if args.action == "preflight":
            Cloud(config).verify_vm(time.monotonic() + config.cycle_timeout_seconds)
            Devices(config).validate()
            print("OK: configured VM and two unmounted disposable disks verified; no writes performed")
        elif args.action == "once":
            return run_cycle(config)
        elif args.action == "serve":
            serve(config, args.config)
        elif args.action == "cleanup":
            reconcile_known(config)
        elif args.action == "metrics":
            print(metrics(config, State(config.state_dir).read(), 0), end="")
        elif args.action == "ack-failure":
            state = State(config.state_dir)
            with (state.directory / "cycle.lock").open("a") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                state.data = state.read()
                if state.data["active"] or state.data["status"] != "pass":
                    raise Blocked("A failure can only be acknowledged after reconciliation and a passing cycle")
                state.data["failure_latched"] = False
                state.save()
        return 0
    except (Blocked, OSError, ValueError):
        print("BLOCKED: configuration, identity or journal precondition failed; see runbook", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
