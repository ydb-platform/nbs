import shlex
import textwrap

import yatest.common as common


def _process_coredumps_script(gdb, backtrace_dir):
    return textwrap.dedent(f"""\
        #!/bin/bash
        set -x

        gdb={shlex.quote(gdb)}
        backtrace_dir={shlex.quote(backtrace_dir)}
        gdb_args=(--batch -iex 'set print thread-events off' -iex 'set auto-load safe-path /' -ex bt)

        sudo mkdir -p "$backtrace_dir"
        sudo sysctl kernel.core_pattern kernel.core_uses_pid fs.suid_dumpable | sudo tee "$backtrace_dir/guest-coredump-sysctl.txt"
        for f in /tmp/run_test.command /tmp/run_test.ulimit.soft /tmp/run_test.ulimit.hard /tmp/run_test.limits; do
            sudo test -f "$f" && sudo cp -f "$f" "$backtrace_dir"
        done
        sudo find /coredumps -maxdepth 1 -type f -printf '%f\\n' | sort | sudo tee "$backtrace_dir/guest-cores.txt"
        sudo dmesg | tail -n 200 | sudo tee "$backtrace_dir/guest-dmesg.txt"

        run_test_command=''
        if sudo test -f /tmp/run_test.command; then
            run_test_command=$(sudo cat /tmp/run_test.command)
        fi

        core_executable_name() {{
            local core="$1"
            printf '%s\\n' "$core" | sed -E 's/\\.[^.]*\\.[^.]*$//'
        }}

        while IFS= read -r core; do
            [ -n "$core" ] || continue
            backtrace="$backtrace_dir/$core.backtrace"
            executable_name=$(core_executable_name "$core")
            run_test_basename="${{run_test_command##*/}}"
            if [ -n "$run_test_command" ] && sudo test -x "$run_test_command" && [ "${{run_test_basename:0:15}}" = "$executable_name" ]; then
                sudo "$gdb" "$run_test_command" "/coredumps/$core" "${{gdb_args[@]}}" | sudo tee "$backtrace"
            else
                sudo "$gdb" -c "/coredumps/$core" "${{gdb_args[@]}}" | sudo tee "$backtrace"
            fi
        done < "$backtrace_dir/guest-cores.txt"
    """)


def setup_coredumps(ssh):
    ssh("sudo mkdir -p /coredumps")
    ssh("sudo chmod 1777 /coredumps")
    ssh("sudo sysctl -w 'kernel.core_pattern=/coredumps/%e.%p.%s'")
    ssh("sudo sysctl -w 'kernel.core_uses_pid=1'")
    # Tests are launched via `sudo /run_test.sh`, so their dumpability is
    # governed by fs.suid_dumpable. Keep dumps enabled for these guest-side
    # privileged launches and store them in a root-owned absolute path.
    ssh("sudo sysctl -w 'fs.suid_dumpable=2'")
    ssh(
        "sudo grep -q '^\\* soft core unlimited$' /etc/security/limits.conf || "
        "printf '%s\\n' '* soft core unlimited' '* hard core unlimited' | "
        "sudo tee -a /etc/security/limits.conf >/dev/null"
    )
    ssh(
        "if command -v visudo >/dev/null 2>&1; then "
        "tmp=$(mktemp) && "
        "printf '%s\\n' "
        "'Defaults rlimit_core=default' "
        "'Defaults env_keep += \"ASAN_OPTIONS MSAN_OPTIONS TSAN_OPTIONS UBSAN_OPTIONS\"' "
        "> \"$tmp\" && "
        "if sudo visudo -c -f \"$tmp\" >/dev/null 2>&1; then "
        "sudo install -m 0440 \"$tmp\" /etc/sudoers.d/98-rlimit; "
        "fi; "
        "rm -f \"$tmp\"; "
        "fi"
    )

    script = _process_coredumps_script(common.runtime.gdb_path(), common.output_path())
    ssh(f"sudo tee /process_coredumps.sh <<'EOF' && sudo chmod +x /process_coredumps.sh\n{script}\nEOF")


def process_coredumps(ssh):
    ssh("sudo /process_coredumps.sh")
