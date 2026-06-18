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
        for f in /tmp/run_test.command.txt /tmp/run_test.ulimit.soft.txt /tmp/run_test.ulimit.hard.txt /tmp/run_test.limits.txt; do
            sudo test -f "$f" && sudo cp -f "$f" "$backtrace_dir"
        done
        sudo find /coredumps -maxdepth 1 -type f ! -name '*.txt' -printf '%f\\n' | sort | sudo tee "$backtrace_dir/guest-cores.txt"
        sudo find /coredumps -maxdepth 1 -type f -name '*.txt' -exec cp -f {{}} "$backtrace_dir" \\;
        sudo dmesg | tail -n 200 | sudo tee "$backtrace_dir/guest-dmesg.txt"

        run_test_command=''
        if sudo test -f /tmp/run_test.command.txt; then
            run_test_command=$(sudo cat /tmp/run_test.command.txt)
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
            executable=''
            if sudo test -s "/coredumps/$core.exe.txt"; then
                executable=$(sudo cat "/coredumps/$core.exe.txt" | sed 's/!/\//g')
            fi
            if [ -n "$executable" ] && sudo test -x "$executable"; then
                sudo "$gdb" "$executable" "/coredumps/$core" "${{gdb_args[@]}}" | sudo tee "$backtrace"
            elif [ -n "$run_test_command" ] && sudo test -x "$run_test_command" && [ "${{run_test_basename:0:15}}" = "$executable_name" ]; then
                sudo "$gdb" "$run_test_command" "/coredumps/$core" "${{gdb_args[@]}}" | sudo tee "$backtrace"
            else
                sudo "$gdb" -c "/coredumps/$core" "${{gdb_args[@]}}" | sudo tee "$backtrace"
            fi
        done < "$backtrace_dir/guest-cores.txt"
    """)


def setup_coredumps(ssh):
    ssh("sudo mkdir -p /coredumps")
    ssh("sudo chmod 1777 /coredumps")
    # this wrapper is needed because we want to use %E in the pattern
    # and not %e, because %E expands to the executable filename, but
    # with slashes replaced by '!' to avoid directory creation. 
    # If the resulting core filename is too long, it is truncated
    # to 128 bytes, including the trailing '*', and the '*' is
    # appended to the truncated filename. (since Linux 5.19)
    # and to get backtraces with correct executable names,
    # So we create "short" core name that can be matched to the executable name
    # and use it in the backtrace script to find the right executable for gdb.
    # it also alleviates issue with too long file names (and simply looks nicer)
    core_helper = textwrap.dedent("""\
        #!/bin/bash
        set -eu

        core_dir=/coredumps
        comm="${1//\\//_}"
        comm="${comm//[[:space:]]/_}"
        pid="$2"
        signal="$3"
        encoded_executable="$4"
        core_name="$comm.$pid.$signal"

        mkdir -p "$core_dir"
        chmod 1777 "$core_dir"

        cat > "$core_dir/$core_name"
        printf '%s\\n' "$encoded_executable" > "$core_dir/$core_name.exe.txt"
    """)
    ssh(f"sudo tee /usr/local/bin/qemu-core-helper <<'EOF' && sudo chmod +x /usr/local/bin/qemu-core-helper\n{core_helper}\nEOF")
    ssh("sudo sysctl -w 'kernel.core_pattern=|/usr/local/bin/qemu-core-helper %e %p %s %E'")
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
