#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "Usage: $0 --report-dir DIR --ya-out DIR [options] -- REPORT_OPTIONS..." >&2
    exit 2
}

report_dir=
ya_out=
evlog=
warning_title="Trace report"
html_url=
otlp_url=
inputs_url=
summary=
summary_heading=
github_output=

while [ "$#" -gt 0 ]; do
    case "$1" in
        --report-dir)
            report_dir=${2:-}
            shift 2
            ;;
        --ya-out)
            ya_out=${2:-}
            shift 2
            ;;
        --evlog)
            evlog=${2:-}
            shift 2
            ;;
        --warning-title)
            warning_title=${2:-}
            shift 2
            ;;
        --html-url)
            html_url=${2:-}
            shift 2
            ;;
        --otlp-url)
            otlp_url=${2:-}
            shift 2
            ;;
        --inputs-url)
            inputs_url=${2:-}
            shift 2
            ;;
        --summary)
            summary=${2:-}
            shift 2
            ;;
        --summary-heading)
            summary_heading=${2:-}
            shift 2
            ;;
        --github-output)
            github_output=${2:-}
            shift 2
            ;;
        --)
            shift
            break
            ;;
        *) usage ;;
    esac
done
if [ -z "$report_dir" ] || [ -z "$ya_out" ]; then
    usage
fi

mkdir -p -- "$report_dir"
rm -f -- \
    "$report_dir/trace.html" \
    "$report_dir/trace.manifest.json" \
    "$report_dir/trace.otlp.jsonl.gz" \
    "$report_dir/trace-inputs.tar.gz"

report_args=(--ya-out "$ya_out" --output-dir "$report_dir")
if [ -n "$evlog" ]; then
    report_args+=(--evlog "$evlog")
fi

if ! python3 -m scripts.tracing.ya_trace_report "${report_args[@]}" "$@"; then
    echo "::warning title=$warning_title::Unable to generate the ya OTLP trace bundle"
fi

script_dir=$(dirname "$(realpath -- "$0")")
if ! bash "$script_dir/pack_ya_trace_inputs.sh" "$report_dir" "$ya_out" "$evlog"; then
    echo "::warning title=$warning_title::Unable to archive the raw ya trace inputs"
fi

report_available=0
inputs_available=0
if [ -f "$report_dir/trace.html" ] && [ -f "$report_dir/trace.otlp.jsonl.gz" ]; then
    report_available=1
fi
if [ -f "$report_dir/trace-inputs.tar.gz" ]; then
    inputs_available=1
fi

if [ -n "$github_output" ]; then
    if [ "$report_available" -eq 1 ]; then
        echo "html_url=$html_url" >> "$github_output"
        echo "otlp_url=$otlp_url" >> "$github_output"
    fi
    if [ "$inputs_available" -eq 1 ]; then
        echo "inputs_url=$inputs_url" >> "$github_output"
    fi
fi

if [ -n "$summary" ]; then
    if [ "$report_available" -eq 1 ]; then
        if [ "$inputs_available" -eq 1 ]; then
            link="[Execution trace]($html_url) ([raw OTLP JSONL]($otlp_url), [raw trace inputs]($inputs_url))"
        else
            link="[Execution trace]($html_url) ([raw OTLP JSONL]($otlp_url))"
        fi
    elif [ "$inputs_available" -eq 1 ]; then
        link="[Raw trace inputs]($inputs_url)"
    else
        link=
    fi
    if [ -n "$link" ]; then
        if [ -n "$summary_heading" ]; then
            printf '\n### %s\n' "$summary_heading" >> "$summary"
        fi
        printf '\n%s\n' "$link" >> "$summary"
    fi
fi

# Trace collection is diagnostic and must never change the build/test result.
exit 0
