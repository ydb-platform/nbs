#!/bin/sh

usage() {
    cat <<EOF
Measure round trip time using icmp6 ping

Usage:
    $(basename $0) [flags] <host>

Arguments:
    <host>  Host, conductor group or xDS wildcard. Similar to pssh
    -b int  Batch size
    -t int  Show only results with p99 exceeding this number (in seconds)
    -p int  Number of different peers to ping
    -d int  Ping duration (in seconds)
EOF
    exit 0
}

[ -z $1 ] && { usage; }

batch_size=500
rtt_threshold=10
peers_to_ping=10
ping_duration=900

while getopts b:t:p:d: flag; do
    case "${flag}" in
        b) batch_size=${OPTARG};;
        t) rtt_threshold=${OPTARG};;
        p) peers_to_ping=${OPTARG};;
        d) ping_duration=${OPTARG};;
    esac
done

shift $((OPTIND-1))

hosts_=`pssh list $@`
read -r -a hosts <<< $hosts_

(for i in `seq 0 $batch_size $((${#hosts[@]} - 1))`; do
    peers=(${hosts[@]:i:batch_size})
    peers_num=${#peers[@]}
    peers_to_ping_=$((peers_num < peers_to_ping ? peers_num : peers_to_ping))
    p50=$((ping_duration * peers_to_ping_ * 50 / 100))
    p99=$((ping_duration * peers_to_ping_ * 99 / 100))
    pssh run "peers=(${peers[@]}); (for peer in \$(shuf -n $peers_to_ping_ -e \${peers[@]/\$(hostname)}); do ping6 -n -i 1 -c $ping_duration \$peer & done) |& awk -F '[ =]' '/icmp/{print \$10}' | sort -n | awk -v h=\$(hostname) 'NR==$p99{p99=\$0}NR==$p50{p50=\$0}END{print h,\"p50=\"p50,\"p99=\"p99}'" -p $batch_size ${peers[@]} 2>&1 &
done) | awk -F '[ =]' -v t=$rtt_threshold '/p99/{if($5>t){print $0}}'
