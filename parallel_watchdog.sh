#!/usr/bin/env bash
# Watchdog for all 10 parallel scraper workers.
# Checks every 2 minutes. Restarts any worker that is:
#   - Missing its tmux session (crashed), OR
#   - Has not written to its log in STUCK_TIMEOUT seconds (hung)

STUCK_TIMEOUT=300  # 5 minutes without log growth = stuck
CHECK_INTERVAL=120
LOG="parallel_watchdog.log"
DIR="$(cd "$(dirname "$0")" && pwd)"

# worker_id -> "start_county end_county"
declare -A RANGES
RANGES[1]="1 8"
RANGES[2]="9 17"
RANGES[3]="18 23"
RANGES[4]="24 30"
RANGES[5]="31 38"
RANGES[6]="39 55"
RANGES[7]="56 62"
RANGES[8]="63 69"
RANGES[9]="70 73"
RANGES[10]="74 87"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$DIR/$LOG"
}

is_done() {
    local w=$1
    grep -q "^DONE$" "$DIR/worker${w}.log" 2>/dev/null
}

start_worker() {
    local w=$1
    local start=$2
    local end=$3
    local session="scraper-w$w"
    tmux kill-session -t "$session" 2>/dev/null
    tmux new-session -d -s "$session" -c "$DIR" \
        "python mn_dnr_lake_scraper.py $start $end --worker $w 2>&1 | tee -a worker${w}.log; echo 'DONE' >> worker${w}.log"
    log "Worker $w restarted (counties $start-$end, session $session)."
}

log "Parallel watchdog started."

while true; do
    for w in $(seq 1 10); do
        if is_done $w; then
            continue
        fi

        range="${RANGES[$w]}"
        start=$(echo $range | cut -d' ' -f1)
        end=$(echo $range | cut -d' ' -f2)
        session="scraper-w$w"
        logfile="$DIR/worker${w}.log"

        # Check if tmux session is alive
        if ! tmux has-session -t "$session" 2>/dev/null; then
            log "Worker $w: session missing. Restarting."
            start_worker $w $start $end
            continue
        fi

        # Check if log has grown recently
        if [[ -f "$logfile" ]]; then
            last_mod=$(stat -c %Y "$logfile" 2>/dev/null || echo 0)
            now=$(date +%s)
            age=$(( now - last_mod ))
            if (( age > STUCK_TIMEOUT )); then
                log "Worker $w: no log activity for ${age}s. Restarting."
                start_worker $w $start $end
            fi
        fi
    done

    sleep $CHECK_INTERVAL
done
