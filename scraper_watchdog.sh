#!/bin/bash
# Watchdog for mn_dnr_lake_scraper.py
# - Restarts if the process dies
# - Restarts and skips the stuck lake if no progress for STUCK_TIMEOUT seconds

SCRIPT="mn_dnr_lake_scraper.py"
LOG="/tmp/scraper_run.log"
PROCESSED="lake_survey_data/.processed_dows.txt"
STUCK_TIMEOUT=90   # seconds on same lake before declaring it stuck
CHECK_INTERVAL=15  # how often to poll

PID=""
last_dow=""
last_dow_time=0

start_scraper() {
    nohup python3 "$SCRIPT" > "$LOG" 2>&1 &
    PID=$!
    echo "[watchdog] $(date '+%H:%M:%S') Started scraper PID=$PID"
}

get_current_dow() {
    # Matches lines like:  [47/193] Erie (DOW 15015200)
    grep -oP '\(DOW \K[0-9]+(?=\))' "$LOG" 2>/dev/null | tail -1
}

skip_dow() {
    local dow=$1
    echo "[watchdog] $(date '+%H:%M:%S') Adding DOW $dow to processed list (skip)"
    echo "$dow" >> "$PROCESSED"
}

# Kill any existing scraper before starting
pkill -f "$SCRIPT" 2>/dev/null
sleep 1

start_scraper
last_dow_time=$(date +%s)

while true; do
    sleep $CHECK_INTERVAL

    if ! kill -0 "$PID" 2>/dev/null; then
        echo "[watchdog] $(date '+%H:%M:%S') Scraper died — restarting..."
        start_scraper
        last_dow=""
        last_dow_time=$(date +%s)
        continue
    fi

    current_dow=$(get_current_dow)
    now=$(date +%s)

    if [ -n "$current_dow" ]; then
        if [ "$current_dow" = "$last_dow" ]; then
            elapsed=$((now - last_dow_time))
            if [ $elapsed -ge $STUCK_TIMEOUT ]; then
                echo "[watchdog] $(date '+%H:%M:%S') Stuck on DOW $current_dow for ${elapsed}s — skipping and restarting..."
                kill "$PID" 2>/dev/null
                wait "$PID" 2>/dev/null
                skip_dow "$current_dow"
                start_scraper
                last_dow=""
                last_dow_time=$now
            fi
        else
            last_dow="$current_dow"
            last_dow_time=$now
        fi
    fi
done
