#!/usr/bin/env bash
# Watchdog for mn_dnr_lake_scraper.py
# Monitors the scraper, and if it dies, adds the last lake it was on to the
# skip list and restarts automatically.

SCRAPER="mn_dnr_lake_scraper.py"
LOG="scraper_output.log"
SKIP_FILE="lake_survey_data/.skip_dows"
WATCHDOG_LOG="watchdog.log"

mkdir -p lake_survey_data

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$WATCHDOG_LOG"
}

start_scraper() {
    nohup python "$SCRAPER" >> "$LOG" 2>&1 &
    echo $!
}

log "Watchdog started."

PID=$(start_scraper)
log "Scraper started (PID $PID)."

while true; do
    sleep 30

    if kill -0 "$PID" 2>/dev/null; then
        # Still running, all good
        continue
    fi

    # Process is gone — find the last DOW it logged
    LAST_DOW=$(grep -oP 'DOW \K[0-9]+' "$LOG" | tail -1)

    if [[ -n "$LAST_DOW" ]]; then
        log "Scraper stopped. Last lake: DOW $LAST_DOW. Adding to skip list."
        echo "$LAST_DOW" >> "$SKIP_FILE"
        # Deduplicate skip file
        sort -u "$SKIP_FILE" -o "$SKIP_FILE"
    else
        log "Scraper stopped. Could not determine last lake."
    fi

    log "Restarting scraper..."
    PID=$(start_scraper)
    log "Scraper restarted (PID $PID)."
done
