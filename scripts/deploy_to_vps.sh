#!/usr/bin/env bash
# Deploy precomputed signals to the Hetzner VPS.
#
# Copies:
#   /home/harlan/data/signal-compute/signals_faers_v*.parquet  -> /srv/shiny-server/faers-mobi/data/signals.parquet
#   /home/harlan/data/faers-pipeline/output/drug_dictionary.parquet  -> .../data/drug_dictionary.parquet
#   /home/harlan/data/faers-pipeline/output/event_dictionary.parquet -> .../data/event_dictionary.parquet
#
# Run: ./scripts/deploy_to_vps.sh [APP_SLUG=faers-mobi]

set -euo pipefail

APP_SLUG="${1:-faers-mobi}"
VPS_USER_HOST="root@5.78.69.136"
VPS_DATA_DIR="/srv/shiny-server/${APP_SLUG}/data"

SIGNALS_GLOB="/home/harlan/data/signal-compute/signals_faers_v*.parquet"
DRUG_DICT="/home/harlan/data/faers-pipeline/output/drug_dictionary.parquet"
EVENT_DICT="/home/harlan/data/faers-pipeline/output/event_dictionary.parquet"

# Pick the most recent signals bundle
SIGNALS=$(ls -1t $SIGNALS_GLOB 2>/dev/null | head -1)
if [[ -z "$SIGNALS" ]]; then
  echo "ERROR: no signals parquet matching $SIGNALS_GLOB" >&2
  exit 1
fi
for f in "$DRUG_DICT" "$EVENT_DICT"; do
  [[ -f "$f" ]] || { echo "ERROR: missing $f" >&2; exit 1; }
done

echo "Deploying to $APP_SLUG:"
echo "  signals:  $SIGNALS ($(du -h "$SIGNALS" | cut -f1))"
echo "  drugs:    $DRUG_DICT ($(du -h "$DRUG_DICT" | cut -f1))"
echo "  events:   $EVENT_DICT ($(du -h "$EVENT_DICT" | cut -f1))"
echo ""

ssh "$VPS_USER_HOST" "mkdir -p $VPS_DATA_DIR"
scp "$SIGNALS"    "$VPS_USER_HOST:$VPS_DATA_DIR/signals.parquet"
scp "$DRUG_DICT"  "$VPS_USER_HOST:$VPS_DATA_DIR/drug_dictionary.parquet"
scp "$EVENT_DICT" "$VPS_USER_HOST:$VPS_DATA_DIR/event_dictionary.parquet"

ssh "$VPS_USER_HOST" "chown -R shiny:shiny $VPS_DATA_DIR && systemctl restart shiny-server"

echo ""
echo "Deployed. Test: curl -sI https://${APP_SLUG#faers-}.mobi/ | head -3  (or the app's domain)"
