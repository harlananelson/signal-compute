#!/usr/bin/env bash
# Deploy precomputed signals + enrichment data to the Hetzner VPS.
#
# Copies (all SCPed under /srv/shiny-server/${APP_SLUG}/data/):
#   signals.parquet           — latest drug-level signals_faers_v*.parquet
#   signals_substance.parquet — substance-rolled signals (DiAna-resolved
#                               active ingredients; ~30% fewer pairs);
#                               drives the substance-mode UI toggle. Skipped
#                               with a warning if the substance build hasn't
#                               run yet.
#   drug_dictionary.parquet   — drug name dictionary (faers-pipeline)
#   event_dictionary.parquet  — event name dictionary (faers-pipeline)
#   fda_labels.parquet        — multiproduct label cache w/ indications_and_usage
#   meddra_hierarchy.parquet  — PT -> UMLS-CUI synonyms cache
#   atc_classes.parquet       — substance -> ATC2/3/4 lookup
#   diana_dictionary.parquet  — DiAna raw-name -> substance dictionary
#   first_approval.parquet    — substance -> first FDA approval date
#
# The five enrichment files are required by signal_timeline.R for the Novel/
# Treats/Class columns and the indication-confound filter (Track D4). Without
# them the app degrades to NA novel flags.
#
# Run: ./scripts/deploy_to_vps.sh [APP_SLUG=faers-mobi]

set -euo pipefail

APP_SLUG="${1:-faers-mobi}"
VPS_USER_HOST="root@5.78.69.136"
VPS_DATA_DIR="/srv/shiny-server/${APP_SLUG}/data"

SIGNALS_GLOB="/home/harlan/data/signal-compute/signals_faers_v*.parquet"
SUBSTANCE_GLOB="/home/harlan/data/signal-compute/substance/signals_faers_v*.parquet"
DRUG_DICT="/home/harlan/data/faers-pipeline/output/drug_dictionary.parquet"
EVENT_DICT="/home/harlan/data/faers-pipeline/output/event_dictionary.parquet"
# Multiproduct cache concatenates all marketed products under a single
# substance — fixes the "label cache picks the wrong formulation" bug
# (Alkindi vs topical hydrocortisone, Peridex vs antiseptic skin solution).
FDA_LABELS="/home/harlan/data/faers-pipeline/output/fda_labels_multiproduct.parquet"
MEDDRA="/home/harlan/data/diana/meddra_hierarchy.parquet"
ATC="/home/harlan/data/diana/atc_classes.parquet"
DIANA="/home/harlan/data/diana/diana_dictionary.parquet"
FIRST_APPROVAL="/home/harlan/data/diana/first_approval.parquet"

# Pick the most recent signals bundle
SIGNALS=$(ls -1t $SIGNALS_GLOB 2>/dev/null | head -1)
if [[ -z "$SIGNALS" ]]; then
  echo "ERROR: no signals parquet matching $SIGNALS_GLOB" >&2
  exit 1
fi
# Substance bundle is optional. The app falls back to drug-level when the
# substance file is missing; emit a warning rather than aborting the deploy.
SUBSTANCE=$(ls -1t $SUBSTANCE_GLOB 2>/dev/null | head -1)
if [[ -z "$SUBSTANCE" ]]; then
  echo "WARNING: no substance parquet at $SUBSTANCE_GLOB — substance toggle will fall back to drug-level on the VPS" >&2
fi
for f in "$DRUG_DICT" "$EVENT_DICT" "$FDA_LABELS" "$MEDDRA" "$ATC" "$DIANA" "$FIRST_APPROVAL"; do
  [[ -f "$f" ]] || { echo "ERROR: missing $f" >&2; exit 1; }
done

echo "Deploying to $APP_SLUG:"
echo "  signals (drug):     $SIGNALS ($(du -h "$SIGNALS" | cut -f1))"
if [[ -n "$SUBSTANCE" ]]; then
  echo "  signals_substance:  $SUBSTANCE ($(du -h "$SUBSTANCE" | cut -f1))"
else
  echo "  signals_substance:  (skipped — file not found)"
fi
echo "  drugs:              $DRUG_DICT ($(du -h "$DRUG_DICT" | cut -f1))"
echo "  events:             $EVENT_DICT ($(du -h "$EVENT_DICT" | cut -f1))"
echo "  fda_labels:         $FDA_LABELS ($(du -h "$FDA_LABELS" | cut -f1))"
echo "  meddra:             $MEDDRA ($(du -h "$MEDDRA" | cut -f1))"
echo "  atc_classes:        $ATC ($(du -h "$ATC" | cut -f1))"
echo "  diana:              $DIANA ($(du -h "$DIANA" | cut -f1))"
echo "  first_approval:     $FIRST_APPROVAL ($(du -h "$FIRST_APPROVAL" | cut -f1))"
echo ""

ssh "$VPS_USER_HOST" "mkdir -p $VPS_DATA_DIR"
scp "$SIGNALS"        "$VPS_USER_HOST:$VPS_DATA_DIR/signals.parquet"
if [[ -n "$SUBSTANCE" ]]; then
  scp "$SUBSTANCE"    "$VPS_USER_HOST:$VPS_DATA_DIR/signals_substance.parquet"
fi
scp "$DRUG_DICT"      "$VPS_USER_HOST:$VPS_DATA_DIR/drug_dictionary.parquet"
scp "$EVENT_DICT"     "$VPS_USER_HOST:$VPS_DATA_DIR/event_dictionary.parquet"
scp "$FDA_LABELS"     "$VPS_USER_HOST:$VPS_DATA_DIR/fda_labels.parquet"
scp "$MEDDRA"         "$VPS_USER_HOST:$VPS_DATA_DIR/meddra_hierarchy.parquet"
scp "$ATC"            "$VPS_USER_HOST:$VPS_DATA_DIR/atc_classes.parquet"
scp "$DIANA"          "$VPS_USER_HOST:$VPS_DATA_DIR/diana_dictionary.parquet"
scp "$FIRST_APPROVAL" "$VPS_USER_HOST:$VPS_DATA_DIR/first_approval.parquet"

ssh "$VPS_USER_HOST" "chown -R shiny:shiny $VPS_DATA_DIR && systemctl restart shiny-server"

echo ""
echo "Deployed. Test: curl -sI https://${APP_SLUG#faers-}.mobi/ | head -3  (or the app's domain)"
