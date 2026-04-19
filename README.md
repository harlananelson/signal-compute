# signal-compute

Runs [`safetysignal`](https://github.com/harlananelson/safetysignal) across
hive-partitioned quarterly contingency parquet from
[`faers-pipeline`](https://github.com/harlananelson/faers-pipeline), producing
a single `signals.parquet` bundle with per-quarter EBGM/PRR/ROR/IC and EWMA
smoothing, ready to scp to the Hetzner VPS.

Runs locally. The VPS only reads the resulting parquet.

## Inputs

- `/home/harlan/data/faers-pipeline/contingency/source=<src>/year=YYYY/quarter=Q/*.parquet`
  (from `python -m faers.aggregate`)
- `/home/harlan/data/faers-pipeline/drug_dictionary.parquet`
- `/home/harlan/data/faers-pipeline/event_dictionary.parquet`

## Output

```
/home/harlan/data/signal-compute/signals_<source>_v<YYYY-MM-DD>.parquet
```

Columns (per drug-event-quarter tuple):
- `drug_concept_id`, `drug_name`, `outcome_concept_id`, `outcome_name`, `quarter`
- `observed` (count in quarter's rolling window)
- **GPS/EBGM:** `eb05`, `eb50`, `eb95`, `is_signal_gps`
- **PRR:** `prr`, `prr_lci`, `prr_uci`, `prr_chisq`, `is_signal_prr`
- **ROR:** `ror`, `ror_lci`, `ror_uci`, `is_signal_ror`
- **IC:** `ic`, `ic025`, `ic975`, `is_signal_ic`
- `n_methods_flagged`, `is_signal_any`
- **EWMA smoothed:** `ewma_eb05`, `ewma_ic025`

## Time-dimensional strategy

Per-quarter detection with a 4-quarter rolling window + pooled prior (see
`PLAN-ae-signal-platform.md`). For each quarter q:

1. Collect contingency for quarters [q-3, q] (4-quarter window)
2. If first quarter of run: fit 2-component Gamma prior on the window data
3. Otherwise: reuse cumulative-to-date prior (set by `prior_strategy`)
4. Run `safetysignal::detect_all_methods()`
5. Append `(drug, event, quarter, ...)` to the long-format result

Finally:
- EWMA smoothing per (drug, event) across quarters with λ = 0.3
- Write a single parquet bundle

## Running

```bash
cd /home/harlan/projects/signal-compute
nix develop
Rscript R/compute_quarterly.R \
  --source faers \
  --window-quarters 4 \
  --prior-strategy cumulative \
  --min-observed 3
```
