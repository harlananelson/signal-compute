#!/usr/bin/env Rscript
# Quarterly disproportionality signal computation.
#
# Reads hive-partitioned contingency parquet written by faers-pipeline,
# loops through quarters with a rolling window, runs
# safetysignal::detect_all_methods() per window, applies EWMA smoothing, and
# writes a long-format signals parquet ready to ship to the VPS.

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(tibble)
  library(cli)
  library(fs)
  library(safetysignal)
})

# ---- Args ----
args <- commandArgs(trailingOnly = TRUE)
parse_arg <- function(name, default = NULL) {
  idx <- which(args == paste0("--", name))
  if (length(idx) == 1 && idx < length(args)) args[idx + 1] else default
}

source_name    <- parse_arg("source", "faers")
window_qtrs    <- as.integer(parse_arg("window-quarters", "4"))
prior_strategy <- parse_arg("prior-strategy", "cumulative")  # or "per_window"
min_observed   <- as.integer(parse_arg("min-observed", "3"))
ewma_lambda    <- as.numeric(parse_arg("ewma-lambda", "0.3"))
contingency_root <- parse_arg("contingency-root",
                              "/home/harlan/data/faers-pipeline/contingency")
output_root    <- parse_arg("output-root",
                            "/home/harlan/data/signal-compute")

cli_h1("signal-compute: quarterly signals for {.field {source_name}}")
cli_inform("Window: {window_qtrs} quarters, prior: {prior_strategy}, ",
           "min_observed: {min_observed}, lambda: {ewma_lambda}")

# ---- Load contingency as a single lazy dataset ----
ds_path <- file.path(contingency_root, paste0("source=", source_name))
if (!dir_exists(ds_path)) {
  cli_abort("No contingency data at {.path {ds_path}}. Run faers-pipeline first.")
}

ds <- open_dataset(
  ds_path,
  format = "parquet",
  partitioning = arrow::hive_partition(year = arrow::int32(),
                                        quarter = arrow::string())
)

quarters_meta <- ds |>
  select("year", "quarter") |>
  distinct() |>
  collect()
# With explicit hive_partition(year=int32, quarter=string), the values are
# just "2024" and "1"/"2"/"3"/"4" after stripping the "name=" prefix.
quarters <- quarters_meta |>
  mutate(q_label = paste0(.data$year, "Q", .data$quarter)) |>
  arrange(.data$q_label) |>
  pull("q_label")

cli_inform("Quarters in dataset: {length(quarters)} ({.field {quarters[1]}} .. {.field {quarters[length(quarters)]}})")

# ---- Per-quarter compute ----
window_data <- function(ds, qtr_labels) {
  # qtr_labels like c("2010Q1", "2010Q2", ...); filter ds via the hive
  # partition keys (year: int, quarter: string "1"/"2"/"3"/"4"). The
  # in-file `quarter` column (which has "2010Q1" style values) is
  # shadowed by the partition key with the same name, so we filter
  # using path-derived partition values.
  years_filter <- as.integer(substr(qtr_labels, 1, 4))
  qs_filter    <- substr(qtr_labels, 6, 6)
  df <- ds |>
    filter(.data$year %in% years_filter, .data$quarter %in% qs_filter) |>
    collect()
  # After collect, re-derive the YYYYQN quarter label from year + quarter
  # so downstream code has the value it expects.
  if (nrow(df) > 0) {
    df$quarter <- paste0(df$year, "Q", df$quarter)
  }
  # faers-pipeline emits (rxcui, rxnorm_name, outcome_name, observed) in the
  # new schema. Map to the (drug, event) shape safetysignal expects. Keep
  # rxcui + rxnorm_name as metadata to surface downstream.
  drug_col <- dplyr::coalesce(df$rxnorm_name, df$rxcui)
  df$drug <- drug_col
  df$event <- df$outcome_name
  df |>
    group_by(.data$drug, .data$event) |>
    summarize(observed = sum(.data$observed), .groups = "drop") |>
    filter(.data$observed >= 1)
}

pooled_prior <- NULL
all_signals <- list()

for (i in seq_along(quarters)) {
  q_now <- quarters[i]
  q_start <- max(1, i - window_qtrs + 1)
  window_qs <- quarters[q_start:i]

  cli_inform("Quarter {.field {q_now}} (window {.field {window_qs[1]}}..{.field {q_now}})")

  oe_window <- window_data(ds, window_qs)
  if (nrow(oe_window) < 10) {
    cli_inform("  <10 pairs in window; skipping")
    next
  }

  # Fit / refit prior according to strategy
  if (prior_strategy == "cumulative") {
    cum_qs <- quarters[1:i]
    oe_cum <- window_data(ds, cum_qs)
    pooled_prior <- safetysignal::fit_prior(
      safetysignal::compute_observed_expected(oe_cum, drug, event, observed)
    )
  } else {
    pooled_prior <- safetysignal::fit_prior(
      safetysignal::compute_observed_expected(oe_window, drug, event, observed)
    )
  }

  result <- tryCatch(
    safetysignal::detect_all_methods(
      oe_window,
      methods = c("gps", "prr", "ror", "ic"),
      min_count = min_observed,
      prior = pooled_prior,
      verbose = FALSE
    ),
    error = function(e) {
      cli_warn("  detect_all_methods failed: {conditionMessage(e)}")
      NULL
    }
  )
  if (is.null(result)) next

  result$quarter <- q_now
  all_signals[[q_now]] <- result
  cli_inform("  {nrow(result)} pairs; {sum(result$is_signal_any, na.rm=TRUE)} flagged by any method")
}

# ---- Bind and EWMA smooth ----
long <- dplyr::bind_rows(all_signals)
cli_inform("Total rows: {nrow(long):,}")

cli_h2("EWMA smoothing (lambda = {ewma_lambda})")
long <- long |>
  arrange(.data$drug, .data$event, .data$quarter) |>
  group_by(.data$drug, .data$event) |>
  mutate(
    ewma_eb05  = stats::filter(.data$eb05,  filter = ewma_lambda,
                                method = "recursive") |> as.numeric(),
    ewma_ic025 = stats::filter(.data$ic025, filter = ewma_lambda,
                                method = "recursive") |> as.numeric()
  ) |>
  ungroup()

# ---- Write output ----
dir_create(output_root)
today <- format(Sys.Date(), "%Y-%m-%d")
out_path <- file.path(output_root,
                      paste0("signals_", source_name, "_v", today, ".parquet"))
arrow::write_parquet(long, out_path, compression = "snappy")
cli_alert_success("Wrote {nrow(long):,} rows to {.path {out_path}}")

cli_h2("Summary")
cat(sprintf("  Quarters processed:   %d\n", length(all_signals)))
cat(sprintf("  Unique drug-events:   %d\n",
            nrow(dplyr::distinct(long, .data$drug, .data$event))))
cat(sprintf("  Signals flagged any:  %d (%.1f%%)\n",
            sum(long$is_signal_any, na.rm = TRUE),
            100 * mean(long$is_signal_any, na.rm = TRUE)))
