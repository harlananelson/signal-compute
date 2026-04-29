#!/usr/bin/env Rscript
# Build a substance-level contingency dataset from the existing drug-level
# contingency, using the DiAna dictionary for normalization.
#
# Output schema is identical to the input contingency (columns: quarter,
# rxcui, rxnorm_name, outcome_name, observed) so signal-compute's
# compute_quarterly.R can read it without modification. The semantics
# differ:
#   - rxnorm_name now holds an active-substance name (e.g. "ibuprofen")
#     for resolved drugs, OR the original raw name for unresolved drugs.
#   - rxcui is NA for substance-aggregated rows; preserved for
#     pass-through (unresolved) rows.
#   - observed is the SUM of observed counts across all raw drug names
#     that resolved to the same substance, within (outcome_name, quarter).
#
# Resolution policy: rows whose lowercased rxnorm_name matches a DiAna
# entry get rolled up to the entry's substance. Rows that don't match
# pass through unchanged (raw name treated as a singleton substance).
# This keeps 100% of FAERS report volume in the dataset; drugs with no
# DiAna match contribute their own (unaggregated) signals.
#
# Compression observed on FAERS contingency (2024Q4 sample):
#   - ~2,594 distinct rxnorm_name in -> ~600 distinct substances out
#     (~4× compression on flagged drugs)
#   - per-substance observed counts go up by the same factor on average,
#     boosting statistical power for ingredient-level signals.
#
# Run: nix develop --command Rscript scripts/build_substance_contingency.R [--source faers|aers] [--out PATH]

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(cli)
  library(fs)
})

# ---- args ----
args <- commandArgs(trailingOnly = TRUE)
source_name <- "faers"
out_root <- NULL
i <- 1
while (i <= length(args)) {
  if (args[i] == "--source") { source_name <- args[i + 1]; i <- i + 2; next }
  if (args[i] == "--out")    { out_root    <- args[i + 1]; i <- i + 2; next }
  i <- i + 1
}
contingency_root <- file.path("/home/harlan/data/faers-pipeline/contingency",
                              paste0("source=", source_name))
if (is.null(out_root)) {
  out_root <- file.path("/home/harlan/data/faers-pipeline/contingency-substance",
                        paste0("source=", source_name))
}

if (!dir.exists(contingency_root)) {
  cli_abort("Contingency root does not exist: {.path {contingency_root}}")
}

cli_h2("Substance-level contingency build")
cli_inform("Source:      {.path {contingency_root}}")
cli_inform("Destination: {.path {out_root}}")

# ---- load DiAna dictionary ----
diana_path <- "/home/harlan/data/diana/diana_dictionary.parquet"
if (!file.exists(diana_path)) cli_abort("Missing DiAna dictionary at {.path {diana_path}}")
diana <- read_parquet(diana_path)
cli_inform("DiAna dictionary: {format(nrow(diana), big.mark = ',')} entries, {length(unique(diana$substance))} distinct substances")

# Build a name -> substance lookup: lowercased drugname column, picking the
# first substance when there are duplicate drugnames (rare in practice).
diana_lookup <- diana %>%
  distinct(drugname, .keep_all = TRUE) %>%
  select(drugname, substance)

# ---- iterate partitions ----
year_dirs <- dir_ls(contingency_root, type = "directory")
total_in <- 0L
total_out <- 0L
total_resolved <- 0L
fs::dir_create(out_root)

for (yd in year_dirs) {
  year <- sub(".*year=", "", yd)
  q_dirs <- dir_ls(yd, type = "directory")
  for (qd in q_dirs) {
    q <- sub(".*quarter=", "", qd)
    parts <- list.files(qd, pattern = "\\.parquet$", full.names = TRUE)
    if (length(parts) == 0) next

    df <- bind_rows(lapply(parts, read_parquet))
    n_in <- nrow(df)
    if (n_in == 0) next

    # Lowercased lookup; preserve outcome_concept_id if present
    lc <- tolower(df$rxnorm_name)
    sub <- diana_lookup$substance[match(lc, diana_lookup$drugname)]
    n_resolved <- sum(!is.na(sub))

    # Where resolved, replace name with substance and drop rxcui (which
    # would no longer be 1:1 after aggregation). Where not, keep as-is.
    df$rxnorm_name <- ifelse(is.na(sub), df$rxnorm_name, sub)
    df$rxcui <- ifelse(is.na(sub), df$rxcui, NA_character_)

    # Aggregate by (rxnorm_name, outcome_name, quarter). rxcui is collapsed
    # by first(); for resolved rows it's NA already, for unresolved it
    # stays the original rxcui.
    out <- df %>%
      group_by(rxnorm_name, outcome_name, quarter) %>%
      summarise(
        observed = sum(observed, na.rm = TRUE),
        rxcui = dplyr::first(rxcui),
        outcome_concept_id = if ("outcome_concept_id" %in% names(df))
          dplyr::first(outcome_concept_id) else NA_character_,
        .groups = "drop"
      ) %>%
      select(any_of(c("quarter", "rxcui", "rxnorm_name", "outcome_name",
                      "observed", "outcome_concept_id")))

    # Write to output partition
    out_qd <- file.path(out_root, paste0("year=", year), paste0("quarter=", q))
    fs::dir_create(out_qd, recurse = TRUE)
    write_parquet(out, file.path(out_qd, "part-0.parquet"),
                  compression = "snappy")

    total_in <- total_in + n_in
    total_out <- total_out + nrow(out)
    total_resolved <- total_resolved + n_resolved
    cli_alert_success("{year}Q{q}: {format(n_in, big.mark=',')} -> {format(nrow(out), big.mark=',')} rows ({format(n_resolved, big.mark=',')} resolved)")
  }
}

cli_h2("Summary")
cat(sprintf("  Input rows:      %s\n", format(total_in,  big.mark = ",")))
cat(sprintf("  Output rows:     %s (%.1f%% compression)\n",
            format(total_out, big.mark = ","),
            100 * (1 - total_out / total_in)))
cat(sprintf("  Rows resolved:   %s (%.1f%%)\n",
            format(total_resolved, big.mark = ","),
            100 * total_resolved / total_in))
cat(sprintf("  Output root:     %s\n", out_root))

cli_alert_success("Substance contingency built. Run signal-compute against this root to produce substance-level signals.")
