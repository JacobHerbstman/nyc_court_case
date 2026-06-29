# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/write_council_district_upgrade_feasibility_memo/code")
# dcp_council_boundary_archive_audit_summary_csv <- "../input/dcp_council_boundary_archive_audit_summary.csv"
# dcp_council_boundary_archive_canonical_regimes_csv <- "../input/dcp_council_boundary_archive_canonical_regimes.csv"
# dcp_council_boundary_archive_feasibility_gate_csv <- "../input/dcp_council_boundary_archive_feasibility_gate.csv"
# out_memo_md <- "../output/council_district_upgrade_feasibility_memo.md"
# out_decision_csv <- "../output/council_district_upgrade_feasibility_decision_table.csv"

suppressPackageStartupMessages({
  library(dplyr)
  library(glue)
  library(readr)
  library(stringr)
  library(tibble)
})

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 5) {
  stop("Expected 5 arguments: dcp_council_boundary_archive_audit_summary_csv dcp_council_boundary_archive_canonical_regimes_csv dcp_council_boundary_archive_feasibility_gate_csv out_memo_md out_decision_csv")
}

dcp_council_boundary_archive_audit_summary_csv <- args[1]
dcp_council_boundary_archive_canonical_regimes_csv <- args[2]
dcp_council_boundary_archive_feasibility_gate_csv <- args[3]
out_memo_md <- args[4]
out_decision_csv <- args[5]

summary_df <- read_csv(dcp_council_boundary_archive_audit_summary_csv, show_col_types = FALSE, na = c("", "NA"))
canonical_df <- read_csv(dcp_council_boundary_archive_canonical_regimes_csv, show_col_types = FALSE, na = c("", "NA"))
gate_df <- read_csv(dcp_council_boundary_archive_feasibility_gate_csv, show_col_types = FALSE, na = c("", "NA"))

metric_value <- function(metric_name) {
  value <- summary_df |>
    filter(metric == metric_name) |>
    pull(value)

  if (length(value) == 0) {
    return(NA_character_)
  }

  value[[1]]
}

decision_table <- bind_rows(
  tibble(
    check_name = "Current council layer stages cleanly",
    result = ifelse(isTRUE(gate_df$current_council_stage_pass[[1]]), "Pass", "Fail"),
    interpretation = glue("Current official council layer stages with {metric_value('current_council_district_count')} districts."),
    implication = "Current council geography is usable for later post-2006 or post-2010 work."
  ),
  tibble(
    check_name = "Official archive inventory is nonempty",
    result = ifelse(isTRUE(gate_df$archive_inventory_nonempty[[1]]), "Pass", "Fail"),
    interpretation = glue("Accessible official DCP BYTES council archive covers {gate_df$earliest_archive_year[[1]]}-{gate_df$latest_archive_year[[1]]}."),
    implication = "Time-varying council districts are feasible from the mid-2000s onward."
  ),
  tibble(
    check_name = "Official pre-1991 council regime exists",
    result = ifelse(isTRUE(gate_df$pre_1991_official_available[[1]]), "Pass", "Fail"),
    interpretation = ifelse(
      isTRUE(gate_df$pre_1991_official_available[[1]]),
      "The accessible official archive contains a pre-1991 council boundary.",
      "The accessible official DCP archive starts in 2006, so no official pre-1991 council boundary is available."
    ),
    implication = "Without an official pre-1991 council map, a 1990 replacement treatment cannot be built without approximation."
  ),
  tibble(
    check_name = "Phase 1 static 1990 replacement treatment allowed",
    result = ifelse(isTRUE(gate_df$phase_1_static_council_treatment_allowed[[1]]), "Pass", "Fail"),
    interpretation = ifelse(
      isTRUE(gate_df$phase_1_static_council_treatment_allowed[[1]]),
      "The hard feasibility gate passes.",
      gate_df$stop_reason[[1]]
    ),
    implication = "The council upgrade stops at the archive audit and feasibility memo rather than replacing the CD treatment."
  )
)

available_regimes <- canonical_df |>
  filter(available_flag) |>
  mutate(regime_text = ifelse(
    !is.na(canonical_release),
    glue("{expected_regime} -> canonical release {canonical_release}"),
    expected_regime
  )) |>
  pull(regime_text)

missing_regimes <- canonical_df |>
  filter(!available_flag) |>
  pull(expected_regime)

memo_lines <- c(
  "# Council-District Upgrade Feasibility Memo",
  "",
  "## Bottom Line",
  "",
  "The official DCP council-boundary archive is usable for later time-varying council work, but it does **not** support a 1990 replacement treatment.",
  "",
  glue("The accessible official BYTES archive runs from **{gate_df$earliest_archive_year[[1]]}** through **{gate_df$latest_archive_year[[1]]}**. That is enough to recover later council regimes, but not enough to recover an official pre-1991 council map."),
  "",
  "## What Passes",
  "",
  glue("- The current official council layer stages cleanly with **{metric_value('current_council_district_count')}** districts."),
  glue("- The accessible official archive is nonempty and contains **{metric_value('archive_json_release_count')}** shoreline-clipped releases."),
  glue("- The archive exposes **{metric_value('observed_regime_count')}** later council regimes: {str_c(available_regimes, collapse = '; ')}."),
  "",
  "## What Fails",
  "",
  glue("- There is **no** accessible official pre-1991 council boundary in the DCP archive."),
  if (length(missing_regimes) > 0) {
    glue("- Missing expected regimes in the accessible archive: {str_c(missing_regimes, collapse = '; ')}.")
  } else {
    NULL
  },
  glue("- Hard gate result: **phase 1 static replacement treatment allowed = {gate_df$phase_1_static_council_treatment_allowed[[1]]}**."),
  "",
  "## Implication",
  "",
  "The current CD treatment remains canonical. A later council-district extension is still feasible for post-2006 or post-2010 work, but the 1990 council replacement design should not be built from this archive because it would require approximation."
)

write_csv(decision_table, out_decision_csv, na = "")
writeLines(memo_lines, out_memo_md)

cat("Wrote council district upgrade feasibility memo to", out_memo_md, "\n")
