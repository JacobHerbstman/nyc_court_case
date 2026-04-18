# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/stage_dcp_cd_profiles_1990_2000/code")
# dcp_cd_profiles_raw_files_csv <- "../input/dcp_cd_profiles_1990_2000_raw_files.csv"
# out_index_csv <- "../output/dcp_cd_profiles_1990_2000_files.csv"
# out_qc_csv <- "../output/dcp_cd_profiles_1990_2000_qc.csv"

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
})

source("../../_lib/source_pipeline_utils.R")

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 3) {
  stop("Expected 3 arguments: dcp_cd_profiles_raw_files_csv out_index_csv out_qc_csv")
}

dcp_cd_profiles_raw_files_csv <- args[1]
out_index_csv <- args[2]
out_qc_csv <- args[3]

expected_page_types <- c(
  "social",
  "social_education",
  "labor_employment",
  "labor_income",
  "income_poverty",
  "housing",
  "housing_economic"
)

token_pattern <- "^-$|^\\$?-?[0-9][0-9,]*(\\.[0-9]+)?$|^\\(\\$?[0-9][0-9,]*(\\.[0-9]+)?\\)$"
footnote_token_pattern <- "^[0-9]{1,2}$"

is_footer_line <- function(line) {
  line == "2000" |
    str_detect(line, "^Source:") |
    str_detect(line, "^Population Division - New York City Department of City Planning") |
    str_detect(line, "^[A-Za-z ]+ Community District\\s+[0-9]{1,2}$") |
    line == "1990 Change 1990-2000" |
    line == "20001990 Change 1990-2000" |
    str_detect(line, "Community District\\s+[0-9]{1,2}\\s+2000\\s*1990 Change 1990-2000$") |
    str_detect(line, "^2000.*Community District.*1990 Change 1990-2000$") |
    str_detect(line, "^Page\\s+[0-9]+$")
}

is_drop_line <- function(line) {
  line == "1990 and 2000 Census" |
    line == "Number Percent Number Percent Number Percent" |
    str_detect(line, "^Socioeconomic Profile ") |
    is_footer_line(line)
}

is_note_line <- function(line) {
  str_detect(line, "^\\([^)]*\\)$")
}

is_section_header <- function(line) {
  if (is_drop_line(line) || is_note_line(line)) {
    return(FALSE)
  }

  tokens <- str_split(str_squish(strip_section_footnotes(line)), " +")[[1]]
  letter_tokens <- tokens[str_detect(tokens, "[A-Za-z]")]
  letters_only <- str_replace_all(paste(letter_tokens, collapse = ""), "[^A-Za-z]", "")

  if (letters_only == "") {
    return(FALSE)
  }

  allowed_lowercase <- c("and", "of", "in", "to", "a", "as")
  token_letters <- str_replace_all(letter_tokens, "[^A-Za-z]", "")
  header_word_flag <- !(str_to_lower(token_letters) %in% allowed_lowercase)

  all(vapply(seq_along(token_letters), function(idx) {
    if (token_letters[idx] == "") {
      return(TRUE)
    }

    token_letters[idx] == str_to_upper(token_letters[idx]) |
      !header_word_flag[idx]
  }, logical(1))) &&
    any(header_word_flag)
}

strip_section_footnotes <- function(line) {
  str_squish(str_remove(line, "(?:\\s+[0-9]{1,2})+$"))
}

parse_number_token <- function(token) {
  if (is.na(token) || token == "" || token == "-") {
    return(NA_real_)
  }

  clean_token <- gsub("\\$", "", token)

  if (str_detect(clean_token, "^\\(")) {
    clean_token <- paste0("-", gsub("[()]", "", clean_token))
  }

  suppressWarnings(as.numeric(gsub(",", "", clean_token)))
}

parse_metric_line <- function(line) {
  tokens <- str_split(str_squish(line), " +")[[1]]

  if (length(tokens) < 3) {
    return(tibble(parse_status = "unparsed"))
  }

  for (actual_count in c(6L, 4L, 2L)) {
    if (length(tokens) <= actual_count) {
      next
    }

    actual_tokens <- tail(tokens, actual_count)

    if (!all(str_detect(actual_tokens, token_pattern))) {
      next
    }

    label_end_idx <- length(tokens) - actual_count
    footnote_start_idx <- label_end_idx + 1L

    while (
      footnote_start_idx > 1L &&
      str_detect(tokens[footnote_start_idx - 1L], footnote_token_pattern)
    ) {
      footnote_start_idx <- footnote_start_idx - 1L
    }

    footnote_tokens <- if (footnote_start_idx <= label_end_idx) {
      tokens[footnote_start_idx:label_end_idx]
    } else {
      character()
    }

    label_tokens <- if (footnote_start_idx > 1L) {
      tokens[seq_len(footnote_start_idx - 1L)]
    } else {
      character()
    }

    if (length(label_tokens) == 0L) {
      next
    }

    out <- tibble(
      metric_label = paste(label_tokens, collapse = " "),
      value_1990_number = NA_real_,
      value_1990_percent = NA_real_,
      value_2000_number = NA_real_,
      value_2000_percent = NA_real_,
      change_number = NA_real_,
      change_percent = NA_real_,
      footnote_markers = if (length(footnote_tokens) == 0) NA_character_ else paste(footnote_tokens, collapse = " "),
      parse_status = case_when(
        actual_count == 6L & length(footnote_tokens) == 0 ~ "parsed_six",
        actual_count == 6L & length(footnote_tokens) > 0 ~ "parsed_six_with_footnotes",
        actual_count == 4L & length(footnote_tokens) == 0 ~ "parsed_four",
        actual_count == 4L & length(footnote_tokens) > 0 ~ "parsed_four_with_footnotes",
        actual_count == 2L & length(footnote_tokens) == 0 ~ "parsed_two",
        TRUE ~ "parsed_two_with_footnotes"
      )
    )

    if (actual_count == 6L) {
      out$value_1990_number <- parse_number_token(actual_tokens[1])
      out$value_1990_percent <- parse_number_token(actual_tokens[2])
      out$value_2000_number <- parse_number_token(actual_tokens[3])
      out$value_2000_percent <- parse_number_token(actual_tokens[4])
      out$change_number <- parse_number_token(actual_tokens[5])
      out$change_percent <- parse_number_token(actual_tokens[6])
    } else if (actual_count == 4L) {
      out$value_1990_number <- parse_number_token(actual_tokens[1])
      out$value_2000_number <- parse_number_token(actual_tokens[2])
      out$change_number <- parse_number_token(actual_tokens[3])
      out$change_percent <- parse_number_token(actual_tokens[4])
    } else if (actual_count == 2L) {
      out$value_1990_number <- parse_number_token(actual_tokens[1])
      out$value_2000_number <- parse_number_token(actual_tokens[2])
    }

    return(out)
  }

  tibble(parse_status = "unparsed")
}

raw_index <- read_csv(dcp_cd_profiles_raw_files_csv, show_col_types = FALSE, na = c("", "NA"))
raw_index <- raw_index[!is.na(raw_index$raw_parquet_path) & file.exists(raw_index$raw_parquet_path), ]

if (nrow(raw_index) == 0) {
  write_csv(tibble(), out_index_csv, na = "")
  write_csv(tibble(), out_qc_csv, na = "")
  quit(save = "no")
}

index_rows <- list()
qc_rows <- list()

for (i in seq_len(nrow(raw_index))) {
  row <- raw_index[i, ]

  raw_df <- read_parquet(row$raw_parquet_path) %>%
    as.data.frame() %>%
    as_tibble() %>%
    mutate(
      line_text = str_squish(line_text),
      district_id = str_pad(as.character(district_id), width = 3, side = "left", pad = "0"),
      pdf_page_number = as.integer(pdf_page_number),
      line_number = as.integer(line_number)
    )

  page_df <- read_csv(row$page_index_csv_path, show_col_types = FALSE, na = c("", "NA")) %>%
    mutate(
      district_id = str_pad(as.character(district_id), width = 3, side = "left", pad = "0"),
      pdf_page_number = as.integer(pdf_page_number)
    )

  parsed_rows <- list()
  unresolved_rows <- list()
  parsed_index <- 1L
  unresolved_index <- 1L

  page_groups <- raw_df %>%
    arrange(borough_code, district_id, pdf_page_number, line_number) %>%
    group_split(borough_code, district_id, pdf_page_number, profile_page_type, .keep = TRUE)

  for (page_group in page_groups) {
    work_df <- page_group %>%
      arrange(line_number) %>%
      filter(!is_drop_line(line_text))

    current_section_name <- NA_character_
    current_section_header_text <- NA_character_
    row_idx <- 1L

    while (row_idx <= nrow(work_df)) {
      line <- work_df$line_text[row_idx]

      if (is_note_line(line)) {
        row_idx <- row_idx + 1L
        next
      }

      if (is_section_header(line)) {
        header_parts <- c(strip_section_footnotes(line))
        next_idx <- row_idx + 1L

        while (next_idx <= nrow(work_df) && is_section_header(work_df$line_text[next_idx])) {
          header_parts <- c(header_parts, strip_section_footnotes(work_df$line_text[next_idx]))
          next_idx <- next_idx + 1L
        }

        current_section_header_text <- str_squish(paste(header_parts, collapse = " "))
        current_section_name <- normalize_names(current_section_header_text)
        row_idx <- next_idx
        next
      }

      parsed_line <- parse_metric_line(line)
      rows_consumed <- 1L
      raw_line_text <- line

      if (
        parsed_line$parse_status == "unparsed" &&
        row_idx < nrow(work_df) &&
        !is_section_header(work_df$line_text[row_idx + 1L]) &&
        !is_note_line(work_df$line_text[row_idx + 1L])
      ) {
        combined_line <- str_squish(paste(line, work_df$line_text[row_idx + 1L]))
        parsed_combined <- parse_metric_line(combined_line)

        if (parsed_combined$parse_status != "unparsed") {
          parsed_line <- parsed_combined
          rows_consumed <- 2L
          raw_line_text <- combined_line
        }
      }

      if (parsed_line$parse_status == "unparsed") {
        unresolved_rows[[unresolved_index]] <- work_df[row_idx, ] %>%
          mutate(
            section_name = current_section_name,
            section_header_text = current_section_header_text,
            parse_status = "unparsed",
            raw_line_text = line
          ) %>%
          select(
            source_id, pull_date, borough_code, borough_name, district_id, district_header,
            page_title, profile_page_type, pdf_path, pdf_page_number, line_number,
            section_name, section_header_text, raw_line_text, parse_status
          )
        unresolved_index <- unresolved_index + 1L
      } else {
        parsed_rows[[parsed_index]] <- work_df[row_idx, ] %>%
          mutate(
            section_name = current_section_name,
            section_header_text = current_section_header_text,
            raw_line_text = raw_line_text,
            source_line_count = rows_consumed
          ) %>%
          bind_cols(parsed_line) %>%
          select(
            source_id, pull_date, borough_code, borough_name, district_id, district_header,
            page_title, profile_page_type, pdf_path, pdf_page_number, line_number,
            section_name, section_header_text, metric_label,
            value_1990_number, value_1990_percent,
            value_2000_number, value_2000_percent,
            change_number, change_percent,
            footnote_markers, raw_line_text, source_line_count, parse_status
          )
        parsed_index <- parsed_index + 1L
      }

      row_idx <- row_idx + rows_consumed
    }
  }

  parsed_df <- if (length(parsed_rows) == 0L) tibble() else bind_rows(parsed_rows)
  unresolved_df <- if (length(unresolved_rows) == 0L) {
    tibble(
      source_id = character(),
      pull_date = character(),
      borough_code = character(),
      borough_name = character(),
      district_id = character(),
      district_header = character(),
      page_title = character(),
      profile_page_type = character(),
      pdf_path = character(),
      pdf_page_number = integer(),
      line_number = integer(),
      section_name = character(),
      section_header_text = character(),
      raw_line_text = character(),
      parse_status = character()
    )
  } else {
    bind_rows(unresolved_rows)
  }

  section_index_df <- if (nrow(parsed_df) == 0) {
    tibble(
      profile_page_type = character(),
      section_name = character(),
      section_header_text = character(),
      metric_label = character()
    )
  } else {
    parsed_df %>%
      distinct(profile_page_type, section_name, section_header_text, metric_label) %>%
      arrange(profile_page_type, section_name, metric_label)
  }

  parquet_local <- file.path("..", "output", paste0("dcp_cd_profiles_1990_2000_", row$pull_date, ".parquet"))
  unresolved_local <- file.path("..", "output", paste0("dcp_cd_profiles_1990_2000_", row$pull_date, "_unresolved_rows.csv"))
  section_index_local <- file.path("..", "output", paste0("dcp_cd_profiles_1990_2000_", row$pull_date, "_section_index.csv"))

  parquet_repo <- file.path("..", "..", "stage_dcp_cd_profiles_1990_2000", "output", basename(parquet_local))
  unresolved_repo <- file.path("..", "..", "stage_dcp_cd_profiles_1990_2000", "output", basename(unresolved_local))
  section_index_repo <- file.path("..", "..", "stage_dcp_cd_profiles_1990_2000", "output", basename(section_index_local))

  write_parquet_if_changed(parsed_df, parquet_local)
  write_csv_if_changed(unresolved_df, unresolved_local)
  write_csv_if_changed(section_index_df, section_index_local)

  index_rows[[i]] <- tibble(
    source_id = row$source_id,
    pull_date = row$pull_date,
    raw_parquet_path = row$raw_parquet_path,
    page_index_csv_path = row$page_index_csv_path,
    parquet_path = parquet_repo,
    unresolved_csv_path = unresolved_repo,
    section_index_csv_path = section_index_repo,
    status = "staged"
  )

  district_coverage <- page_df %>%
    distinct(district_id, profile_page_type)

  qc_rows[[i]] <- tibble(
    source_id = row$source_id,
    pull_date = row$pull_date,
    parsed_metric_count = nrow(parsed_df),
    unresolved_row_count = nrow(unresolved_df),
    district_count = n_distinct(parsed_df$district_id),
    page_type_count = n_distinct(parsed_df$profile_page_type),
    districts_with_seven_page_types = district_coverage %>%
      count(district_id, name = "page_type_n") %>%
      summarise(sum(page_type_n == length(expected_page_types))) %>%
      pull(),
    distinct_section_count = n_distinct(parsed_df$section_name),
    parsed_with_footnotes_count = sum(!is.na(parsed_df$footnote_markers)),
    status = if (
      n_distinct(parsed_df$district_id) == 59 &&
        all(expected_page_types %in% parsed_df$profile_page_type)
    ) "staged" else "review_required"
  )
}

write_csv(bind_rows(index_rows), out_index_csv, na = "")
write_csv(bind_rows(qc_rows), out_qc_csv, na = "")
cat("Wrote DCP CD profile staging outputs to", dirname(out_index_csv), "\n")
