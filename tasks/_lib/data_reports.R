# Shared CSV writer for task outputs. Make owns execution and report directories.
save_csv <- function(data, path, key) {
  stopifnot(all(key %in% names(data)), !anyNA(data[key]), !anyDuplicated(data[key]))
  readr::write_csv(data, path, na = "")
  columns <- lapply(data, function(values) {
    present <- values[!is.na(values)]
    result <- list(type = class(values), missing = sum(is.na(values)),
      distinct_nonmissing = length(unique(present)))
    if (is.numeric(present) && length(present)) {
      result$minimum <- min(present)
      result$maximum <- max(present)
    }
    result
  })
  jsonlite::write_json(list(dataset = basename(path), rows = nrow(data), key = key,
    unique_nonmissing_key = TRUE, md5_csv_bytes = unname(tools::md5sum(path)),
    fingerprint_scope = "Saved CSV bytes, including row order; not proof of source correctness.",
    columns = columns),
    file.path("../report", paste0(tools::file_path_sans_ext(basename(path)), ".json")),
    pretty = TRUE, auto_unbox = TRUE, null = "null")
}
