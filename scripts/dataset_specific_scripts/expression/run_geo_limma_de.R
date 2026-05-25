#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(GEOquery)
  library(limma)
  library(Biobase)
})

args <- commandArgs(trailingOnly = TRUE)

get_arg <- function(name, default = NULL) {
  flag <- paste0("--", name)
  idx <- match(flag, args)
  if (is.na(idx) || idx == length(args)) {
    return(default)
  }
  args[[idx + 1]]
}

has_flag <- function(name) {
  paste0("--", name) %in% args
}

curation_csv <- get_arg("curation-csv")
output_csv <- get_arg("output-csv")
cache_dir <- get_arg("cache-dir", "GEO_cache")
threshold <- as.numeric(get_arg("adjusted-p-value-threshold", "0.05"))
force_normalize <- has_flag("force-normalize") ||
  tolower(get_arg("force-normalize", "false")) %in% c("true", "1", "yes")

if (is.null(curation_csv) || is.null(output_csv)) {
  stop("--curation-csv and --output-csv are required")
}

dir.create(dirname(output_csv), recursive = TRUE, showWarnings = FALSE)
dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)

split_samples <- function(value) {
  value <- trimws(as.character(value))
  if (is.na(value) || value == "") {
    return(character())
  }
  unique(trimws(unlist(strsplit(value, "[;,[:space:]]+"))))
}

choose_eset <- function(gsets, platform) {
  if (inherits(gsets, "ExpressionSet")) {
    return(gsets)
  }
  if (!is.list(gsets) || length(gsets) == 0) {
    stop("No ExpressionSet returned by GEOquery")
  }
  if (!is.na(platform) && platform != "") {
    for (item in gsets) {
      if (annotation(item) == platform) {
        return(item)
      }
    }
  }
  sizes <- vapply(gsets, function(item) ncol(exprs(item)), numeric(1))
  gsets[[which.max(sizes)]]
}

find_gene_symbol <- function(top_table, fdata) {
  candidates <- c(
    "Gene.symbol", "Gene Symbol", "GENE_SYMBOL", "Symbol", "SYMBOL",
    "gene_symbol", "Gene.symbols", "Gene ID", "Gene.ID", "ID"
  )
  for (candidate in candidates) {
    if (candidate %in% colnames(top_table)) {
      return(as.character(top_table[[candidate]]))
    }
  }
  for (candidate in candidates) {
    if (candidate %in% colnames(fdata)) {
      symbols <- as.character(fdata[rownames(top_table), candidate])
      if (length(symbols) == nrow(top_table)) {
        return(symbols)
      }
    }
  }
  rownames(top_table)
}

is_log_scale <- function(matrix_values) {
  quant <- quantile(matrix_values, c(0, 0.25, 0.5, 0.75, 0.99, 1), na.rm = TRUE)
  (quant[[5]] < 100 && quant[[6]] - quant[[1]] < 50)
}

append_rows <- function(rows, output_csv) {
  if (length(rows) == 0) {
    return(invisible(NULL))
  }
  frame <- do.call(rbind, rows)
  write.table(
    frame,
    file = output_csv,
    sep = ",",
    row.names = FALSE,
    col.names = !file.exists(output_csv),
    append = file.exists(output_csv),
    quote = TRUE
  )
}

curation <- read.csv(curation_csv, stringsAsFactors = FALSE, check.names = FALSE)
if (!"approved" %in% colnames(curation)) {
  stop("Curation manifest must contain an approved column")
}
curation <- curation[tolower(as.character(curation$approved)) %in% c("true", "1", "yes", "y"), ]

if (file.exists(output_csv)) {
  file.remove(output_csv)
}

for (i in seq_len(nrow(curation))) {
  row <- curation[i, ]
  accession <- as.character(row$study_accession)
  message(sprintf("Processing %s (%d/%d)", accession, i, nrow(curation)))

  tryCatch({
    case_ids <- split_samples(row$case_sample_accessions)
    control_ids <- split_samples(row$control_sample_accessions)
    if (length(case_ids) < 2 || length(control_ids) < 2) {
      stop("At least two case and two control samples are required")
    }

    gsets <- getGEO(accession, GSEMatrix = TRUE, AnnotGPL = TRUE, destdir = cache_dir)
    eset <- choose_eset(gsets, row$platform)
    available_samples <- sampleNames(eset)
    missing <- setdiff(c(case_ids, control_ids), available_samples)
    if (length(missing) > 0) {
      stop(paste("Missing sample accessions:", paste(missing, collapse = ";")))
    }

    eset <- eset[, c(control_ids, case_ids)]
    expression_values <- exprs(eset)
    if (!is_log_scale(expression_values)) {
      expression_values <- log2(expression_values + 1)
    }
    if (force_normalize) {
      expression_values <- normalizeBetweenArrays(expression_values)
    }
    exprs(eset) <- expression_values

    group <- factor(c(rep("CTRL", length(control_ids)), rep("CASE", length(case_ids))), levels = c("CTRL", "CASE"))
    design <- model.matrix(~ 0 + group)
    colnames(design) <- c("CTRL", "CASE")
    fit <- lmFit(eset, design)
    contrast <- makeContrasts(contrasts = "CASE-CTRL", levels = design)
    fit2 <- eBayes(contrasts.fit(fit, contrast))
    result <- topTable(fit2, adjust.method = "BH", number = Inf, sort.by = "P")
    result$Gene.symbol <- find_gene_symbol(result, fData(eset))
    result$direction <- ifelse(
      result$adj.P.Val < threshold & result$logFC > 0,
      "up",
      ifelse(result$adj.P.Val < threshold & result$logFC < 0, "down", "not_significant")
    )

    output <- data.frame(
      gene_id = result$Gene.symbol,
      gene_symbol = result$Gene.symbol,
      gene_id_source = "platform_annotation",
      study_accession = accession,
      source_database = row$source_database,
      source_url = row$source_url,
      assay_type = row$assay_type,
      platform = annotation(eset),
      species = row$species,
      tissue = row$tissue,
      cell_type = row$cell_type,
      disease_id = row$disease_id,
      disease_name = row$disease_name,
      phenotype_label_original = row$phenotype_label_original,
      phenotype_label_normalized = row$phenotype_label_normalized,
      contrast_name = row$contrast_name,
      case_group_label = row$case_group_label,
      control_group_label = row$control_group_label,
      n_case = length(case_ids),
      n_control = length(control_ids),
      log2_fold_change = result$logFC,
      p_value = result$P.Value,
      adjusted_p_value = result$adj.P.Val,
      fdr_method = "BH",
      direction = result$direction,
      significance_threshold = threshold,
      analysis_method = "GEOquery_limma",
      analysis_package_version = paste0("limma=", as.character(packageVersion("limma")), ";GEOquery=", as.character(packageVersion("GEOquery"))),
      preprocessing_method = ifelse(force_normalize, "auto_log2_plus_quantile_normalization_if_requested", "auto_log2_if_needed"),
      covariates_used = "",
      batch_correction_method = "",
      date_processed = as.character(Sys.Date()),
      quality_score = NA,
      notes = row$notes,
      stringsAsFactors = FALSE
    )
    output <- output[!is.na(output$gene_symbol) & output$gene_symbol != "", ]
    append_rows(list(output), output_csv)
  }, error = function(err) {
    warning(sprintf("Failed %s: %s", accession, conditionMessage(err)))
  })
}
