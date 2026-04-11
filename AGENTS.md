# Aldermanic Privilege Project Guidelines

## Data Analysis workflow
- This project uses a task-based workflow. Every task in the paper has a dedicated folder in `tasks/` with its own `code/`, `input/`, and `output/` subfolders.
- Each task should have its own makefile. Each makefile should be as clean and simple as possible to make them readable.
- In makefiles, limit comments and additional targets such as ``clean''. Typical makefiles should only have a default ``all'' target and a ``link-inputs'' target.
- Tasks that run many regression specifications, for example, should always call arguments from the makefile and use loops in the makefile to create target filenames for each specification.
  Then the R script should read in those arguments from the command line and produce the corresponding output file, named according to the arguments in the makefile.
- Tasks that use output from ``upstream`` tasks should use symlinking and makefiles to connect them together.
  It should be easy to trace the path out via makefiles from the `data_raw/` folder to final outputs.

  ## Project Structure
- `paper/` - LaTeX paper and sections
- `slides/` - Presentation slides
- `data_raw/` - Raw data files (not to be modified)
- `tasks/` - Analysis tasks, each with `code/`, `input/`, and `output/` subfolders
- R scripts in `tasks/*/code/` generate outputs (tables, figures) used by the paper

## Compiling the Paper
- Always use `make` in the `paper/` folder to compile LaTeX
- Use version control (e.g., Git) to track changes in both code and paper, leave clear and simple commit messages
- Always execute tasks by running `make` from the `code` folder within any task and make sure all paths are relative
- Do NOT use `latexmk` directly

## Workflow
- When modifying tables/figures, edit the R script that generates them, not the output files directly
- After running R scripts that update outputs, recompile the paper with `make` in `paper/`

## Modeling Guardrails
- Do NOT use `log1p`, inverse-hyperbolic-sine (arcsinh), or similar "zero-handling hacks" in place of log transforms.
- If a logged outcome has zeros, handle them by dropping zero observations for the logged specification unless explicitly instructed otherwise.
- For the alderman uncertainty index, keep the current control set unless explicitly asked to change it.

## Make Incrementality Rules
- Do not call recursive upstream builds inside active task Makefiles (for example, no `$(MAKE) -C ../../...` inside symlink/input rules).
- `link-inputs` should only create symlinks and should not orchestrate upstream task execution.
- Each symlink input should depend on the specific upstream output file path, not on broad/coarse gate files when avoidable.
- Prefer narrow dependency edges over single-report anchors that can trigger unnecessary relinking and downstream invalidation.
- Stamp-file workflows can obscure real dependency edges; use them sparingly and only when there is no clearer file-target alternative.
- Before expensive runs, prefer `make -n` to inspect what will rebuild.
- Do not duplicate Make's incrementality inside active scripts with executable `!exists(...)`, `file.exists(...)`, or "skip if already built" branches.

## Makefile Readability Rules
- Keep Makefiles minimal, linear, and easy to scan.
- Keep comments brief and structural only.
- Keep default workflow targets limited to essentials (`all`, `link-inputs`, and task-specific essential file targets).
- Keep one concise recipe per logical output producer.
- Keep output and input names explicit and traceable.
- Favor readability over clever Make metaprogramming unless scale requires it.

## Makefile Path Style
- Write file paths directly in targets and recipes.
- Do not use path indirection blocks like `*_IN`, `*_UP`, `*_OUT`, `INPUT`, `OUTPUTS`, or similar path alias variables.
- Keep only scalar/config variables in Makefiles (dates, thresholds, flags, spec lists, tool executables).

## RStudio Interactive Block Standard
- For every active R script that accepts CLI arguments, include a top-of-file commented interactive block.
- That block must include:
  - a commented `setwd(...)` line to the task `code/` folder
  - one commented named assignment line per example CLI argument, in CLI order, using the exact variable names used by the script's interactive fallback or CLI unpacking
- Do not include a commented `Rscript ...` line in the interactive block.
- Do not include bundled commented argument vectors/lists (for example, no `args <- c(...)` block).
- Do not include placeholder paths such as `tasks/"task"/code`.
- Interactive examples must mirror current Makefile defaults/paths and run end-to-end when uncommented from the task `code/` directory.
- CLI parsing remains canonical for non-interactive runs; interactive blocks are for readability/debugging only.
- For interactive runs, scripts may either unpack CLI args into named variables once near the top or rebuild `args`/`cli_args` from the uncommented named variables before validation.
- Do not use executable `!exists(...)` checks as the bridge between interactive runs and CLI runs; use direct positional-arg validation plus one named unpacking block instead.

## Script Path Style (R + Python)
- Avoid path alias variables for simple I/O handoff when direct use is clear.
- Prefer direct call-site reads/writes (`read_csv(args[1])`, `write_csv(df, args[2])`, and R equivalents).
- Keep path handling explicit and local to each read/write call unless reuse materially improves clarity.
- For CLI scripts, keep positional `args[i]` or `cli_args[i]` as the canonical Make interface, then introduce one short top-of-script unpacking block to named variables when that makes interactive execution clearer.
- Do not use `normalizePath()` in active task scripts for standard Make-managed inputs/outputs; keep task paths relative and direct.
- Do not use `dir.create()` in active task scripts for standard task `input/`, `output/`, or `temp/` directories; Make should own directory creation.

## Spatial CRS Standard
- For Chicago point data with raw longitude/latitude columns, create points in `4326` first.
- Immediately transform those points to `3435` for all Chicago spatial work, including boundary checks, joins, distances, block assignment, ward assignment, and segment assignment.
- Treat `3435` as the working/final Chicago geometry standard unless a task explicitly needs flat longitude/latitude columns for a non-spatial export.
- If a task exports a flat CSV/parquet for downstream non-spatial use, it may convert geometry back to longitude/latitude after all spatial work in `3435` is complete.

## Linear Active Script Standard
- For non-archived active tasks, prefer scripts that run top-to-bottom in a linear, readable sequence once the user is in the task `code/` folder.
- Inline one-off cleaning, merge, transformation, modeling, and export steps instead of wrapping them in local helper functions.
- Use functions in active scripts only when logic is clearly reused within that same script and duplication would materially hurt readability.
- Do not leave the main active script as a thin wrapper around one large helper pipeline.
- Put genuinely reusable cross-task helpers in `tasks/_lib`.
- Allow a task-family helper file only when multiple active sibling scripts share substantial logic and moving it to `tasks/_lib` would be less clear.
- When refactoring for readability, preserve current methods and outputs unless fixing a confirmed bug.

## Root-Cause First (No Shortcuts)
- Do not bypass missing dependencies with wildcard hacks, dummy targets, or optionalized prerequisite checks.
- Do not use forwarding wrappers (for example, an R script that only calls `python3 some_script.py`).
- Do not add placeholder smoke targets.
- No silent fallback branches. If a secondary source is required, make source-priority logic explicit with row-level reason codes and deterministic unresolved/manual-review outputs.
- Fix producer/root data issues upstream rather than suppressing validation downstream.

## Environment Setup Conventions
- Keep `tasks/setup_environment/code/` as the bootstrap entry point.
- Keep package bootstrap scripts (R/Stata) current and log installed package versions.
- Reuse `run.sbatch` where SLURM execution is needed; symlink it only in tasks that actually require it.

## Terse/Clear Code Preference
- Prefer simpler argument parsing and fewer helper abstractions when they do not improve clarity.
- Keep scripts short, direct, and clearly mapped to Makefile arguments and outputs.
- Avoid unnecessary indirection so readers can trace input -> transform -> output quickly.

## Collaboration Preferences
- Keep commit messages clear and minimal.
- When changing a figure/table, edit the generating script and rerun the relevant task.
- Prefer incremental, testable changes over large rewrites.
