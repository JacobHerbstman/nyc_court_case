# ─────────────────────────────────────────────────────────────────────────────
# generic.make  (to be included from each task's code/Makefile)
# ─────────────────────────────────────────────────────────────────────────────

SHELL := bash
.DELETE_ON_ERROR:

# ----------------------------------------------------------------------------
# Create the standard folders if they don't exist
# ----------------------------------------------------------------------------
../input ../output ../temp slurmlogs:
	mkdir -p $@

# ----------------------------------------------------------------------------
# SLURM wrapper (path is still relative to each task's code/ folder)
# ----------------------------------------------------------------------------
run.sbatch: ../../setup_environment/code/run.sbatch | slurmlogs
	@test "$$(readlink "$@")" = "$<" || ln -sf $< $@

# ----------------------------------------------------------------------------
# Remove accidental Finder-style duplicates like "file 2.csv" when canonical
# files already exist in the same task folder.
# ----------------------------------------------------------------------------
.PHONY: sanitize-numbered-duplicates
sanitize-numbered-duplicates: ../input
	@for dir in ../input ../output; do \
		[ -d "$$dir" ] || continue; \
		find "$$dir" -maxdepth 1 \( -type f -o -type l \) | while IFS= read -r path; do \
			canonical=$$(printf '%s\n' "$$path" | sed -E 's/ [0-9]+(\.[^./]+)$$/\1/'); \
			if [ "$$canonical" != "$$path" ] && { [ -e "$$canonical" ] || [ -L "$$canonical" ]; }; then \
				rm -f "$$path"; \
			fi; \
		done; \
	done

link-inputs: sanitize-numbered-duplicates

UPSTREAM_TASKS := $(notdir $(patsubst %/code,%,$(wildcard ../../*/code)))
AUDIT_TASKS := $(notdir $(patsubst %/code,%,$(wildcard ../../audits/*/code)))

.PHONY: FORCE
.PRECIOUS: ../../% ../../audits/%

define UPSTREAM_OUTPUT_RULE
../../$(1)/output/%: FORCE
	$$(MAKE) -C ../../$(1)/code ../output/$$*
endef

define AUDIT_OUTPUT_RULE
../../audits/$(1)/output/%: FORCE
	$$(MAKE) -C ../../audits/$(1)/code ../output/$$*
endef

$(foreach task,$(UPSTREAM_TASKS),$(eval $(call UPSTREAM_OUTPUT_RULE,$(task))))
$(foreach task,$(AUDIT_TASKS),$(eval $(call AUDIT_OUTPUT_RULE,$(task))))

FORCE:
