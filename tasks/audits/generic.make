SHELL := bash
.DELETE_ON_ERROR:

FUNCTIONS = $(shell cat ../../../shell_functions.sh)
STATA = @$(FUNCTIONS); stata_with_flag
R = @$(FUNCTIONS); R_pc_and_slurm

ifneq (,$(findstring n,$(MAKEFLAGS)))
STATA := STATA
R := R
endif

../input ../output ../temp slurmlogs:
	mkdir -p $@

run.sbatch: ../../../setup_environment/code/run.sbatch | slurmlogs
	@test "$$(readlink "$@")" = "$<" || ln -sf $< $@

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

UPSTREAM_TASKS := $(notdir $(patsubst %/code,%,$(wildcard ../../../*/code)))
AUDIT_TASKS := $(notdir $(patsubst %/code,%,$(wildcard ../../*/code)))

.PHONY: FORCE
.PRECIOUS: ../../../% ../../%

define UPSTREAM_OUTPUT_RULE
../../../$(1)/output/%: FORCE
	$$(MAKE) -C ../../../$(1)/code ../output/$$*
endef

define AUDIT_OUTPUT_RULE
../../$(1)/output/%: FORCE
	$$(MAKE) -C ../../$(1)/code ../output/$$*
endef

$(foreach task,$(UPSTREAM_TASKS),$(eval $(call UPSTREAM_OUTPUT_RULE,$(task))))
$(foreach task,$(AUDIT_TASKS),$(eval $(call AUDIT_OUTPUT_RULE,$(task))))

FORCE:
