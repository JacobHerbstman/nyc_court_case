SHELL := bash
SHARED_CODE := $(dir $(lastword $(MAKEFILE_LIST)))
include $(SHARED_CODE)shell_functions.make

.NOTPARALLEL:
.DELETE_ON_ERROR:
.SECONDARY:

../input ../output ../report ../temp slurmlogs:
	mkdir -p $@

run.sbatch: $(SHARED_CODE)setup_environment/code/run.sbatch | slurmlogs
	ln -sf $< $@

.PHONY: FORCE_UPSTREAM
FORCE_UPSTREAM:

.SECONDEXPANSION:
../tasks/% ../../% ../../../% ../../../../%: $$(if $$(findstring /output/,$$@),FORCE_UPSTREAM)
	@case "$@" in \
		*/output/*) target="$@"; task="$${target%/output/*}"; output="../output/$${target##*/output/}" ;; \
		*) echo "Missing prerequisite: $@" >&2; exit 1 ;; \
	esac; \
	$(MAKE) -C "$$task/code" "$$output"
