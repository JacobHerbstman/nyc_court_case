SHELL := bash
.DELETE_ON_ERROR:
.SECONDARY:

../input ../output ../temp slurmlogs:
	mkdir -p $@

run.sbatch: ../../setup_environment/code/run.sbatch | slurmlogs
	@test "$$(readlink "$@")" = "$<" || ln -sf $< $@

UPSTREAM_TASKS := $(notdir $(patsubst %/code,%,$(wildcard ../../*/code)))
AUDIT_TASKS := $(notdir $(patsubst %/code,%,$(wildcard ../../audits/*/code)))

.PRECIOUS: ../../% ../../audits/%

define UPSTREAM_OUTPUT_RULE
../../$(1)/output/%:
	$$(MAKE) -C ../../$(1)/code ../output/$$*
endef

define AUDIT_OUTPUT_RULE
../../audits/$(1)/output/%:
	$$(MAKE) -C ../../audits/$(1)/code ../output/$$*
endef

$(foreach task,$(UPSTREAM_TASKS),$(eval $(call UPSTREAM_OUTPUT_RULE,$(task))))
$(foreach task,$(AUDIT_TASKS),$(eval $(call AUDIT_OUTPUT_RULE,$(task))))
