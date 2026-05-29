all: ../output/overrule_project_bundle_audited.csv \
	../output/overrule_project_bundle_candidates.csv \
	../output/overrule_action_crosswalk.csv \
	../output/overrule_time_series_year.csv \
	../output/overrule_residential_mixed_case_anatomy.csv \
	../output/overrule_candidate_qc.csv

../output/overrule_project_bundle_audited.csv: build_member_deference_overrule_candidates.R charter_overrule_seed_cases.csv ../../../_lib/source_pipeline_utils.R ../input/zap_housing_project_base_audited.csv ../input/council_land_use_matter.csv | ../output
	Rscript --vanilla $<

../output/overrule_project_bundle_candidates.csv: ../output/overrule_project_bundle_audited.csv
	@test -s "$@"

../output/overrule_action_crosswalk.csv: ../output/overrule_project_bundle_audited.csv
	@test -s "$@"

../output/overrule_time_series_year.csv: ../output/overrule_project_bundle_audited.csv
	@test -s "$@"

../output/overrule_residential_mixed_case_anatomy.csv: ../output/overrule_project_bundle_audited.csv
	@test -s "$@"

../output/overrule_candidate_qc.csv: ../output/overrule_project_bundle_audited.csv
	@test -s "$@"

../input/zap_housing_project_base_audited.csv: ../../../build_zap_housing_pipeline_from_raw/output/zap_housing_project_base_audited.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/council_land_use_matter.csv: ../../../stage_council_land_use_records/output/council_land_use_matter.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_housing_project_base_audited.csv ../input/council_land_use_matter.csv

include ../../generic.make
