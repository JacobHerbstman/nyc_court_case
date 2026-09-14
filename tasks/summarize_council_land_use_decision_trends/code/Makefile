all: ../output/council_land_use_adoption_over_local_member_rollcall_opposition_rolling5_with_raw_clean.pdf \
	../output/council_land_use_adoption_over_local_member_rollcall_opposition_count_rolling5_with_raw_clean.pdf \
	../output/council_land_use_adoption_over_local_member_rollcall_opposition_rolling5_with_raw_clean.csv

../%/council_land_use_adoption_over_local_member_rollcall_opposition_rolling5_with_raw_clean.pdf ../%/council_land_use_adoption_over_local_member_rollcall_opposition_count_rolling5_with_raw_clean.pdf ../%/council_land_use_adoption_over_local_member_rollcall_opposition_rolling5_with_raw_clean.csv: summarize_council_land_use_decision_trends.R \
	../input/council_land_use_decision_panel.csv | ../output
	Rscript $<

../input/council_land_use_decision_panel.csv: ../../build_council_land_use_decision_panel/output/council_land_use_decision_panel.csv | ../input
	ln -sf "$<" "$@"

link-inputs: ../input/council_land_use_decision_panel.csv

include ../../generic.make
